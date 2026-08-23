"""Shared helpers for DAG-semantics regression tests.

The DAG trigger engine lives in :meth:`FlowRunnerBase.trigger` and depends on
several external boundaries (Prefect client, MongoDB deployment context, the
``run_deployment`` dispatcher and the deployment-instance resolver). These
helpers wire those boundaries to the in-memory fakes so the engine's semantics
can be asserted directly without a running Prefect server.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from prefect.client.schemas.objects import State, StateType

from prefect_pipeline.core import runner_base
from prefect_pipeline.core.condition import Condition, PeersPolicy
from prefect_pipeline.core.deployment import Deployment, Node
from prefect_pipeline.models import DeploymentContext, NodeInfo


def make_node_info(ns: str, active: bool = True) -> NodeInfo:
    return NodeInfo(ns=ns, active=active)


def make_context(
    *,
    ns: str,
    peer_tails: list[NodeInfo],
    downstream: list[NodeInfo],
    active: bool = True,
) -> DeploymentContext:
    return DeploymentContext(ns=ns, active=active, peer_tails=peer_tails, downstream=downstream)


def make_flow_run(
    *,
    deployment_id: str,
    state: StateType = StateType.COMPLETED,
    starter_id: str | None = "starter-1",
    name: str = "A",
    parameters: dict[str, Any] | None = None,
    extra_job_variables: dict[str, Any] | None = None,
) -> MagicMock:
    """Build a Prefect ``FlowRun``-like mock for the trigger engine."""
    run = MagicMock()
    run.id = "run-" + deployment_id
    run.deployment_id = deployment_id
    run.name = name
    run.end_time = MagicMock()
    run.parameters = parameters or {}

    job_variables: dict[str, Any] = {"starter_id": starter_id, "master_node": name}
    if extra_job_variables:
        job_variables.update(extra_job_variables)
    run.job_variables = job_variables
    run.state = State(type=state)
    return run


def make_flow(*, name: str = "flow", deployment_name: str = "A") -> MagicMock:
    flow = MagicMock()
    flow.name = name
    return flow


class FakePrefectClient:
    """Programmable stand-in for the Prefect async client used by ``trigger``.

    The DAG trigger engine relies on ``read_flow_runs`` (peer discovery) and
    ``create_flow_run_from_deployment`` (downstream dispatch recording). It is a
    real async-context-manager (not a bare ``MagicMock``) so ``async with
    get_client()`` resolves cleanly.
    """

    def __init__(self) -> None:
        self._harness: TriggerHarness | None = None
        self.flow_runs: list[Any] = []
        self.updated: list[tuple[Any, dict[str, Any]]] = []

    async def __aenter__(self) -> FakePrefectClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def read_flow_runs(self, **kwargs: Any) -> list[Any]:
        if self._harness is not None:
            return list(self._harness.peer_runs)
        return []

    async def create_flow_run_from_deployment(self, deployment_id: Any, parameters: dict[str, Any]) -> Any:
        run = MagicMock()
        run.id = deployment_id
        run.name = f"run-{len(self.flow_runs)}"
        run.parameters = parameters
        self.flow_runs.append(run)
        return run

    async def update_flow_run(
        self, flow_run_id: Any, name: str | None = None, parameters: dict[str, Any] | None = None
    ) -> None:
        self.updated.append((flow_run_id, {"name": name, "parameters": parameters}))


class TriggerHarness:
    """Wires the trigger engine's external boundaries to fakes/recorders.

    Usage::

        harness = TriggerHarness()
        harness.context = make_context(...)
        harness.peer_runs = [make_flow_run(...)]
        await harness.run(trigger_cls, flow_run)

        assert harness.dispatched == ["downstream/ns"]
    """

    def __init__(self, monkeypatch: Any) -> None:
        self._monkeypatch = monkeypatch
        self.context: DeploymentContext | None = None
        self.peer_runs: list[MagicMock] = []
        self.dispatched: list[str] = []
        self.triggered_signals: list[Any] = []
        self._patches: list[Any] = []
        self._downstream_deployments: dict[str, Deployment] = {}

    def _is_triggered(self, *args: Any, **kwargs: Any) -> bool:
        self.triggered_signals.append(args)
        if self.is_triggered_queue:
            return self.is_triggered_queue.pop(0)
        return False

    def install(self, trigger_cls: type) -> None:
        # 1. Deployment context resolver
        self._monkeypatch.setattr(
            trigger_cls,
            "get_deployment_context",
            AsyncMock(return_value=self.context),
        )
        # 2. Downstream-signal dedup (returns from ``is_triggered_queue`` in order)
        self.is_triggered_queue: list[bool] = []
        self._monkeypatch.setattr(
            trigger_cls,
            "is_triggered",
            AsyncMock(side_effect=self._is_triggered),
        )
        # 3. Prefect client — provides peer runs via the in-memory fake.
        client = FakePrefectClient()
        client._harness = self
        # ``trigger`` does ``from prefect import get_client`` which copies the
        # binding into ``runner_base``'s namespace, so we must patch the module
        # attribute there (not the source symbol) to intercept it.
        self._monkeypatch.setattr(runner_base, "get_client", lambda *a, **k: client)
        self._client = client
        # 4. run_deployment dispatcher — record instead of really dispatching.
        # Like get_client, this is a copied binding in ``runner_base``.
        self._run_deployment = AsyncMock(side_effect=self._record_dispatch)
        self._monkeypatch.setattr(runner_base, "run_deployment", self._run_deployment)
        # 5. Deployment-instance resolver — return prebuilt downstream deployments
        self._monkeypatch.setattr(
            runner_base,
            "get_deployment_instance",
            lambda fns, dns: self._downstream_deployments[f"{fns}/{dns}"],
        )

    def _record_dispatch(self, name: str, **kwargs: Any) -> MagicMock:
        self.dispatched.append(name)
        run = MagicMock()
        run.name = name
        return run

    def register_downstream(self, ns: str, runner_cls: type, *, peers_policy: PeersPolicy = PeersPolicy.ALL) -> None:
        """Register a downstream deployment keyed by its ``ns`` (``flow/deployment``)."""
        node = Node(runner_cls, upstream_condition=Condition(peers_policy=peers_policy))
        dep = Deployment(node, name=ns.split("/")[-1])
        self._downstream_deployments[ns] = dep

    async def run(self, trigger_cls: type, flow_run: MagicMock, flow: MagicMock | None = None) -> None:
        if flow is None:
            flow = make_flow()
        # ``trigger`` is ``@classmethod`` wrapping ``@Hook.on`` wrapping the
        # coroutine. Resolving ``Hook.fn`` yields the classmethod; its
        # ``__func__`` is the raw coroutine (Prefect invokes it via the
        # collected ``_on_completion`` / ``_on_failure`` hook lists).
        classmethod_obj = trigger_cls.trigger.fn  # type: ignore[attr-defined]
        raw_fn = classmethod_obj.__func__  # type: ignore[attr-defined]
        await raw_fn(trigger_cls, flow, flow_run, State(type=StateType.COMPLETED))

    def stop(self) -> None:
        for p in self._patches:
            p.stop()
