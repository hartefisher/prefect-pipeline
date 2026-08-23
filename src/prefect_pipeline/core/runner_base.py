from __future__ import annotations

import inspect
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NotRequired,
    TypedDict,
    cast,
)
from uuid import UUID
from zoneinfo import ZoneInfo

from motor.motor_asyncio import AsyncIOMotorCollection
from prefect import Flow, State, get_client
from prefect.client.schemas import FlowRun
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterName,
    FlowRunFilter,
    FlowRunFilterStartTime,
)
from prefect.client.types.flexible_schedule_list import FlexibleScheduleList
from prefect.deployments import run_deployment
from prefect.flows import FlowStateHook
from prefect.logging.loggers import flow_run_logger
from prefect.results import ResultSerializer, ResultStorage
from prefect.runtime import deployment

from ..infra.db import MongoDB, get_prefect
from ..infra.utils import get_current_time
from ..models import DeploymentContext, ExtraContext
from .condition import Condition
from .configs import ENVIRONMENT, MACRO_VARIABLES, TIMEZONE
from .ns_converter import get_deployment_instance

if TYPE_CHECKING:
    from .deployment import Deployment

type FlowStateHooks = list[FlowStateHook[..., Any]]


class Hook:
    """Decorator-style wrapper that binds a function to one or more lifecycle events."""

    def __init__(self, fn: Callable[..., Any], event: str | list[str] | None = None) -> None:
        self.fn: Callable[..., Any] = fn
        if isinstance(event, str):
            self.events: list[str] = [event]
        elif event is None:
            self.events = [
                "completion",
                "failure",
                "crashed",
                "cancellation",
                "running",
            ]
        else:
            self.events = event

    @classmethod
    def on(
        cls,
        fn: Callable[..., Any] | None = None,
        *,
        event: str | list[str] | None = None,
    ) -> Hook | partial[Hook]:
        """Use as ``@Hook.on(event="running")`` or ``Hook.on(fn, event="running")``."""
        if fn is None:
            return partial(cls.on, event=event)  # type: ignore[return-value, arg-type]
        return cls(fn, event)


class FlowParemeter(TypedDict):
    """Parameters dict consumed by ``@flow(**flow_parameters)``."""

    flow_run_name: NotRequired[Callable[[], str] | str | None]
    on_cancellation: NotRequired[FlowStateHooks | None]
    on_completion: NotRequired[FlowStateHooks | None]
    on_failure: NotRequired[FlowStateHooks | None]
    on_crashed: NotRequired[FlowStateHooks | None]
    on_running: NotRequired[FlowStateHooks | None]
    persist_result: NotRequired[bool | None]
    result_storage: NotRequired[ResultStorage | None]
    result_serializer: NotRequired[ResultSerializer | None]
    retries: int | None
    retry_delay_seconds: int | float | None
    timeout_seconds: int | float | None
    log_prints: NotRequired[bool | None]


class FlowRunnerBase[Injectors: tuple[Callable[..., Any], ...] = tuple[Callable[..., Any], ...]]:
    """Base class for all flow runners.

    Subclasses define ``run()``, ``setup()``, and optionally attach ``Hook``
    decorated methods for lifecycle events. The framework's DAG engine and
    trigger logic operate on ``Deployment`` objects built from runners.
    """

    log_prints: ClassVar[bool | None] = None
    persist_result: ClassVar[bool | None] = None
    result_storage: ClassVar[ResultStorage | None] = None
    result_serializer: ClassVar[ResultSerializer | None] = None
    cache_result_in_memory: ClassVar[bool] = True
    on_running: ClassVar[FlowStateHooks | None] = None
    on_cancellation: ClassVar[FlowStateHooks | None] = None
    on_completion: ClassVar[FlowStateHooks | None] = None
    on_failure: ClassVar[FlowStateHooks | None] = None
    on_crashed: ClassVar[FlowStateHooks | None] = None
    _on_running: ClassVar[FlowStateHooks | None] = None
    _on_cancellation: ClassVar[FlowStateHooks | None] = None
    _on_completion: ClassVar[FlowStateHooks | None] = None
    _on_failure: ClassVar[FlowStateHooks | None] = None
    _on_crashed: ClassVar[FlowStateHooks | None] = None
    retries: ClassVar[int | None] = None
    retry_delay_seconds: ClassVar[int | float | None] = None
    timeout_seconds: ClassVar[int | float | None] = None
    tags: ClassVar[list[str]] = []
    flag: ClassVar[str | None] = None
    variant: ClassVar[str | None] = None
    project_name: ClassVar[str] = "default"

    def __init__(self, *args: Any, **extra: Any) -> None:
        self._extra: dict[str, Any] = extra

    async def setup(self, *injectors: Any, **extra: Any) -> None:
        pass

    async def start(self) -> Any:
        try:
            await self.run()
        finally:
            await self.clear()

    async def run(self) -> Any:
        pass

    def __init_subclass__(cls, **kwargs: Any) -> None:
        hooks: defaultdict[str, list[Callable[..., Any]]] = defaultdict(list)
        for cls_ in cls.__mro__:
            for k, v in cls_.__dict__.items():
                if k.startswith("__") or k.endswith("__"):
                    continue
                if isinstance(v, Hook):
                    for event in v.events:
                        hooks[event].append(v.fn.__get__(None, cls))

        for event, cls_hooks in hooks.items():
            setattr(cls, f"_on_{event}", cls_hooks)

    # ------------------------------------------------------------------
    # Time helpers
    # ------------------------------------------------------------------

    @classmethod
    def get_time_window(
        cls,
        dt: str | None = None,
        days: int = 1,
        offset: int = 0,
        backfill: bool | None = None,
        fill_direction: Literal["backward", "forward"] = "backward",
        fmt: str = "%Y%m%d",
    ) -> list[str]:
        end: datetime = datetime.strptime(dt, "%Y%m%d") if dt else cls.get_current_date()
        end += timedelta(days=offset)
        start: datetime = end

        days = 1 if backfill else days
        if days - 1:
            i: int = -1 if fill_direction == "backward" else 1
            start = end + timedelta(days=i * (days - 1))
        start_date: str = start.strftime(fmt)
        end_date: str = end.strftime(fmt)
        return sorted([start_date, end_date])

    @classmethod
    def flow_run_name(
        cls,
        dt: str | None = None,
        days: int = 1,
        offset: int = 0,
        backfill: bool | None = None,
        fill_direction: Literal["backward", "forward"] = "backward",
        **kwargs: Any,
    ) -> str:
        start_date, end_date = cls.get_time_window(dt, days, offset, backfill, fill_direction)
        if start_date != end_date:
            run_name: str = f"{start_date}-{end_date}"
        else:
            run_name = start_date
            if backfill:
                run_name = f"Backfill-{run_name}"

        if deployment_name := deployment.get_name():
            run_name = f"{deployment_name} / {run_name}"

        return run_name

    @staticmethod
    def get_current_date() -> datetime:
        """Return the framework's "current date" (yesterday in configured timezone)."""
        return datetime.now(ZoneInfo(TIMEZONE)) - timedelta(days=1)

    # ------------------------------------------------------------------
    # Built-in hooks
    # ------------------------------------------------------------------

    @Hook.on(event="running")  # type: ignore[operator, untyped-decorator]
    @classmethod
    async def update_flow_run(cls, flow: Flow[Any, Any], flow_run: FlowRun, state: State[Any]) -> None:
        logger = flow_run_logger(flow_run, flow)

        new_parameters: dict[str, Any] = {**flow_run.parameters}
        macro_variables: dict[str, Any] = (
            cast(dict[str, Any], flow_run.job_variables.get("macro_variables", {})) if flow_run.job_variables else {}
        )

        for k in MACRO_VARIABLES.get(cls.project_name, {}):
            if k in flow.parameters.properties and (k not in flow_run.parameters or flow_run.parameters[k] is None):
                if k in macro_variables:
                    new_parameters[k] = macro_variables[k]
                elif k == "dt":
                    new_parameters[k] = cls.get_current_date().strftime("%Y%m%d")

        name: str = cls.flow_run_name(**{**macro_variables, **new_parameters})
        parameters: dict[str, Any] | None = new_parameters if new_parameters != flow_run.parameters else None

        async with get_client() as client:
            await client.update_flow_run(flow_run_id=flow_run.id, name=name, parameters=parameters)
        logger.info("Updated flow run name and parameters.")

    @Hook.on(event="completion")  # type: ignore[operator, untyped-decorator]
    @classmethod
    async def schedule_next_run(cls, flow: Flow[Any, Any], flow_run: FlowRun, state: State[Any]) -> None:
        logger = flow_run_logger(flow_run, flow)
        backfill: bool = flow_run.parameters.get("backfill", False)
        if not backfill:
            return

        current_date: str | None = flow_run.parameters.get("dt")
        days: int = flow_run.parameters.get("days", 1)
        fill_direction: str = flow_run.parameters.get("fill_direction", "backward")

        if days <= 1:
            logger.info(f"Backfill task has done with the final date: {current_date}.")
            return
        async with get_client() as client:
            if current_date is None:
                return

            offset: int = -1 if fill_direction == "backward" else 1
            next_date: datetime = datetime.strptime(current_date, "%Y%m%d") + timedelta(days=offset)
            next_date_str: str = next_date.strftime("%Y%m%d")

            if flow_run.deployment_id is None:
                return

            parameters: dict[str, Any] = {
                **flow_run.parameters,
                "dt": next_date_str,
                "days": days - 1,
            }
            await client.create_flow_run_from_deployment(
                deployment_id=flow_run.deployment_id,
                parameters=parameters,
            )

    async def clear(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Deployment context (C3.1: generated.deployments import)
    # ------------------------------------------------------------------

    @classmethod
    async def get_deployment_context(cls, deployment_name: str) -> DeploymentContext | None:
        # C3.1: version_id is written by FlowsLoader.write_deployment_map()
        # into prefect_pipeline/generated/deployments.py. The import is lazy
        # so the framework can be used without the generated module present.
        try:
            from ..generated.deployments import version_id
        except ImportError:
            print("Warning: generated.deployments module not found. Run FlowsLoader.load() to generate it.")
            return None

        PREFECT: MongoDB = get_prefect()
        try:
            collection: AsyncIOMotorCollection[dict[str, Any]] = PREFECT.deployments
            record: dict[str, Any] | None = await collection.find_one(
                {
                    "ns": deployment_name,
                    "version_id": version_id,
                    "environment": ENVIRONMENT,
                },
                {"_id": 0, "ns": 1, "active": 1, "downstream": 1, "peer_tails": 1},
            )
            if record is None:
                return None
            return DeploymentContext(**record)
        except Exception as e:
            print(e)
            return None
        finally:
            PREFECT.close()

    @classmethod
    async def is_triggered(
        cls,
        starter_id: str | None,
        peer_nss: str | None,
        ds: str,
        flow_run_id: str,
    ) -> bool:
        """Record a downstream trigger signal for concurrency control.

        Returns ``True`` if another task has already triggered the downstream
        (signal insertion failed = duplicate), ``False`` otherwise.
        """
        if not starter_id:
            return False

        PREFECT: MongoDB = get_prefect()
        try:
            await PREFECT.downstream_signal.insert_one(
                {
                    "starter_id": starter_id,
                    "peer_nss": peer_nss,
                    "ds": ds,
                    "trigger_time": get_current_time(),
                    "flow_run_id": flow_run_id,
                }
            )
            return False
        except Exception as e:
            print(e)
            return True
        finally:
            PREFECT.close()

    # ------------------------------------------------------------------
    # Trigger engine (core orchestration driver)
    # ------------------------------------------------------------------

    @Hook.on(event=["completion", "failure"])  # type: ignore[operator, untyped-decorator]
    @classmethod
    async def trigger(cls, flow: Flow[Any, Any], flow_run: FlowRun, state: State[Any]) -> None:
        """Core trigger engine — inspects deployment context and peer runs,
        then dispatches downstream deployments via ``run_deployment``.

        See DESIGN.md for the full step-by-step walkthrough.
        """

        logger = flow_run_logger(flow_run, flow)

        # Step 1: parse job_variables, check disable_trigger
        job_variables: dict[str, Any] = flow_run.job_variables or {}
        extra_context: ExtraContext = ExtraContext(**job_variables)
        if extra_context.disable_trigger:
            return

        # Step 2: get current deployment context
        deployment_name: str | None = extra_context.master_node
        if deployment_name is None:
            deployment_name = f"{flow.name}/{deployment.get_name()}"

        if deployment_name is None:
            return

        deployment_context_: DeploymentContext | None = await cls.get_deployment_context(deployment_name)
        if deployment_context_ is None:
            logger.info(f"No deployment context: {deployment_name}.")
            return

        deployment_context: DeploymentContext = deployment_context_

        # Step 3: validate active state and downstream
        if not deployment_context.active:
            return

        downstream: list[Any] = deployment_context.downstream or []
        if not downstream:
            return

        # Step 4: collect peer deployment names
        peer_deployment_names: list[str] = [
            peer.ns.split("/")[-1] for peer in deployment_context.peer_tails if peer.active
        ]

        peer_runs: dict[UUID, FlowRun] = {}
        completed_runs: list[FlowRun] = []

        if peer_deployment_names:
            async with get_client() as client:
                completed_runs = await client.read_flow_runs(
                    deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=peer_deployment_names)),
                    flow_run_filter=FlowRunFilter(
                        start_time=FlowRunFilterStartTime(after_=datetime.now(UTC) - timedelta(days=7))
                    ),
                    limit=100,
                )

        # Step 5: filter peer runs by same batch (starter_id) and recency
        for peer_run in completed_runs:
            peer_job_variables: dict[str, Any] = peer_run.job_variables or {}
            peer_extra_context: ExtraContext = ExtraContext(**peer_job_variables)

            if peer_extra_context.starter_id != extra_context.starter_id:
                continue

            if peer_run.deployment_id is None or peer_run.end_time is None:
                continue

            if peer_run.deployment_id not in peer_runs:
                peer_runs[peer_run.deployment_id] = peer_run
                continue

            if (end_time := peer_runs[peer_run.deployment_id].end_time) is None:
                continue

            if end_time < peer_run.end_time:
                peer_runs[peer_run.deployment_id] = peer_run

        # Step 6: trigger downstream
        if peer_deployment_names:
            logger.info(f"Completed peer runs: {len(peer_runs)}/{len(peer_deployment_names)}.")

        if len(peer_deployment_names) != len(peer_runs):
            return

        states: list[State[Any] | None] = [
            state,
            *(run.state for run in peer_runs.values()),
        ]

        for ds in downstream:
            if not ds.active:
                continue

            fns: str
            dns: str
            fns, dns = ds.ns.split("/")
            _deployment: Deployment = get_deployment_instance(fns, dns)
            if not _deployment.node:
                continue

            if not _deployment.node.upstream_condition.check_all(states):
                continue

            if peer_deployment_names:
                is_trig: bool = await cls.is_triggered(
                    extra_context.starter_id,
                    deployment_context.peer_nss,
                    ds.ns,
                    str(flow_run.id),
                )
                if is_trig:
                    logger.warning(f"Downstream run has been triggered: {ds.ns}.")
                    continue

            # Prepare downstream parameters and context
            job_variables_: dict[str, Any] = extra_context.model_dump(include={"macro_variables", "starter_id"})

            if extra_context.macro_variables is None:
                macro_variables: dict[str, Any] = {}
                for var in MACRO_VARIABLES.get(cls.project_name, {}):
                    if var in flow_run.parameters:
                        macro_variables[var] = flow_run.parameters[var]
                job_variables_["macro_variables"] = macro_variables

            if extra_context.starter_id is None:
                job_variables_["starter_id"] = str(flow_run.id)

            parameters: dict[str, Any] = {
                p: job_variables_["macro_variables"][p]
                for p in _deployment.node.parameters
                if p in job_variables_["macro_variables"]
            }

            run = run_deployment(
                name=ds.ns,
                parameters=parameters,
                timeout=0,
                job_variables=job_variables_,
            )
            if not isinstance(run, FlowRun):
                run = await run
                logger.info(f"Created flow run for '{ds.ns}': {run.name}.")

    # ------------------------------------------------------------------
    # Introspection & flow parameter assembly
    # ------------------------------------------------------------------

    @classmethod
    def extract_parameters(cls, **kwargs: Any) -> dict[str, inspect.Parameter]:
        runner_signature: inspect.Signature = inspect.signature(cls.__init__)
        return {name: parameter for name, parameter in runner_signature.parameters.items() if name != "self"}

    @classmethod
    def copy_signature(cls, fn: Callable[..., Any], **kwargs: Any) -> inspect.Signature:
        runner_parameters: dict[str, inspect.Parameter] = cls.extract_parameters(**kwargs)
        signature: inspect.Signature = inspect.signature(fn)
        return signature.replace(parameters=[*runner_parameters.values()])

    @classmethod
    def get_hooks(cls, event: str) -> FlowStateHooks | None:
        external_hooks: FlowStateHooks | None = cast(FlowStateHooks | None, getattr(cls, f"on_{event}", None))
        cls_hooks: FlowStateHooks | None = cast(FlowStateHooks | None, getattr(cls, f"_on_{event}", None))

        if external_hooks is None and cls_hooks is None:
            return None
        return (external_hooks or []) + (cls_hooks or [])

    @classmethod
    def set_flow_parameters(cls) -> FlowParemeter:
        flow_parameters: FlowParemeter = {
            "on_running": cls.get_hooks("running"),
            "on_cancellation": cls.get_hooks("cancellation"),
            "on_completion": cls.get_hooks("completion"),
            "on_failure": cls.get_hooks("failure"),
            "on_crashed": cls.get_hooks("crashed"),
            "retries": cls.retries,
            "retry_delay_seconds": cls.retry_delay_seconds,
            "timeout_seconds": cls.timeout_seconds,
            "persist_result": cls.persist_result,
            "result_storage": cls.result_storage,
            "result_serializer": cls.result_serializer,
            "log_prints": cls.log_prints,
        }
        return flow_parameters

    # ------------------------------------------------------------------
    # Deploy factory
    # ------------------------------------------------------------------

    @classmethod
    def deploy(
        cls: Any,
        *injectors: *Injectors,  # type: ignore[valid-type]
        name: str | None = None,
        injector_ns: str | None = None,
        upstream_condition: Condition | None = None,
        schedules: FlexibleScheduleList | None = None,
        tag: list[str] | str | None = None,
        flag: str | None = None,
        workflow_pool: str | None = None,
        **kwargs: Any,
    ) -> Deployment:
        from .deployment import Deployment, Node

        node: Node = Node(
            cls,
            *injectors,
            upstream_condition=upstream_condition,
            injector_ns=injector_ns,
        )
        return Deployment(
            node,
            name=name,
            schedules=schedules,
            tag=tag,
            flag=flag,
            workflow_pool=workflow_pool,
            **kwargs,
        )
