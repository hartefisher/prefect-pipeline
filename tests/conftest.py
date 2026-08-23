"""Global pytest fixtures and fake-service registry for the framework test-suite.

Provides in-memory stand-ins for the external services the framework talks to
(MongoDB, Qdrant, LLM gateway, HTTP transport) plus a programmable fake for
the Prefect async client used by the DAG trigger engine. See
``docs/milestones/M6-testing/DESIGN.md`` for the design rationale.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from prefect_pipeline.core import runner_base
from prefect_pipeline.infra import db as db_module
from prefect_pipeline.models import BaseItem
from tests.fakes import FakeLLM, FakeMongoDB, FakeQdrantClient, FakeTransport


# ---------------------------------------------------------------------------
# Toy domain model
# ---------------------------------------------------------------------------
class ToyItem(BaseItem):
    """Minimal custom item used across integration / regression tests."""

    title: str = ""
    score: int = 0


@pytest.fixture
def toy_item() -> ToyItem:
    return ToyItem(url="https://example.com/toy", title="Toy", score=1)


# ---------------------------------------------------------------------------
# Fake MongoDB
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_db(monkeypatch: pytest.MonkeyPatch) -> FakeMongoDB:
    """Inject an in-memory ``FakeMongoDB`` in place of the real ``get_prefect``."""
    store = FakeMongoDB("prefect")
    monkeypatch.setattr(db_module, "get_prefect", lambda: store)
    # The trigger engine also reads a ``downstream_signal`` collection.
    _ = store.downstream_signal
    return store


# ---------------------------------------------------------------------------
# Fake Qdrant
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_qdrant(monkeypatch: pytest.MonkeyPatch) -> FakeQdrantClient:
    """Patch ``AsyncQdrantClient`` used by the vector components."""
    client = FakeQdrantClient()
    monkeypatch.setattr(
        "prefect_pipeline.components.vector.AsyncQdrantClient",
        lambda *a, **k: client,
    )
    return client


# ---------------------------------------------------------------------------
# Fake LLM
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_llm() -> FakeLLM:
    return FakeLLM()


# ---------------------------------------------------------------------------
# Fake HTTP transport
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_transport() -> FakeTransport:
    return FakeTransport()


# ---------------------------------------------------------------------------
# Fake Prefect client (DAG trigger engine)
# ---------------------------------------------------------------------------
class FakePrefectClient:
    """Programmable stand-in for ``prefect.client.get_client``.

    The DAG trigger engine (:meth:`FlowRunnerBase.trigger`) relies on the
    client for ``read_flow_runs``, ``create_flow_run_from_deployment`` and
    ``update_flow_run``. This fake records created runs and lets tests assert
    how many downstream deployments were dispatched. ``run_deployment`` is
    patched separately so tests can either emulate or stub real execution.
    """

    def __init__(self) -> None:
        self.flow_runs: list[Any] = []
        self.updated: list[tuple[Any, dict[str, Any]]] = []
        self._read_result: list[Any] = []

    async def __aenter__(self) -> FakePrefectClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    def seed_flow_runs(self, runs: list[Any]) -> None:
        """Preload peer runs returned by ``read_flow_runs``."""
        self._read_result = runs

    async def read_flow_runs(self, **kwargs: Any) -> list[Any]:
        return list(self._read_result)

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


@pytest.fixture
def fake_prefect_client(monkeypatch: pytest.MonkeyPatch) -> FakePrefectClient:
    """Patch ``prefect.client.get_client`` to return a scriptable fake."""
    client = FakePrefectClient()
    monkeypatch.setattr(runner_base, "get_client", lambda *a, **k: client)
    return client


@pytest.fixture
def prefect_ephemeral() -> dict[str, Any]:
    """Provide ephemeral-mode configuration knobs for E2E runs.

    Prefect runs in ephemeral mode automatically when no API server is
    configured, so this fixture mainly documents the expectation and yields a
    context dict tests can assert against.
    """
    return {"api_url": None, "ephemeral": True}
