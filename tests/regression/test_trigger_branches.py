"""Extra trigger-engine branch coverage (M6.4).

Pinpoints the edge branches of ``FlowRunnerBase.trigger`` that the main DAG
semantics suite does not exercise: ``disable_trigger`` short-circuit, peer-run
starter-id filtering, missing peer identifiers, down-stream ``active``/``node``
skips, and duplicate peer-deployment de-duplication. All use the shared
:class:`TriggerHarness` so no real Prefect server is required.
"""

from __future__ import annotations

import pytest
from prefect.client.schemas.objects import StateType

from prefect_pipeline.core.deployment import Deployment
from prefect_pipeline.core.runner_base import FlowRunnerBase
from tests.regression.conftest import (
    TriggerHarness,
    make_context,
    make_flow_run,
    make_node_info,
)


class ToyTriggerRunner(FlowRunnerBase):
    """No-op runner hosting the inherited ``trigger`` classmethod."""


# --------------------------------------------------------------------------- #
# disable_trigger short-circuit
# --------------------------------------------------------------------------- #
async def test_disable_trigger_skips_dispatch(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A", peer_tails=[make_node_info("flow/A")], downstream=[make_node_info("flow/B")]
    )
    harness.register_downstream("flow/B", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    run = make_flow_run(deployment_id="A", name="A", extra_job_variables={"disable_trigger": True})
    harness.peer_runs = [run]
    await harness.run(ToyTriggerRunner, run)
    assert harness.dispatched == []


# --------------------------------------------------------------------------- #
# starter_id mismatch filters out a peer run
# --------------------------------------------------------------------------- #
async def test_peer_with_different_starter_id_is_ignored(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A", peer_tails=[make_node_info("flow/A")], downstream=[make_node_info("flow/B")]
    )
    harness.register_downstream("flow/B", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    # The flow_run being triggered uses the default starter-1; the peer run
    # belongs to a different starter batch → filtered out (line 445).
    flow_run = make_flow_run(deployment_id="A", name="A", starter_id="starter-1")
    peer = make_flow_run(deployment_id="A", name="A", starter_id="other-batch")
    harness.peer_runs = [peer]
    await harness.run(ToyTriggerRunner, flow_run)
    assert harness.dispatched == []


# --------------------------------------------------------------------------- #
# peer run missing deployment_id / end_time is skipped
# --------------------------------------------------------------------------- #
async def test_peer_missing_deployment_id_skipped(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A", peer_tails=[make_node_info("flow/A")], downstream=[make_node_info("flow/B")]
    )
    harness.register_downstream("flow/B", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    run = make_flow_run(deployment_id="A", name="A")
    run.deployment_id = None  # type: ignore[assignment]
    harness.peer_runs = [run]
    await harness.run(ToyTriggerRunner, run)
    assert harness.dispatched == []


async def test_peer_missing_end_time_skipped(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A", peer_tails=[make_node_info("flow/A")], downstream=[make_node_info("flow/B")]
    )
    harness.register_downstream("flow/B", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    run = make_flow_run(deployment_id="A", name="A")
    run.end_time = None
    harness.peer_runs = [run]
    await harness.run(ToyTriggerRunner, run)
    assert harness.dispatched == []


# --------------------------------------------------------------------------- #
# duplicate peer deployment keeps the latest end_time
# --------------------------------------------------------------------------- #
async def test_duplicate_peer_deployment_keeps_latest(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    # single peer tail A; two runs for A with different end_times
    harness.context = make_context(
        ns="flow/A", peer_tails=[make_node_info("flow/A")], downstream=[make_node_info("flow/B")]
    )
    harness.register_downstream("flow/B", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    older = make_flow_run(deployment_id="A", name="A")
    newer = make_flow_run(deployment_id="A", name="A")
    # newer wins the end_time comparison (line 455-459) — use comparable ints
    older.end_time = 0  # type: ignore[assignment]
    newer.end_time = 1  # type: ignore[assignment]
    harness.peer_runs = [older, newer]
    await harness.run(ToyTriggerRunner, newer)
    assert harness.dispatched == ["flow/B"]


# --------------------------------------------------------------------------- #
# downstream active=False short-circuits that branch (line 477)
# --------------------------------------------------------------------------- #
async def test_downstream_inactive_is_skipped(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    # both peers complete; downstream D inactive, E active
    harness.context = make_context(
        ns="flow/A",
        peer_tails=[make_node_info("flow/A"), make_node_info("flow/B")],
        downstream=[make_node_info("flow/D", active=False), make_node_info("flow/E")],
    )
    harness.register_downstream("flow/E", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    run_a = make_flow_run(deployment_id="A", name="A", state=StateType.COMPLETED)
    run_b = make_flow_run(deployment_id="B", name="B", state=StateType.COMPLETED)
    harness.peer_runs = [run_a, run_b]
    await harness.run(ToyTriggerRunner, run_a)

    # only the active downstream (E) is dispatched; D is skipped
    assert harness.dispatched == ["flow/E"]


# --------------------------------------------------------------------------- #
# downstream with node=None is skipped (line 484)
# --------------------------------------------------------------------------- #
async def test_downstream_missing_node_is_skipped(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A",
        peer_tails=[make_node_info("flow/A"), make_node_info("flow/B")],
        downstream=[make_node_info("flow/E")],
    )
    harness.register_downstream("flow/E", ToyTriggerRunner)
    # Inject a downstream whose deployment has node=None (line 484 skip)
    harness._downstream_deployments["flow/E"] = Deployment(node=None, name="E")
    harness.install(ToyTriggerRunner)

    run_a = make_flow_run(deployment_id="A", name="A", state=StateType.COMPLETED)
    run_b = make_flow_run(deployment_id="B", name="B", state=StateType.COMPLETED)
    harness.peer_runs = [run_a, run_b]
    await harness.run(ToyTriggerRunner, run_a)

    assert harness.dispatched == []
