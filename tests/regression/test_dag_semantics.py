"""DAG semantics regression: sequence / parallel / mixed / peer-check / condition.

These tests pin the orchestration engine's trigger semantics (the final
arbiter of a correct migration). See DESIGN.md §3 for the asserted cases.
"""

from __future__ import annotations

import pytest
from prefect.client.schemas.objects import StateType

from prefect_pipeline.core.runner_base import FlowRunnerBase
from tests.regression.conftest import (
    TriggerHarness,
    make_context,
    make_flow_run,
    make_node_info,
)


class ToyTriggerRunner(FlowRunnerBase):
    """A no-op runner used only to host the inherited ``trigger`` classmethod."""


# ---------------------------------------------------------------------------
# 1. Sequence  A >> B
# ---------------------------------------------------------------------------
async def test_sequence_triggers_downstream_once(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A", peer_tails=[make_node_info("flow/A")], downstream=[make_node_info("flow/B")]
    )
    harness.register_downstream("flow/B", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    # A's own completed run is discoverable as its single peer tail
    run = make_flow_run(deployment_id="A", name="A", parameters={"dt": "20260101"})
    harness.peer_runs = [run]
    await harness.run(ToyTriggerRunner, run)

    assert harness.dispatched == ["flow/B"]
    # starter_id must be propagated to the downstream job variables
    dispatched_kwargs = harness._run_deployment.call_args.kwargs
    assert dispatched_kwargs["job_variables"]["starter_id"] == "starter-1"


# ---------------------------------------------------------------------------
# 2. Parallel  A + B  >> D  (peer check)
# ---------------------------------------------------------------------------
async def test_parallel_triggers_only_when_all_peers_complete(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    # A completes; its context declares peers A & B and downstream D
    harness.context = make_context(
        ns="flow/A",
        peer_tails=[make_node_info("flow/A"), make_node_info("flow/B")],
        downstream=[make_node_info("flow/D")],
    )
    harness.register_downstream("flow/D", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    # Only A has run so far → B still missing → no dispatch
    run_a = make_flow_run(deployment_id="A", name="A")
    await harness.run(ToyTriggerRunner, run_a)
    assert harness.dispatched == []


async def test_parallel_triggers_when_both_peers_complete(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A",
        peer_tails=[make_node_info("flow/A"), make_node_info("flow/B")],
        downstream=[make_node_info("flow/D")],
    )
    harness.register_downstream("flow/D", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    # Both A and B have completed under the same starter_id
    run_a = make_flow_run(deployment_id="A", name="A")
    run_b = make_flow_run(deployment_id="B", name="B")
    harness.peer_runs = [run_a, run_b]

    await harness.run(ToyTriggerRunner, run_a)
    assert harness.dispatched == ["flow/D"]


# ---------------------------------------------------------------------------
# 3. Mixed  A >> (B + C) >> D  with downstream dedup
# ---------------------------------------------------------------------------
async def test_mixed_dedups_downstream_to_single_dispatch(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    # B's perspective: peers B & C, downstream D
    harness.context = make_context(
        ns="flow/B",
        peer_tails=[make_node_info("flow/B"), make_node_info("flow/C")],
        downstream=[make_node_info("flow/D")],
    )
    harness.register_downstream("flow/D", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    run_b = make_flow_run(deployment_id="B", name="B")
    run_c = make_flow_run(deployment_id="C", name="C")
    harness.peer_runs = [run_b, run_c]

    # First completion (B) dispatches D; is_triggered returns False (first time)
    await harness.run(ToyTriggerRunner, run_b)
    assert harness.dispatched == ["flow/D"]

    # Second completion (C) would also dispatch D, but dedup says already triggered
    harness.is_triggered_queue = [True]
    await harness.run(ToyTriggerRunner, run_c)
    assert harness.dispatched == ["flow/D"]  # still exactly one


# ---------------------------------------------------------------------------
# 4. Condition ALL — any failed upstream blocks the trigger
# ---------------------------------------------------------------------------
async def test_condition_all_blocks_on_failed_peer(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A",
        peer_tails=[make_node_info("flow/A"), make_node_info("flow/B")],
        downstream=[make_node_info("flow/D")],
    )
    harness.register_downstream("flow/D", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    run_a = make_flow_run(deployment_id="A", name="A", state=StateType.COMPLETED)
    run_b = make_flow_run(deployment_id="B", name="B", state=StateType.FAILED)
    harness.peer_runs = [run_a, run_b]

    await harness.run(ToyTriggerRunner, run_a)
    assert harness.dispatched == []


async def test_condition_all_passes_when_all_completed(monkeypatch: pytest.MonkeyPatch):
    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A",
        peer_tails=[make_node_info("flow/A"), make_node_info("flow/B")],
        downstream=[make_node_info("flow/D")],
    )
    harness.register_downstream("flow/D", ToyTriggerRunner)
    harness.install(ToyTriggerRunner)

    run_a = make_flow_run(deployment_id="A", name="A", state=StateType.COMPLETED)
    run_b = make_flow_run(deployment_id="B", name="B", state=StateType.COMPLETED)
    harness.peer_runs = [run_a, run_b]

    await harness.run(ToyTriggerRunner, run_a)
    assert harness.dispatched == ["flow/D"]
