"""Trigger lifecycle regression: hook dispatch and disable_trigger.

These pin two framework behaviors:
* lifecycle hooks registered via ``@Hook.on`` are collected per event and
  invoked by the Prefect state-hook machinery;
* ``ExtraContext.disable_trigger`` short-circuits the trigger engine so no
  downstream deployment is ever dispatched.
"""

from __future__ import annotations

from typing import ClassVar
from unittest.mock import MagicMock

import pytest
from prefect.client.schemas.objects import State, StateType

from prefect_pipeline.core.runner_base import FlowRunnerBase, Hook


# ---------------------------------------------------------------------------
# Hook dispatch
# ---------------------------------------------------------------------------
class LifecycleRunner(FlowRunnerBase):
    """Runner that registers explicit completion / failure hooks."""

    completion_calls: ClassVar[list[str]] = []
    failure_calls: ClassVar[list[str]] = []

    @Hook.on(event="completion")  # type: ignore[operator, untyped-decorator]
    @classmethod
    async def on_done(cls, flow: MagicMock, flow_run: MagicMock, state: State[object]) -> None:
        cls.completion_calls.append(flow_run.name)

    @Hook.on(event="failure")  # type: ignore[operator, untyped-decorator]
    @classmethod
    async def on_fail(cls, flow: MagicMock, flow_run: MagicMock, state: State[object]) -> None:
        cls.failure_calls.append(flow_run.name)


def test_hooks_collected_by_event():
    completion = LifecycleRunner.get_hooks("completion") or []
    failure = LifecycleRunner.get_hooks("failure") or []
    # The subclass hook must be present alongside the inherited lifecycle hooks.
    assert any(getattr(h, "__self__", None) is LifecycleRunner for h in completion)
    assert any(getattr(h, "__self__", None) is LifecycleRunner for h in failure)


async def test_completion_hook_invoked_on_completed_state():
    LifecycleRunner.completion_calls = []
    flow = MagicMock()
    flow_run = MagicMock()
    flow_run.name = "A"
    hook_fn = LifecycleRunner.on_done.fn.__func__  # type: ignore[attr-defined]
    await hook_fn(LifecycleRunner, flow, flow_run, State(type=StateType.COMPLETED))
    assert LifecycleRunner.completion_calls == ["A"]


async def test_failure_hook_invoked_on_failed_state():
    LifecycleRunner.failure_calls = []
    flow = MagicMock()
    flow_run = MagicMock()
    flow_run.name = "B"
    hook_fn = LifecycleRunner.on_fail.fn.__func__  # type: ignore[attr-defined]
    await hook_fn(LifecycleRunner, flow, flow_run, State(type=StateType.FAILED))
    assert LifecycleRunner.failure_calls == ["B"]


# ---------------------------------------------------------------------------
# disable_trigger
# ---------------------------------------------------------------------------
async def test_disable_trigger_skips_dispatch(monkeypatch: pytest.MonkeyPatch):
    from tests.regression.conftest import (
        TriggerHarness,
        make_context,
        make_flow_run,
        make_node_info,
    )

    class ToyRunner(FlowRunnerBase):
        pass

    harness = TriggerHarness(monkeypatch)
    harness.context = make_context(
        ns="flow/A", peer_tails=[make_node_info("flow/A")], downstream=[make_node_info("flow/B")]
    )
    harness.register_downstream("flow/B", ToyRunner)
    harness.install(ToyRunner)

    run = make_flow_run(
        deployment_id="A",
        name="A",
        parameters={"dt": "20260101"},
        extra_job_variables={"disable_trigger": True},
    )
    harness.peer_runs = [run]
    await harness.run(ToyRunner, run)

    assert harness.dispatched == []
