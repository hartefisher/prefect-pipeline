"""Coverage for ``FlowRunnerBase`` plumbing methods (M6.4).

Exercises the non-trigger base methods: ``start`` / ``run`` / ``clear``,
time-window helpers (``get_time_window`` / ``flow_run_name`` /
``get_current_date``), and the introspection / deploy helpers. The trigger
engine (DAG dispatch) is covered separately by the regression suite.
"""

from __future__ import annotations

import pytest

from prefect_pipeline.core.runner_base import FlowRunnerBase
from prefect_pipeline.runners.base import PipelineFlow


class _PlainRunner(FlowRunnerBase):
    """A runner with no injected components, for base-method coverage."""

    async def run(self) -> int:  # type: ignore[override]
        self.ran = True  # type: ignore[attr-defined]
        return 42

    async def clear(self) -> None:
        self.cleared = True  # type: ignore[attr-defined]


# --------------------------------------------------------------------------- #
# start / run / clear
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_base_start_runs_and_clears():
    runner = _PlainRunner()
    await runner.start()
    assert runner.ran is True  # type: ignore[attr-defined]
    assert runner.cleared is True  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_base_run_default_is_none():
    # FlowRunnerBase.run is a no-op returning None.
    assert await FlowRunnerBase().run() is None


# --------------------------------------------------------------------------- #
# time-window helpers
# --------------------------------------------------------------------------- #
def test_get_time_window_single_day():
    start, end = PipelineFlow.get_time_window(dt="20240101", days=1)
    assert start == end == "20240101"


def test_get_time_window_multi_day_backward():
    start, end = PipelineFlow.get_time_window(dt="20240103", days=3, fill_direction="backward")
    assert start == "20240101"
    assert end == "20240103"


def test_get_time_window_multi_day_forward():
    start, end = PipelineFlow.get_time_window(dt="20240101", days=3, fill_direction="forward")
    assert start == "20240101"
    assert end == "20240103"


def test_get_time_window_with_offset():
    start, end = PipelineFlow.get_time_window(dt="20240110", days=1, offset=2)
    assert start == end == "20240112"


def test_get_time_window_backfill_uses_single_day():
    start, end = PipelineFlow.get_time_window(dt="20240101", days=5, backfill=True)
    assert start == end == "20240101"


def test_flow_run_name_backfill_prefix():
    name = PipelineFlow.flow_run_name(dt="20240101", backfill=True)
    assert name.startswith("Backfill-")


def test_flow_run_name_multi_day_range():
    name = PipelineFlow.flow_run_name(dt="20240103", days=3)
    assert name == "20240101-20240103"


def test_get_current_date_returns_datetime():
    d = FlowRunnerBase.get_current_date()
    assert hasattr(d, "year")


# --------------------------------------------------------------------------- #
# introspection / deploy helpers
# --------------------------------------------------------------------------- #
def test_extract_parameters_excludes_self():
    params = PipelineFlow.extract_parameters()
    assert "self" not in params


def test_copy_signature_preserves_runner_params():
    # copy_signature replaces the wrapped fn's signature with the runner's
    # (dt / days / offset / backfill / fill_direction / **extra).
    sig = PipelineFlow.copy_signature(lambda a, b: None)
    assert "dt" in sig.parameters
    assert "days" in sig.parameters
    assert "backfill" in sig.parameters


def test_get_hooks_running_includes_builtin_hook():
    # FlowRunnerBase defines an `update_flow_run` on_running hook; it must surface.
    hooks = PipelineFlow.get_hooks("running")
    assert hooks is not None
    assert len(hooks) >= 1


def test_get_hooks_unknown_event_is_none():
    assert PipelineFlow.get_hooks("nonexistent_event") is None


def test_set_flow_parameters_shape():
    fp = PipelineFlow.set_flow_parameters()
    assert "on_completion" in fp
    assert "retries" in fp


def test_deploy_returns_deployment():
    dep = PipelineFlow.deploy(name="base_smoke")
    assert dep.node is not None
    assert dep.node.runner is PipelineFlow
