"""Tests for prefect_pipeline.core.runner_base — Hook registration."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from typing import Any

from prefect_pipeline.core.runner_base import FlowRunnerBase, Hook

# --------------------------------------------------------------------------- #
# Hook construction
# --------------------------------------------------------------------------- #


def test_hook_default_events():
    h = Hook(fn=lambda: None)
    assert "completion" in h.events
    assert "failure" in h.events
    assert "crashed" in h.events
    assert "cancellation" in h.events
    assert "running" in h.events


def test_hook_single_event():
    h = Hook(fn=lambda: None, event="running")
    assert h.events == ["running"]


def test_hook_multiple_events():
    h = Hook(fn=lambda: None, event=["running", "completion"])
    assert h.events == ["running", "completion"]


# --------------------------------------------------------------------------- #
# Hook.on decorator
# --------------------------------------------------------------------------- #


def test_hook_on_with_fn():
    def my_fn() -> None:
        pass

    result = Hook.on(my_fn, event="running")
    assert isinstance(result, Hook)
    assert result.events == ["running"]


def test_hook_on_as_decorator():
    @Hook.on(event="running")
    def my_fn() -> None:
        pass

    assert isinstance(my_fn, Hook)
    assert my_fn.events == ["running"]


# --------------------------------------------------------------------------- #
# Minimal base class (no inherited hooks) for isolated testing
# --------------------------------------------------------------------------- #


class _HookTestBase:
    """Minimal base with __init_subclass__ hook collection, no pre-existing hooks."""

    on_running: Any = None
    on_cancellation: Any = None
    on_completion: Any = None
    on_failure: Any = None
    on_crashed: Any = None
    _on_running: Any = None
    _on_cancellation: Any = None
    _on_completion: Any = None
    _on_failure: Any = None
    _on_crashed: Any = None

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

    @classmethod
    def get_hooks(cls, event: str) -> list[Any] | None:
        external_hooks: Any = getattr(cls, f"on_{event}", None)
        cls_hooks: Any = getattr(cls, f"_on_{event}", None)
        if external_hooks is None and cls_hooks is None:
            return None
        return (external_hooks or []) + (cls_hooks or [])


# --------------------------------------------------------------------------- #
# __init_subclass__ hook collection
# --------------------------------------------------------------------------- #


def test_subclass_collects_hooks():
    class MyRunner(_HookTestBase):
        @Hook.on(event="running")
        @classmethod
        async def on_run(cls, *args: object) -> None:
            pass

    hooks = MyRunner._on_running
    assert hooks is not None
    assert len(hooks) == 1


def test_subclass_no_hooks():
    class PlainRunner(_HookTestBase):
        pass

    assert PlainRunner._on_running is None


def test_subclass_multiple_events():
    class MultiRunner(_HookTestBase):
        @Hook.on(event="running")
        @classmethod
        async def on_run(cls, *args: object) -> None:
            pass

        @Hook.on(event="completion")
        @classmethod
        async def on_done(cls, *args: object) -> None:
            pass

    assert MultiRunner._on_running is not None
    assert len(MultiRunner._on_running) == 1
    assert MultiRunner._on_completion is not None
    assert len(MultiRunner._on_completion) == 1


def test_get_hooks_combines_external_and_cls():
    class HookRunner(_HookTestBase):
        @Hook.on(event="completion")
        @classmethod
        async def on_done(cls, *args: object) -> None:
            pass

    # External hook
    HookRunner.on_completion = [lambda *a: None]  # type: ignore[assignment]

    hooks = HookRunner.get_hooks("completion")
    assert hooks is not None
    assert len(hooks) == 2  # 1 external + 1 from __init_subclass__


def test_get_hooks_returns_none_when_empty():
    class EmptyRunner(_HookTestBase):
        pass

    assert EmptyRunner.get_hooks("running") is None


# --------------------------------------------------------------------------- #
# FlowRunnerBase inherited hooks (integration)
# --------------------------------------------------------------------------- #


def test_subclass_inherits_base_hooks():
    class SubRunner(FlowRunnerBase):
        @Hook.on(event="running")
        @classmethod
        async def custom_hook(cls, *args: object) -> None:
            pass

    # Should have 2: 1 inherited from FlowRunnerBase + 1 from SubRunner
    assert SubRunner._on_running is not None
    assert len(SubRunner._on_running) == 2


def test_plain_subclass_inherits_base_hooks():
    class PlainSub(FlowRunnerBase):
        pass

    # PlainSub inherits FlowRunnerBase's hooks
    assert PlainSub._on_running is not None
    assert len(PlainSub._on_running) == 1  # update_flow_run


# --------------------------------------------------------------------------- #
# set_flow_parameters / extract_parameters
# --------------------------------------------------------------------------- #


def test_set_flow_parameters_returns_dict():
    class ParamRunner(FlowRunnerBase):
        retries = 3
        timeout_seconds = 300

    params = ParamRunner.set_flow_parameters()
    assert params["retries"] == 3
    assert params["timeout_seconds"] == 300
    assert "on_running" in params
    assert "on_completion" in params


def test_extract_parameters_excludes_self():
    class SimpleRunner(FlowRunnerBase):
        def __init__(self, dt: str = "", days: int = 1, **extra: object) -> None:
            super().__init__(**extra)

    params = SimpleRunner.extract_parameters()
    assert "self" not in params
    assert "dt" in params
    assert "days" in params
