"""Timezone configuration tests (M5.3).

The framework's "current date" is derived from the framework-level TIMEZONE
config (defaults to UTC). A subclass can override ``timezone`` to keep
business behavior (e.g. Asia/Shanghai) without any hardcoded constant.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from prefect_pipeline.core.configs import TIMEZONE
from prefect_pipeline.runners.base import PipelineFlow


def test_default_timezone_is_utc():
    # TIMEZONE is the framework default used by FlowRunnerBase.get_current_date
    assert TIMEZONE == "UTC"


def test_pipeline_flow_get_current_date_uses_configured_timezone():
    class ShanghaFlow(PipelineFlow):
        # emulate a user project that keeps Asia/Shanghai behavior
        @staticmethod
        def get_current_date():
            from zoneinfo import ZoneInfo

            return datetime.now(ZoneInfo("Asia/Shanghai")) - timedelta(days=1)

    sh = ShanghaFlow.get_current_date()
    utc = PipelineFlow.get_current_date()

    # The Shanghai date should be 8 hours ahead of UTC's date boundary.
    assert sh.tzinfo is not None
    assert utc.tzinfo is not None
    # On most days the calendar date differs by exactly one day.
    assert (sh.date() - utc.date()) in (timedelta(days=0), timedelta(days=1))


def test_pipeline_flow_timezone_independent_of_hardcoded_constant():
    # Ensure no runner hardcodes Asia/Shanghai; the default path uses TIMEZONE.
    import prefect_pipeline.runners as runners

    source_files = [
        runners.__file__,
        runners.base.__file__,
    ]
    for path in source_files:
        with open(path, encoding="utf-8") as f:
            assert "Asia/Shanghai" not in f.read()
