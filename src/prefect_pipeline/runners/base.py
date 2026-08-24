from __future__ import annotations

from typing import Any, Literal

from ..core.runner_base import FlowRunnerBase


class Retry(FlowRunnerBase):
    """Shared retry policy for flows that should transparently retry failures."""

    retries = 4
    retry_delay_seconds = 60
    # timeout_seconds = 720


class PipelineFlow[Injectors: tuple[type[Any], ...] = tuple[type[Any], ...]](FlowRunnerBase[Injectors]):
    """Generic base class for all pipeline flows.

    Replaces the source project's ``ProductHuntFlow``. The ``project_name``
    namespace is supplied by the framework-level ``PROJECT_NAME`` config
    (env-driven, defaults to ``"default"``), so business projects declare it
    once in ``.env`` instead of hardcoding it on every runner. The timezone is
    delegated to the framework-level ``TIMEZONE`` config (defaults to UTC in
    :meth:`FlowRunnerBase.get_current_date`), so no per-flow timezone constant
    is required.

    Subclasses typically also override :meth:`setup` to receive injected
    component classes and :meth:`run` to drive them.
    """

    dt_fmt: str = "%Y%m%d"

    def __init__(
        self,
        *,
        dt: str | None = None,
        days: int = 1,
        offset: int = 0,
        backfill: bool | None = None,
        fill_direction: Literal["backward", "forward"] = "backward",
        **extra: Any,
    ) -> None:
        self.dt = dt
        self.days = days
        self.offset = offset
        self.backfill = backfill
        self.fill_direction = fill_direction
        self._extra = extra
        self.start_date, self.end_date = self.get_time_window(
            dt,
            days,
            offset=offset,
            backfill=backfill,
            fill_direction=fill_direction,
            fmt=self.dt_fmt,
        )

    @classmethod
    def extract_parameters(cls, **kwargs: Any) -> dict[str, Any]:
        parameters = super().extract_parameters(**kwargs)
        if cls.variant == "Overall":
            return {
                name: parameter
                for name, parameter in parameters.items()
                if name not in ("dt", "days", "offset", "backfill", "fill_direction")
            }
        return parameters

    @property
    def filter(self) -> dict[str, Any]:
        if self.variant == "Overall":
            return {}

        return {"dt": {"$gte": self.start_date, "$lte": self.end_date}}

    @property
    def data_flag(self) -> str | None:
        if self.variant == "Overall":
            return None

        if self.start_date != self.end_date:
            return f"{self.start_date} - {self.end_date}"

        return self.start_date
