from __future__ import annotations

from typing import Any, ClassVar, Literal

from prefect import get_run_logger, task

from ..components.data import DataTransformer
from ..infra.utils import NO_SELF
from .base import PipelineFlow


class AggregationFlow(PipelineFlow[tuple[type[DataTransformer[Any]]]]):
    """Aggregate items via a MongoDB aggregation pipeline (injected transformer)."""

    output_mode: ClassVar[Literal["merge", "out", "none"]] = "out"

    async def setup(self, Agg: type[DataTransformer[Any]], **extra: Any) -> None:
        self.aggregator = Agg(**extra)
        self.aggregator.output_mode = self.output_mode  # type: ignore[assignment]

    @task(
        cache_policy=NO_SELF,
        retries=3,
        retry_delay_seconds=[5, 10, 30],
    )
    async def aggregate(self) -> None:
        logger = get_run_logger()
        if self.data_flag:
            logger.info(f"Start to aggregate data ({self.data_flag})")
        await self.aggregator.get_data(filter=self.filter)
        logger.info(f"Transformed {0} items.")

    async def run(self) -> None:
        logger = get_run_logger()
        task_name = self.aggregator.collection.name
        if self.aggregator.output_collection is not None:
            task_name += f" -> {self.aggregator.output_collection.name}"
        logger.info(f"Start running aggregation task: {task_name}")
        await self.aggregate()  # type: ignore[call-overload]

    async def clear(self) -> None:
        await self.aggregator.close()


class OverallAggregationFlow(AggregationFlow):
    variant: ClassVar[str] = "Overall"
