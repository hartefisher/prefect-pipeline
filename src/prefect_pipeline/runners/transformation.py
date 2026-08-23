from __future__ import annotations

from typing import Any, ClassVar

from prefect import get_run_logger, task
from prefect.runtime import flow_run

from ..components.data import DataTransformer
from ..infra.utils import NO_SELF
from .base import PipelineFlow


class TransformationFlow(PipelineFlow[tuple[type[DataTransformer[Any]]]]):
    """Transform items in place via an injected :class:`DataTransformer`."""

    async def setup(self, Transformer: type[DataTransformer[Any]], **extra: Any) -> None:
        batch_id = flow_run.get_id()
        self.transformer = Transformer(batch_id=batch_id, **extra)

    @task(
        cache_policy=NO_SELF,
        retries=3,
        retry_delay_seconds=[5, 10, 30],
    )
    async def transform(self) -> int:
        logger = get_run_logger()
        if self.data_flag:
            logger.info(f"Start to transform items for {self.data_flag}")
        upserted_count = await self.transformer.run(filter=self.filter)
        logger.info(f"Transformed {upserted_count} items.")
        return upserted_count

    async def run(self) -> int:
        logger = get_run_logger()
        task_name = self.transformer.collection.name
        if self.transformer.output_collection is not None:
            task_name += f" -> {self.transformer.output_collection.name}"
        logger.info(f"Start running transformation task: {task_name}")
        modified_count = await self.transform()  # type: ignore[call-overload]
        return int(modified_count)

    async def clear(self) -> None:
        await self.transformer.close()


class OverallTransformationFlow(TransformationFlow):
    variant: ClassVar[str] = "Overall"
