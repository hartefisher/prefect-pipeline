from __future__ import annotations

from typing import Any, ClassVar

from prefect import get_run_logger, task

from ..components.data import DataFetcher, EmbeddingHandler
from ..infra.utils import NO_SELF
from ..models import BaseItem
from .base import PipelineFlow


class EmbeddingFlow[Item: BaseItem](PipelineFlow[tuple[type[EmbeddingHandler[Item]], type[DataFetcher[Item]]]]):
    """Vectorize items via an injected :class:`EmbeddingHandler`."""

    async def setup(
        self,
        EmbeddingHandler: type[EmbeddingHandler[Item]],
        DataFetcher: type[DataFetcher[Item]],
        **extra: Any,
    ) -> None:
        self.handler = EmbeddingHandler(**extra)
        self.data_fetcher = DataFetcher()
        self.embedded_count = 0

    async def clear(self) -> None:
        await self.data_fetcher.close()
        await self.handler.close()

    @task(
        cache_policy=NO_SELF,
        retries=3,
        retry_delay_seconds=[5, 10, 30],
    )
    async def embed(self, items: list[Item]) -> None:
        logger = get_run_logger()
        embedded_count = await self.handler.run(items)
        logger.info(f"Embedded {embedded_count} items.")
        self.embedded_count += embedded_count

    async def run(self) -> None:
        logger = get_run_logger()
        logger.info(f"Start running embedding task: {self.handler.collection_name}")
        batch = self.data_fetcher.get_batch_items(filter=self.filter)
        if self.data_flag:
            logger.info(f"Start to embed items for {self.data_flag}")

        async for items in batch:
            await self.embed(items)  # type: ignore[call-overload]
        logger.info(f"Embedded {self.embedded_count} items by total.")


class OverallEmbeddingFlow(EmbeddingFlow[BaseItem]):
    variant: ClassVar[str] = "Overall"
