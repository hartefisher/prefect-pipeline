from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from prefect import get_run_logger, task

from ..components.data import DataFetcher, QueryDict
from ..components.spider import HTTPSpider, SpiderBase
from ..infra.utils import NO_SELF
from ..models import BaseItem
from .base import PipelineFlow


class WebScrapingFlow[Item: BaseItem](
    PipelineFlow[tuple[type[SpiderBase[Item]] | type[HTTPSpider[Item]], type[DataFetcher[Item]]]]
):
    """Scrape external sites for items via an injected :class:`SpiderBase`."""

    # NOTE: the source project attached `retry_scraping` (a *task* decorator
    # factory) here, but flow-state hooks must be callables with the
    # `(flow, flow_run, state)` signature. `retry_scraping` is applied per-task
    # via `@retry_scraping(...)` on `request` instead, so no flow hook is set.
    on_failure: ClassVar[list[Any] | None] = None

    async def setup(
        self,
        Spider: type[SpiderBase[Item]] | type[HTTPSpider[Item]],
        DataFetcher: type[DataFetcher[Item]],
        **extra: Any,
    ) -> None:
        from prefect.runtime import flow_run

        batch_id = flow_run.get_id()
        self.spider = Spider(batch_id=batch_id, **extra)
        self.data_fetcher = DataFetcher()

    async def clear(self) -> None:
        await self.spider.close()

    @task(cache_policy=NO_SELF)
    async def check_data(self) -> list[Item]:
        logger = get_run_logger()
        if self.data_flag:
            logger.info(f"Start to check items for {self.data_flag}")

        kwargs: QueryDict = {}
        if self.filter:
            kwargs["filter"] = self.filter
        if self.spider.limit:
            kwargs["limit"] = self.spider.limit
        items = await self.data_fetcher.get_items(**kwargs)
        logger.info(f"{len(items)} items need to update.")
        return items

    @task(
        cache_policy=NO_SELF,
        retries=3,
        retry_delay_seconds=[10, 30, 60],
        retry_condition_fn=None,
        timeout_seconds=180,
    )
    async def request(self, item: Item) -> None:
        await self.spider.run(item)

    async def request_and_upsert(self, semaphore: asyncio.Semaphore, item: Item) -> None:
        async with semaphore:
            await self.request(item)  # type: ignore[call-overload]
            await asyncio.sleep(0.5)

    async def run(self) -> None:
        await self.spider.start()
        items = await self.check_data()  # type: ignore[call-overload]

        semaphore = asyncio.Semaphore(self.spider.concurrent_requests)
        await asyncio.gather(*[self.request_and_upsert(semaphore, item) for item in items])


class OverallWebScrapingFlow(WebScrapingFlow[BaseItem]):
    variant: ClassVar[str] = "Overall"
