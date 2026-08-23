from __future__ import annotations

import asyncio
import json
from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar

from motor.motor_asyncio import AsyncIOMotorCollection
from prefect import get_client, get_run_logger, task
from prefect.runtime import flow_run

from ..components.batch import BatchReasoningJob
from ..components.data import DataFetcher, QueryDict
from ..components.llm import (
    BatchLLMExtractionStrategy,
    GenericExtractor,
    LLMExtractionStrategy,
)
from ..infra.db import get_prefect
from ..infra.error_handlers import (
    REASONING_RETRY_TIMES,
    llm_request_error_handler,
    retry_reasoning,
)
from ..infra.exceptions import (
    RETRIABLE_STATUS_CODES,
    BadResponseError,
    BatchJobFailed,
    BatchJobNotCompleted,
    BatchJobResponseError,
    IgnoreRequest,
    RetryReasoning,
    retriable_exceptions,
)
from ..infra.utils import NO_SELF
from ..models import BaseItem
from ..models.llm import (
    CompletionConfig,
    get_llm_provider,
)
from .base import PipelineFlow


class ReasoningFlow[Item: BaseItem](PipelineFlow[tuple[type[GenericExtractor[Item]], type[DataFetcher[Item]]]]):
    """LLM reasoning over items via an injected :class:`GenericExtractor`.

    Supports two modes selected by the resolved LLM config:

    * **realtime** — one completion per item through ``GenericExtractor.run``.
    * **batch** — build a JSONL file, submit it to a batch provider, poll, then
      upsert results. The batch handler class is pluggable via the
      ``batch_handler_class`` class attribute (defaults to the framework's
      OpenAI-style :class:`BatchReasoningJob`).
    """

    ns: str | None = None
    on_failure: ClassVar[list[Any] | None] = [retry_reasoning]

    #: Class of batch handler to instantiate. Override in subclasses / via the
    #: injected LLM config to use a provider-specific handler (e.g. Ark).
    batch_handler_class: ClassVar[type[BatchReasoningJob]] = BatchReasoningJob

    async def setup(
        self,
        Extractor: type[GenericExtractor[Item]],
        DataFetcher: type[DataFetcher[Item]],
        *,
        llm_config: dict[str, Any] | None = None,
        batch_job_id: str | None = None,
        retry_times: int = 0,
        previous_retriable_requests: int = 0,
        **extra: Any,
    ) -> None:
        ns = f"{Extractor.__module__}.{Extractor.__name__}"
        llm_config_ = await self.get_llm_config(ns, llm_config)
        if llm_config_ is None:
            raise ValueError(f"Can't find llm config: {ns}")

        strategy = BatchLLMExtractionStrategy if llm_config_.batch else LLMExtractionStrategy
        batch_id = flow_run.get_id()
        self.extractor = Extractor(llm_config_, strategy=strategy, batch_id=batch_id, **extra)
        self.batch_job_id = batch_job_id
        self.data_fetcher = DataFetcher()
        self.retriable_requests = 0
        self.retry_times = retry_times
        self.previous_retriable_requests = previous_retriable_requests

    @task(cache_policy=NO_SELF)
    async def check_data(self) -> list[Item]:
        logger = get_run_logger()
        if self.data_flag:
            logger.info(f"Start to check items for {self.data_flag}")
        filter = (
            self.filter if self.extractor.override else {**self.filter, self.extractor.output_field: {"$exists": False}}
        )
        kwargs: QueryDict = {"filter": filter}
        if self.extractor.limit:
            kwargs["limit"] = self.extractor.limit
        items = await self.data_fetcher.get_items(**kwargs)
        logger.info(f"{len(items)} items need to update.")
        return items

    async def clear(self) -> None:
        await self.data_fetcher.close()

    async def get_llm_config(
        self,
        ns: str,
        update: dict[str, Any] | None = None,
    ) -> CompletionConfig | None:
        """Resolve an LLM config for namespace ``ns``.

        Looks up the ``extractors`` collection joined with ``llm_configs`` and
        instantiates the matching :class:`CompletionConfig` subclass. Provider
        classes are resolved through the registry populated by
        :func:`register_llm_provider` (no hardcoded business import path).
        """
        try:
            extractors: AsyncIOMotorCollection[dict[str, Any]] = get_prefect().extractors
            records = await extractors.aggregate(
                [
                    {"$match": {"ns": ns}},
                    {
                        "$lookup": {
                            "from": "llm_configs",
                            "localField": "model",
                            "foreignField": "ns",
                            "as": "llm_configs",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$llm_configs",
                            "preserveNullAndEmptyArrays": False,
                        }
                    },
                    {"$project": {"_id": 0, "model": 0, "ns": 0}},
                    {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$llm_configs", "$$ROOT"]}}},
                    {"$project": {"_id": 0, "llm_configs": 0, "ns": 0}},
                ]
            ).to_list()
        except Exception:
            # DB unavailable (or no registry). If a config dict was injected
            # directly, build from it so the runner stays usable without Mongo.
            if update:
                return CompletionConfig(**update)
            return None

        if not records:
            # No DB-resolved record: if a config dict was injected directly,
            # build a base CompletionConfig from it so the runner is usable
            # without a MongoDB-backed registry (also the test path).
            if update:
                return CompletionConfig(**update)
            return None

        config = {**records[0], **(update or {})}
        llm_cls: type[CompletionConfig] = CompletionConfig
        if provider_ns := config.pop("provider", None):
            llm_cls = get_llm_provider(provider_ns) or CompletionConfig
        return llm_cls(**config)

    async def run(self) -> None:
        if self.extractor.llm_config.batch:
            async with get_client() as client:
                await client.set_flow_run_name(flow_run.id, f"[BATCH]{flow_run.name}")
            await self.batch_inference()
        else:
            await self.inference()
        self.check_retriable_requests()

    @task(
        cache_policy=NO_SELF,
        retries=2,
        retry_delay_seconds=[10, 30],
        retry_condition_fn=llm_request_error_handler,
        task_run_name="{item._slug}",
        timeout_seconds=1200,
    )
    async def request(self, item: Item) -> None:
        try:
            await self.extractor.run(item)
        except IgnoreRequest as e:
            logger = get_run_logger()
            logger.warning(f"Ignore item {item.url} due to {e.args[0]}.")
            return None

    async def request_and_upsert(self, semaphore: asyncio.Semaphore, item: Item) -> None:
        async with semaphore:
            try:
                await self.request(item)  # type: ignore[call-overload]
            except retriable_exceptions:
                self.retriable_requests += 1
            except TimeoutError:
                self.retriable_requests += 1
            except Exception as e:
                print(e)

    def check_retriable_requests(self) -> None:
        if self.retriable_requests <= 0:
            return

        if self.retry_times == REASONING_RETRY_TIMES and self.retriable_requests < 3:
            return

        if (
            self.retry_times >= REASONING_RETRY_TIMES - 1
            and self.retriable_requests < 5
            and self.retriable_requests == self.previous_retriable_requests
        ):
            return

        raise RetryReasoning(
            self.retriable_requests,
            self.extractor.llm_config.batch,
        )

    async def inference(self) -> None:
        logger = get_run_logger()
        products = await self.check_data()  # type: ignore[call-overload]
        logger.info(f"Start running reasoning task: {self.extractor.output_field}")
        if self.extractor.concurrent_requests > 1:
            semaphore = asyncio.Semaphore(self.extractor.concurrent_requests)
            await asyncio.gather(*[self.request_and_upsert(semaphore, product) for product in products])
        else:
            for product in products:
                await self.request(product)  # type: ignore[call-overload]

    # ---------------------------------------------------------
    # Batch inference
    # ---------------------------------------------------------
    @cached_property
    def batch_handler(self) -> BatchReasoningJob:
        if self.extractor is None or self.extractor.llm_config is None:
            raise RuntimeError("batch_handler requires a resolved llm_config")
        ps = self.extractor.output_field.split(".")
        if self.ns:
            ps.append(self.ns)
        self.job_name = "-".join(ps).replace("_", "-")
        if self.data_flag:
            self.job_name += f"-{self.data_flag.replace(' ', '')}"
        file_path = f"batch_reasoning/{'/'.join(ps)}"
        llm_config = self.extractor.llm_config
        return self.batch_handler_class(
            os_bucket=f"{self.project_name}",
            job_name=self.job_name,
            file_path=file_path,
            model_name=llm_config.model_name,
            model_version=llm_config.model_version,
        )

    @task(
        cache_policy=NO_SELF,
        retries=240,
        retry_delay_seconds=120,
    )
    async def check_batch_job(self, batch_job_id: str) -> bool:
        logger = get_run_logger()
        response = self.batch_handler.check_batch_job(batch_job_id)
        logger.debug(f"Check batch job: {response}")

        if response is None:
            return False

        if response.request_counts is None:
            return False

        progress = f" Progress: {response.request_counts.completed}/{response.request_counts.total}."

        if response.status == "Completed":
            logger.info("Batch job completed." + progress)
            return True
        elif response.status in ("Terminating", "Terminated", "Failed"):
            logger.info(f"Batch job {response.status.lower()}." + progress)
            return bool(response.request_counts.completed and response.request_counts.completed > 0)
        msg = f"Phase of batch job: {response.status}."
        if response.status in ("Running", "Completed"):
            msg += progress
        raise BatchJobNotCompleted(msg)

    async def construct_json(self, item: Item) -> str | None:
        try:
            content = await self.extractor.get_instruction(item)
            batch_config = self.extractor.llm_config.batch_config
            data = self.batch_handler.construct_json(item.url, content, batch_config)
            return json.dumps(data)
        except IgnoreRequest:
            return None

    @task(
        cache_policy=NO_SELF,
        retries=3,
        retry_delay_seconds=[60, 120, 180],
    )
    async def prepare_data(self) -> int:
        logger = get_run_logger()
        items = await self.check_data()  # type: ignore[call-overload]

        if not items:
            logger.info("No items found.")
            return 0

        logger.info(f"Found {len(items)} items.")

        file = Path(self.batch_handler.local_file_path)
        uploaded = 0
        file.parent.mkdir(parents=True, exist_ok=True)
        with file.open("w") as f:
            for item in items:
                js = await self.construct_json(item)
                if js:
                    print(js, file=f)
                    uploaded += 1

        self.batch_handler.upload_data()
        logger.info(f"Uploaded {uploaded} items.")
        return uploaded

    @task(cache_policy=NO_SELF)
    def load_results(self, results_file_name: str, errors_file_name: str) -> dict[str, Any]:
        logger = get_run_logger()
        results: dict[str, Any] = {}

        for file_name in [results_file_name, errors_file_name]:
            if not file_name:
                continue

            data_file = Path(file_name)
            count = 0
            with data_file.open() as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    result = json.loads(line)
                    item_id = result["custom_id"]
                    results[item_id] = result["response"]
                    count += 1
            logger.info(f"Loaded {count} records from data file: {file_name}.")

        return results

    async def batch_upsert(
        self,
        semaphore: asyncio.Semaphore,
        item: Item,
        result: Any,
    ) -> None:
        logger = get_run_logger()
        async with semaphore:
            try:
                await self.extractor.run(item, result=result)
            except BatchJobResponseError as e:
                if e.status_code in RETRIABLE_STATUS_CODES:
                    self.retriable_requests += 1
                logger.error(f"Item {item._slug} encountered error: {e}.")
            except BadResponseError:
                self.retriable_requests += 1
            except IgnoreRequest as e:
                logger.warning(f"Ignore item {item._slug} due to {e.args[0]}.")

    @task(cache_policy=NO_SELF)
    async def create_batch_job(self) -> str:
        logger = get_run_logger()
        batch_job_id = await self.batch_handler.create_batch_job()
        logger.info(f"Created batch job: {batch_job_id}.")
        return batch_job_id

    @task(cache_policy=NO_SELF, retries=3, retry_delay_seconds=[60, 120, 180])
    def get_result(self, batch_job_id: str) -> tuple[str, str]:
        logger = get_run_logger()
        results_file_name, errors_file_name = self.batch_handler.get_result(batch_job_id)
        logger.info(f"Got result: {batch_job_id}. Saved to '{results_file_name}'.")
        if errors_file_name:
            logger.info(f"Errors ocurred: {batch_job_id}. Check '{errors_file_name}'")
        return results_file_name, errors_file_name

    @task(cache_policy=NO_SELF)
    async def get_data(self, results_file_name: str, errors_file_name: str) -> list[tuple[Item, Any]]:
        results = self.load_results(results_file_name, errors_file_name)  # type: ignore[call-overload]
        products = await self.check_data()  # type: ignore[call-overload]
        return [(product, results.get(product.url)) for product in products]

    async def batch_inference(self) -> None:
        has_data = await self.prepare_data()  # type: ignore[call-overload]
        if not has_data:
            raise BatchJobFailed("No data to process in batch job.")

        if self.batch_job_id is None:
            batch_job_id = await self.create_batch_job()  # type: ignore[call-overload]
            ok = await self.check_batch_job(batch_job_id)  # type: ignore[call-overload]
            if not ok:
                raise BatchJobFailed(f"Batch job {batch_job_id} failed or completed with no results.")
        else:
            batch_job_id = self.batch_job_id
        results_file_name, errors_file_name = self.get_result(batch_job_id)  # type: ignore[call-overload]
        data = await self.get_data(results_file_name, errors_file_name)  # type: ignore[call-overload]

        semaphore = asyncio.Semaphore(30)
        await asyncio.gather(*[self.batch_upsert(semaphore, product, response) for product, response in data])


class OverallReasoningFlow(ReasoningFlow[BaseItem]):
    variant: ClassVar[str] = "Overall"
