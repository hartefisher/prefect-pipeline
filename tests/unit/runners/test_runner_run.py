"""Runner run-path integration tests (M6.4 coverage).

These exercise each framework runner's ``run()`` orchestration end-to-end on
Prefect's ephemeral engine (no external server required), using the no-op toy
components from :mod:`tests.unit.runners.conftest`. They cover the runner
``run`` methods, their ``@task`` work units, and the base ``start`` / ``filter``
/ ``data_flag`` plumbing without touching Mongo, Qdrant or any LLM endpoint.

Each runner is executed inside a minimal ``@flow`` wrapper so the
``get_run_logger()`` calls inside the runners have an active flow context.
"""

from __future__ import annotations

import pytest
from prefect import flow

from prefect_pipeline import (
    AggregationFlow,
    EmbeddingFlow,
    ReasoningFlow,
    TransformationFlow,
    WebScrapingFlow,
)
from tests.unit.runners.conftest import (
    ToyDataFetcher,
    ToyDataTransformer,
    ToyEmbeddingHandler,
    ToyItem,
    ToyRealtimeExtractor,
    ToySpiderForRun,
)


@flow
async def _run_flow(flow_runner: object) -> object:
    """Provide an active flow context so runner logging works."""
    return await flow_runner.run()  # type: ignore[attr-defined]


class ToyItemFetcher(ToyDataFetcher):
    """A DataFetcher that returns a single toy item (no DB needed)."""

    async def get_items(self, **kwargs: object) -> list[ToyItem]:  # type: ignore[override]
        return [ToyItem(url="https://example.com/1", title="t", score=1)]

    async def get_batch_items(self, **kwargs: object):  # type: ignore[override]
        if False:  # pragma: no cover - async generator that yields one batch
            yield []
        yield [ToyItem(url="https://example.com/1", title="t", score=1)]


# --------------------------------------------------------------------------- #
# Transformation / Aggregation run paths
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_transformation_run_returns_count():
    flow_runner = TransformationFlow()
    await flow_runner.setup(ToyDataTransformer)
    result = await _run_flow(flow_runner)
    assert result == 0  # toy transformer.run returns 0


@pytest.mark.asyncio
async def test_aggregation_run_completes():
    flow_runner = AggregationFlow()
    await flow_runner.setup(ToyDataTransformer)
    # AggregationFlow.run returns None; assert it runs without error.
    result = await _run_flow(flow_runner)
    assert result is None


# --------------------------------------------------------------------------- #
# Embedding run path (batch items from a toy fetcher)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_embedding_run_counts_items():
    flow_runner = EmbeddingFlow()
    await flow_runner.setup(ToyEmbeddingHandler, ToyItemFetcher)
    await _run_flow(flow_runner)
    # one batch of one item -> handler.run returns 1
    assert flow_runner.embedded_count == 1


# --------------------------------------------------------------------------- #
# Web scraping run path
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_scraping_run_requests_each_item():
    flow_runner = WebScrapingFlow()
    await flow_runner.setup(ToySpiderForRun, ToyItemFetcher)
    await _run_flow(flow_runner)  # spider.start + check_data + request per item
    # The toy spider.run is a no-op, so this mainly asserts orchestration runs.


# --------------------------------------------------------------------------- #
# Reasoning realtime inference path
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_reasoning_realtime_inference_over_items():
    flow_runner = ReasoningFlow()
    await flow_runner.setup(
        ToyRealtimeExtractor,
        ToyItemFetcher,
        llm_config={"model": "toy", "batch": False},
    )
    # request_and_upsert -> request -> extractor.run(item) returns a dict
    await _run_flow(flow_runner)
    assert flow_runner.retriable_requests == 0


@pytest.mark.asyncio
async def test_reasoning_check_retriable_requests_no_retry():
    flow_runner = ReasoningFlow()
    await flow_runner.setup(
        ToyRealtimeExtractor,
        ToyItemFetcher,
        llm_config={"model": "toy", "batch": False},
    )
    flow_runner.retriable_requests = 0
    flow_runner.retry_times = 0
    flow_runner.previous_retriable_requests = 0
    # Zero retriable requests -> must not raise RetryReasoning
    flow_runner.check_retriable_requests()
