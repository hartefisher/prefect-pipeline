"""Runner registration, Overall-variant, and dependency-injection tests (M5.3)."""
from __future__ import annotations

import pytest
from tests.unit.runners.conftest import (
    ToyDataFetcher,
    ToyDataTransformer,
    ToyEmbeddingHandler,
    ToyExtractor,
    ToySpider,
)

from prefect_pipeline import (
    AggregationFlow,
    EmbeddingFlow,
    PipelineFlow,
    ReasoningFlow,
    TransformationFlow,
    WebScrapingFlow,
)
from prefect_pipeline.core.deployment import Deployment


# --------------------------------------------------------------------------- #
# PipelineFlow base behavior
# --------------------------------------------------------------------------- #
def test_pipeline_flow_default_project_name():
    assert PipelineFlow.project_name == "default"


def test_pipeline_flow_time_window_default():
    flow = PipelineFlow()
    assert flow.start_date == flow.end_date  # single day by default


def test_pipeline_flow_filter_non_overall():
    flow = PipelineFlow()
    assert "dt" in flow.filter


def test_overall_variant_skips_time_window():
    class MyFlow(PipelineFlow):
        variant = "Overall"

    flow = MyFlow()
    assert flow.filter == {}
    assert flow.data_flag is None


def test_overall_variant_excludes_parameters():
    class MyFlow(PipelineFlow):
        variant = "Overall"

    params = MyFlow.extract_parameters()
    for excluded in ("dt", "days", "offset", "backfill", "fill_direction"):
        assert excluded not in params


# --------------------------------------------------------------------------- #
# deploy() produces a framework Deployment bound to the runner
# --------------------------------------------------------------------------- #
def test_transformation_deploy_returns_deployment():
    dep = TransformationFlow.deploy(ToyDataTransformer, name="toy_transform")
    assert isinstance(dep, Deployment)
    assert dep.node is not None
    assert dep.node.runner is TransformationFlow


def test_reasoning_deploy_returns_deployment():
    dep = ReasoningFlow.deploy(ToyExtractor, ToyDataFetcher, name="toy_reasoning")
    assert isinstance(dep, Deployment)
    assert dep.node.runner is ReasoningFlow


def test_embedding_deploy_returns_deployment():
    dep = EmbeddingFlow.deploy(ToyEmbeddingHandler, ToyDataFetcher, name="toy_embed")
    assert isinstance(dep, Deployment)
    assert dep.node.runner is EmbeddingFlow


def test_aggregation_deploy_returns_deployment():
    dep = AggregationFlow.deploy(ToyDataTransformer, name="toy_aggregate")
    assert isinstance(dep, Deployment)
    assert dep.node.runner is AggregationFlow


def test_scraping_deploy_returns_deployment():
    dep = WebScrapingFlow.deploy(ToySpider, ToyDataFetcher, name="toy_scrape")
    assert isinstance(dep, Deployment)
    assert dep.node.runner is WebScrapingFlow


# --------------------------------------------------------------------------- #
# Overall variants still deploy
# --------------------------------------------------------------------------- #
def test_overall_transformation_deploys():
    from prefect_pipeline import OverallTransformationFlow

    dep = OverallTransformationFlow.deploy(ToyDataTransformer, name="toy_overall_t")
    assert isinstance(dep, Deployment)


# --------------------------------------------------------------------------- #
# Dependency injection: setup() instantiates injected component classes
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_transformation_injects_transformer():
    flow = TransformationFlow()
    await flow.setup(ToyDataTransformer)
    assert isinstance(flow.transformer, ToyDataTransformer)
    await flow.clear()


@pytest.mark.asyncio
async def test_embedding_injects_handler_and_fetcher():
    flow = EmbeddingFlow()
    await flow.setup(ToyEmbeddingHandler, ToyDataFetcher)
    assert isinstance(flow.handler, ToyEmbeddingHandler)
    assert isinstance(flow.data_fetcher, ToyDataFetcher)
    await flow.clear()


@pytest.mark.asyncio
async def test_reasoning_injects_extractor_and_fetcher():
    flow = ReasoningFlow()
    await flow.setup(
        ToyExtractor,
        ToyDataFetcher,
        llm_config={"model": "toy", "batch": False},
    )
    assert isinstance(flow.extractor, ToyExtractor)
    assert isinstance(flow.data_fetcher, ToyDataFetcher)
    await flow.clear()


@pytest.mark.asyncio
async def test_scraping_injects_spider_and_fetcher():
    flow = WebScrapingFlow()
    await flow.setup(ToySpider, ToyDataFetcher)
    assert isinstance(flow.spider, ToySpider)
    assert isinstance(flow.data_fetcher, ToyDataFetcher)
    await flow.clear()
