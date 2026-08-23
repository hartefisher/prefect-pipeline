"""Public API export tests (M5.3, DESIGN §5)."""
from __future__ import annotations

import importlib

import prefect_pipeline


def test_all_names_importable():
    """Every name in prefect_pipeline.__all__ must actually resolve."""
    missing = []
    for name in prefect_pipeline.__all__:
        if not hasattr(prefect_pipeline, name):
            missing.append(name)
    assert missing == [], f"Missing from package: {missing}"


def test_no_circular_import_on_fresh_interpreter():
    # Re-import in a clean submodule namespace to catch circular-import errors.
    mod = importlib.import_module("prefect_pipeline")
    assert hasattr(mod, "PipelineFlow")
    assert hasattr(mod, "ReasoningFlow")
    assert hasattr(mod, "Deployment")


def test_runners_subpackage_exports_all_flows():
    from prefect_pipeline.runners import (
        AggregationFlow,
        EmbeddingFlow,
        OverallAggregationFlow,
        OverallEmbeddingFlow,
        OverallReasoningFlow,
        OverallTransformationFlow,
        OverallWebScrapingFlow,
        PipelineFlow,
        ReasoningFlow,
        Retry,
        TransformationFlow,
        WebScrapingFlow,
    )

    assert all(
        c is not None
        for c in (
            PipelineFlow,
            TransformationFlow,
            ReasoningFlow,
            EmbeddingFlow,
            AggregationFlow,
            WebScrapingFlow,
            Retry,
            OverallTransformationFlow,
            OverallReasoningFlow,
            OverallEmbeddingFlow,
            OverallAggregationFlow,
            OverallWebScrapingFlow,
        )
    )
