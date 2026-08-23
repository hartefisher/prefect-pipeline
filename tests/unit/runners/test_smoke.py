"""Smoke test: toy Flow deploys and its deployment parameters assemble (M5.3)."""
from __future__ import annotations

import pytest
from tests.unit.runners.conftest import ToyDataTransformer

from prefect_pipeline import Deployment, TransformationFlow
from prefect_pipeline.core.deployment import Node


def test_toy_flow_deploy_and_parameters():
    dep = TransformationFlow.deploy(ToyDataTransformer, name="toy_smoke")
    assert isinstance(dep, Deployment)

    params = dep.set_deployment_parameters()
    assert params["name"]
    assert "toy_smoke" in params["name"]
    # variant tag is attached for Overall variants; plain flow has no variant tag
    assert isinstance(params["parameters"], dict)
    assert params["schedules"] is None


def test_toy_flow_node_builds_runner_instance():
    """Node.runner produces a PipelineFlow instance bound to the injectors."""
    node = Node(TransformationFlow, ToyDataTransformer, name="toy_node")
    flow = node.runner()
    assert isinstance(flow, TransformationFlow)


@pytest.mark.asyncio
async def test_toy_flow_setup_and_clear_idempotent():
    flow = TransformationFlow()
    await flow.setup(ToyDataTransformer)
    assert flow.transformer is not None
    # clear must be safe to call
    await flow.clear()
