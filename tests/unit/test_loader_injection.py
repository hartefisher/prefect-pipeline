"""Tests for ``FlowsLoader`` auto-injection of framework deployments (L3).

The deployment context manager should be injected into the deployment pool
automatically, with no business-side file required, and must not be registered
twice if a business project still ships its own ``ManageDeploymentContext``.
"""

from __future__ import annotations

import pytest

from prefect_pipeline.core.loader import WORKFLOW_POOL, FlowsLoader
from prefect_pipeline.runners.context import ManageDeploymentContext


@pytest.fixture
def loader():
    return FlowsLoader("test-version")


def test_injects_manage_deployment_context(loader):
    loader._inject_framework_deployments()

    injected = [
        d
        for ds in loader.flows.values()
        for d in ds
        if d.name == "ManageDeploymentContext"
    ]
    assert injected, "ManageDeploymentContext should be injected"
    assert injected[0] is ManageDeploymentContext
    # 框架管理 flow 始终随业务一同部署：其 workflow_pool 对齐 WORKFLOW_POOL
    assert injected[0].workflow_pool == WORKFLOW_POOL


def test_no_duplicate_when_business_defines_its_own(loader):
    # 模拟业务旧文件已定义同名部署
    loader.flows["src.flows.context_manager"] = [ManageDeploymentContext]

    loader._inject_framework_deployments()

    total = sum(
        1
        for ds in loader.flows.values()
        for d in ds
        if d.name == "ManageDeploymentContext"
    )
    assert total == 1, "不应重复注册 ManageDeploymentContext"
