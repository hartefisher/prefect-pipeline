"""Smoke test for the framework entry point (M6.4).

Exercises ``main.main()`` without standing up a real Prefect server or loading
real deployments: ``FlowsLoader.load`` is replaced with a stub and
``prefect.aserve`` is mocked, so the entry-point assembly logic is covered.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_main_assembles_and_serves_empty_pool():
    import prefect

    from prefect_pipeline import main

    fake_deployments = []

    with (
        patch.object(main.FlowsLoader, "load", return_value=fake_deployments),
        patch.object(prefect, "aserve", new=AsyncMock()) as mock_aserve,
    ):
        await main.main()

    mock_aserve.assert_awaited_once_with(*fake_deployments)


@pytest.mark.asyncio
async def test_main_passes_deployments_to_aserve():
    import prefect

    from prefect_pipeline import main
    from prefect_pipeline.main import RunnerDeployment

    fake_deployments = [RunnerDeployment(name="d1"), RunnerDeployment(name="d2")]

    with (
        patch.object(main.FlowsLoader, "load", return_value=fake_deployments),
        patch.object(prefect, "aserve", new=AsyncMock()) as mock_aserve,
    ):
        await main.main()

    # aserve receives the deployments as positional args (already RunnerDeployment)
    mock_aserve.assert_awaited_once_with(*fake_deployments)
