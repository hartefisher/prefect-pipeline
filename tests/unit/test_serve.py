"""Unit tests for ``serve`` (framework unified serve entrypoint)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from prefect.deployments.runner import RunnerDeployment

from prefect_pipeline.serve import run, serve


async def test_serve_loads_env_runs_hooks_and_aserves():
    fake_rd = MagicMock(spec=RunnerDeployment)
    coro_rd = MagicMock(spec=RunnerDeployment)

    async def make_coro():
        return coro_rd

    async def fake_load():
        # 第二个部署以协程形式提供，需被 coerce 成 RunnerDeployment
        return [fake_rd, make_coro()]

    hook_called = {"n": 0}

    async def fake_hook() -> None:
        hook_called["n"] += 1

    with patch("prefect_pipeline.serve.load_dotenv") as mock_dotenv, patch(
        "prefect.aserve", new=AsyncMock()
    ) as mock_aserve, patch(
        "prefect_pipeline.core.loader.FlowsLoader"
    ) as MockLoader:
        MockLoader.return_value.load = AsyncMock(side_effect=fake_load)
        await serve(setup_hooks=[fake_hook])

    mock_dotenv.assert_called_once()
    assert hook_called["n"] == 1
    # 协程型部署被 await 后并入同一池，最终 aserve 收到两个 RunnerDeployment
    mock_aserve.assert_awaited_once()
    pool = mock_aserve.call_args[0]
    assert len(pool) == 2
    assert pool[0] is fake_rd
    assert pool[1] is coro_rd


async def test_serve_passes_version_id_when_given():
    with patch("prefect_pipeline.serve.load_dotenv"), patch(
        "prefect.aserve", new=AsyncMock()
    ) as mock_aserve, patch(
        "prefect_pipeline.core.loader.FlowsLoader"
    ) as MockLoader:
        instance = MockLoader.return_value
        instance.load = AsyncMock(return_value=[])
        await serve(version_id="custom-vid")

    # 自定义 version_id 透传给 FlowsLoader，而非框架默认 VERSION_ID
    assert MockLoader.call_args.args[0] == "custom-vid"
    mock_aserve.assert_awaited_once()


def test_run_wraps_serve_in_asyncio():
    with patch("prefect_pipeline.serve.serve", new=AsyncMock()) as mock_serve:
        run()
    mock_serve.assert_awaited_once_with(setup_hooks=None, version_id=None)
