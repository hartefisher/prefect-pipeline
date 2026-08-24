from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from dotenv import load_dotenv


async def serve(
    *,
    setup_hooks: list[Callable[[], Awaitable[None]]] | None = None,
    version_id: str | None = None,
) -> None:
    """框架统一服务入口：加载 env → 业务前置钩子 → 发现并部署全部 flow → aserve。

    业务 ``main`` 只需 ``serve(setup_hooks=[start_local_browser])``，无需自己写
    ``load_dotenv`` / ``FlowsLoader`` / ``aserve`` 的重复样板。``load_dotenv`` 与
    所有读取 env 的 configs 导入都在本函数内部、且在 dotenv 之后发生，避免 env
    被提前读走（configs 在模块导入期即读取 env）。
    """
    load_dotenv()

    import prefect
    from prefect.deployments.runner import RunnerDeployment

    from .core.configs import VERSION_ID
    from .core.loader import FlowsLoader

    for hook in setup_hooks or []:
        await hook()

    pool: list[RunnerDeployment] = []
    deployments = await FlowsLoader(version_id or VERSION_ID).load()
    for deployment in deployments:
        if isinstance(deployment, RunnerDeployment):
            pool.append(deployment)
        else:
            pool.append(await deployment)

    await prefect.aserve(*pool)


def run(
    setup_hooks: list[Callable[[], Awaitable[None]]] | None = None,
    version_id: str | None = None,
) -> None:
    """同步包装，供 console_script 入口调用（如 ``product-hunt-serve``）。"""
    asyncio.run(serve(setup_hooks=setup_hooks, version_id=version_id))
