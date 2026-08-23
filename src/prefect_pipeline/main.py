from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import prefect  # noqa: E402
from prefect.deployments.runner import RunnerDeployment  # noqa: E402

from .core.configs import VERSION_ID  # noqa: E402
from .core.loader import FlowsLoader  # noqa: E402


async def main() -> None:
    """Framework entry point: load all deployments and serve them via Prefect."""
    pool: list[RunnerDeployment] = []
    deployments = await FlowsLoader(VERSION_ID).load()
    for deployment in deployments:
        if not isinstance(deployment, RunnerDeployment):
            deployment = await deployment
        pool.append(deployment)

    await prefect.aserve(*pool)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
