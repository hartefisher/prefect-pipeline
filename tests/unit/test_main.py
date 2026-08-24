"""Smoke test for the framework entry point (M6.4).

``prefect_pipeline.main`` 是薄封装：``python -m prefect_pipeline.main`` 会调用
``run()``，进而委托给 ``serve.serve``（加载部署并 aserve）。装配逻辑由
``tests/unit/test_serve.py`` 覆盖，这里只验证入口的委托关系。
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch


def test_main_run_delegates_to_serve():
    import prefect_pipeline.main as main

    with patch("prefect_pipeline.serve.serve", new=AsyncMock()) as mock_serve:
        main.run()

    mock_serve.assert_awaited_once_with(setup_hooks=None, version_id=None)
