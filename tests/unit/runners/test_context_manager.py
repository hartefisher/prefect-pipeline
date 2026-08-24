"""Unit tests for ``DeploymentContextManager`` (runners/context)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from prefect_pipeline.runners.context import DeploymentContextManager, ManageDeploymentContext


class _ToyManager(DeploymentContextManager):
    project_name = "demo"

    def __init__(self) -> None:
        super().__init__(branch="main")
        self._fake_orch = MagicMock()

    def get_orchestration(self, branch, start, stop):
        return self._fake_orch


def _node(active: bool = True):
    return {"active": active, "peer_tails": [], "downstream": []}


async def test_get_context_builds_node_info():
    mgr = _ToyManager()
    mgr._fake_orch.node_map = {("m", "n"): _node()}
    mgr.deployment_map = {("m", "n"): {"qualname": "flow/deploy"}}

    ctx = mgr.get_context(("m", "n"))
    assert ctx == {"ns": "flow/deploy", "active": True, "peer_tails": [], "downstream": []}


async def test_get_context_skips_undeployed_ns():
    mgr = _ToyManager()
    mgr._fake_orch.node_map = {("m", "n"): _node()}
    mgr.deployment_map = {}  # 未部署

    assert mgr.get_context(("m", "n")) is None


async def test_get_context_returns_none_for_unknown_ns():
    mgr = _ToyManager()
    mgr._fake_orch.node_map = {}
    mgr.deployment_map = {}

    assert mgr.get_context(("x", "y")) is None


async def test_default_get_orchestration_resolves_orchestration_entry(monkeypatch):
    """默认实现按 ORCHESTRATION_ENTRY 契约注入业务 DAG。

    验证 start/stop 中的节点名被归一化为编排模块内的同名对象后透传。
    """
    import prefect_pipeline.runners.context as ctx_mod

    fake_orchestration = MagicMock(name="Orchestration")
    orch_module = MagicMock()
    orch_module.__dict__ = {
        "StartNode": "START_OBJ",
        "StopNode": "STOP_OBJ",
        "get_orchestration": lambda branch, start, stop: (
            fake_orchestration.__setattr__("call", (branch, start, stop)) or fake_orchestration
        ),
    }

    monkeypatch.setattr(ctx_mod, "ORCHESTRATION_ENTRY", "src.orchestrations:get_orchestration")
    monkeypatch.setattr(
        "prefect_pipeline.runners.context.importlib.import_module",
        lambda path: orch_module if path == "src.orchestrations" else __import__(path),
    )

    mgr = DeploymentContextManager(branch="dev")
    result = mgr.get_orchestration("dev", ["StartNode"], "StopNode")

    assert result is fake_orchestration
    assert fake_orchestration.call == ("dev", ["START_OBJ"], "STOP_OBJ")


async def test_run_writes_context_to_mongodb(monkeypatch):
    mgr = _ToyManager()
    mgr._fake_orch.node_map = {("m", "n"): _node()}
    mgr.deployment_map = {("m", "n"): {"qualname": "flow/deploy"}}

    # 跳过真实的 MongoDB 清单读取
    async def fake_load_manifest() -> None:
        pass

    monkeypatch.setattr(mgr, "load_manifest", fake_load_manifest)

    fake_collection = MagicMock()
    fake_collection.update_one = AsyncMock()
    fake_prefect = MagicMock()
    fake_prefect.deployments = fake_collection
    fake_prefect.close = MagicMock()
    with patch("prefect_pipeline.runners.context.get_prefect", return_value=fake_prefect):
        await mgr.run()

    fake_collection.update_one.assert_awaited_once()
    filter_arg, update_arg = fake_collection.update_one.call_args[0]
    assert filter_arg["ns"] == "flow/deploy"
    assert update_arg["$set"]["active"] is True
    assert update_arg["$set"]["peer_tails"] == []
    fake_prefect.close.assert_called_once()


def test_framework_provides_manage_deployment_context():
    """框架应开箱即用提供 ManageDeploymentContext，业务侧无需任何文件。"""
    assert ManageDeploymentContext is not None
    assert ManageDeploymentContext.name == "ManageDeploymentContext"
    assert ManageDeploymentContext.node.runner is DeploymentContextManager

