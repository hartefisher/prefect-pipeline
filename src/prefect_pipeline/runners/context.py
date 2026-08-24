from __future__ import annotations

import importlib
from collections.abc import Callable
from functools import cached_property
from typing import Any, ClassVar

from ..core.configs import (
    ENVIRONMENT,
    ORCHESTRATION_ENTRY,
    PROJECT_NAME,
    TIMEZONE,
    VERSION_ID,
)
from ..core.orchestration import Orchestration
from ..core.runner_base import FlowRunnerBase
from ..infra.db import get_prefect
from ..infra.types import DeploymentContextDict, NodeInfoDict
from ..infra.utils import get_current_time


class DeploymentContextManager(FlowRunnerBase):
    """通用部署上下文管理器 flow（开箱即用，业务侧无需子类化）。

    默认实现按框架契约注入业务 DAG：

    - ``ORCHESTRATION_ENTRY``（默认 ``src.orchestrations:get_orchestration``）
      指向业务项目的 DAG 入口函数，签名为
      ``get_orchestration(branch, start, stop) -> Orchestration``；
    - ``start`` / ``stop`` 既可以是节点名字符串/列表，也可以直接是
      ``Deployment`` 对象，框架统一归一化后透传给该入口；
    - ``project_name`` 来自框架级配置 ``PROJECT_NAME``（env 驱动），
      不再需要业务 runner 硬编码。

    非标布局的业务项目只需覆写 :meth:`get_orchestration`，或通过
    ``ORCHESTRATION_ENTRY`` 环境变量重定向 DAG 入口。

    框架承载通用逻辑：从 MongoDB 读部署清单（``deployment_manifest``）、计算
    各节点的 DAG 上下文（active/peer_tails/downstream）、写回 ``deployments``
    collection 供运行时 trigger 查询。
    """

    project_name: ClassVar[str] = PROJECT_NAME

    def __init__(
        self,
        *,
        branch: str = "main",
        start: list[str] | str | None = None,
        stop: list[str] | str | None = None,
    ) -> None:
        self.deployment_map: dict[tuple[str, ...], dict[str, str]] = {}
        self.branch = branch
        self.start_node = start
        self.stop_node = stop

    async def load_manifest(self) -> None:
        """从 MongoDB ``deployment_manifest`` 读当前版本的部署清单。"""
        pref = get_prefect()
        records = await pref.deployment_manifest.find(
            {"version_id": VERSION_ID}, {"_id": 0}
        ).to_list(None)
        pref.close()
        self.deployment_map = {
            tuple(record["ns"]): {
                "name": record["name"],
                "flow_name": record["flow_name"],
                "qualname": record["qualname"],
            }
            for record in records
        }

    def get_orchestration(
        self,
        branch: str,
        start: list[str] | str | None,
        stop: list[str] | str | None,
    ) -> Orchestration:
        """按 ``ORCHESTRATION_ENTRY`` 契约注入业务 DAG。

        默认实现解析 ``ORCHESTRATION_ENTRY``（``<module>:<attr>``），将
        ``start`` / ``stop`` 中的节点名归一化为模块内同名对象后调用入口函数。
        """
        from typing import cast

        from ..core.orchestration import Orchestration as _Orchestration

        module_path, attr = ORCHESTRATION_ENTRY.split(":")
        orch_module = importlib.import_module(module_path)
        get_orchestration = cast("Callable[..., _Orchestration]", getattr(orch_module, attr))

        def resolve(name: list[str] | str | None) -> Any:
            if not name:
                return None
            if isinstance(name, list):
                return [orch_module.__dict__[n] for n in name if n in orch_module.__dict__]
            return orch_module.__dict__.get(name)

        return get_orchestration(branch, start=resolve(start), stop=resolve(stop))

    @cached_property
    def orch(self) -> Orchestration:
        return self.get_orchestration(self.branch, self.start_node, self.stop_node)

    def get_context(self, ns: tuple[str, ...]) -> DeploymentContextDict | None:
        def get_node_info(ns: tuple[str, ...]) -> NodeInfoDict | None:
            if (node := self.orch.node_map.get(ns)) is None:
                return None
            if ns not in self.deployment_map:
                return None
            return {
                "ns": self.deployment_map[ns]["qualname"],
                "active": node["active"],
            }

        if (node := self.orch.node_map.get(ns)) is None:
            return None

        if node_info := get_node_info(ns):
            peer_tails: list[NodeInfoDict] = []
            for peer_tail in node["peer_tails"]:
                if node_info_ := get_node_info(peer_tail.ns):
                    peer_tails.append(node_info_)

            downstream: list[NodeInfoDict] = []
            for ds in node["downstream"]:
                if node_info_ := get_node_info(ds.ns):
                    downstream.append(node_info_)

            return {**node_info, "peer_tails": peer_tails, "downstream": downstream}
        return None

    async def run(self) -> None:
        await self.load_manifest()

        pref = get_prefect()
        try:
            for ns in self.deployment_map:
                if ctx := self.get_context(ns):
                    await pref.deployments.update_one(
                        {"ns": ctx["ns"], "version_id": VERSION_ID},
                        {
                            "$set": {
                                "active": ctx["active"],
                                "peer_tails": ctx["peer_tails"],
                                "downstream": ctx["downstream"],
                                "environment": ENVIRONMENT,
                                "updated_at": get_current_time(TIMEZONE),
                            }
                        },
                        upsert=True,
                    )
        finally:
            pref.close()

    async def clear(self) -> None:
        pass


# 框架提供的管理 flow，FlowsLoader 会自动注入部署池；业务项目无需任何文件。
ManageDeploymentContext = DeploymentContextManager.deploy()
assert ManageDeploymentContext.node is not None
ManageDeploymentContext.name = "ManageDeploymentContext"
ManageDeploymentContext.node._name = "ManageDeploymentContext"
