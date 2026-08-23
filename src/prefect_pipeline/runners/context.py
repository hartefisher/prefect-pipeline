from __future__ import annotations

from functools import cached_property
from typing import ClassVar

from ..core.configs import ENVIRONMENT, TIMEZONE, VERSION_ID
from ..core.orchestration import Orchestration
from ..core.runner_base import FlowRunnerBase
from ..infra.db import get_prefect
from ..infra.types import DeploymentContextDict, NodeInfoDict
from ..infra.utils import get_current_time


class DeploymentContextManager(FlowRunnerBase):
    """通用部署上下文管理器 flow。

    业务侧继承并覆盖：
    - ``project_name``：命名空间（如 ``"ph"``）
    - ``get_orchestration(branch, start, stop)``：注入业务 DAG，返回 ``Orchestration``

    框架承载通用逻辑：从 MongoDB 读部署清单（``deployment_manifest``）、计算
    各节点的 DAG 上下文（active/peer_tails/downstream）、写回 ``deployments``
    collection 供运行时 trigger 查询。
    """

    project_name: ClassVar[str] = "default"

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
        """业务侧覆盖此方法，注入业务 DAG 并返回 :class:`Orchestration`。"""
        raise NotImplementedError("业务子类需覆盖 get_orchestration 注入 DAG")

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
