from __future__ import annotations

import importlib
import time
from collections import defaultdict
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, TypedDict
from uuid import UUID

from prefect import Flow, flow
from prefect.deployments.runner import RunnerDeployment
from prefect.runtime import deployment, flow_run

from ..infra.db import get_prefect
from .configs import FLOWS_DIRECTORY, WORKFLOW_POOL
from .deployment import Deployment
from .ns_converter import (
    entrypoint_2_ns,
    get_deployment_instance,
    ns_2_entrypoint,
    path_2_flow_name,
)

type PrefectDeploymentList = list[RunnerDeployment | Coroutine[Any, Any, RunnerDeployment]]


async def entry_fn(*args: Any, **kwargs: Any) -> None:
    flow_name: str | None = flow_run.get_flow_name()
    deployment_name: str | None = deployment.get_name()
    if flow_name and deployment_name:
        _deployment: Deployment = get_deployment_instance(flow_name, deployment_name)
        await _deployment.run(*args, **kwargs)


class EntryPoints:
    """Lazy proxy that builds Prefect ``Flow`` objects on first access."""

    def __init__(self) -> None:
        self.flows: dict[str, Flow[Any, Any]] = {}

    def create_flow(self, entrypoint: str, fn: Callable[..., Any], inject_qualname: bool = False) -> Flow[Any, Any]:
        flow_name, deployment_name = entrypoint_2_ns(entrypoint)
        _deployment: Deployment = get_deployment_instance(flow_name, deployment_name)
        if _deployment.node is None:
            raise ValueError(f"Deployment '{deployment_name}' not found in flow '{flow_name}'.")
        new_signature: Any = _deployment.node.copy_signature(fn)
        fn.__signature__ = new_signature  # type: ignore[attr-defined]
        if inject_qualname:
            fn.__qualname__ = f"entry_points.{entrypoint}"
        flow_parameters: dict[str, Any] = _deployment.node.set_flow_parameters()

        return flow(name=flow_name, **flow_parameters)(fn)

    def __getattr__(self, entrypoint: str) -> Flow[Any, Any]:
        if entrypoint not in self.flows:
            self.flows[entrypoint] = self.create_flow(entrypoint, entry_fn)

        return self.flows[entrypoint]


entry_points: EntryPoints = EntryPoints()


class DeploymentDetail(TypedDict):
    instance: Deployment
    name: str
    qualname: str
    flow_name: str


class FlowsLoader:
    """Discovers ``Deployment`` objects across the flows directory, renders
    a deployment map, creates Prefect deployments, and persists the manifest
    to MongoDB for runtime introspection.
    """

    def __init__(self, version_id: str | UUID, fp: str | None = None) -> None:
        self.version_id: str | UUID = version_id
        self.root_dir: Path = Path(FLOWS_DIRECTORY)
        self.flows_dir: Path = self.root_dir / (fp or "")
        self.flows: defaultdict[str, list[Deployment]] = defaultdict(list)
        self.prefect_deployments: PrefectDeploymentList = []
        self.deployment_map: dict[tuple[str, ...], DeploymentDetail] = {}

    def iter_files(self, fp: Path) -> None:
        for f in fp.rglob("__init__.py"):
            if f.stat().st_size == 0:
                continue

            t1: float = time.perf_counter()
            ns: str = f.parent.as_posix()
            module_name: str = ns.replace("/", ".")
            module: Any = importlib.import_module(module_name)
            for k, v in module.__dict__.items():
                if k.startswith("__"):
                    continue

                if isinstance(v, Deployment) and v.node:
                    v.name = k
                    v.node._name = k
                    self.flows[module_name].append(v)

            t2: float = time.perf_counter()
            print(f"load {f.as_posix()}:", round(t2 - t1, 2), "s")

    async def write_deployment_manifest(self) -> None:
        """把部署清单 upsert 到 MongoDB，替代写 generated/deployments.py。

        清单 = {ns → {name, flow_name, qualname}}，供业务侧 DeploymentContextManager
        读取「实际已部署的节点」并据此写 DAG 上下文。ns 为 tuple，MongoDB 存 list。
        """
        PREFECT = get_prefect()
        try:
            collection = PREFECT.deployment_manifest
            version_id = str(self.version_id)
            for ns, detail in self.deployment_map.items():
                await collection.update_one(
                    {"ns": list(ns), "version_id": version_id},
                    {
                        "$set": {
                            "ns": list(ns),
                            "name": detail["name"],
                            "flow_name": detail["flow_name"],
                            "qualname": detail["qualname"],
                            "version_id": version_id,
                        }
                    },
                    upsert=True,
                )
        finally:
            PREFECT.close()

    def render_deployment_map(self) -> None:
        for flow_path, deployments in self.flows.items():
            for deployment_ in deployments:
                if deployment_.node is None or deployment_.name is None:
                    continue

                flow_name: str = path_2_flow_name(flow_path, deployment_.node.runner.project_name)
                deployment_name: str = deployment_.get_deployment_name()
                deployment_qualname: str = f"{flow_name}/{deployment_name}"
                self.deployment_map[deployment_.node.ns] = {
                    "instance": deployment_,
                    "flow_name": flow_name,
                    "name": deployment_.name,
                    "qualname": deployment_qualname,
                }

    def _deploy(self, deployment: Deployment, entrypoint: str) -> RunnerDeployment:
        async def fn(*args: Any, **kwargs: Any) -> None:
            pass

        my_flow: Flow[Any, Any] = entry_points.create_flow(entrypoint, fn, inject_qualname=True)
        deployment_parameters: dict[str, Any] = deployment.set_deployment_parameters()
        return my_flow.to_deployment(**deployment_parameters)  # type: ignore[return-value]

    def deploy(self) -> None:
        for _ns, detail in self.deployment_map.items():
            if detail["instance"].workflow_pool != WORKFLOW_POOL:
                continue

            entrypoint: str = ns_2_entrypoint(detail["flow_name"], detail["name"])
            prefect_deployment: RunnerDeployment = self._deploy(detail["instance"], entrypoint)
            self.prefect_deployments.append(prefect_deployment)

    async def load(self) -> PrefectDeploymentList:
        t1: float = time.perf_counter()

        self.iter_files(self.flows_dir)
        self.render_deployment_map()
        self.deploy()
        await self.write_deployment_manifest()

        t2: float = time.perf_counter()
        print("load flows:", round(t2 - t1, 2), "s")

        return self.prefect_deployments
