from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Self, TypedDict

from prefect.client.types.flexible_schedule_list import FlexibleScheduleList

from .condition import Condition
from .configs import IS_TEST, ROOT_PATH
from .ns_converter import gen_deployment_name
from .runner_base import FlowRunnerBase


class Node:
    def __init__(
        self,
        runner: type[FlowRunnerBase],
        *injectors: Callable[..., Any],
        upstream_condition: Condition | None = None,
        name: str | None = None,
        injector_ns: str | None = None,
    ):
        self.runner = runner
        self.injectors = injectors
        self.upstream_condition = upstream_condition or Condition()
        self._name = name
        self._injector_ns = injector_ns

    async def run(self, *args: Any, **kwargs: Any) -> Any:
        runner = self.runner(*args, **kwargs)
        extra = getattr(runner, "_extra", {})
        await runner.setup(*self.injectors, **extra)
        await runner.start()

    @property
    def injector_ns(self) -> str:
        injector_ns = (
            f"{self.injectors[0].__module__.replace(ROOT_PATH, '')[1:]}.{self.injectors[0].__name__}"
            if self.injectors
            else "None"
        )
        if self._injector_ns:
            injector_ns += f" / {self._injector_ns}"
        return injector_ns

    @property
    def ns(self) -> tuple[str, str]:
        runner_ns = f"{self.runner.__module__}.{self.runner.__name__}"
        return (runner_ns, self.injector_ns)

    @property
    def name(self) -> str:
        runner_ns = self.runner.__name__
        return self._name or f"Node({runner_ns}, {self.injector_ns})"

    @property
    def parameters(self) -> dict[str, Any]:
        return self.runner.extract_parameters()

    def copy_signature(self, fn: Callable[..., Any]) -> inspect.Signature:
        return self.runner.copy_signature(fn)

    def set_flow_parameters(self) -> Any:
        return self.runner.set_flow_parameters()

    def __repr__(self) -> str:
        return self.name


class NodeDeployment(TypedDict):
    node: Node
    peer_tails: set[Node]
    downstream: set[Node]
    active: bool


class Deployment:
    def __init__(
        self,
        node: Node | None = None,
        name: str | None = None,
        schedules: FlexibleScheduleList | None = None,
        tag: list[str] | str | None = None,
        flag: str | None = None,
        workflow_pool: str | None = None,
        **parameters: Any,
    ):
        self.node = node
        self.name = name
        self.children: list[Self] = []
        self.peers: list[Self] = []
        self.schedules = schedules
        self.tag = tag
        self.flag = flag
        self.workflow_pool = workflow_pool
        self.parameters = parameters

    async def run(self, *args: Any, **kwargs: Any) -> None:
        if self.node:
            await self.node.run(*args, **kwargs)

    def get_deployment_name(self) -> str:
        if self.name is None:
            raise ValueError(
                "Deployment name must be set before setting deployment parameters."
            )

        flags = ["Test"] if IS_TEST else []
        if self.node and self.node.runner:
            if self.node.runner.flag:
                flags.append(self.node.runner.flag)

        if self.flag:
            flags.append(self.flag)

        return gen_deployment_name(self.name, flags)

    def set_deployment_parameters(self, **extra: Any) -> dict[str, Any]:
        tags = []
        parameters = {**self.parameters}
        if self.node and self.node.runner:
            tags.extend(self.node.runner.tags)

            if "extra" in self.node.parameters:
                parameters["extra"] = extra
            if self.node.runner.variant:
                tags.append(self.node.runner.variant)

        if isinstance(self.tag, str):
            tags.append(self.tag)
        elif isinstance(self.tag, list):
            tags.extend(self.tag)

        return {
            "name": self.get_deployment_name(),
            "tags": tags,
            "parameters": parameters,
            "schedules": self.schedules,
        }

    @classmethod
    def create(
        cls,
        node: Node | None = None,
    ) -> Self:
        return cls(node)

    def add_peer(self, peer: Self) -> Self:
        if peer not in self.peers:
            self.peers.append(peer)
        return self

    def add_peers(self, peers: Self | list[Self]) -> Self:
        if isinstance(peers, list):
            for peer in peers:
                self.add_peer(peer)
        else:
            self.add_peer(peers)
        return self

    def add_child(self, child: Self) -> Self:
        self.children.append(child)
        return self

    def add_children(self, children: Self | list[Self]) -> Self:
        if isinstance(children, list):
            for child in children:
                self.add_child(child)
        else:
            self.add_child(children)
        return self

    def __add__(self, right_side: Self) -> Deployment:
        # 将右侧挂载为当前结构的并行 peer
        deployment = self.create()

        # 如果左侧是一个纯净的（没有挂载下游 children），则将其扁平化展开
        if self.node is None and not self.children:
            deployment.add_peers(self.peers)
        else:
            deployment.add_peer(self)

        # 同样处理右侧
        if right_side.node is None and not right_side.children:
            deployment.add_peers(right_side.peers)
        else:
            deployment.add_peer(right_side)

        return deployment

    def __or__(self, right_side: Self) -> Self:
        # 预留给失败备选
        return self

    def get_tails(self) -> list[Self]:
        # 如果当前节点/组没有下游节点，那它自己就是链条的尾巴
        if not self.children:
            return [self]

        tails = []
        for child in self.children:
            tails.extend(child.get_tails())
        return tails

    def get_terminal_nodes(self) -> set[Node]:
        """递归获取一个部署子图的【真实尾部节点集合】"""
        # 1. 如果有串行子代 (children)，尾部必然是最后一个/最后一批子代的尾部
        if self.children:
            tails: set[Node] = set()
            for child in self.children:
                tails.update(child.get_terminal_nodes())
            return tails

        # 2. 如果没有子代，检查自身是不是实体节点
        result: set[Node] = set()
        if self.node is not None:
            result.add(self.node)
        # 3. 如果自身是虚拟并行组，尾部是所有同行节点 (peers) 的尾部
        elif self.peers:
            for peer in self.peers:
                result.update(peer.get_terminal_nodes())
        return result

    def get_head_nodes(self) -> set[Node]:
        """递归获取一个部署子图的【真实头部节点集合】"""
        heads: set[Node] = set()
        # 头部只看当前节点或者并行分支，不看 children（因为 children 是下游）
        if self.node is not None:
            heads.add(self.node)
        elif self.peers:
            for peer in self.peers:
                heads.update(peer.get_head_nodes())
        return heads

    def parse_topology(self) -> tuple[set[Node], set[Node]]:
        """提取当前 deployment 尾部节点和直系下游头部节点，用于组装 node_map"""
        peer_tails: set[Node] = set()
        downstream: set[Node] = set()

        # 计算当前层级的尾部 (Tail)
        if self.node is not None:
            # 如果是具体节点，自身的尾部就是自己
            peer_tails.add(self.node)
        elif self.peers:
            # 如果是并行包裹器，遍历其所有 peer，递归寻找它们的真实最末端
            for peer in self.peers:
                peer_tails.update(peer.get_terminal_nodes())

        # 计算直系下游的头部 (Head)
        for child in self.children:
            # 递归寻找子代的真实头部节点
            downstream.update(child.get_head_nodes())

        return peer_tails, downstream

    def __repr__(self) -> str:
        if self.node:
            return f"{self.node}"
        elif len(self.peers) == 1:
            return f"{self.peers[0]}"
        else:
            return f"({' + '.join([f'{peer}' for peer in self.peers])})"

    def __rshift__(self, right_side: Self | list[Self]) -> Self:
        # 将 right_side 挂载到当前结构所有尾部节点的 children 中
        for tail in self.get_tails():
            tail.add_children(right_side)
        return self

    def __set_name__(self, owner: Any, name: str) -> None:
        print(f"Setting name: {name} for owner: {owner}")
        self.name = name
        if self.node:
            self.node._name = name
