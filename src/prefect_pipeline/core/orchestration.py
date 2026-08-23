from __future__ import annotations

from .deployment import Deployment, NodeDeployment


class Orchestration:
    """DAG analysis engine for deployment orchestration graphs.

    A single traversal of the orchestration graph produces:
    1. A precise ``node_map`` containing upstream/downstream relationships
       and the true ``active`` state of each node.
    2. A formatted, annotated orchestration string for visualization.
    """

    def __init__(
        self,
        orchestration: Deployment,
        start: list[Deployment] | Deployment | None = None,
        stop: list[Deployment] | Deployment | None = None,
    ) -> None:
        self.orchestration: Deployment = orchestration
        self.deployments: dict[str, Deployment] = {}
        self.start: list[Deployment] = start if isinstance(start, list) else ([start] if start else [])
        self.stop: list[Deployment] = stop if isinstance(stop, list) else ([stop] if stop else [])
        self.initial_active: bool = False if self.start else True
        self.node_map: dict[tuple[str, ...], NodeDeployment] = {}
        self.graph: str = self.analyze()

    def get_node_info(self, name: str) -> NodeDeployment | None:
        if name in self.deployments:
            if node := self.deployments[name].node:
                if node.ns in self.node_map:
                    return self.node_map[node.ns]
        return None

    @staticmethod
    def get_node_name(d: Deployment) -> str:
        if d.name:
            return d.name
        if d.node is not None:
            return str(d.node)
        return "Unknown"

    @staticmethod
    def append_suffix(text: str, suffix: str) -> str:
        """Safely append a suffix without corrupting trailing comments."""
        lines: list[str] = text.split("\n")
        last_line: str = lines[-1]
        if "  #" in last_line:
            content, comment = last_line.rsplit("  #", 1)
            lines[-1] = f"{content}{suffix}  #{comment}"
        else:
            lines[-1] = f"{last_line}{suffix}"
        return "\n".join(lines)

    def process(
        self,
        deployment: Deployment | list[Deployment],
        incoming_active: bool,
        indent: int = 0,
    ) -> tuple[str, bool]:
        ind: str = "    " * indent
        nl: str = "\n"

        # ---------------- 1. List case (parallel multi-branch) ----------------
        if isinstance(deployment, list):
            if not deployment:
                return "[]", incoming_active
            inner: list[str] = []
            out_actives: list[bool] = []
            for s in deployment:
                s_str, s_out_active = self.process(s, incoming_active, indent + 1)
                inner.append(f"{ind}    {self.append_suffix(s_str, ',')}")
                out_actives.append(s_out_active)

            list_str: str = f"[{nl}{nl.join(inner)}{nl}{ind}]"
            return list_str, any(out_actives) if out_actives else incoming_active

        name: str | None = deployment.name
        if name is None and deployment.node:
            name = deployment.node.name
        if name:
            self.deployments[name] = deployment

        # ---------------- 2. State takeover ----------------
        node_active: bool = incoming_active
        if self.start and deployment in self.start:
            node_active = True

        outgoing_active: bool = node_active
        if self.stop and deployment in self.stop:
            outgoing_active = False

        tail_active_states: dict[object, bool] = {}
        result_str: str = ""

        # ------------- 3. Render parallel nodes & compute real tail state -------------
        if deployment.node is None and deployment.peers:
            peer_strs: list[str] = []
            peer_out_actives: list[bool] = []
            for peer in deployment.peers:
                p_str, p_out = self.process(peer, node_active, indent + 2)

                if "\n" in p_str:
                    peer_strs.append(f"({nl}{ind}        {p_str}{nl}{ind}    )")
                else:
                    peer_strs.append(p_str)

                peer_out_actives.append(p_out)

                for ptail in peer.get_tails():
                    if ptail.node is not None:
                        tail_active_states[ptail.node] = p_out

            joined_peers: str = f"{nl}{ind}    + ".join(peer_strs)
            result_str = f"({nl}{ind}    {joined_peers}{nl}{ind})"

            if self.stop and deployment in self.stop:
                outgoing_active = False
                for k in tail_active_states:
                    tail_active_states[k] = False
            else:
                outgoing_active = any(peer_out_actives) if peer_out_actives else node_active

        # ------------- 4. Render normal entity node -------------
        elif deployment.node is not None:
            node_str: str = self.get_node_name(deployment)

            status_tag: str = " 🟢" if node_active else ""
            result_str = f"{node_str}{status_tag}"

            tail_active_states[deployment.node] = outgoing_active

        # ------------- 5. Generate topology map (core integration) -------------
        peer_tails, downstream = deployment.parse_topology()
        for tail_node in peer_tails:
            is_tail_active: bool = tail_active_states.get(tail_node, outgoing_active)
            self.node_map[tail_node.ns] = {
                "node": tail_node,
                "peer_tails": peer_tails - {tail_node},
                "downstream": downstream,
                "active": is_tail_active,
            }

        # ------------- 6. Pass to downstream -------------
        continuations: list[Deployment] = deployment.children
        if not continuations:
            return result_str, outgoing_active
        elif len(continuations) == 1:
            child_str, child_out = self.process(
                continuations[0], outgoing_active, indent
            )
            return f"{result_str}{nl}{ind}>> {child_str}", child_out
        else:
            child_str, child_out = self.process(continuations, outgoing_active, indent)
            return f"{result_str}{nl}{ind}>> {child_str}", child_out

    def analyze(self) -> str:
        """Single-pass analysis of the orchestration graph."""
        graph, _ = self.process(self.orchestration, self.initial_active)
        return graph
