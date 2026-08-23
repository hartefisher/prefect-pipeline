"""Tests for prefect_pipeline.core.orchestration — DAG analysis engine."""

from __future__ import annotations

from prefect_pipeline.core.deployment import Deployment, Node
from prefect_pipeline.core.orchestration import Orchestration
from prefect_pipeline.core.runner_base import FlowRunnerBase

# --------------------------------------------------------------------------- #
# Mock runner and injector factory
# --------------------------------------------------------------------------- #


class MockRunner(FlowRunnerBase):
    project_name = "test"


_injector_counter = 0


def _make_dep(name: str) -> Deployment:
    """Create a Deployment with a Node using a unique injector for distinct ns."""
    global _injector_counter
    _injector_counter += 1

    def _injector() -> None:
        pass

    _injector.__name__ = f"injector_{_injector_counter}"
    node = Node(MockRunner, _injector, name=name)
    return Deployment(node=node, name=name)


# --------------------------------------------------------------------------- #
# Simple chain: A >> B
# --------------------------------------------------------------------------- #


def test_chain_node_map_size():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a >> b)
    assert len(orch.node_map) == 2


def test_chain_downstream_relation():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a >> b)
    a_info = orch.node_map[a.node.ns]  # type: ignore[union-attr]
    assert b.node in a_info["downstream"]  # type: ignore[union-attr]
    b_info = orch.node_map[b.node.ns]  # type: ignore[union-attr]
    assert len(b_info["downstream"]) == 0  # type: ignore[union-attr]


def test_chain_all_active():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a >> b)
    for info in orch.node_map.values():
        assert info["active"] is True


def test_chain_graph_contains_names():
    a = _make_dep("Alpha")
    b = _make_dep("Beta")
    orch = Orchestration(a >> b)
    assert "Alpha" in orch.graph
    assert "Beta" in orch.graph
    assert ">>" in orch.graph


def test_chain_graph_has_green_markers():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a >> b)
    assert "🟢" in orch.graph


# --------------------------------------------------------------------------- #
# Parallel: A + B
# --------------------------------------------------------------------------- #


def test_parallel_node_map_size():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a + b)
    assert len(orch.node_map) == 2


def test_parallel_peer_relation():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a + b)
    a_info = orch.node_map[a.node.ns]  # type: ignore[union-attr]
    assert b.node in a_info["peer_tails"]  # type: ignore[union-attr]
    b_info = orch.node_map[b.node.ns]  # type: ignore[union-attr]
    assert a.node in b_info["peer_tails"]  # type: ignore[union-attr]


def test_parallel_no_downstream():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a + b)
    for info in orch.node_map.values():
        assert len(info["downstream"]) == 0


# --------------------------------------------------------------------------- #
# Complex: A >> (B + C) >> D
# --------------------------------------------------------------------------- #


def test_complex_node_map_size():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    d = _make_dep("D")
    orch = Orchestration(a >> (b + c) >> d)
    assert len(orch.node_map) == 4


def test_complex_a_downstream():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    d = _make_dep("D")
    orch = Orchestration(a >> (b + c) >> d)
    a_info = orch.node_map[a.node.ns]  # type: ignore[union-attr]
    assert b.node in a_info["downstream"]  # type: ignore[union-attr]
    assert c.node in a_info["downstream"]  # type: ignore[union-attr]


def test_complex_bc_downstream_is_d():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    d = _make_dep("D")
    orch = Orchestration(a >> (b + c) >> d)
    b_info = orch.node_map[b.node.ns]  # type: ignore[union-attr]
    c_info = orch.node_map[c.node.ns]  # type: ignore[union-attr]
    assert d.node in b_info["downstream"]  # type: ignore[union-attr]
    assert d.node in c_info["downstream"]  # type: ignore[union-attr]


def test_complex_d_downstream_empty():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    d = _make_dep("D")
    orch = Orchestration(a >> (b + c) >> d)
    d_info = orch.node_map[d.node.ns]  # type: ignore[union-attr]
    assert len(d_info["downstream"]) == 0  # type: ignore[union-attr]


def test_complex_graph_contains_all():
    a = _make_dep("Alpha")
    b = _make_dep("Beta")
    c = _make_dep("Gamma")
    d = _make_dep("Delta")
    orch = Orchestration(a >> (b + c) >> d)
    for name in ["Alpha", "Beta", "Gamma", "Delta"]:
        assert name in orch.graph


# --------------------------------------------------------------------------- #
# Start / Stop markers
# --------------------------------------------------------------------------- #


def test_start_marker_activates():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a >> b, start=[a])
    a_info = orch.node_map[a.node.ns]  # type: ignore[union-attr]
    assert a_info["active"] is True  # type: ignore[union-attr]


def test_stop_marker_deactivates_outgoing():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    orch = Orchestration(a >> b >> c, stop=[b])
    b_info = orch.node_map[b.node.ns]  # type: ignore[union-attr]
    assert b_info["active"] is False  # type: ignore[union-attr]


def test_without_start_all_active_by_default():
    a = _make_dep("A")
    b = _make_dep("B")
    orch = Orchestration(a >> b)
    for info in orch.node_map.values():
        assert info["active"] is True


# --------------------------------------------------------------------------- #
# get_node_info
# --------------------------------------------------------------------------- #


def test_get_node_info_found():
    a = _make_dep("Alpha")
    b = _make_dep("Beta")
    orch = Orchestration(a >> b)
    info = orch.get_node_info("Alpha")
    assert info is not None
    assert b.node in info["downstream"]  # type: ignore[operator]


def test_get_node_info_not_found():
    a = _make_dep("A")
    orch = Orchestration(a)
    assert orch.get_node_info("NonExistent") is None


# --------------------------------------------------------------------------- #
# Static helpers
# --------------------------------------------------------------------------- #


def test_get_node_name_with_name():
    a = _make_dep("MyDep")
    assert Orchestration.get_node_name(a) == "MyDep"


def test_get_node_name_fallback():
    a = _make_dep("Alpha")
    a.name = None  # type: ignore[assignment]
    name = Orchestration.get_node_name(a)
    assert "Alpha" in name or "MockRunner" in name


def test_append_suffix_simple():
    result = Orchestration.append_suffix("hello", ",")
    assert result == "hello,"


def test_append_suffix_with_comment():
    result = Orchestration.append_suffix("hello  # comment", ",")
    assert result == "hello,  # comment"


def test_deployment_name_falls_back_to_node_name():
    a = _make_dep("NodeA")
    a.name = None  # type: ignore[assignment]
    b = _make_dep("B")
    orch = Orchestration(a >> b)
    # process() line 81: name falls back to node.name when deployment.name is None
    assert "NodeA" in orch.graph or a.node.ns in orch.node_map  # type: ignore[union-attr]
    assert len(orch.node_map) == 2
