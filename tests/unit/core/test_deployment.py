"""Tests for prefect_pipeline.core.deployment — DAG operators and topology."""
from __future__ import annotations

from prefect_pipeline.core.deployment import Deployment, Node
from prefect_pipeline.core.runner_base import FlowRunnerBase

# --------------------------------------------------------------------------- #
# Mock runner and injectors
# --------------------------------------------------------------------------- #

class MockRunner(FlowRunnerBase):
    project_name = "test"


def injector_a() -> None:
    pass


def injector_b() -> None:
    pass


def _make_dep(name: str, runner: type[FlowRunnerBase] = MockRunner) -> Deployment:
    """Create a simple Deployment with a single injector."""
    node = Node(runner, injector_a, name=name)
    return Deployment(node=node, name=name)


# --------------------------------------------------------------------------- #
# Basic Deployment construction
# --------------------------------------------------------------------------- #

def test_deployment_create_empty():
    dep = Deployment.create()
    assert dep.node is None
    assert dep.children == []
    assert dep.peers == []


def test_deployment_with_node():
    dep = _make_dep("A")
    assert dep.node is not None
    assert dep.name == "A"


def test_deployment_name_set_via_init():
    dep = Deployment(name="MyDep")
    assert dep.name == "MyDep"


# --------------------------------------------------------------------------- #
# Peer / child management
# --------------------------------------------------------------------------- #

def test_add_peer():
    a = _make_dep("A")
    b = _make_dep("B")
    a.add_peer(b)
    assert b in a.peers


def test_add_peers_list():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    a.add_peers([b, c])
    assert b in a.peers
    assert c in a.peers


def test_add_child():
    a = _make_dep("A")
    b = _make_dep("B")
    a.add_child(b)
    assert b in a.children


def test_add_children_list():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    a.add_children([b, c])
    assert b in a.children
    assert c in a.children


def test_add_peer_no_duplicate():
    a = _make_dep("A")
    b = _make_dep("B")
    a.add_peer(b)
    a.add_peer(b)
    assert len(a.peers) == 1


# --------------------------------------------------------------------------- #
# __add__ (parallel) operator
# --------------------------------------------------------------------------- #

def test_add_operator_creates_parallel():
    a = _make_dep("A")
    b = _make_dep("B")
    group = a + b
    assert group.node is None
    assert len(group.peers) == 2


def test_add_operator_flattens_empty_peers():
    a = _make_dep("A")
    b = _make_dep("B")
    group_ab = a + b
    c = _make_dep("C")
    group_abc = group_ab + c
    # group_ab has no node and no children, so its peers are flattened
    assert len(group_abc.peers) == 3


# --------------------------------------------------------------------------- #
# __rshift__ (serial) operator
# --------------------------------------------------------------------------- #

def test_rshift_creates_child():
    a = _make_dep("A")
    b = _make_dep("B")
    a >> b
    assert b in a.children


def test_rshift_list_creates_children():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    a >> [b, c]
    assert b in a.children
    assert c in a.children


def test_rshift_chain():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    a >> b >> c
    assert b in a.children
    assert c in b.children


# --------------------------------------------------------------------------- #
# Topology: get_tails, get_terminal_nodes, get_head_nodes, parse_topology
# --------------------------------------------------------------------------- #

def test_get_tails_single():
    a = _make_dep("A")
    tails = a.get_tails()
    assert len(tails) == 1
    assert tails[0] is a


def test_get_tails_with_children():
    a = _make_dep("A")
    b = _make_dep("B")
    a >> b
    tails = a.get_tails()
    assert len(tails) == 1
    assert tails[0] is b


def test_get_terminal_nodes_single():
    a = _make_dep("A")
    assert a.node is not None
    terminals = a.get_terminal_nodes()
    assert a.node in terminals


def test_get_terminal_nodes_chain():
    a = _make_dep("A")
    b = _make_dep("B")
    a >> b
    # b is the terminal node of the chain
    terminals = a.get_terminal_nodes()
    assert b.node in terminals
    assert a.node not in terminals


def test_get_terminal_nodes_parallel():
    a = _make_dep("A")
    b = _make_dep("B")
    group = a + b
    terminals = group.get_terminal_nodes()
    assert a.node in terminals
    assert b.node in terminals


def test_get_head_nodes_single():
    a = _make_dep("A")
    assert a.node is not None
    heads = a.get_head_nodes()
    assert a.node in heads


def test_get_head_nodes_parallel():
    a = _make_dep("A")
    b = _make_dep("B")
    group = a + b
    heads = group.get_head_nodes()
    assert a.node in heads
    assert b.node in heads


def test_parse_topology_single():
    a = _make_dep("A")
    peer_tails, downstream = a.parse_topology()
    assert a.node in peer_tails
    assert downstream == set()


def test_parse_topology_chain():
    a = _make_dep("A")
    b = _make_dep("B")
    a >> b
    peer_tails, downstream = a.parse_topology()
    assert a.node in peer_tails
    assert b.node in downstream


def test_parse_topology_parallel_with_child():
    a = _make_dep("A")
    b = _make_dep("B")
    c = _make_dep("C")
    group = a + b
    group >> c
    peer_tails, downstream = group.parse_topology()
    assert a.node in peer_tails
    assert b.node in peer_tails
    assert c.node in downstream


# --------------------------------------------------------------------------- #
# get_deployment_name
# --------------------------------------------------------------------------- #

def test_get_deployment_name_basic():
    a = _make_dep("TestFlow")
    name = a.get_deployment_name()
    assert "Test Flow" in name


def test_get_deployment_name_raises_without_name():
    dep = Deployment(node=Node(MockRunner, injector_a))
    try:
        dep.get_deployment_name()
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


# --------------------------------------------------------------------------- #
# set_deployment_parameters
# --------------------------------------------------------------------------- #

def test_set_deployment_parameters():
    a = _make_dep("TestFlow")
    params = a.set_deployment_parameters()
    assert "name" in params
    assert "tags" in params
    assert "parameters" in params
    assert "schedules" in params


def test_set_deployment_parameters_with_tag():
    a = _make_dep("TestFlow")
    a.tag = "daily"
    params = a.set_deployment_parameters()
    assert "daily" in params["tags"]


def test_set_deployment_parameters_with_extra():
    a = _make_dep("TestFlow")
    a.tag = ["daily", "fast"]
    params = a.set_deployment_parameters()
    assert "daily" in params["tags"]
    assert "fast" in params["tags"]


# --------------------------------------------------------------------------- #
# Node properties
# --------------------------------------------------------------------------- #

def test_node_ns():
    node = Node(MockRunner, injector_a)
    runner_ns, injector_ns = node.ns
    assert "MockRunner" in runner_ns
    assert "injector_a" in injector_ns


def test_node_name_default():
    node = Node(MockRunner, injector_a)
    name = node.name
    assert "MockRunner" in name


def test_node_name_custom():
    node = Node(MockRunner, injector_a, name="CustomName")
    assert node.name == "CustomName"


def test_node_repr():
    node = Node(MockRunner, injector_a, name="MyNode")
    assert repr(node) == "MyNode"


def test_node_no_injectors():
    node = Node(MockRunner)
    injector_ns = node.injector_ns
    assert injector_ns == "None"
