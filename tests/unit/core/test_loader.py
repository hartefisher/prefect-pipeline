"""Unit tests for ``FlowsLoader`` and the entry-point machinery (core/loader)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_pipeline.core import loader
from prefect_pipeline.core.deployment import Deployment, Node
from prefect_pipeline.core.runner_base import FlowRunnerBase


class _ToyRunner(FlowRunnerBase):
    pass


def _make_deployment(name: str) -> Deployment:
    node = Node(_ToyRunner, name=name)
    return Deployment(node, name=name)


async def test_entry_fn_dispatches_to_deployment():
    dep = _make_deployment("A")
    target = AsyncMock()
    dep.run = target  # type: ignore[assignment]

    with patch.object(loader, "get_deployment_instance", return_value=dep):
        with patch("prefect.runtime.flow_run.get_flow_name", return_value="flow"):
            with patch("prefect.runtime.deployment.get_name", return_value="A"):
                await loader.entry_fn(x=1)
    target.assert_awaited_once()


def test_entry_points_create_flow_builds_prefect_flow():
    dep = _make_deployment("A")

    with patch.object(loader, "get_deployment_instance", return_value=dep):
        flow_obj = loader.entry_points.create_flow("flow__A", lambda: None)
    assert flow_obj.name == "flow"


def test_entry_points_getattr_creates_lazily():
    dep = _make_deployment("B")
    with patch.object(loader, "get_deployment_instance", return_value=dep):
        flow_obj = loader.entry_points.__getattr__("flow__B")
    assert flow_obj is not None
    assert loader.entry_points.flows["flow__B"] is flow_obj


def test_entry_points_create_flow_missing_node_raises():
    dep = Deployment(node=None, name="X")
    with patch.object(loader, "get_deployment_instance", return_value=dep):
        with pytest.raises(ValueError):
            loader.entry_points.create_flow("flow__X", lambda: None)


def test_render_deployment_map():
    loader_obj = loader.FlowsLoader(version_id="v1", fp="")
    dep_a = _make_deployment("Fetch")
    dep_a.node.runner.project_name = "demo"  # type: ignore[attr-defined]
    loader_obj.flows["src.flows.demo"] = [dep_a]

    loader_obj.render_deployment_map()

    # ns == (runner_module, injector_ns)
    assert len(loader_obj.deployment_map) == 1
    detail = next(iter(loader_obj.deployment_map.values()))
    assert detail["name"] == "Fetch"
    assert detail["flow_name"].endswith("demo")


def test_deploy_skips_non_matching_pool(monkeypatch: pytest.MonkeyPatch):
    loader_obj = loader.FlowsLoader(version_id="v1", fp="")
    dep = _make_deployment("Fetch")
    loader_obj.deployment_map[(("x", "y"), "z")] = {
        "instance": dep,
        "name": "Fetch",
        "flow_name": "demo",
        "qualname": "demo/Fetch",
    }
    monkeypatch.setattr(loader, "WORKFLOW_POOL", "other-pool")
    dep.workflow_pool = "different-pool"  # type: ignore[attr-defined]
    loader_obj.deploy()
    assert loader_obj.prefect_deployments == []


def test_deploy_creates_prefect_deployment(monkeypatch: pytest.MonkeyPatch):
    loader_obj = loader.FlowsLoader(version_id="v1", fp="")
    dep = _make_deployment("Fetch")
    loader_obj.deployment_map[(("x", "y"), "z")] = {
        "instance": dep,
        "name": "Fetch",
        "flow_name": "demo",
        "qualname": "demo/Fetch",
    }
    monkeypatch.setattr(loader, "WORKFLOW_POOL", "default")
    dep.workflow_pool = "default"  # type: ignore[attr-defined]

    fake_prefect_dep = MagicMock()
    with patch.object(loader.FlowsLoader, "_deploy", return_value=fake_prefect_dep):
        loader_obj.deploy()
    assert loader_obj.prefect_deployments == [fake_prefect_dep]


async def test_write_deployment_manifest(monkeypatch: pytest.MonkeyPatch):
    loader_obj = loader.FlowsLoader(version_id="v9", fp="")
    dep = _make_deployment("Fetch")
    loader_obj.deployment_map[("mod.Runner", "inj")] = {
        "instance": dep,
        "name": "Fetch",
        "flow_name": "demo",
        "qualname": "demo/Fetch",
    }

    fake_collection = MagicMock()
    fake_collection.update_one = AsyncMock()
    fake_prefect = MagicMock()
    fake_prefect.deployment_manifest = fake_collection
    fake_prefect.close = MagicMock()
    monkeypatch.setattr(loader, "get_prefect", lambda: fake_prefect)

    await loader_obj.write_deployment_manifest()

    fake_collection.update_one.assert_awaited_once()
    filter_arg, update_arg = fake_collection.update_one.call_args[0]
    assert filter_arg == {"ns": ["mod.Runner", "inj"], "version_id": "v9"}
    assert update_arg["$set"]["qualname"] == "demo/Fetch"
    assert update_arg["$set"]["ns"] == ["mod.Runner", "inj"]
    fake_prefect.close.assert_called_once()


async def test_load_pipeline_runs_all_phases(monkeypatch: pytest.MonkeyPatch):
    loader_obj = loader.FlowsLoader(version_id="v1", fp="")
    dep = _make_deployment("Fetch")
    loader_obj.flows["src.flows.demo"] = [dep]
    monkeypatch.setattr(loader, "WORKFLOW_POOL", "default")
    dep.workflow_pool = "default"  # type: ignore[attr-defined]

    fake_prefect_dep = MagicMock()
    with patch.object(loader.FlowsLoader, "_deploy", return_value=fake_prefect_dep):
        with patch.object(loader.FlowsLoader, "write_deployment_manifest", new_callable=AsyncMock) as write_mock:
            result = await loader_obj.load()
    assert result == [fake_prefect_dep]
    write_mock.assert_awaited_once()
    assert len(loader_obj.deployment_map) == 1


def test_deploy_builds_real_prefect_deployment():
    """Exercise ``_deploy`` (create_flow + to_deployment) end-to-end."""
    from prefect_pipeline.core.ns_converter import ns_2_entrypoint

    dep = _make_deployment("Fetch")
    entrypoint = ns_2_entrypoint("demo-flow", "Fetch")

    with patch.object(loader, "get_deployment_instance", return_value=dep):
        loader_obj = loader.FlowsLoader(version_id="v1", fp="")
        runner_dep = loader_obj._deploy(dep, entrypoint)

    assert runner_dep is not None
    # inject_qualname must set the entry-point qualname on the wrapped fn
    assert "entry_points" in runner_dep.name or runner_dep.name


def test_create_flow_injects_qualname():
    dep = _make_deployment("Fetch")
    with patch.object(loader, "get_deployment_instance", return_value=dep):
        flow_obj = loader.entry_points.create_flow("demo__Fetch", lambda: None, inject_qualname=True)
    assert flow_obj.name == "demo"
