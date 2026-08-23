"""Tests for prefect_pipeline.core.ns_converter."""

from __future__ import annotations

from prefect_pipeline.core.configs import ROOT_PATH
from prefect_pipeline.core.ns_converter import (
    capitalize_ns,
    entrypoint_2_ns,
    gen_deployment_name,
    ns_2_entrypoint,
    path_2_flow_name,
)

# --------------------------------------------------------------------------- #
# gen_deployment_name
# --------------------------------------------------------------------------- #


def test_gen_deployment_name_simple():
    assert gen_deployment_name("TestFlow") == "Test Flow"


def test_gen_deployment_name_with_flags():
    result = gen_deployment_name("TestFlow", ["DAILY", "FAST"])
    assert result == "[DAILY][FAST]Test Flow"


def test_gen_deployment_name_no_flags():
    assert gen_deployment_name("TestFlow", None) == "Test Flow"


def test_gen_deployment_name_empty_flags():
    assert gen_deployment_name("TestFlow", []) == "Test Flow"


def test_gen_deployment_name_camel_case():
    assert gen_deployment_name("SpiderScraper") == "Spider Scraper"


# --------------------------------------------------------------------------- #
# path_2_flow_name
# --------------------------------------------------------------------------- #


def test_path_2_flow_name_with_prefix():
    full_path = f"{ROOT_PATH}.spider.test"
    result = path_2_flow_name(full_path, "demo")
    assert result == "demo-spider-test"


def test_path_2_flow_name_without_prefix():
    full_path = f"{ROOT_PATH}.spider.test"
    result = path_2_flow_name(full_path)
    assert result == "spider-test"


# --------------------------------------------------------------------------- #
# ns_2_entrypoint / entrypoint_2_ns round-trip
# --------------------------------------------------------------------------- #


def test_ns_2_entrypoint_basic():
    result = ns_2_entrypoint("demo-spider-test", "DailyRun")
    assert result == "demo__spider__test__DailyRun"


def test_entrypoint_2_ns_basic():
    flow_name, dep_name = entrypoint_2_ns("demo__spider__test__DailyRun")
    assert flow_name == "demo-spider-test"
    assert dep_name == "DailyRun"


def test_entrypoint_round_trip():
    original_flow = "demo-spider-test"
    original_dep = "Daily Run"
    entrypoint = ns_2_entrypoint(original_flow, original_dep)
    flow_name, dep_name = entrypoint_2_ns(entrypoint)
    assert flow_name == original_flow
    assert dep_name == original_dep.replace(" ", "")


# --------------------------------------------------------------------------- #
# capitalize_ns
# --------------------------------------------------------------------------- #


def test_capitalize_ns_camel_case():
    result = capitalize_ns("spiderFlow")
    assert result == ["spider", "Flow"]


def test_capitalize_ns_multi_camel():
    result = capitalize_ns("TestFlowRunner")
    assert result == ["Test", "Flow", "Runner"]


def test_capitalize_ns_empty():
    assert capitalize_ns("") == []
    assert capitalize_ns(None) == []


def test_capitalize_ns_with_digits():
    result = capitalize_ns("Flow2Runner")
    assert result == ["Flow2", "Runner"]


def test_capitalize_ns_with_underscore_delimiter():
    # No camelCase segments and no uppercase -> delimiter branch (line 33).
    # split('.') yields a single segment, capitalized as one phrase.
    result = capitalize_ns("spider_flow")
    assert result == ["Spider Flow"]


def test_capitalize_ns_with_dot_delimiter():
    result = capitalize_ns("spider.flow.runner")
    assert result == ["Spider", "Flow", "Runner"]
