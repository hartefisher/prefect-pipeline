from prefect_pipeline.models import (
    BaseItem,
    BatchJobResponse,
    DeploymentContext,
    ExtraContext,
    NodeInfo,
)
from prefect_pipeline.models.llm import CompletionConfig
from prefect_pipeline.models.schemas import SchemaBase, SummarySchema


def test_base_item_slug():
    item = BaseItem(url="https://example.com/foo?x=1")
    assert item._slug == "foo"


def test_deployment_context_peer_nss_sorted():
    ctx = DeploymentContext(
        ns="a",
        active=True,
        peer_tails=[NodeInfo(ns="b", active=True), NodeInfo(ns="a", active=True)],
        downstream=[],
    )
    assert ctx.peer_nss == "a, a, b"


def test_extra_context_defaults():
    ec = ExtraContext()
    assert ec.disable_trigger is None
    assert ec.master_node is None
    assert ec.starter_id is None
    assert ec.macro_variables is None


def test_completion_config_provider_and_modifier():
    cfg = CompletionConfig(model="gpt-4")
    assert cfg.provider == "completionconfig"
    assert cfg.model_modifier == "completionconfig/gpt-4"


def test_completion_config_model_name_version():
    cfg = CompletionConfig(model="claude-3-20240101")
    assert cfg.model_name == "claude-3"
    assert cfg.model_version == "20240101"


def test_completion_config_debug_key():
    cfg = CompletionConfig(model="deepseek-r1-250528", temperature=0.3)
    assert cfg.debug_key == "ds_r1_03"


def test_completion_config_export_masks_key():
    cfg = CompletionConfig(model="gpt-4", api_key="abcdefghijklmnop", temperature=0.3)
    exp = cfg.export()
    assert exp["model"] == "completionconfig/gpt-4"
    assert exp["api_key"] == "abcd********mnop"
    assert exp["temperature"] == 0.3


def test_completion_config_copy_config_excludes_secrets():
    cfg = CompletionConfig(model="gpt-4", temperature=0.3, api_key="secret")
    c = cfg.copy_config()
    assert c["model"] == "gpt-4"
    assert c["temperature"] == 0.3
    assert "api_key" not in c
    assert "openai_compatible" not in c


def test_schema_base_get_instruction_no_doc():
    class Empty(SchemaBase):
        pass

    assert Empty.get_instruction() == ""


def test_schema_base_none_default():
    s = SummarySchema(summary=None)
    assert s.summary == ""


def test_schema_base_model_schema_excludes_internal():
    schema = SummarySchema.get_model_schema()
    assert "error" not in schema["properties"]
    assert "index" not in schema["properties"]
    assert "summary" in schema["properties"]


def test_batch_job_response_construction():
    resp = BatchJobResponse(id="b1", status="Completed", request_counts=None)
    assert resp.id == "b1"
