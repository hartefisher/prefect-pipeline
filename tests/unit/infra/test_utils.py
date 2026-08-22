from prefect_pipeline.infra.utils import (
    TimeCounter,
    consistent_string_id,
    extract_xml_data,
    get_current_time,
    get_fields,
)
from prefect_pipeline.models.schemas import SummarySchema


def test_extract_xml_normal():
    assert extract_xml_data("blocks", "<blocks>hello</blocks>") == "hello"


def test_extract_xml_malformed_returns_original():
    assert extract_xml_data("blocks", "no tags here") == "no tags here"


def test_consistent_string_id_deterministic():
    a = consistent_string_id("foo")
    b = consistent_string_id("foo")
    c = consistent_string_id("bar")
    assert a == b
    assert a != c
    assert isinstance(a, int)


def test_get_current_time_format():
    s = get_current_time()
    # YYYY-MM-DD HH:MM:SS.ffffff => 26 chars
    assert len(s) == 26
    assert s[4] == "-"


def test_time_counter_records_elapsed():
    tc = TimeCounter()
    tc.stop()
    assert tc.elapsed_seconds >= 0


def test_get_fields_excludes_internal():
    fields = get_fields(SummarySchema)
    assert "summary" in fields
    assert "error" not in fields
    assert "index" not in fields
