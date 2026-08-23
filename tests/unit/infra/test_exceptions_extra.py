"""Extra coverage for infra.exceptions and infra.utils (M6.4)."""

from __future__ import annotations

import pytest

from prefect_pipeline.infra import exceptions as exc
from prefect_pipeline.infra.utils import (
    TimeStatistic,
    consistent_string_id,
    construct_dict_variable_snippet,
    extract_xml_data,
    get_current_time,
    quote_key_value,
)


# --------------------------------------------------------------------------- #
# exceptions: constructors + __str__ / __repr__
# --------------------------------------------------------------------------- #
def test_result_is_empty():
    e = exc.ResultIsEmpty()
    assert isinstance(e, Exception)
    with pytest.raises(exc.ResultIsEmpty):
        raise exc.ResultIsEmpty("boom")


def test_ignore_request():
    e = exc.IgnoreRequest("skip")
    assert str(e) == "skip"


def test_item_model_missing():
    e = exc.ItemModelMissing("m")
    assert str(e) == "m"


def test_retry_reasoning_str_and_repr():
    e = exc.RetryReasoning(3, True)
    assert e.retriable_requests == 3
    assert e.batch is True
    assert "3" in str(e)
    assert str(e) == repr(e)


def test_bad_response_error_str_and_repr():
    e = exc.BadResponseError("boom", response="<xml>")
    assert e.message == "boom"
    assert e.response == "<xml>"
    assert str(e) == "boom"
    assert repr(e) == "boom"


def test_batch_job_not_completed_and_failed():
    e1 = exc.BatchJobNotCompleted("nope")
    e2 = exc.BatchJobFailed("fail")
    assert str(e1) == "nope"
    assert str(e2) == "fail"


def test_batch_job_response_error_str_and_repr():
    e = exc.BatchJobResponseError(500, message="err", code="X", param="p")
    assert e.status_code == 500
    assert e.message == "err"
    assert e.code == "X"
    assert e.param == "p"
    assert str(e) == "err"
    assert repr(e) == "err"


# --------------------------------------------------------------------------- #
# utils: consistent_string_id algorithm branches
# --------------------------------------------------------------------------- #
def test_consistent_string_id_md5():
    v = consistent_string_id("a", algorithm="md5", output_bits=32)
    assert isinstance(v, int)


def test_consistent_string_id_sha1():
    v = consistent_string_id("a", algorithm="sha1", output_bits=64)
    assert isinstance(v, int)


def test_consistent_string_id_sha512():
    v = consistent_string_id("a", algorithm="sha512", output_bits=128)
    assert isinstance(v, int)


def test_consistent_string_id_full_bytes():
    v = consistent_string_id("a", algorithm="sha512", output_bits=256)
    assert isinstance(v, int)


def test_consistent_string_id_unsupported_algorithm():
    with pytest.raises(ValueError, match="不支持的算法"):
        consistent_string_id("a", algorithm="md9")


# --------------------------------------------------------------------------- #
# utils: quote_key_value branches
# --------------------------------------------------------------------------- #
def test_quote_key_value_str():
    assert quote_key_value("x") == "'x'"


def test_quote_key_value_number():
    assert quote_key_value(5) == "5"
    assert quote_key_value(1.5) == "1.5"


def test_quote_key_value_class_and_function():
    class Foo:
        pass

    def bar() -> None:
        pass

    assert quote_key_value(Foo) == "Foo"
    assert quote_key_value(bar) == "bar"


def test_quote_key_value_dict():
    out = quote_key_value({"k": "v", "n": 1})
    assert "'k'" in out and "'v'" in out and "1" in out


def test_quote_key_value_list_and_tuple():
    assert quote_key_value([1, 2]) == "[1, 2]"
    assert quote_key_value((1,)) == "(1,)"
    assert quote_key_value((1, 2)) == "(1, 2)"


def test_quote_key_value_fallback():
    assert quote_key_value(object()) != ""


# --------------------------------------------------------------------------- #
# utils: construct_dict_variable_snippet
# --------------------------------------------------------------------------- #
def test_construct_dict_variable_snippet():
    snippet = construct_dict_variable_snippet("cfg", {"a": "b", "n": 3}, tabs=1)
    assert snippet.startswith("cfg = {")
    assert "'a': 'b'" in snippet
    assert "'n': 3" in snippet
    assert snippet.strip().endswith("}")


# --------------------------------------------------------------------------- #
# utils: TimeStatistic context manager
# --------------------------------------------------------------------------- #
def test_time_statistic_context_manager():
    with TimeStatistic() as ts:
        pass
    assert ts.elapsed_seconds >= 0
    d = ts.to_dict()
    assert "start_time" in d
    assert "end_time" in d
    assert "elapsed_seconds" in d


def test_time_statistic_str():
    with TimeStatistic() as ts:
        pass
    assert "Time taken" in str(ts)


# --------------------------------------------------------------------------- #
# utils: small helpers already covered, ensure parity
# --------------------------------------------------------------------------- #
def test_get_current_time_and_extract_xml():
    assert len(get_current_time()) == 26
    assert extract_xml_data("b", "<b>x</b>") == "x"
    assert extract_xml_data("b", "plain") == "plain"
