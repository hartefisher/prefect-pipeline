from unittest.mock import AsyncMock, patch

import pytest
from litellm import Choices, Message, ModelResponse, Usage
from tests.unit.components.conftest import ToyItem, ToySchema

from prefect_pipeline.components.llm import (
    GenericExtractor,
    LLMExtractionStrategy,
    LLMExtractor,
)
from prefect_pipeline.models.llm import CompletionConfig


def _make_config() -> CompletionConfig:
    return CompletionConfig(model="gpt-4o", api_key="sk-test")


def _fake_model_response(content: str) -> ModelResponse:
    return ModelResponse(
        choices=[Choices(message=Message(role="assistant", content=content))],
        usage=Usage(completion_tokens=1, prompt_tokens=2, total_tokens=3),
    )


async def test_extraction_strategy_parse_xml():
    strat = LLMExtractionStrategy(_make_config())
    parsed = strat.parse_response("<blocks>{\"summary\": \"hi\"}</blocks>")
    assert parsed == {"summary": "hi", "error": False}


async def test_extraction_strategy_parse_json_force():
    strat = LLMExtractionStrategy(_make_config(), force_json_response=True)
    parsed = strat.parse_response('{"summary": "hi"}')
    assert parsed["summary"] == "hi"


async def test_extraction_strategy_bad_response_raises():
    from prefect_pipeline.infra.exceptions import BadResponseError

    strat = LLMExtractionStrategy(_make_config())
    with pytest.raises(BadResponseError):
        strat.parse_response("not json")


async def test_extract_calls_acompletion_and_parses():
    strat = LLMExtractionStrategy(_make_config())
    fake = _fake_model_response("<blocks>{\"summary\": \"ok\"}</blocks>")
    with patch(
        "prefect_pipeline.components.llm.acompletion", new=AsyncMock(return_value=fake)
    ):
        result = await strat.extract("do it")
    assert result == {"summary": "ok", "error": False}
    assert strat.total_usage.total_tokens == 3


class ToyExtractor(LLMExtractor[ToyItem]):
    schema_model = ToySchema  # type: ignore[assignment]


async def test_llm_extractor_run_end_to_end():
    cfg = _make_config()
    extractor = ToyExtractor(cfg)
    extractor.output_collection = None
    extractor.stats_collection = None

    fake = _fake_model_response("<blocks>{\"summary\": \"extracted\"}</blocks>")
    item = ToyItem(url="https://x.com/1", title="t")

    with patch(
        "prefect_pipeline.components.llm.acompletion", new=AsyncMock(return_value=fake)
    ):
        result = await extractor.run(item)

    assert result is not None
    assert result["url"] == "https://x.com/1"
    assert result["extracted_content"] == {"summary": "extracted", "error": False}


class ToyGeneric(GenericExtractor[ToyItem]):
    schema_model = ToySchema  # type: ignore[assignment]
    ns = "toy_content"


def test_generic_extractor_output_field_no_suffix():
    ext = ToyGeneric(_make_config())
    ext.suffix = None
    ext.debug = False
    assert ext.output_field == "toy_content"


def test_generic_extractor_transform_result_single_field():
    ext = ToyGeneric(_make_config())
    # schema has single field "summary"
    result = ext.transform_result(
        ToyItem(url="u"), {"summary": "s", "error": False, "index": 0}
    )
    assert result == "s"
