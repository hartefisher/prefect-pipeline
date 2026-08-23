import json
from functools import cached_property
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from litellm import (
    Choices,
    CustomStreamWrapper,
    ImageURLListItem,
    ImageURLObject,
    Message,
    ModelResponse,
    acompletion,
)
from litellm.types.utils import Usage

from prefect_pipeline.components.helper import AutoItemModel, ExtraContextMixin, PipelineHelper
from prefect_pipeline.core.configs import PROMPTS_DIRECTORY
from prefect_pipeline.infra.exceptions import BadResponseError, BatchJobResponseError, IgnoreRequest
from prefect_pipeline.infra.types import BatchResponse
from prefect_pipeline.infra.utils import TimeCounter, extract_xml_data, get_field_name, get_fields
from prefect_pipeline.models import BaseItem
from prefect_pipeline.models.llm import CompletionConfig
from prefect_pipeline.models.schemas import SCHEMA_SPEC, SchemaBase


class LLMExtractionStrategy:
    def __init__(
        self,
        config: CompletionConfig,
        verbose: bool = False,
        force_json_response: bool = False,
        max_attempts: int = 3,
        base_delay: int = 2,
        tag_name: str = "blocks",
        **extra_args: Any,
    ) -> None:
        self.config = config
        self.messages: list[Message] = []
        self.usages: list[Usage] = []
        self.total_usage: Usage = Usage(
            completion_tokens=0,
            prompt_tokens=0,
            total_tokens=0,
            completion_tokens_details={},
            prompt_tokens_details={},
        )
        self.verbose = verbose
        self.force_json_response = force_json_response
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.tag_name = tag_name

    def refresh(self) -> None:
        """
        Refresh the messages list to ensure it is empty.
        This is useful for resetting the state before a new extraction.
        """
        self.messages.clear()

    def update_messages(
        self,
        msg: Message | str | None = None,
        image_urls: list[str] | None = None,
    ) -> None:
        if isinstance(msg, Message):
            self.messages.append(msg)
        elif self.messages and self.messages[-1].get("role") == "assistant" and msg is None and image_urls is None:
            self.messages.pop()
        elif msg is not None or image_urls is not None:
            images = (
                [
                    ImageURLListItem(image_url=ImageURLObject(url=url), index=i, type="image_url")
                    for i, url in enumerate(image_urls)
                ]
                if image_urls
                else None
            )
            self.messages.append(Message(role="user", content=msg, images=images))

    async def extract(
        self,
        instruction: str | None = None,
        refresh_context: bool = True,
        images: list[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        if refresh_context:
            self.refresh()

        self.update_messages(instruction, images)

        response = await self.perform_completion(**kwargs)
        if isinstance(response, ModelResponse):
            usage = cast(Usage, response.usage)
            self.update_usage(usage)
            choices = cast(list[Choices], response.choices)
            message = choices[0].message
            self.update_messages(message)
            return self.parse_response(message.content)
        return None

    def update_usage(self, usage: Usage) -> None:
        # Track usage
        self.usages.append(usage)

        # Update totals
        self.total_usage.completion_tokens += usage.completion_tokens
        self.total_usage.prompt_tokens += usage.prompt_tokens
        self.total_usage.total_tokens += usage.total_tokens

    def parse_response(self, response: str | None) -> dict[str, Any]:
        try:
            if response is None:
                raise ValueError("No response to parse.")
            if self.force_json_response:
                parsed = json.loads(response)
            else:
                raw_data = extract_xml_data(self.tag_name, response)
                parsed = json.loads(raw_data)

            if parsed and isinstance(parsed, list):
                parsed = parsed[0]

            if isinstance(parsed, dict):
                parsed["error"] = False
                return parsed
            else:
                raise ValueError("Parsed response is not a dict")

        except Exception as e:
            raise BadResponseError(str(e), response) from None

    async def perform_completion(
        self, prompt: str | None = None, **kwargs: Any
    ) -> ModelResponse | CustomStreamWrapper | None:
        self.update_messages(prompt)
        if self.force_json_response:
            kwargs["response_format"] = {"type": "json_object"}
        if kwargs:
            self.config = self.config.model_copy(update=kwargs)
        completion_kwargs = self.config.export(encrypted=False)
        response = await acompletion(
            messages=self.messages,
            allowed_openai_params=["reasoning_effort"],
            **completion_kwargs,
        )
        return response


class BatchLLMExtractionStrategy(LLMExtractionStrategy):
    async def perform_completion(self, prompt: str | None = None, **kwargs: Any) -> ModelResponse | None:
        self.update_messages(prompt)

        if response := cast(BatchResponse, kwargs.get("result")):
            if response["status_code"] == 200:
                return ModelResponse(**response["body"])
            elif "error" in response["body"]:
                raise BatchJobResponseError(response["status_code"], **response["body"]["error"])
        return None


class LLMExtractor[Item: BaseItem](AutoItemModel[Item], PipelineHelper, ExtraContextMixin):
    schema_model: type[SchemaBase]
    ns: str = "extracted_content"
    verbose: bool = False
    annotate_schema: bool = False
    instruction: str | None = None
    debug: bool = False
    version: str = "p0"
    concurrent_requests: int = 30

    def __init__(
        self,
        llm_config: CompletionConfig,
        *,
        strategy: type[LLMExtractionStrategy] = LLMExtractionStrategy,
        batch_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.update_extra_context(**kwargs)
        self.llm_config = llm_config
        self.extaction_strategies: dict[str, LLMExtractionStrategy] = {}
        self.batch_id = batch_id or str(uuid4())
        self.strategy = strategy

    async def get_instruction_variables(self, item: Item) -> dict[str, Any]:
        return {}

    @cached_property
    def _instruction(self) -> Any:
        return self.instruction or self.schema_model.__doc__

    @cached_property
    def output_field(self) -> str:
        return self.ns

    async def get_instruction(self, item: Item, instruction_variables: dict[str, Any] | None = None) -> str:
        variables = await self.get_instruction_variables(item)
        if instruction_variables:
            variables.update(instruction_variables)
        if self._instruction:
            instruction = cast(str, self._instruction.format(**variables))
            if self.annotate_schema:
                return instruction + SCHEMA_SPEC.format(
                    schema=json.dumps(self.schema_model.get_model_schema(), indent=2)
                )
            return instruction
        return ""

    @cached_property
    def model_schema(self) -> dict[str, Any]:
        return self.schema_model.get_model_schema()

    def get_extraction_strategy(self, key: str) -> LLMExtractionStrategy:
        if key not in self.extaction_strategies:
            self.extaction_strategies[key] = self.strategy(
                verbose=self.verbose,
                config=self.llm_config,
            )

        extaction_strategy = self.extaction_strategies[key]
        return extaction_strategy

    def get_images(self, item: Item) -> list[str] | None:
        return None

    def get_url(self, item: Item) -> str:
        return item.url

    async def check_request(self, item: Item) -> dict[str, Any] | None:
        return None

    async def on_saved(self, item: Item, result: dict[str, Any] | None) -> None:
        pass

    async def run(self, item: Item, **kwargs: Any) -> dict[str, Any] | None:
        extra_data = await self.check_request(item)
        instruction_variables = extra_data.get("instruction_variables") if extra_data else None
        images = self.get_images(item)
        extraction_strategy = self.get_extraction_strategy(item.url)
        extracted_content: dict[str, Any] | str | None = "_EMPTY_"
        time_stats = TimeCounter()
        try:
            instruction = await self.get_instruction(item, instruction_variables)
            extracted_content = await extraction_strategy.extract(
                instruction, refresh_context=False, images=images, **kwargs
            )
            result = self.get_result(item, extracted_content, extraction_strategy)
            await self.save_result(result)
            await self.on_saved(item, extracted_content)
            return result
        except Exception as e:
            if not isinstance(e, IgnoreRequest):
                extracted_content = {
                    "error": True,
                    "reason": str(e),
                }
                if isinstance(e, BadResponseError):
                    extracted_content["content"] = e.response
            raise e
        finally:
            if extracted_content != "_EMPTY_":
                time_stats.stop()
                stats = self.get_stats(
                    item,
                    cast("dict[str, Any] | None", extracted_content),
                    extraction_strategy,
                    time_stats,
                )
                await self.log_stats(stats)

    def transform_result(
        self,
        item: Item,
        result: dict[str, Any],
    ) -> dict[str, Any] | None:
        return result

    def get_result(
        self,
        item: Item,
        extracted_content: dict[str, Any] | None,
        extraction_strategy: LLMExtractionStrategy,
    ) -> dict[str, Any] | None:
        if extracted_content is None:
            raise BadResponseError("Result is None.")
        if not extracted_content.get("error", False):
            print(f"Total usage for {item.url}: {extraction_strategy.total_usage.total_tokens} tokens")
            result = {
                "url": item.url,
                self.output_field: self.transform_result(item, extracted_content),
            }
            if self.batch_id:
                result["batch_id"] = self.batch_id
            return result
        return None

    def get_stats(
        self,
        item: Item,
        result: dict[str, Any] | None,
        extraction_strategy: LLMExtractionStrategy,
        time_stats: TimeCounter,
    ) -> dict[str, Any]:
        messages = [msg.model_dump() for msg in extraction_strategy.messages]

        return {
            "extractor_name": self.__class__.__name__,
            "collection_name": self.collection_name,
            "batch_id": self.batch_id,
            "url": item.url,
            "extracted_content": result,
            "llm_messages": messages,
            "provider": extraction_strategy.config.model_modifier,
            "completion_kwargs": extraction_strategy.config.export(),
            "completion_tokens": extraction_strategy.total_usage.completion_tokens,
            "prompt_tokens": extraction_strategy.total_usage.prompt_tokens,
            "total_tokens": extraction_strategy.total_usage.total_tokens,
            "schema": self.model_schema,
            "debug_key": self.output_field,
            "version": self.version,
            **time_stats.to_dict(),
        }


class GenericExtractor[Item: BaseItem](LLMExtractor[Item]):
    annotate_schema = True
    # 业务项目可覆盖：LLM 抽取统计写入的集合（默认不写，由用户项目注入）。
    stats_collection: Any = None
    ns: str
    limit: int = 0
    override: bool = False
    suffix: str | None = None

    @cached_property
    def _instruction(self) -> str:
        ps = [*self.ns.split("."), self.version]
        md = Path(f"{PROMPTS_DIRECTORY}/{'/'.join(ps)}.md")
        return md.read_text()

    @cached_property
    def output_field(self) -> str:
        ns = f"{self.ns}_{self.suffix}" if self.suffix else self.ns
        if self.debug:
            ps = [ns, self.version, self.llm_config.debug_key]
            return "__".join(ps)
        return ns

    def transform_result(self, item: Item, result: dict[str, Any]) -> Any:
        fields = get_fields(self.schema_model)
        transformed_result: Any = result
        if len(fields) == 1:
            field_name = get_field_name(fields[0])
            transformed_result = result.get(field_name)
        if transformed_result is not None:
            result.pop("error", None)
            result.pop("index", None)
            return transformed_result
        else:
            raise BadResponseError("There was no result.", json.dumps(result))
