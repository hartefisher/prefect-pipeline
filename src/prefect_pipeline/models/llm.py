import re
from collections.abc import Mapping
from typing import Any, Literal

import litellm
from litellm import (
    AnthropicThinkingParam,
    ChatCompletionAudioParam,
    ChatCompletionModality,
    ChatCompletionPredictionContentParam,
    OpenAIWebSearchOptions,
)
from pydantic import BaseModel, Field, computed_field

litellm.drop_params = True


class CompletionConfig(BaseModel):
    """Generic LLM completion configuration shape.

    The framework only provides the *shape* of a completion config. Concrete
    model instances (e.g. provider-specific presets) are declared by the user
    project and injected into extraction strategies. No model instances are
    pre-built by the framework.
    """

    model: str = Field(..., exclude=True)
    openai_compatible: bool = Field(default=False, exclude=True)
    batch: bool = Field(default=False, exclude=True)
    functions: list[Any] | None = None
    function_call: str | None = None
    timeout: float | int | None = None
    temperature: float | None = None
    top_p: float | None = None
    n: int | None = None
    stream: bool | None = None
    stream_options: dict[str, Any] | None = None
    stop: str | None = None
    max_tokens: int | None = None
    max_completion_tokens: int | None = None
    modalities: list[ChatCompletionModality] | None = None
    prediction: ChatCompletionPredictionContentParam | None = None
    audio: ChatCompletionAudioParam | None = None
    presence_penalty: float | None = None
    frequency_penalty: float | None = None
    logit_bias: dict[str, Any] | None = None
    user: str | None = None
    response_format: dict[str, Any] | type[BaseModel] | None = None
    seed: int | None = None
    tools: list[Any] | None = None
    tool_choice: str | dict[str, Any] | None = None
    parallel_tool_calls: bool | None = None
    logprobs: bool | None = None
    top_logprobs: int | None = None
    deployment_id: str | None = None
    reasoning_effort: Literal["none", "minimal", "low", "medium", "high", "xhigh", "default"] | None = None
    verbosity: Literal["low", "medium", "high"] | None = None
    safety_identifier: str | None = None
    service_tier: str | None = None
    base_url: str | None = None
    api_version: str | None = None
    api_key: str | None = None
    model_list: list[Any] | None = None
    extra_headers: dict[str, Any] | None = None
    thinking: AnthropicThinkingParam | None = None
    web_search_options: OpenAIWebSearchOptions | None = None
    extra_body: dict[str, Any] | None = None

    @computed_field
    @property
    def provider(self) -> str:
        return self.__class__.__name__.lower()

    @computed_field
    @property
    def model_modifier(self) -> str:
        return self.provider + "/" + self.model

    def copy_config(self, by_alias: bool | None = None, update: Mapping[str, Any] | None = None) -> dict[str, Any]:
        exclude = {"api_key", "base_url", "openai_compatible"}
        config = self.model_dump(
            exclude_none=True,
            exclude_computed_fields=True,
            by_alias=by_alias,
            exclude=exclude,
        )

        for n, f in self.__class__.model_fields.items():
            if f.exclude and n not in exclude:
                name = f.serialization_alias or n if by_alias else n
                if (value := getattr(self, name, None)) is not None:
                    config[name] = value

        return {**config, **(update or {})}

    def export(self, exclude: set[str] | None = None, encrypted: bool = True) -> dict[str, Any]:
        config = self.model_dump(
            exclude_none=True,
            exclude=exclude,
            exclude_computed_fields=True,
        )
        provider = "openai" if self.openai_compatible else self.provider
        config["model"] = provider + "/" + self.model

        if encrypted and (api_key := config.get("api_key")):
            config["api_key"] = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:]
        return config

    @computed_field
    @property
    def debug_key(self) -> str:
        provider_mapping = {
            "doubao-seed": "db",
            "gemini": "gm",
            "deepseek": "ds",
            "qwen": "qw",
            "claude-sonnet": "cl_s",
            # "mimo": "mi",
            "minimax": "mm",
            "kimi": "km",
        }
        ps = []
        model = self.model.lower().replace(".", "-").split("/")[-1]
        for provider, value in provider_mapping.items():
            if model.startswith(provider):
                ps.append(value)
                model = model.replace(provider, "")
                break

        if model.endswith("preview"):
            model = model.replace("preview", "")

        if s := re.search(r"([\d]{6,}$)", model):
            model = model.replace(s.group(), "")

        ps += filter(None, model.split("-"))

        enable_thinking = self.extra_body.get("enable_thinking") if self.extra_body else None
        enable_thinking_a = self.thinking.get("type") if self.thinking else None

        if enable_thinking is False:
            pass
        elif self.reasoning_effort:
            ps.append(self.reasoning_effort[:2])
        elif enable_thinking is True or enable_thinking_a == "enabled":
            ps.append("th")

        if self.temperature:
            ps.append(str(self.temperature).replace(".", ""))

        return "_".join(ps)

    @computed_field
    @property
    def model_name(self) -> str:
        return re.sub(r"-[\d]{6,}$", "", self.model)

    @computed_field
    @property
    def model_version(self) -> str | None:
        if s := re.search(r"-([\d]{6,}$)", self.model):
            return s.groups()[0]
        return None
