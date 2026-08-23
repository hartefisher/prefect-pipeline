"""Toy fixtures for M5 runner tests.

These provide minimal, dependency-free stand-ins for the real framework
components so runner registration / injection / timezone behavior can be
exercised without MongoDB, Qdrant or any network access.
"""
from __future__ import annotations

from typing import Any

from prefect_pipeline.components.data import DataFetcher, DataTransformer
from prefect_pipeline.components.llm import GenericExtractor
from prefect_pipeline.components.spider import HTTPSpider, SpiderBase
from prefect_pipeline.models import BaseItem


class ToyItem(BaseItem):
    """Generic custom item used by toy runners."""

    title: str = ""
    score: int = 0


# --------------------------------------------------------------------------- #
# Toy LLM config (stand-in for a CompletionConfig subclass)
# --------------------------------------------------------------------------- #
class ToyLLMConfig:
    """Minimal LLM config shape consumed by GenericExtractor / ReasoningFlow."""

    def __init__(self, *, batch: bool = False) -> None:
        self.batch = batch
        self.output_field = "toy_content"
        self.override = False
        self.limit = 0
        self.concurrent_requests = 1
        self.model = "toy-model"
        self.model_name = "toy-model"
        self.model_version = None
        self.batch_config: dict[str, Any] = {"model": self.model}


# --------------------------------------------------------------------------- #
# Toy components
# --------------------------------------------------------------------------- #
class ToyDataFetcher(DataFetcher[ToyItem]):
    """A DataFetcher that yields no documents (no DB needed)."""

    async def get_batch_items(self, **kwargs: Any):
        if False:  # pragma: no cover - async generator that yields nothing
            yield []

    async def get_items(self, **kwargs: Any) -> list[ToyItem]:
        return []

    async def close(self) -> None:
        pass


class ToyDataTransformer(DataTransformer[ToyItem]):
    """A DataTransformer that reports an empty collection (no DB needed)."""

    collection: Any = None  # type: ignore[assignment]
    output_collection: Any = None  # type: ignore[assignment]
    output_mode: str = "out"

    async def run(self, **kwargs: Any) -> int:
        return 0

    async def close(self) -> None:
        pass


class ToyEmbeddingHandler:
    """Stand-in EmbeddingHandler exposing the attributes runners touch."""

    collection_name = "toy_embeddings"

    def __init__(self, **extra: Any) -> None:
        self.extra = extra

    async def run(self, items: list[Any]) -> int:
        return len(items)

    async def close(self) -> None:
        pass


class ToyExtractor(GenericExtractor[ToyItem]):
    """A GenericExtractor wired with a ToyLLMConfig (no real LLM calls)."""

    ns = "toy"

    def __init__(
        self,
        llm_config: Any = None,
        *,
        strategy: Any = None,
        batch_id: str | None = None,
        **extra: Any,
    ) -> None:
        cfg = llm_config or ToyLLMConfig()
        super().__init__(cfg, strategy=strategy, batch_id=batch_id, **extra)


class ToySpider(SpiderBase[ToyItem]):
    """A SpiderBase whose crawl is a no-op (no browser / network needed)."""

    limit: int = 0
    concurrent_requests: int = 1

    def __init__(self, batch_id: str | None = None, **kwargs: Any) -> None:
        self.batch_id = batch_id

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def crawl(self, item: ToyItem) -> Any:
        return None

    async def run(self, item: ToyItem) -> tuple[Any, Any]:
        return None, None


class ToyHTTPSpider(HTTPSpider[ToyItem], ToySpider):
    """HTTPSpider variant reusing ToySpider's no-op crawl."""
