from unittest.mock import AsyncMock, MagicMock

from tests.unit.components.conftest import ToyItem

from prefect_pipeline.components.data import DataFetcher, DataTransformer
from prefect_pipeline.components.helper import UrlSet


class ToyFetcher(DataFetcher[ToyItem]):
    collection: MagicMock  # type: ignore[assignment]


class ToyTransformer(DataTransformer[ToyItem]):
    collection: MagicMock  # type: ignore[assignment]


class _AsyncCursor:
    """Minimal async-iterable + to_list cursor mock for motor collections."""

    def __init__(self, docs):
        self._docs = docs

    def to_list(self, *args, **kwargs):
        return AsyncMock(return_value=self._docs)()

    def batch_size(self, n):
        return self

    def __aiter__(self):
        async def gen():
            for d in self._docs:
                yield d

        return gen()


def _make_collection(docs):
    coll = MagicMock()
    cursor = _AsyncCursor(docs)
    coll.find.return_value = cursor
    coll.aggregate.return_value = cursor
    return coll


def test_data_fetcher_wrap_item_builds_models():
    fetcher = ToyFetcher()
    items = fetcher.wrap_item([{"url": "u1", "title": "a"}, {"url": "u2", "title": "b"}])
    assert len(items) == 2
    assert all(isinstance(i, ToyItem) for i in items)
    assert items[0].title == "a"


def test_data_fetcher_wrap_item_none_returns_empty():
    fetcher = ToyFetcher()
    assert fetcher.wrap_item(None) == []


async def test_data_fetcher_get_items():
    docs = [{"url": "u1", "title": "a"}]
    coll = _make_collection(docs)
    fetcher = ToyFetcher()
    fetcher.collection = coll
    items = await fetcher.get_items()
    assert items[0].url == "u1"


async def test_data_transformer_run_calls_bulk_write():
    docs = [{"url": "u1", "title": "a"}, {"url": "u2", "title": "b"}]
    coll = _make_collection(docs)
    coll.index_information = AsyncMock(return_value={})
    coll.create_index = AsyncMock()
    coll.delete_many = AsyncMock()
    coll.bulk_write = AsyncMock(return_value=MagicMock(modified_count=2))

    transformer = ToyTransformer()
    transformer.collection = coll
    transformer.output_collection = coll
    transformer.update_op = UrlSet
    count = await transformer.run()
    assert count == 2
    coll.bulk_write.assert_awaited()


async def test_data_transformer_ensure_unique_dedupes():
    docs = [{"url": "u1", "title": "a"}, {"url": "u1", "title": "a2"}]
    coll = _make_collection(docs)
    coll.index_information = AsyncMock(return_value={})
    coll.create_index = AsyncMock()
    coll.bulk_write = AsyncMock(return_value=MagicMock(modified_count=1))

    transformer = ToyTransformer()
    transformer.collection = coll
    transformer.output_collection = coll
    transformer.ensure_unique = True
    # Manually drive process to confirm dedup key tracking
    transformer.unique_keys = set()
    r1 = await transformer.process(ToyItem(url="u1", title="a"))
    r2 = await transformer.process(ToyItem(url="u1", title="a2"))
    assert len(r1) == 1
    assert r2 == []  # duplicate index key skipped
