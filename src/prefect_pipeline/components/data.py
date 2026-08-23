import asyncio
from collections.abc import (
    AsyncGenerator,
    Mapping,
    Sequence,
)
from functools import cached_property
from inspect import isclass
from typing import (
    Any,
    ClassVar,
    Literal,
    NotRequired,
    TypedDict,
    Unpack,
    cast,
)

from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import BaseModel
from qdrant_client import models
from qdrant_client.models import ScoredPoint

from prefect_pipeline.components.helper import (
    AutoItemModel,
    ExtraContextMixin,
    IncOp,
    UpdateOp,
    UrlSet,
    bulk_write,
)
from prefect_pipeline.core.configs import INCREMENTAL_TRANSFORMATION
from prefect_pipeline.infra.db import DB
from prefect_pipeline.infra.exceptions import ItemModelMissing
from prefect_pipeline.models import BaseItem, Point


class QueryDict(TypedDict):
    filter: NotRequired[dict[str, Any]]
    sort: NotRequired[dict[str, Any]]
    skip: NotRequired[int]
    limit: NotRequired[int]
    batch_size: NotRequired[int]
    projection: NotRequired[dict[str, Any]]


class DataFetcher[OutputItem: BaseModel](AutoItemModel[OutputItem]):
    collection: AsyncIOMotorCollection[dict[str, Any]]
    filter: ClassVar[dict[str, Any]] = {}
    sort: ClassVar[dict[str, Any]] = {}
    batch_size: int | None = None
    pipeline: ClassVar[list[dict[str, Any]]] = []
    output_collection: AsyncIOMotorCollection[dict[str, Any]] | None = None
    output_mode: Literal["merge", "out", "none"] = "none"

    def query(self, **kwargs: Unpack[QueryDict]) -> Any:
        pipeline = self.construct_pipeline(**kwargs)
        cursor: Any
        if isinstance(pipeline, Mapping):
            cursor = self.collection.find(**pipeline)
        else:
            cursor = self.collection.aggregate(pipeline)
        return cursor

    def wrap_item(self, data: list[dict[str, Any]] | None) -> list[OutputItem]:
        if isclass(self.item_model) and issubclass(self.item_model, BaseModel):
            if data is None:
                return []
            return [
                cast(type[OutputItem], self.item_model)(**item)
                for item in data
                if isinstance(item, dict)
            ]
        else:
            raise ItemModelMissing("Please add item type to class annotion.")

    async def get_batch_data(
        self, **kwargs: Unpack[QueryDict]
    ) -> AsyncGenerator[list[dict[str, Any]], Any]:
        batch_size = kwargs.pop("batch_size", self.batch_size)
        cursor = self.query(**kwargs)

        if self.output_mode == "none":
            if batch_size:
                batch: list[dict[str, Any]] = []
                cursor = cursor.batch_size(batch_size)
                async for doc in cursor:
                    batch.append(doc)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                yield batch
            else:
                data: list[dict[str, Any]] = await cursor.to_list()
                yield data
        else:
            await cursor.to_list()

    async def get_batch_items(
        self, **kwargs: Unpack[QueryDict]
    ) -> AsyncGenerator[list[OutputItem], Any]:
        batches = self.get_batch_data(**kwargs)
        async for batch in batches:
            yield self.wrap_item(batch)

    async def get_data(self, **kwargs: Unpack[QueryDict]) -> Any:
        cursor = self.query(**kwargs)
        data = await cursor.to_list()

        if self.output_mode == "none":
            return data

    async def get_items(self, **kwargs: Unpack[QueryDict]) -> list[OutputItem]:
        data = await self.get_data(**kwargs)
        return self.wrap_item(data)

    def construct_pipeline(
        self, **kwargs: Unpack[QueryDict]
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        if self.pipeline:
            if self.output_collection is not None and self.output_mode == "out":
                if not self.sort:
                    return [*self.pipeline, {"$out": self.output_collection.name}]
                return [*self.pipeline, {"$sort": self.sort}, {"$out": self.output_collection.name}]
            return self.pipeline
        else:
            filter = {**self.filter, **kwargs.pop("filter", {})}
            projection = (
                kwargs["projection"]
                if "projection" in kwargs
                else {k: 1 for k in self.item_model.model_fields}
            )
            sort = {**self.sort, **kwargs.pop("sort", {})}
            return {
                "filter": filter,
                "projection": {"_id": 0, **projection},
                "sort": sort,
                **kwargs,
            }

    async def close(self) -> None:
        DB.close()


class DataTransformer[InputItem: BaseModel](DataFetcher[InputItem], ExtraContextMixin):
    bulk_write_size: int = 100
    ensure_unique: bool = True
    check_index: bool = True
    update_op: type[UpdateOp] = UrlSet
    output_mode = "none"
    concurrent_requests: int = 30
    batch_size: int | None = 300

    def __init__(self, batch_id: str | None = None, **kwargs: Any) -> None:
        self.batch_id = batch_id
        self.unique_keys: set[Any] = set()
        self.processed_count = 0
        self.update_extra_context(**kwargs)

    async def process_item(
        self, item: InputItem
    ) -> AsyncGenerator[UpdateOp | dict[str, Any], Any]:
        if isinstance(item, BaseModel):
            yield self.update_op(
                item.model_dump(exclude_computed_fields=True, exclude_unset=True),
            )
        else:
            yield self.update_op(item)

    async def _check_index(
        self, output_collection: AsyncIOMotorCollection[dict[str, Any]]
    ) -> None:
        if not self.check_index or not self.ensure_unique:
            return

        index_info = await output_collection.index_information()
        index_name = f"{'_'.join(self.update_op.index_keys)}_index"
        if index_name not in index_info:
            print(f"Create index: {index_name}")
            await output_collection.create_index(
                [(idx, 1) for idx in self.update_op.index_keys],
                unique=True,
                name=index_name,
            )

    async def setup(
        self, output_collection: AsyncIOMotorCollection[dict[str, Any]]
    ) -> None:
        if (
            self.output_collection is not None
            and issubclass(self.update_op, IncOp)
            and INCREMENTAL_TRANSFORMATION == "false"
        ):
            await self.output_collection.delete_many({})

        await self._check_index(output_collection)

    async def run(self, **kwargs: Unpack[QueryDict]) -> int:
        output_collection = (
            self.collection
            if self.output_collection is None
            else self.output_collection
        )

        await self.setup(output_collection)
        transformed_data = self.transform(**kwargs)
        upserted_count = await bulk_write(
            output_collection,
            transformed_data,
            self.bulk_write_size,
            "one" if self.ensure_unique else "many",
        )
        return upserted_count

    async def process(self, item: InputItem) -> list[UpdateOp]:
        fetched_items: list[UpdateOp] = []
        async for fetched_item in self.process_item(item):
            if not isinstance(fetched_item, UpdateOp):
                fetched_item = self.update_op(fetched_item)
            index_value = tuple(fetched_item.filter.values())
            if index_value is None or (
                self.ensure_unique and index_value in self.unique_keys
            ):
                continue
            self.unique_keys.add(index_value)

            if self.batch_id:
                fetched_item._data["batch_id"] = self.batch_id

            fetched_items.append(fetched_item)

        return fetched_items

    async def process_concurrently(
        self, semaphore: asyncio.Semaphore, item: InputItem
    ) -> list[UpdateOp]:
        async with semaphore:
            return await self.process(item)

    async def transform(
        self, **kwargs: Unpack[QueryDict]
    ) -> AsyncGenerator[UpdateOp, Any]:
        batch = self.get_batch_items(**kwargs)
        async for items in batch:
            if self.concurrent_requests > 1:
                semaphore = asyncio.Semaphore(self.concurrent_requests)
                results = await asyncio.gather(
                    *[self.process_concurrently(semaphore, item) for item in items]
                )
                for result in results:
                    for fetched_item in result:
                        yield fetched_item
            else:
                for item in items:
                    for fetched_item in await self.process(item):
                        yield fetched_item
            self.processed_count += len(items)
            print(f"processed count: {self.processed_count}.")


class EmbeddingHandler[Item: BaseItem](ExtraContextMixin):
    collection_name: str
    max_seq_length: int | None = None
    multi_process: bool | int = False
    chunk_size: int = 512  # 1024
    batch_size: int = 64  # 128

    def __init__(self, **kwargs: Any) -> None:
        from prefect_pipeline.components.vector import AsyncQdrantClient, EmbeddingModel

        self.qdrant = AsyncQdrantClient()
        self.model = EmbeddingModel(
            "BAAI/bge-base-en-v1.5",
            max_seq_length=self.max_seq_length,
            multi_process=self.multi_process,
        )
        self.inited = False
        self.update_extra_context(**kwargs)

    async def process_item(self, item: Item) -> Point | None:
        pass

    async def run(self, items: list[Item]) -> int:
        if not self.inited:
            await self.qdrant.get_or_create_collection(self.collection_name)
            self.inited = True
        total = 0
        points: list[Point] = []
        print("start to struct points.")
        for item in items:
            point = await self.process_item(item)
            if point is None:
                continue
            points.append(point)
            total += 1
        structed_points = self.model.struct_points(points, batch_size=self.batch_size)

        print("start to upsert points.")
        await self.qdrant.upsert(
            collection_name=self.collection_name,
            points=structed_points,
        )
        print(f"Embedding progress: {total}")
        return total

    async def close(self) -> None:
        await self.qdrant.close()
        self.model.clear()


class SimilarityStats[InputItem: BaseModel](DataTransformer[InputItem]):
    embedding_collection: str
    max_seq_length: int | None = None
    multi_process: bool | int = False

    def __init__(self, batch_id: str | None = None, **kwargs: Any) -> None:
        from prefect_pipeline.components.vector import AsyncQdrantClient, EmbeddingModel

        self.qdrant = AsyncQdrantClient()
        self.model = EmbeddingModel(
            "BAAI/bge-base-en-v1.5",
            max_seq_length=self.max_seq_length,
            multi_process=self.multi_process,
        )
        super().__init__(batch_id, **kwargs)

    @cached_property
    def vectors(self) -> dict[str, list[ScoredPoint]]:
        return {}

    def text_to_embed(self, item: InputItem) -> str:
        raise NotImplementedError("Must implement method text_to_embed.")

    def embed_key(self, item: InputItem) -> str:
        raise NotImplementedError("Must implement method embed_key.")

    async def get_batch_items(
        self, **kwargs: Unpack[QueryDict]
    ) -> AsyncGenerator[list[InputItem], Any]:
        batch = super().get_batch_items(**kwargs)

        async for items in batch:
            texts_to_embed = []
            valid_items = []
            print("start to embed texts.")
            for item in items:
                if self.embed_key(item) in self.vectors:
                    continue
                if text := self.text_to_embed(item):
                    texts_to_embed.append(text)
                    valid_items.append(item)
            vectors: list[list[float]] = self.model.embed_batch(
                texts_to_embed, batch_size=128
            )
            print("start to query points.")
            query_responses = await self.query_batch_points(valid_items, vectors)
            print("points query completed.")
            for item, query_response in zip(valid_items, query_responses):
                self.vectors[self.embed_key(item)] = query_response.points
            yield items

    async def construct_query(self, item: InputItem) -> dict[str, Any]:
        return {}

    async def query_batch_points(
        self, items: list[InputItem], vectors: list[list[float]]
    ) -> Any:
        requests = []
        for item, vector in zip(items, vectors):
            extra = await self.construct_query(item)
            requests.append(models.QueryRequest(query=vector, **extra))

        batch_results = await self.qdrant.query_batch_points(
            collection_name=self.embedding_collection, requests=requests, timeout=60
        )
        return batch_results

    async def close(self) -> None:
        await super().close()
        await self.qdrant.close()
        self.model.clear()
