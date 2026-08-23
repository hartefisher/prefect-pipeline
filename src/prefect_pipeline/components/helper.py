from collections.abc import AsyncGenerator, Callable, Generator, Iterable, Mapping
from functools import cached_property
from typing import (
    Any,
    Literal,
)

from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import BaseModel
from pymongo import UpdateMany, UpdateOne

from prefect_pipeline.infra.utils import get_current_time
from prefect_pipeline.models import BaseItem

type FilterAndUpdate = tuple[dict[str, Any], dict[str, Any]]


class UpdateOp:
    index_keys: Iterable[str]
    upsert: bool = True

    def __init__(self, data: dict[str, Any]):
        self.filter: dict[str, Any] = {idx: data[idx] for idx in self.index_keys}
        self._data = data

    @property
    def update(self) -> dict[str, Any]:
        return {"$set": self._data}


class IncOp(UpdateOp):
    stamp_keys: Iterable[str] = []
    stamp_templates: Mapping[str, Callable[[], str]] = {"updated_at": get_current_time}

    @property
    def update(self) -> dict[str, Any]:
        inc: dict[str, Any] = {}
        setOnInsert: dict[str, Any] = {}
        stamp: dict[str, Any] = {}
        for sk in self.stamp_keys:
            if sk in self.stamp_templates:
                stamp[sk] = self.stamp_templates[sk]()
            elif sk in self._data:
                stamp[sk] = self._data[sk]

        for k, v in self._data.items():
            if isinstance(v, (int, float)):
                inc[k] = v
            elif self.upsert and k not in self.index_keys and k not in self.stamp_keys:
                setOnInsert[k] = v

        update: dict[str, Any] = {"$inc": inc}
        # [TODO] 既非inc也非stamp的都被归到setOnInsert，导致有些非inc的字段update失败
        if setOnInsert:
            update["$setOnInsert"] = setOnInsert
        if stamp:
            update["$set"] = stamp
        return update


class UrlSet(UpdateOp):
    index_keys = ["url"]  # noqa: RUF012


class IdSet(UpdateOp):
    index_keys = ["id"]  # noqa: RUF012


class UrlInc(IncOp):
    index_keys = ["url"]  # noqa: RUF012
    stamp_keys = ["batch_id", "updated_at"]  # noqa: RUF012


class IdInc(IncOp):
    index_keys = ["id"]  # noqa: RUF012
    stamp_keys = ["batch_id", "updated_at"]  # noqa: RUF012


async def bulk_write(
    db: AsyncIOMotorCollection[dict[str, Any]],
    ops: Iterable[UpdateOp] | AsyncGenerator[UpdateOp, Any] | Generator[UpdateOp, Any],
    bulk_write_size: int = 100,
    mode: Literal["one", "many"] = "one",
) -> int:
    requests: dict[str, list[Any]] = {"current": []}
    update_count: dict[str, int] = {"current": 0}
    Update = UpdateMany if mode == "many" else UpdateOne

    async def process(threshold: int = 1) -> None:
        if len(requests["current"]) >= threshold:
            result = await db.bulk_write(requests["current"])
            requests["current"] = []
            print(f"Updating {result.modified_count} items for {db.name}")
            update_count["current"] += result.modified_count

    try:
        if isinstance(ops, AsyncGenerator):
            async for op in ops:
                requests["current"].append(Update(op.filter, op.update, op.upsert))
                await process(bulk_write_size)
        else:
            for op in ops:
                requests["current"].append(Update(op.filter, op.update, op.upsert))
                await process(bulk_write_size)

        await process()
    except Exception as e:
        print(f"Error while updating item: {e}")
        raise e

    print(f"Updated {update_count['current']} items by total for {db.name}")
    return update_count["current"]


class PipelineHelper:
    output_collection: AsyncIOMotorCollection[dict[str, Any]] | None = None
    stats_collection: AsyncIOMotorCollection[dict[str, Any]] | None = None
    bulk_write_size: int = 100
    update_op: type[UpdateOp] = UrlSet

    def get_records(
        self, data: dict[str, Any]
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return data

    def transform_record(self, record: dict[str, Any]) -> UpdateOp | dict[str, Any]:
        return self.update_op(record)

    def generate(
        self, records: list[dict[str, Any]]
    ) -> Generator[UpdateOp, Any]:
        for record in records:
            o = self.transform_record(record)
            if not isinstance(o, UpdateOp):
                o = self.update_op(o)
            yield o

    async def save_result(self, data: dict[str, Any] | None = None) -> None:
        if self.output_collection is not None and data:
            records = self.get_records(data)
            if not isinstance(records, list):
                records = [records]

            await bulk_write(
                self.output_collection,
                self.generate(records),
                self.bulk_write_size,
            )

    async def log_stats(self, stats: dict[str, Any]) -> None:
        if self.stats_collection is not None:
            await self.stats_collection.insert_one(stats)

    @cached_property
    def collection_name(self) -> str | None:
        return None if self.output_collection is None else self.output_collection.name


class AutoItemModel[Item: BaseModel]:
    item_model: type[BaseItem] = BaseItem

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "__orig_bases__"):
            item_model = cls.__orig_bases__[0].__args__[0]
            if item_model is not cls.item_model:
                cls.item_model = item_model


class ExtraContextMixin:
    def update_extra_context(self, **extra: Any) -> None:
        for k in dir(self.__class__):
            if not k.startswith("__") and not k.endswith("__") and k in extra:
                setattr(self, k, extra[k])
