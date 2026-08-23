"""In-memory stand-in for ``motor`` / ``pymongo`` collections.

Implements the subset of the async MongoDB API that the framework uses:
``insert_one``, ``insert_many``, ``update_many``, ``bulk_write``, ``find``,
``find_one``, ``count_documents``, ``aggregate`` (``$match`` / ``$group`` /
``$sort`` / ``$limit`` supported).

All operations are synchronous in memory but exposed as async to match the
real driver interface. Filtering supports a pragmatic subset of the Mongo
query language: equality, ``$gte`` / ``$lte`` / ``$gt`` / ``$lt`` range
operators, ``$in``, and ``$regex``.
"""

from __future__ import annotations

import asyncio
import copy
import re
import uuid
from typing import Any


def _matches(doc: dict[str, Any], query: dict[str, Any]) -> bool:
    """Return True when ``doc`` satisfies the (subset) Mongo ``query``."""
    for key, cond in query.items():
        if key == "$and":
            if not all(_matches(doc, sub) for sub in cond):
                return False
            continue
        if key == "$or":
            if not any(_matches(doc, sub) for sub in cond):
                return False
            continue

        value = doc.get(key)
        if isinstance(cond, dict) and any(k.startswith("$") for k in cond):
            for op, operand in cond.items():
                if op == "$eq":
                    if value != operand:
                        return False
                elif op == "$ne":
                    if value == operand:
                        return False
                elif op == "$gte":
                    if value is None or value < operand:
                        return False
                elif op == "$lte":
                    if value is None or value > operand:
                        return False
                elif op == "$gt":
                    if value is None or value <= operand:
                        return False
                elif op == "$lt":
                    if value is None or value >= operand:
                        return False
                elif op == "$in":
                    if value not in operand:
                        return False
                elif op == "$nin":
                    if value in operand:
                        return False
                elif op == "$regex":
                    if value is None or re.search(operand, str(value)) is None:
                        return False
                else:  # pragma: no cover - unknown operator
                    raise ValueError(f"Unsupported query operator: {op}")
            continue

        # Plain equality
        if value != cond:
            return False
    return True


class FakeCursor:
    """Minimal async cursor over an in-memory result list.

    Mirrors ``motor``'s ``AsyncIOMotorCursor`` where ``await collection.find()``
    returns the cursor itself (awaitable) and ``.to_list()`` materialises it.
    """

    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def __await__(self) -> Any:
        # ``await collection.find(...)`` yields the cursor itself (motor semantics).
        # Yield a zero-delay sleep so asyncio treats this as a valid awaitable.
        yield from asyncio.sleep(0).__await__()
        return self

    def sort(self, key: str, direction: int = 1) -> FakeCursor:
        self._docs.sort(key=lambda d: d.get(key, 0), reverse=direction < 0)
        return self

    def limit(self, n: int) -> FakeCursor:
        self._docs = self._docs[:n]
        return self

    async def to_list(self, length: int | None = None) -> list[dict[str, Any]]:
        if length is None:
            return copy.deepcopy(self._docs)
        return copy.deepcopy(self._docs[:length])


class FakeMongoCollection:
    """In-memory collection with the async driver surface the framework uses."""

    def __init__(self, name: str = "collection") -> None:
        self.name = name
        self._docs: list[dict[str, Any]] = []

    # -- write ops ---------------------------------------------------------- #
    async def insert_one(self, document: dict[str, Any]) -> Any:
        doc = copy.deepcopy(document)
        if "_id" not in doc:
            doc["_id"] = uuid.uuid4()
        self._docs.append(doc)
        return type("InsertOneResult", (), {"inserted_id": doc["_id"]})()

    async def insert_many(self, documents: list[dict[str, Any]]) -> Any:
        for doc in documents:
            await self.insert_one(doc)
        return type("InsertManyResult", (), {"inserted_ids": [d["_id"] for d in self._docs[-len(documents) :]]})()

    async def update_one(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> Any:
        matched = [d for d in self._docs if _matches(d, filter)]
        modified = 0
        if not matched and upsert:
            new_doc = copy.deepcopy(filter)
            for k, v in update.get("$set", {}).items():
                new_doc[k] = v
            await self.insert_one(new_doc)
            modified = 1
        else:
            for d in matched:
                for k, v in update.get("$set", {}).items():
                    d[k] = v
                modified = len(matched)
        return type("UpdateResult", (), {"modified_count": modified, "matched_count": len(matched)})()

    async def update_many(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> Any:
        return await self.update_one(filter, update, upsert)

    async def delete_many(self, filter: dict[str, Any]) -> Any:
        before = len(self._docs)
        self._docs = [d for d in self._docs if not _matches(d, filter)]
        return type("DeleteResult", (), {"deleted_count": before - len(self._docs)})()

    async def bulk_write(self, operations: list[Any]) -> Any:
        """Support ``UpdateOne`` / ``InsertOne`` operation objects."""
        for op in operations:
            if hasattr(op, "filter") and hasattr(op, "_doc"):
                # UpdateOne
                await self.update_one(op.filter, {"$set": op._doc}, upsert=True)
            elif hasattr(op, "_doc"):
                # InsertOne
                await self.insert_one(op._doc)
        return type("BulkWriteResult", (), {"acknowledged": True})()

    # -- read ops ----------------------------------------------------------- #
    def find(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
    ) -> FakeCursor:
        docs = [d for d in self._docs if _matches(d, filter or {})]
        if projection:
            included = [k for k, v in projection.items() if v]
            docs = [{k: d[k] for k in included if k in d} for d in docs]
        return FakeCursor(docs)

    async def find_one(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        docs = [d for d in self._docs if _matches(d, filter or {})]
        if not docs:
            return None
        doc = copy.deepcopy(docs[0])
        if projection:
            doc = {k: v for k, v in doc.items() if k in projection}
        return doc

    async def count_documents(self, filter: dict[str, Any] | None = None) -> int:
        return len([d for d in self._docs if _matches(d, filter or {})])

    async def aggregate(self, pipeline: list[dict[str, Any]]) -> FakeCursor:
        docs = copy.deepcopy(self._docs)
        for stage in pipeline:
            if "$match" in stage:
                docs = [d for d in docs if _matches(d, stage["$match"])]
            elif "$sort" in stage:
                for field, direction in stage["$sort"].items():
                    docs.sort(key=lambda d: d.get(field, 0), reverse=direction < 0)
            elif "$limit" in stage:
                docs = docs[: stage["$limit"]]
            elif "$group" in stage:
                docs = self._aggregate_group(docs, stage["$group"])
            elif "$project" in stage:
                docs = [{k: d.get(v, d.get(k)) for k, v in stage["$project"].items() if v} for d in docs]
        return FakeCursor(docs)

    @staticmethod
    def _aggregate_group(docs: list[dict[str, Any]], spec: dict[str, Any]) -> list[dict[str, Any]]:
        group_id = spec.get("_id")
        groups: dict[Any, list[dict[str, Any]]] = {}
        for d in docs:
            if isinstance(group_id, str):
                # Mongo `$group` `_id: "$field"` references the field value.
                field = group_id[1:] if group_id.startswith("$") else group_id
                key = d.get(field)
            elif isinstance(group_id, dict):
                key = tuple(d.get(g[1:] if g.startswith("$") else g) for g in group_id.values())
            else:
                key = None
            groups.setdefault(key, []).append(d)
        result: list[dict[str, Any]] = []
        for key, members in groups.items():
            out: dict[str, Any] = {"_id": key}
            for field, op_spec in spec.items():
                if field == "_id":
                    continue
                if "$sum" in op_spec:
                    operand = op_spec["$sum"]
                    if operand == 1:
                        out[field] = len(members)
                    else:
                        col = operand[1:] if isinstance(operand, str) and operand.startswith("$") else operand
                        out[field] = sum(m.get(col, 0) for m in members)
                elif "$max" in op_spec:
                    col = op_spec["$max"]
                    out[field] = max((m.get(col, 0) for m in members), default=0)
                elif "$min" in op_spec:
                    col = op_spec["$min"]
                    out[field] = min((m.get(col, 0) for m in members), default=0)
                elif "$avg" in op_spec:
                    col = op_spec["$avg"]
                    vals = [m.get(col, 0) for m in members]
                    out[field] = sum(vals) / len(vals) if vals else 0
            result.append(out)
        return result


class FakeMongoDB:
    """In-memory database: attribute access returns a named ``FakeMongoCollection``."""

    def __init__(self, db_name: str = "prefect") -> None:
        self.db_name = db_name
        self._collections: dict[str, FakeMongoCollection] = {}

    def __getattr__(self, name: str) -> FakeMongoCollection:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._collections:
            self._collections[name] = FakeMongoCollection(name)
        return self._collections[name]

    def close(self) -> None:  # pragma: no cover - parity with real driver
        pass
