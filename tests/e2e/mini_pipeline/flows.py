"""Toy mini-pipeline used by the E2E integration test (M6.3).

Three toy flows emulate a realistic ``fetch -> transform -> embed`` DAG and
exercise the framework's runner + Prefect task machinery end-to-end on top of
the in-memory fakes. No external service (Mongo / Qdrant / LLM) is touched.
"""

from __future__ import annotations

from typing import Any

from prefect import task

from prefect_pipeline.runners.base import PipelineFlow


class ToyFetch(PipelineFlow):
    """Produce three toy items and persist them to a fake collection."""

    def setup(self, *, collection: Any, **extra: Any) -> None:
        self.collection = collection

    @task(cache_policy=None)  # type: ignore[call-overload]
    async def fetch(self) -> int:
        docs = [{"url": f"https://x.com/{i}", "title": f"t{i}", "score": i} for i in range(3)]
        for d in docs:
            await self.collection.insert_one(d)
        return len(docs)

    async def run(self) -> None:
        await self.fetch()  # type: ignore[call-overload]


class ToyTransform(PipelineFlow):
    """Read raw items and upsert a transformed copy into another collection."""

    def setup(self, *, src: Any, dst: Any, **extra: Any) -> None:
        self.src = src
        self.dst = dst

    @task(cache_policy=None)  # type: ignore[call-overload]
    async def transform(self) -> int:
        cursor = await self.src.find({})
        docs = await cursor.to_list()
        count = 0
        for d in docs:
            d["transformed"] = True
            await self.dst.update_one({"url": d["url"]}, {"$set": d}, upsert=True)
            count += 1
        return count

    async def run(self) -> None:
        await self.transform()  # type: ignore[call-overload]


class ToyEmbed(PipelineFlow):
    """Read transformed items and upsert a vector per item into fake Qdrant."""

    def setup(self, *, src: Any, qdrant: Any, collection_name: str, **extra: Any) -> None:
        self.src = src
        self.qdrant = qdrant
        self.collection_name = collection_name

    @task(cache_policy=None)  # type: ignore[call-overload]
    async def embed(self) -> int:
        await self.qdrant.get_or_create_collection(self.collection_name)
        cursor = await self.src.find({})
        docs = await cursor.to_list()
        points = [type("P", (), {"id": i, "vector": [float(i), 0.0, 0.0], "payload": d})() for i, d in enumerate(docs)]
        await self.qdrant.upsert(self.collection_name, points)
        return len(points)

    async def run(self) -> None:
        await self.embed()  # type: ignore[call-overload]
