"""Smoke tests for the shared test fakes (M6.1 infrastructure)."""

from __future__ import annotations

import pytest

from tests.conftest import ToyItem
from tests.fakes import FakeLLM, FakeMongoDB, FakeQdrantClient, FakeTransport


async def test_fake_mongo_crud():
    db = FakeMongoDB()
    await db.items.insert_one({"url": "https://x.com", "score": 5})
    await db.items.insert_one({"url": "https://y.com", "score": 9})
    assert await db.items.count_documents({}) == 2
    found = await db.items.find_one({"score": {"$gte": 9}})
    assert found is not None and found["url"] == "https://y.com"
    await db.items.update_many({"score": {"$lt": 10}}, {"$set": {"flag": True}})
    flagged = await db.items.count_documents({"flag": True})
    assert flagged == 2


async def test_fake_mongo_aggregate_group():
    db = FakeMongoDB()
    for i in range(3):
        await db.posts.insert_one({"project": "a", "score": i})
    await db.posts.insert_one({"project": "b", "score": 7})
    cursor = await db.posts.aggregate([{"$group": {"_id": "$project", "total": {"$sum": "$score"}}}])
    groups = {g["_id"]: g["total"] for g in await cursor.to_list()}
    assert groups == {"a": 0 + 1 + 2, "b": 7}


async def test_fake_qdrant_upsert_query():
    client = FakeQdrantClient()
    await client.get_or_create_collection("emb")
    await client.upsert("emb", [type("P", (), {"id": 1, "vector": [1.0, 0.0], "payload": {"u": "x"}})()])
    results = await client.query("emb", [1.0, 0.0], limit=1)
    assert len(results) == 1
    assert results[0].payload["u"] == "x"


async def test_fake_llm_queued_responses():
    llm = FakeLLM()
    llm.queue_text("first", "second")
    r1 = await llm.acompletion(messages=[])
    r2 = await llm.acompletion(messages=[])
    assert "first" in r1.choices[0].message.content
    assert "second" in r2.choices[0].message.content
    assert llm.call_count == 2


async def test_fake_llm_raises_scripted_error():
    llm = FakeLLM()
    llm.queue_error(RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        await llm.acompletion(messages=[])


def test_fake_transport_routing():
    transport = FakeTransport()
    transport.add_get("https://api.test/health", "ok")
    resp = transport.handler(type("Req", (), {"method": "GET", "url": "https://api.test/health"})())
    assert resp.text == "ok"


def test_toy_item_fixture_shape():
    item = ToyItem(url="https://x.com", title="T", score=2)
    assert item.url == "https://x.com"
    assert item.score == 2
