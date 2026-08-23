from unittest.mock import AsyncMock, MagicMock

import numpy as np

from prefect_pipeline.components.vector import AsyncQdrantClient, EmbeddingModel
from prefect_pipeline.models import Point


def test_async_qdrant_client_get_or_create_collection_existing():
    client = AsyncQdrantClient(url="http://localhost:6333")
    client.get_collection = AsyncMock(return_value="existing")
    client.create_collection = AsyncMock()

    async def run():
        return await client.get_or_create_collection("coll")

    import asyncio

    result = asyncio.run(run())
    assert result == "existing"
    client.create_collection.assert_not_awaited()


def test_async_qdrant_client_get_or_create_collection_creates():
    client = AsyncQdrantClient(url="http://localhost:6333")
    client.get_collection = AsyncMock(side_effect=Exception("missing"))
    client.create_collection = AsyncMock(return_value="created")

    import asyncio

    result = asyncio.run(client.get_or_create_collection("coll"))
    assert result == "created"
    client.create_collection.assert_awaited_once()


def test_embedding_model_struct_points_uses_mock_model():
    model = EmbeddingModel.__new__(EmbeddingModel)
    fake_embed = MagicMock()
    fake_embed.embed.side_effect = lambda t, **kw: iter(
        [np.array([0.1, 0.2]), np.array([0.3, 0.4])]
    )
    model.model = fake_embed
    model.parallel = None

    points = [
        Point(text="a", id=1, payload={"k": "v"}),
        Point(text="b", id=2, payload={}),
    ]
    structs = model.struct_points(points, batch_size=64)
    assert len(structs) == 2
    assert structs[0].id == 1
    assert structs[0].payload == {"k": "v"}
    assert structs[0].vector == [0.1, 0.2]
    assert structs[1].vector == [0.3, 0.4]


def test_embedding_model_embed_single():
    model = EmbeddingModel.__new__(EmbeddingModel)
    fake_embed = MagicMock()
    fake_embed.embed.side_effect = lambda t, **kw: iter([np.array([0.5, 0.6, 0.7])])
    model.model = fake_embed
    vec = model.embed("hello")
    assert vec == [0.5, 0.6, 0.7]
