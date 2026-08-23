"""E2E integration test: a 3-flow mini pipeline on Prefect (ephemeral).

The DAG ``ToyFetch >> ToyTransform >> ToyEmbed`` is executed through the real
Prefect task runner (ephemeral mode, no external server required). Each toy
runner's ``@task`` methods run via Prefect's task engine while the runners are
chained in the framework's own ``Node.run`` execution order. The test asserts
that every flow runs to completion and that data flows correctly through the
in-memory Mongo and Qdrant fakes.

Marked ``slow`` because the ephemeral Prefect engine adds a little overhead.
"""

from __future__ import annotations

import pytest

from tests.e2e.mini_pipeline.flows import ToyEmbed, ToyFetch, ToyTransform
from tests.fakes import FakeMongoDB, FakeQdrantClient


@pytest.mark.slow
async def test_mini_pipeline_runs_end_to_end():
    db = FakeMongoDB("workflow")
    qdrant = FakeQdrantClient()

    fetch = ToyFetch()
    transform = ToyTransform()
    embed = ToyEmbed()

    # fetch -> raw
    fetch.setup(collection=db.raw)
    await fetch.start()

    # transform: raw -> processed
    transform.setup(src=db.raw, dst=db.processed)
    await transform.start()

    # embed: processed -> qdrant
    embed.setup(src=db.processed, qdrant=qdrant, collection_name="toy_embeddings")
    await embed.start()

    raw = await db.raw.count_documents({})
    processed = await db.processed.count_documents({})
    vectors = len(qdrant._points.get("toy_embeddings", []))

    assert raw == 3
    assert processed == 3
    assert vectors == 3
