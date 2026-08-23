from unittest.mock import AsyncMock, MagicMock

from prefect_pipeline.components.helper import (
    AutoItemModel,
    IdSet,
    IncOp,
    PipelineHelper,
    UrlSet,
    bulk_write,
)
from prefect_pipeline.models import BaseItem
from tests.unit.components.conftest import ToyItem


def test_url_set_filter_and_update():
    op = UrlSet({"url": "https://x.com/1", "title": "t"})
    assert op.filter == {"url": "https://x.com/1"}
    assert op.update == {"$set": {"url": "https://x.com/1", "title": "t"}}


def test_id_set_filter():
    op = IdSet({"id": 42, "name": "n"})
    assert op.filter == {"id": 42}
    assert op.update == {"$set": {"id": 42, "name": "n"}}


def test_inc_op_upsert_mode_marks_setoninsert():
    class MyInc(IncOp):
        index_keys = ["url"]  # noqa: RUF012
        stamp_keys = ["batch_id"]  # noqa: RUF012

    op = MyInc({"url": "u", "cnt": 5, "extra": "e"})
    update = op.update
    assert update["$inc"] == {"cnt": 5}
    assert update["$setOnInsert"] == {"extra": "e"}


def test_auto_item_model_infers_generic():
    class Fetcher(AutoItemModel[ToyItem]):
        pass

    assert Fetcher.item_model is ToyItem


def test_auto_item_model_default_is_base_item():
    class Plain(AutoItemModel[BaseItem]):
        pass

    assert Plain.item_model is BaseItem


async def test_bulk_write_invokes_motor():
    db = MagicMock()
    db.bulk_write = AsyncMock(return_value=MagicMock(modified_count=3))
    ops = [UrlSet({"url": "a"}), UrlSet({"url": "b"})]
    count = await bulk_write(db, ops, bulk_write_size=10, mode="one")
    assert count == 3
    assert db.bulk_write.await_count == 1


async def test_pipeline_helper_save_result_uses_output_collection():
    helper = PipelineHelper()
    helper.output_collection = MagicMock()
    helper.output_collection.name = "out_coll"
    helper.output_collection.bulk_write = AsyncMock(return_value=MagicMock(modified_count=1))
    helper.bulk_write_size = 50
    await helper.save_result({"url": "x", "v": 1})
    helper.output_collection  # referenced
    assert helper.collection_name == "out_coll"


async def test_pipeline_helper_no_output_collection_noop():
    helper = PipelineHelper()
    helper.output_collection = None
    # should not raise
    await helper.save_result({"url": "x"})
