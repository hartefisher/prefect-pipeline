import json
from unittest.mock import MagicMock

import pytest
from litellm import BatchRequestCounts

from prefect_pipeline.components.batch import (
    JOB_STATUS_MAPPING,
    BatchReasoningJob,
    check_jsonl_file,
)


def _write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_check_jsonl_file_counts_valid_lines(tmp_path):
    fp = tmp_path / "in.jsonl"
    _write_jsonl(
        fp,
        [
            {"custom_id": "a", "body": {"x": 1}},
            {"custom_id": "b", "body": {"x": 2}},
        ],
    )
    assert check_jsonl_file(str(fp)) == 2


def test_check_jsonl_file_skips_blank_lines(tmp_path):
    fp = tmp_path / "in.jsonl"
    _write_jsonl(
        fp,
        [{"custom_id": "a", "body": {}}, {"custom_id": "b", "body": {}}],
    )
    with open(fp, "a", encoding="utf-8") as f:
        f.write("\n\n")
    assert check_jsonl_file(str(fp)) == 2


def test_check_jsonl_file_rejects_non_json(tmp_path):
    fp = tmp_path / "in.jsonl"
    with open(fp, "w", encoding="utf-8") as f:
        f.write("not json\n")
    with pytest.raises(Exception, match="非json数据"):
        check_jsonl_file(str(fp))


def test_check_jsonl_file_rejects_missing_custom_id(tmp_path):
    fp = tmp_path / "in.jsonl"
    _write_jsonl(fp, [{"body": {}}])
    with pytest.raises(Exception, match="custom_id不存在"):
        check_jsonl_file(str(fp))


def test_check_jsonl_file_rejects_duplicate_custom_id(tmp_path):
    fp = tmp_path / "in.jsonl"
    _write_jsonl(
        fp,
        [
            {"custom_id": "dup", "body": {}},
            {"custom_id": "dup", "body": {}},
        ],
    )
    with pytest.raises(Exception, match="存在重复"):
        check_jsonl_file(str(fp))


def test_check_jsonl_file_rejects_non_dict_body(tmp_path):
    fp = tmp_path / "in.jsonl"
    _write_jsonl(fp, [{"custom_id": "a", "body": "not-a-dict"}])
    with pytest.raises(Exception, match="body非json字符串"):
        check_jsonl_file(str(fp))


def test_job_status_mapping_covers_expected_phases():
    assert JOB_STATUS_MAPPING["completed"] == "Completed"
    assert JOB_STATUS_MAPPING["in_progress"] == "Running"
    assert JOB_STATUS_MAPPING["cancelled"] == "Terminated"
    assert JOB_STATUS_MAPPING["failed"] == "Failed"


def test_construct_json_includes_method_and_url():
    job = BatchReasoningJob(
        os_bucket="b", job_name="j", file_path="p", model_name="m", model_version="v"
    )
    data = job.construct_json("id-1", "hello", {"temperature": 0.2})
    assert data["custom_id"] == "id-1"
    assert data["method"] == "POST"
    assert data["url"] == "/v1/chat/completions"
    assert data["body"]["messages"][0]["content"] == "hello"
    assert data["body"]["temperature"] == 0.2


def test_create_batch_job_requires_input_file_id():
    job = BatchReasoningJob(os_bucket="b", job_name="j", file_path="p")
    with pytest.raises(ValueError, match="Input file ID is not set"):
        # 不设置 input_file_id（upload_data 未执行），create 应直接报错
        import asyncio

        asyncio.run(job.create_batch_job())


def test_check_batch_job_returns_response_when_counts_present():
    job = BatchReasoningJob(os_bucket="b", job_name="j", file_path="p")
    fake_batch = MagicMock()
    fake_batch.id = "job-123"
    fake_batch.status = "completed"
    fake_batch.request_counts = BatchRequestCounts(total=10, completed=10, failed=0)
    fake_batch.output_file_id = "out-1"
    fake_batch.error_file_id = "err-1"
    fake_client = MagicMock()
    fake_client.batches.retrieve.return_value = fake_batch
    job.client = fake_client  # 覆盖 cached_property，避免真实 OpenAI 客户端

    result = job.check_batch_job("job-123")
    assert result is not None
    assert result.id == "job-123"
    assert result.status == "Completed"
    fake_client.batches.retrieve.assert_called_once_with("job-123")


def test_check_batch_job_returns_none_without_counts():
    job = BatchReasoningJob(os_bucket="b", job_name="j", file_path="p")
    fake_batch = MagicMock()
    fake_batch.request_counts = None
    fake_client = MagicMock()
    fake_client.batches.retrieve.return_value = fake_batch
    job.client = fake_client

    assert job.check_batch_job("job-456") is None
