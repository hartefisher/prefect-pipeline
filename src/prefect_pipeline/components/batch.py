import json
from functools import cached_property
from pathlib import Path
from typing import Any, cast

from litellm import BatchRequestCounts
from openai import OpenAI

from prefect_pipeline.core.configs import (
    BAILIAN_API_KEY,
    BAILIAN_BASE_URL,
    VOLC_ACCESSKEY,
    VOLC_SECRETKEY,
)
from prefect_pipeline.models import BatchJobResponse, BatchJobStatus

# tos / volcenginesdk* 为火山方舟批量推理的可选依赖，仅在 ArkBatchReasoningJob
# 中使用，故延迟导入，避免无该 SDK 时影响整个 components 包加载。


def check_jsonl_file(file_path: str) -> int:
    with open(file_path, encoding="utf-8") as file:
        total = 0
        custom_id_set = set()
        for line in file:
            if line.strip() == "":
                continue
            try:
                line_dict = json.loads(line)
            except json.decoder.JSONDecodeError:
                raise Exception(f"批量推理输入文件格式错误，第{total + 1}行非json数据") from None
            if not line_dict.get("custom_id"):
                raise Exception(f"批量推理输入文件格式错误，第{total + 1}行custom_id不存在")
            if not isinstance(line_dict.get("custom_id"), str):
                raise Exception(f"批量推理输入文件格式错误，第{total + 1}行custom_id不是string")
            if line_dict.get("custom_id") in custom_id_set:
                raise Exception(f"批量推理输入文件格式错误，custom_id={line_dict.get('custom_id', '')}存在重复")
            else:
                custom_id_set.add(line_dict.get("custom_id"))
            if not isinstance(line_dict.get("body", ""), dict):
                raise Exception(
                    f"批量推理输入文件格式错误，custom_id={line_dict.get('custom_id', '')}的body非json字符串"
                )
            total += 1
    return total


JOB_STATUS_MAPPING: dict[str, BatchJobStatus] = {
    "failed": "Failed",
    "completed": "Completed",
    "expired": "Terminated",
    "cancelling": "Terminating",
    "cancelled": "Terminated",
    "validating": "Initializing",
    "in_progress": "Running",
    "finalizing": "Running",
}


class BatchReasoningJobBase:
    method: str | None = None
    url: str | None = None

    def __init__(
        self,
        *,
        os_bucket: str,
        job_name: str,
        file_path: str,
        local_root: str = "./.data",
        model_name: str | None = None,
        model_version: str | None = None,
        project_name: str = "default",
    ) -> None:
        self.os_bucket = os_bucket
        self.job_name = job_name
        self.file_path = file_path
        self.local_folder = f"{local_root}/{file_path}"
        self.local_file_path = f"{self.local_folder}/{job_name}.jsonl"
        self.input_object_key = f"{file_path}/{job_name}.jsonl"
        self.model_name = model_name
        self.model_version = model_version
        self.project_name = project_name
        self.input_file_id: str | None = None
        self.output_file_id: str | None = None
        self.error_file_id: str | None = None

    def upload_data(self) -> None:
        raise NotImplementedError("BatchReasoningJob is an abstract class, please use its subclass to upload data")

    async def create_batch_job(self) -> str:
        raise NotImplementedError("BatchReasoningJob is an abstract class, please use its subclass to create batch job")

    def check_batch_job(self, batch_job_id: str) -> BatchJobResponse | None:
        raise NotImplementedError("BatchReasoningJob is an abstract class, please use its subclass to check batch job")

    def get_result(self, batch_job_id: str) -> tuple[str, str]:
        raise NotImplementedError(
            "BatchReasoningJob is an abstract class, please use its subclass to get batch job result"
        )

    def construct_json(
        self,
        custom_id: str,
        content: str,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        body = {
            "messages": [
                {"role": "user", "content": content},
            ],
            **config,
        }
        data = {
            "custom_id": custom_id,
            "body": body,
        }
        if self.method:
            data["method"] = self.method

        if self.url:
            data["url"] = self.url

        return data


class BatchReasoningJob(BatchReasoningJobBase):
    method = "POST"
    url = "/v1/chat/completions"

    @cached_property
    def client(self) -> OpenAI:
        return OpenAI(
            api_key=BAILIAN_API_KEY,
            base_url=BAILIAN_BASE_URL,
        )

    def upload_data(self) -> None:
        total_lines = check_jsonl_file(self.local_file_path)
        print(f"文件中有效JSON数据的行数为: {total_lines}")

        # 上传文件
        file_object = self.client.files.create(file=Path(self.local_file_path), purpose="batch")
        self.input_file_id = file_object.id

    async def create_batch_job(self) -> str:
        if not self.input_file_id:
            raise ValueError("Input file ID is not set. Please upload data first.")
        batch = self.client.batches.create(
            input_file_id=self.input_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={
                "ds_name": self.job_name,
            },
        )
        self.batch_job_id = batch.id
        print(f"创建批量推理任务，任务ID：{batch.id}")
        return batch.id

    def check_batch_job(self, batch_job_id: str) -> BatchJobResponse | None:
        batch = self.client.batches.retrieve(batch_job_id)

        if batch.request_counts:
            self.output_file_id = batch.output_file_id
            self.error_file_id = batch.error_file_id
            return BatchJobResponse(
                id=batch.id,
                status=JOB_STATUS_MAPPING[batch.status],
                request_counts=batch.request_counts,
            )
        return None

    def get_result(self, batch_job_id: str) -> tuple[str, str]:
        results_file_name = ""
        errors_file_name = ""

        if self.output_file_id:
            results_file_name = f"{self.local_folder}/{self.job_name}-results-{batch_job_id}.jsonl"
            results = self.client.files.content(file_id=self.output_file_id)

            # 保存结果文件至本地
            results.write_to_file(results_file_name)

        if self.error_file_id:
            errors_file_name = f"{self.local_folder}/{self.job_name}-errors-{batch_job_id}.jsonl"
            errors = self.client.files.content(file_id=self.error_file_id)

            # 保存错误文件至本地
            errors.write_to_file(errors_file_name)

        return results_file_name, errors_file_name


class ArkBatchReasoningJob(BatchReasoningJobBase):
    region: str = "cn-beijing"
    endpoint: str = "tos-cn-beijing.volces.com"

    @cached_property
    def os_client(self) -> Any:
        import tos

        return tos.TosClientV2(VOLC_ACCESSKEY, VOLC_SECRETKEY, self.endpoint, self.region)

    @cached_property
    def client(self) -> Any:
        import volcenginesdkark
        import volcenginesdkcore

        configuration = volcenginesdkcore.Configuration()
        configuration.ak = VOLC_ACCESSKEY
        configuration.sk = VOLC_SECRETKEY
        configuration.region = self.region
        configuration.client_side_validation = True
        volcenginesdkcore.Configuration.set_default(configuration)
        return volcenginesdkark.ARKApi(volcenginesdkcore.ApiClient(configuration))

    def upload_data(self) -> None:
        # 上传任务文件的文件路径
        total_lines = check_jsonl_file(self.local_file_path)
        print(f"文件中有效JSON数据的行数为: {total_lines}")

        # 上传文件
        self.os_client.put_object_from_file(self.os_bucket, self.input_object_key, self.local_file_path)

    # 创建批量推理任务
    async def create_batch_job(self) -> str:
        import volcenginesdkark

        input_file_tos_location = volcenginesdkark.InputFileTosLocationForCreateBatchInferenceJobInput(
            bucket_name=self.os_bucket,
            object_key=self.input_object_key,
        )
        output_dir_tos_location = volcenginesdkark.OutputDirTosLocationForCreateBatchInferenceJobInput(
            bucket_name=self.os_bucket, object_key=f"{self.file_path}/"
        )
        foundation_model = volcenginesdkark.FoundationModelForCreateBatchInferenceJobInput(
            model_version=self.model_version, name=self.model_name
        )
        model_reference = volcenginesdkark.ModelReferenceForCreateBatchInferenceJobInput(
            foundation_model=foundation_model
        )
        req = volcenginesdkark.CreateBatchInferenceJobRequest(
            input_file_tos_location=input_file_tos_location,
            model_reference=model_reference,
            name=self.job_name,
            output_dir_tos_location=output_dir_tos_location,
            project_name=self.project_name,
        )

        resp = cast(
            "volcenginesdkark.models.CreateBatchInferenceJobResponse",
            self.client.create_batch_inference_job(req),
        )
        print(f"创建批量推理任务，任务ID：{resp.id}")
        return cast(str, resp.id)

    def check_batch_job(self, batch_job_id: str) -> BatchJobResponse | None:
        import volcenginesdkark

        filter = volcenginesdkark.FilterForListBatchInferenceJobsInput(ids=[batch_job_id])
        req = volcenginesdkark.ListBatchInferenceJobsRequest(filter=filter)
        resp = cast(
            "volcenginesdkark.models.ListBatchInferenceJobsResponse",
            self.client.list_batch_inference_jobs(req),
        )
        if resp.items:
            result = cast(
                "volcenginesdkark.models.ItemForListBatchInferenceJobsOutput",
                resp.items[0],
            )
            status = cast(
                "volcenginesdkark.models.StatusForListBatchInferenceJobsOutput",
                result.status,
            )
            request_counts = cast(
                "volcenginesdkark.models.RequestCountsForListBatchInferenceJobsOutput",
                result.request_counts,
            )
            if result.id and status.phase and request_counts:
                return BatchJobResponse(
                    id=cast(str, result.id),
                    status=cast(BatchJobStatus, status.phase),
                    request_counts=BatchRequestCounts(
                        total=request_counts.total or 0,
                        failed=request_counts.failed or 0,
                        completed=request_counts.completed or 0,
                    ),
                )
        return None

    def get_result(self, batch_job_id: str) -> tuple[str, str]:
        results_key = f"{self.file_path}/{batch_job_id}/output/results.jsonl"
        errors_key = f"{self.file_path}/{batch_job_id}/error/errors.jsonl"
        results_file_name = f"{self.local_folder}/{self.job_name}-results-{batch_job_id}.jsonl"
        errors_file_name = f"{self.local_folder}/{self.job_name}-errors-{batch_job_id}.jsonl"
        # self.client.get_object_to_file(self.tos_bucket, results_key, results_file_name)
        output = self.os_client.get_object(self.os_bucket, results_key)
        output_file = Path(results_file_name)
        with output_file.open("wb") as f:
            for content in output.content:
                f.write(content)

        try:
            output = self.os_client.get_object(self.os_bucket, errors_key)
            output_file = Path(errors_file_name)
            with output_file.open("wb") as f:
                for content in output.content:
                    f.write(content)
        except Exception:
            print("errors.jsonl not exist or other error")
            errors_file_name = ""

        return results_file_name, errors_file_name
