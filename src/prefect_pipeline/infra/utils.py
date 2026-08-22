import hashlib
import re
import struct
import types
from collections.abc import Iterable
from datetime import datetime
from inspect import isclass, isfunction
from typing import Any

from prefect.cache_policies import INPUTS, RUN_ID, TASK_SOURCE
from pytz import timezone

from prefect_pipeline.models.schemas import SchemaBase


def get_field_name(output_field: str) -> str:
    output_field = output_field.split(".")[-1]
    return output_field.split("__")[0]


def get_fields(schema_model: type[SchemaBase]) -> list[str]:
    model_fields = schema_model.model_fields
    return [name for name, info in model_fields.items() if not info.exclude]


def get_current_time(tz_name: str = "UTC") -> str:
    tz = timezone(tz_name)
    current_time = datetime.now(tz)
    return current_time.strftime("%Y-%m-%d %H:%M:%S.%f")


class TimeCounter:
    def __init__(self, tz_name: str = "UTC"):
        self.tz = timezone(tz_name)
        self.end_time = self.start_time = datetime.now(tz=self.tz)
        self.elapsed_seconds = 0.0

    def stop(self) -> None:
        self.end_time = datetime.now(tz=self.tz)
        self.elapsed_seconds = (self.end_time - self.start_time).total_seconds()

    def __str__(self) -> str:
        return f"Time taken: {self.elapsed_seconds} seconds"

    def to_dict(self) -> dict[str, str | float]:
        return {
            "start_time": self.start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "end_time": self.end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "elapsed_seconds": self.elapsed_seconds,
        }


class TimeStatistic:
    def __init__(self, tz_name: str = "UTC"):
        self.tz = timezone(tz_name)
        self.end_time = self.start_time = datetime.now(tz=self.tz)
        self.elapsed_seconds = 0.0

    def __enter__(self) -> "TimeStatistic":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.end_time = datetime.now(tz=self.tz)
        self.elapsed_seconds = (self.end_time - self.start_time).total_seconds()

    def __str__(self) -> str:
        return f"Time taken: {self.elapsed_seconds} seconds"

    def to_dict(self) -> dict[str, str | float]:
        return {
            "start_time": self.start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "end_time": self.end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "elapsed_seconds": self.elapsed_seconds,
        }


NO_SELF = (INPUTS - "self") + TASK_SOURCE + RUN_ID


def extract_xml_data(tag: str, string: str) -> str:
    pattern = f".*<{tag}>(.*?)</{tag}>"
    match = re.search(pattern, string, re.DOTALL)

    if match:
        return match.group(1).strip()
    else:
        return string


def consistent_string_id(text: str, algorithm: str = "sha256", output_bits: int = 64) -> int:
    """
    跨进程一致的字符串数字标识

    参数:
    - text: 输入字符串
    - algorithm: 哈希算法，可选 'md5', 'sha1', 'sha256', 'sha512'
    - output_bits: 输出位数，通常为 32, 64, 128, 256
    """
    # 选择哈希算法
    if algorithm == "md5":
        hash_obj = hashlib.md5()
    elif algorithm == "sha1":
        hash_obj = hashlib.sha1()
    elif algorithm == "sha256":
        hash_obj = hashlib.sha256()
    elif algorithm == "sha512":
        hash_obj = hashlib.sha512()
    else:
        raise ValueError(f"不支持的算法: {algorithm}")

    # 计算哈希（编码必须固定）
    hash_obj.update(text.encode("utf-8"))
    hash_bytes = hash_obj.digest()

    # 转换为整数
    if output_bits == 32:
        # 取前4字节
        return int(struct.unpack(">I", hash_bytes[:4])[0])
    elif output_bits == 64:
        # 取前8字节
        return int(struct.unpack(">Q", hash_bytes[:8])[0])
    elif output_bits == 128:
        # 取前16字节
        return int.from_bytes(hash_bytes[:16], byteorder="big")
    else:
        # 使用全部字节
        return int.from_bytes(hash_bytes, byteorder="big")


def quote_key_value(inp: Any) -> str:
    if isinstance(inp, str):
        return f"'{inp}'"
    elif isinstance(inp, (int, float)):
        return str(inp)
    elif isclass(inp) or isfunction(inp):
        return inp.__name__
    elif isinstance(inp, dict):
        ps = [f"{quote_key_value(k)}: {quote_key_value(v)}" for k, v in inp.items()]
    elif isinstance(inp, Iterable):
        ps = [quote_key_value(i) for i in inp]
        if isinstance(inp, tuple):
            return f"({', '.join(ps)}{',' if len(inp) == 1 else ''})"
        return f"[{', '.join(ps)}]"
    return str(inp)


def construct_dict_variable_snippet(name: str, data: dict[str, Any], *, tabs: int = 0, tab_str: str = "\t") -> str:
    start = name + " = {\n"
    snippets = []
    for k, v in data.items():
        k = quote_key_value(k)
        v = quote_key_value(v)
        snippets.append(f"{tab_str * (tabs + 1)}{k}: {v},")
    end = "\n" + tab_str * tabs + "}"
    return start + "\n".join(snippets) + end
