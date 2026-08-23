from __future__ import annotations

import importlib
import re
from typing import TYPE_CHECKING

from .configs import ROOT_PATH

if TYPE_CHECKING:
    from .deployment import Deployment


def capitalize_ns(ns: str | None) -> list[str]:
    if not ns:
        return []

    # 去除空格和特殊符号
    stripped_ns = ns.replace(" ", "").replace("_", "").replace("-", "").replace(".", "")
    # 使用re.split在以下位置分割：
    # 1. 小写字母后跟大写字母: (?<=[a-z])(?=[A-Z])
    # 2. 大写字母后跟大写字母+小写字母: (?<=[A-Z])(?=[A-Z][a-z])
    # 3. 数字后跟字母: (?<=\d)(?=[A-Za-z])
    pattern = r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=\d)(?=[A-Z])"

    # 在匹配的位置进行分割
    parts = re.split(pattern, stripped_ns)

    # 如果分割后只有一个部分，可能是分隔符的情况
    if len(parts) > 1 or re.search(r"[A-Z]", stripped_ns):
        return parts
    else:
        # 处理有分隔符的情况
        return [" ".join(pp.capitalize() for pp in p.replace("-", "_").split("_")) for p in ns.split(".")]


def gen_deployment_name(name: str, flags: list[str] | None = None) -> str:
    formatted_flags = "".join([f"[{flag.upper()}]" for flag in (flags or [])])
    pattern = r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=\d)(?=[A-Z])"
    deployment_name = " ".join(re.split(pattern, name))
    return formatted_flags + deployment_name


def path_2_flow_name(path: str, prefix: str | None = None) -> str:
    flow_name = path.replace(f"{ROOT_PATH}.", "").replace(".", "-")
    if prefix:
        return f"{prefix.lower()}-{flow_name}"
    return flow_name


def ns_2_entrypoint(flow_name: str, deployment_name: str) -> str:
    return f"{flow_name.replace('-', '__')}__{deployment_name.replace(' ', '')}"


def entrypoint_2_ns(entrypoint: str) -> tuple[str, str]:
    *ns, deployment_name = entrypoint.split("__")
    return "-".join(ns), deployment_name


def get_deployment_instance(flow_name: str, deployment_name: str) -> Deployment:
    _project_name, *flow_ns = flow_name.split("-")
    module_name = f"{ROOT_PATH}.{'.'.join(flow_ns)}"

    module = importlib.import_module(module_name)
    dn = deployment_name.split("]")[-1].replace(" ", "")
    deployment = module.__dict__.get(dn)
    if deployment:
        return deployment  # type: ignore[no-any-return]
    else:
        raise ValueError(f"Deployment '{deployment_name}' not found in flow '{flow_name}'.")
