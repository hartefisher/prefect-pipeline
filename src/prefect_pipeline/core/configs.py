import os

from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------------------------------------------
# 框架级环境变量（泛化后）
#
# 框架只保留"机制"，不预置任何业务专属内容。业务专属的环境变量
# （如各厂商 LLM API Key、业务 API URL、爬虫相关开关等）一律移除，
# 由接入框架的业务项目在自己的配置模块中声明。
# ----------------------------------------------------------------------------

ENVIRONMENT = os.getenv("ENVIRONMENT", "prod")
# 业务 Flow 所在的包目录（相对仓库根），FlowsLoader 据此发现 Deployment
FLOWS_DIRECTORY = os.getenv("FLOWS_DIRECTORY", "./src/flows")
# 提示词目录（供业务 Flow 读取 prompt 模板）
PROMPTS_DIRECTORY = os.getenv("PROMPTS_DIRECTORY", "./src/prompts")
# Prefect work pool 名称；Deployment 仅在其 workflow_pool 与框架 WORKFLOW_POOL 一致时才部署
WORKFLOW_POOL = os.getenv("WORKFLOW_POOL")
# 版本戳，用于 Deployment 上下文比对与代码生成产物
VERSION_ID = os.getenv("VERSION_ID", "as8f9ds09ksd")
# 框架默认时区，用于 get_current_date / flow_run_name 生成；业务项目可覆盖
TIMEZONE = os.getenv("TIMEZONE", "UTC")
# 项目命名空间（原示例业务项目硬编码在 runner 上的 "ph" 等前缀）。
# 现统一为框架级配置，业务项目在 .env 中声明一次即可，无需在代码中写 project_name。
PROJECT_NAME = os.getenv("PROJECT_NAME", "default")
# DAG 编排入口契约：``<module>:<attr>``，默认指向业务项目的
# ``src.orchestrations:get_orchestration(branch, start, stop)``。非标布局可通过
# 该环境变量覆盖，框架的 DeploymentContextManager 据此注入业务 DAG。
ORCHESTRATION_ENTRY = os.getenv("ORCHESTRATION_ENTRY", "src.orchestrations:get_orchestration")

# 宏变量：由业务项目在启动时通过 register_macro_variables 注入，
# 框架不预置任何业务前缀（原示例业务项目的 "demo" / "ph" 等前缀已全部移除）。
MACRO_VARIABLES: dict[str, list[str]] = {}

# 增量转换开关：业务项目可覆盖。为 "false" 时，DataTransformer.setup 会先清空
# 目标集合再写入（全量刷新语义）；默认 "true"（增量 upsert 语义）。
INCREMENTAL_TRANSFORMATION = os.getenv("INCREMENTAL_TRANSFORMATION", "true")

# 批量推理 provider 凭证（框架仅读取，不预置任何值；业务项目在自己的环境中声明）。
# 当前内置 BatchReasoningJob（OpenAI 兼容 / 百炼）与 ArkBatchReasoningJob（火山方舟）两类实现，
# C4.1 决策前保持 litellm 直连，未来可抽象为 BatchProvider 协议。
BAILIAN_API_KEY = os.environ.get("BAILIAN_API_KEY")
BAILIAN_BASE_URL = os.environ.get("BAILIAN_BASE_URL")
VOLC_ACCESSKEY = os.environ.get("VOLC_ACCESSKEY", "")
VOLC_SECRETKEY = os.environ.get("VOLC_SECRETKEY", "")


def register_macro_variables(project: str, variables: list[str]) -> None:
    """业务项目注册自己的宏变量。

    示例业务项目会在 main.py 中调用：
        register_macro_variables("demo", ["dt", "days", "offset", "backfill", "fill_direction"])

    框架据此在 trigger() 中把上游的参数透传给下游 Flow。
    """
    MACRO_VARIABLES[project] = variables


IS_TEST = ENVIRONMENT.lower() == "test"
ROOT_PATH = FLOWS_DIRECTORY.replace("./", "").replace("/", ".")
