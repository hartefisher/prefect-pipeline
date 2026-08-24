"""Prefect Pipeline Framework.

一个基于 Prefect v3 的可编排流水线框架，让开发者用运算符重载定义 DAG、用模块
扫描自动注册 Flow、用统一网关调度多模型 LLM 推理——开箱即用，领域无关。

详见 docs/PRD.md 与 docs/milestones/。
"""

from __future__ import annotations

__version__ = "0.1.0.dev0"

# ----------------------------------------------------------------------
# 编排核心
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 组件
# ----------------------------------------------------------------------
from .components.batch import (
    ArkBatchReasoningJob,
    BatchReasoningJob,
    BatchReasoningJobBase,
    check_jsonl_file,
)
from .components.data import (
    DataFetcher,
    DataTransformer,
    EmbeddingHandler,
    SimilarityStats,
)
from .components.helper import (
    AutoItemModel,
    ExtraContextMixin,
    IncOp,
    PipelineHelper,
    UpdateOp,
    UrlSet,
    bulk_write,
)
from .components.llm import (
    BatchLLMExtractionStrategy,
    GenericExtractor,
    LLMExtractionStrategy,
    LLMExtractor,
)
from .components.spider import (
    BrowserSpider,
    CDPSpider,
    DockerSpider,
    HTTPSpider,
    SpiderBase,
)
from .components.vector import AsyncQdrantClient, EmbeddingModel
from .core.condition import Condition, PeersPolicy, ResultType
from .core.deployment import Deployment, Node, NodeDeployment
from .core.loader import EntryPoints, FlowsLoader
from .core.orchestration import Orchestration
from .core.runner_base import FlowParemeter, FlowRunnerBase, FlowStateHooks, Hook
from .infra.db import MongoDB
from .infra.exceptions import (
    RETRIABLE_STATUS_CODES,
    BadResponseError,
    BatchJobFailed,
    BatchJobNotCompleted,
    BatchJobResponseError,
    IgnoreRequest,
    ItemModelMissing,
    ResultIsEmpty,
    RetryReasoning,
    retriable_exceptions,
    unretriable_exceptions,
)

# ----------------------------------------------------------------------
# 模型与基础设施
# ----------------------------------------------------------------------
from .models import (
    BaseItem,
    BatchJobResponse,
    BatchJobStatus,
    DeploymentContext,
    ExtraContext,
    NodeInfo,
    Point,
)
from .models.llm import (
    CompletionConfig,
    get_llm_provider,
    register_llm_provider,
)

# ----------------------------------------------------------------------
# Runner 类型
# ----------------------------------------------------------------------
from .runners import (
    AggregationFlow,
    DeploymentContextManager,
    EmbeddingFlow,
    OverallAggregationFlow,
    OverallEmbeddingFlow,
    OverallReasoningFlow,
    OverallTransformationFlow,
    OverallWebScrapingFlow,
    PipelineFlow,
    ReasoningFlow,
    Retry,
    TransformationFlow,
    WebScrapingFlow,
)
from .serve import run, serve

__all__ = [
    "RETRIABLE_STATUS_CODES",
    "AggregationFlow",
    "ArkBatchReasoningJob",
    "AsyncQdrantClient",
    "AutoItemModel",
    "BadResponseError",
    "BaseItem",
    "BatchJobFailed",
    "BatchJobNotCompleted",
    "BatchJobResponse",
    "BatchJobResponseError",
    "BatchJobStatus",
    "BatchLLMExtractionStrategy",
    "BatchReasoningJob",
    "BatchReasoningJobBase",
    "BrowserSpider",
    "CDPSpider",
    "CompletionConfig",
    "Condition",
    "DataFetcher",
    "DataTransformer",
    "Deployment",
    "DeploymentContext",
    "DeploymentContextManager",
    "DockerSpider",
    "EmbeddingFlow",
    "EmbeddingHandler",
    "EmbeddingModel",
    "EntryPoints",
    "ExtraContext",
    "ExtraContextMixin",
    "FlowParemeter",
    "FlowRunnerBase",
    "FlowStateHooks",
    "FlowsLoader",
    "GenericExtractor",
    "HTTPSpider",
    "Hook",
    "IgnoreRequest",
    "IncOp",
    "ItemModelMissing",
    "LLMExtractionStrategy",
    "LLMExtractor",
    "MongoDB",
    "Node",
    "NodeDeployment",
    "NodeInfo",
    "Orchestration",
    "OverallAggregationFlow",
    "OverallEmbeddingFlow",
    "OverallReasoningFlow",
    "OverallTransformationFlow",
    "OverallWebScrapingFlow",
    "PeersPolicy",
    "PipelineFlow",
    "PipelineHelper",
    "Point",
    "ReasoningFlow",
    "ResultIsEmpty",
    "ResultType",
    "Retry",
    "RetryReasoning",
    "SimilarityStats",
    "SpiderBase",
    "TransformationFlow",
    "UpdateOp",
    "UrlSet",
    "WebScrapingFlow",
    "bulk_write",
    "check_jsonl_file",
    "get_llm_provider",
    "register_llm_provider",
    "retriable_exceptions",
    "run",
    "serve",
    "unretriable_exceptions",
]
