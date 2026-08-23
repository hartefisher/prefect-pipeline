# Prefect Pipeline Framework — PRD

> **版本**: v0.2  
> **日期**: 2026-08-22  
> **状态**: 已确认（Q1-Q5 开放问题已全部决策，见 §13.3）  
> **作者**: —  
> **里程碑分册**: [docs/milestones/](./milestones/) — 每个里程碑有独立 PRD + 设计文档，功能点全部挂 GitHub Issue

---

## 1. 项目背景与愿景

### 1.1 背景

`示例业务项目` 项目在 Prefect v3 之上构建了一套自研编排层，实现了 DAG 驡动流水线、Flow 自动发现与注册、LLM 批量推理、向量检索、容错重试等能力。然而这些能力深度耦合在 Product Hunt 业务代码中——编排引擎、数据组件、LLM 网关等通用基础设施与 业务逻辑（业务数据模型、API 采集器、分类标准化算法等）混在同一 codebase。

这套编排层本身是领域无关的。将其提取为独立框架后，任何需要"LLM 驱动的非结构化数据处理流水线"的项目都能直接复用。

### 1.2 愿景

**一个基于 Prefect v3 的可编排流水线框架，让开发者用运算符重载定义 DAG、用模块扫描自动注册 Flow、用统一网关调度多模型 LLM 推理——开箱即用，领域无关。**

---

## 2. 目标与非目标

### 2.1 目标

| #  | 目标                                           | 度量方式                               |
| -- | -------------------------------------------- | ---------------------------------- |
| G1 | 从 示例业务项目 中提取领域无关的框架层代码，消除 业务耦合      | 框架代码中零 `示例业务项目` / `biz_` 引用   |
| G2 | 保持与现有 示例业务项目 流水线的 100% 兼容              | 迁移后 源项目流水线行为不变，回归测试通过              |
| G3 | 框架可独立安装 (`pip install`)，作为 示例业务项目 的依赖项 | `pyproject.toml` 完整，import 路径无业务前缀 |
| G4 | 提供清晰的公共 API 和扩展点                             | 核心接口有类型标注 + docstring + 示例         |
| G5 | 推送到 GitHub 公开仓库                              | 仓库可 clone、README 可引导快速开始           |
| G6 | 提供框架级测试套件：单元测试随代码迁移交付，集成/回归测试专项建设 | pytest 全绿；core 层覆盖率 ≥85%；DAG 语义回归通过 |

### 2.2 非目标

- **不**重新设计编排引擎的 DAG 语义（`>>` 序列、`+` 并行），保持现有语义
- **不**替换 Prefect 为其他编排引擎
- **不**替换 litellm / motor / qdrant_client 为其他库
- **不**在本期实现 Web UI 或可视化看板
- **不**提供 Dockerfile 模板（Q4 已决策：暂不提供，用户项目自行编写）
- **不**预置任何 LLM model 实例配置（Q3 已决策：框架零预置，仅保留 `CompletionConfig` 配置模型）

---

## 3. 用户画像与使用场景

### 3.1 用户画像

| 角色         | 画像                                     | 核心需求                         |
| ---------- | -------------------------------------- | ---------------------------- |
| **流水线开发者** | 有 Python + Prefect 经验，需要构建 LLM 数据处理流水线 | 用最少的样板代码定义 Flow、DAG、LLM 推理步骤 |
| **框架贡献者**  | 开源社区开发者，想扩展框架能力                        | 清晰的扩展点、稳定的公共 API、可理解的架构      |
| **运维人员**   | 负责部署和监控 Prefect 服务                     | 一键启动、可配置的部署模式                |

### 3.2 典型使用场景

[hartefisher/prefect-pipeline](https://github.com/hartefisher/prefect-pipeline)

---

## 4. 系统架构

### 4.1 分层架构

```
┌─────────────────────────────────────────────────────────┐
│                    用户项目 (如 示例业务项目)               │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │  flows/     │  │  prompts/    │  │  orchestrations  │  │
│  │  (业务 Flow) │  │  (提示词)    │  │  (DAG 定义)      │  │
│  └──────┬──────┘  └──────────────┘  └────────┬────────┘  │
│         │                                      │           │
├─────────┼──────────────────────────────────────┼───────────┤
│         ▼                                      ▼           │
│  ┌─────────────────────────────────────────────────────────┐
│  │              Prefect Pipeline Framework                  │
│  │                                                         │
│  │  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  │ 编排层       │  │ 组件层        │  │ 基础设施层     │  │
│  │  │              │  │              │  │               │  │
│  │  │ Orchestration│  │ DataFetcher  │  │ MongoDB       │  │
│  │  │ Deployment   │  │ Transformer  │  │ Qdrant        │  │
│  │  │ RunnerBase   │  │ LLMExtractor │  │ LLM Gateway   │  │
│  │  │ FlowsLoader  │  │ Embedding    │  │ ErrorHandlers │  │
│  │  │ Condition    │  │ Batch        │  │ Exceptions     │  │
│  │  └─────────────┘  └──────────────┘  └───────────────┘  │
│  └─────────────────────────────────────────────────────────┘
│         │                                      │           │
├─────────┼──────────────────────────────────────┼───────────┤
│         ▼                                      ▼           │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐ │
│  │  Prefect v3  │  │  litellm     │  │  motor/qdrant_client│
│  │  (调度引擎)   │  │  (多模型网关) │  │  (数据库客户端)   │ │
│  └─────────────┘  └──────────────┘  └───────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 核心数据流

```
启动流程:
  main() → FlowsLoader.load()
    → iter_files(): 扫描 flows/ 下所有 __init__.py，收集 Deployment 实例
    → render_deployment_map(): 构建 (flow_name, deployment_name) → Deployment 映射
    → deploy(): 生成 Prefect RunnerDeployment 列表
    → write_deployment_map(): 自动生成 src/generated/deployments.py
  → prefect.aserve(*deployments): 启动 Prefect 服务

运行时触发:
  Flow 完成 → Hook.on("completion") → trigger()
    → 获取 DeploymentContext (下游 + peer 信息)
    → 检查 peer 状态 (同批次 peer 是否全部完成)
    → Condition.check_all(): 校验上游条件
    → run_deployment(): 发起下游任务
```

---

## 5. 框架分层设计

### 5.1 编排层 (Orchestration Layer)

框架的核心层，负责 DAG 定义、Flow 注册、部署上下文管理和下游触发。

| 模块                 | 对应 示例业务项目 源文件        | 职责                                                         |
| ------------------ | -------------------------- | ---------------------------------------------------------- |
| `orchestration.py` | `src/lib/orchestration.py` | DAG 分析引擎，遍历编排图谱生成 node_map 和可视化文本                          |
| `deployment.py`    | `src/lib/deployment.py`    | `Deployment` + `Node` 核心类，`>>` (序列) 和 `+` (并行) 运算符重载       |
| `runner_base.py`   | `src/lib/runner_base.py`   | `FlowRunnerBase` 基类：Hook 系统、trigger() 触发器、时间窗口、Backfill 调度 |
| `loader.py`        | `src/loader.py`            | `FlowsLoader` 自动发现 + `EntryPoints` 动态 Flow 创建 + 代码生成       |
| `condition.py`     | `src/lib/condition.py`     | `Condition` 上游状态检查器 (ALL/ANY 策略)                           |
| `ns_converter.py`  | `src/lib/ns_converter.py`  | 命名空间转换：path → flow_name、ns → entrypoint                    |
| `configs.py`       | `src/lib/configs.py`       | 框架级配置 (环境变量、路径)                                            |

#### 5.1.1 DAG 定义语法 (保持不变)

```python
# 序列: A 完成后触发 B
A_deployment >> B_deployment

# 并行: A 和 B 同时执行，都完成后触发下游
A_deployment + B_deployment

# 混合: A 完成后，B 和 C 并行执行，都完成后触发 D
A >> (B + C) >> D

# 每个 deployment 通过 Runner.deploy() 创建
A = RunnerClass.deploy(
    InjectorClass1, InjectorClass2,
    name="A",
    schedules=[...],
)
```

#### 5.1.2 Hook 系统 (保持不变)

```python
class MyRunner(FlowRunnerBase):
    @Hook.on(event="completion")
    @classmethod
    async def my_hook(cls, flow, flow_run, state):
        # Flow 完成后自动执行
        ...

    @Hook.on(event=["completion", "failure"])
    @classmethod
    async def my_hook2(cls, flow, flow_run, state):
        ...
```

#### 5.1.3 trigger() 触发器 (保持不变，核心机制)

`trigger()` 是整个编排系统的驱动核心。当一个 Flow 完成时：

1. 检查 `disable_trigger` 标志
2. 获取 Deployment 上下文 (下游节点 + peer 节点)
3. 查询同批次 peer 运行状态 (通过 `starter_id` 血缘追踪)
4. 校验 `Condition.check_all()`
5. 并发控制 (downstream_signal 去重)
6. 透传 macro_variables + starter_id
7. `run_deployment()` 发起下游

### 5.2 组件层 (Components Layer)

可复用的数据处理组件，用户通过泛型注入 Item 类型。

| 模块                     | 对应源文件                          | 职责                                                                                                               |
| ---------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `components/data.py`   | `src/lib/components/data.py`   | `DataFetcher` (MongoDB 查询/聚合)、`DataTransformer` (批量 upsert)、`EmbeddingHandler` (向量入库)                            |
| `components/llm.py`    | `src/lib/components/llm.py`    | `LLMExtractionStrategy` (单次推理)、`BatchLLMExtractionStrategy` (批量推理)、`LLMExtractor` / `GenericExtractor` (提取器基类)   |
| `components/vector.py` | `src/lib/components/vector.py` | `EmbeddingModel` (fastembed 封装)、`AsyncQdrantClient` (Qdrant 异步客户端)                                               |
| `components/helper.py` | `src/lib/components/helper.py` | `PipelineHelper` (save_result / log_stats)、`AutoItemModel` (泛型 Item 推断)、`UpdateOp` / `IncOp` (MongoDB upsert 操作) |

#### 5.2.1 组件扩展点

```python
# DataFetcher: 继承并实现 query 逻辑
class MyFetcher(DataFetcher[MyItem]):
    collection = DB.my_collection
    filter = {"status": "active"}
    sort = {"created_at": -1}

# DataTransformer: 继承并实现 process_item
class MyTransformer(DataTransformer[MyItem]):
    collection = DB.source
    output_collection = DB.target
    async def process_item(self, item):
        yield self.update_op({"url": item.url, "processed": True})

# LLMExtractor: 继承并定义 schema 和 instruction
class MyExtractor(GenericExtractor[MyItem]):
    schema_model = MySchema
    ns = "my_extraction"
    version = "p0"
```

### 5.3 基础设施层 (Infrastructure Layer)

| 模块                   | 对应源文件                        | 职责                                                                |
| -------------------- | ---------------------------- | ----------------------------------------------------------------- |
| `db.py`              | `src/lib/db.py`              | `MongoDB` 连接管理、collection 访问                                      |
| `models/__init__.py` | `src/lib/models/__init__.py` | 基础数据模型 (`BaseItem`, `Point`, `ExtraContext`, `DeploymentContext`) |
| `models/llm.py`      | `src/lib/models/llm.py`      | `CompletionConfig` LLM 配置模型                                       |
| `exceptions.py`      | `src/lib/exceptions.py`      | 自定义异常 + 可重试/不可重试异常分类                                              |
| `error_handlers.py`  | `src/lib/error_handlers.py`  | HTTP 重试、LLM 请求重试、Reasoning 重试、Scraping 重试                         |
| `utils.py`           | `src/lib/utils.py`           | `TimeCounter`、`extract_xml_data`、`consistent_string_id` 等         |
| `types.py`           | `src/lib/types.py`           | 类型定义 (`BatchResponse` 等)                                          |

### 5.4 Runner 类型层 (Runner Types)

从 `runners.py` 中提取通用 Runner，去掉 业务前缀。

| 框架类名                 | 原 业务类名           | 职责                                                       |
| -------------------- | ----------------- | -------------------------------------------------------- |
| `PipelineFlow`       | `DemoFlow` | 通用基类：时间窗口、filter、data_flag                               |
| `TransformationFlow` | 同名                | 数据转换：注入 `DataTransformer`，批量 upsert                      |
| `ReasoningFlow`      | 同名                | LLM 推理：注入 `GenericExtractor` + `DataFetcher`，支持实时/批量两种模式 |
| `EmbeddingFlow`      | 同名                | 向量化：注入 `EmbeddingHandler` + `DataFetcher`                |
| `AggregationFlow`    | 同名                | 聚合：注入 `DataTransformer`，MongoDB aggregation pipeline     |
| `WebScrapingFlow`    | 同名                | 爬虫：注入 `Spider` + `DataFetcher`，并发请求                      |
| `Retry`              | 同名                | 重试基类：retries=4, retry_delay=60s                          |
| 以上各 `Overall*` 变体    | 同名                | 全量模式 (variant="Overall")，跳过时间窗口过滤                        |

---

## 6. 从 示例业务项目 提取的改造清单

### 6.1 需要泛化的硬编码

| 位置                  | 当前硬编码                                          | 改造方案                                     |
| ------------------- | ---------------------------------------------- | ---------------------------------------- |
| `configs.py`        | `MACRO_VARIABLES = {"demo": [...]}`              | 改为框架不预置，由用户项目自行配置                        |
| `configs.py`        | `EXAMPLE_API_URL`, `EXAMPLE_DEV_TOKEN` 等 业务专属环境变量       | 移除，留在 示例业务项目 项目内                   |
| `runners.py`        | `class DemoFlow: project_name = "demo"`   | 重命名为 `PipelineFlow`，`project_name` 由子类设置 |
| `runners.py`        | `get_current_date()` 使用 `Asia/Shanghai` 时区        | 改为可配置参数，默认 UTC                           |
| `db.py`             | 30+ 个 `biz_posts_*` collection 常量               | 全部移除，collection 由用户项目自行定义                |
| `db.py`             | `WORKFLOW_DB = os.getenv("WORKFLOW_DB", "yc")` | 保留泛化版本                                   |
| `error_handlers.py` | `handle_authorization()` 中的 源项目 API 鉴权 逻辑      | 移除，留在 示例业务项目 项目内                   |
| `error_handlers.py` | `retry_scraping` 中的 业务专属 403 处理                  | 保留通用重试逻辑，业务 特定逻辑移出                       |
| `components/llm.py` | `DemoItemExtractor` 中的 `https://example.com/{slug}`     | 移除 `DemoItemExtractor`，留在 示例业务项目       |

### 6.2 需要保留原样的模块 (无需泛化)

以下模块本身已是领域无关的，直接迁移：

- `orchestration.py` — 纯 DAG 分析引擎，零业务耦合
- `deployment.py` — Deployment/Node 核心类 + 运算符重载
- `runner_base.py` — FlowRunnerBase 基类 + Hook 系统 + trigger()
- `loader.py` — FlowsLoader 自动发现机制
- `condition.py` — Condition 状态检查器
- `ns_converter.py` — 命名空间转换工具
- `components/helper.py` — PipelineHelper / AutoItemModel / UpdateOp / bulk_write
- `components/vector.py` — EmbeddingModel / AsyncQdrantClient
- `exceptions.py` — 自定义异常
- `utils.py` — 工具函数
- `types.py` — 类型定义

### 6.3 需要小幅调整的模块

| 模块                   | 调整内容                                                                                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `models/__init__.py` | 保留 `BaseItem`、`Point`、`ExtraContext`、`DeploymentContext`；移除 业务模型 (`Post`、`Product`、`PostNeighbor` 等)                                                                   |
| `models/llm.py`      | 保留 `CompletionConfig` 和 LLM 配置机制；业务专属的 model 实例定义移到 示例业务项目                                                                                                         |
| `components/llm.py`  | 保留 `LLMExtractionStrategy`、`BatchLLMExtractionStrategy`、`LLMExtractor`、`GenericExtractor`；移除 `DemoItemExtractor`                                                              |
| `components/data.py` | 保留 `DataFetcher`、`DataTransformer`、`EmbeddingHandler`、`SimilarityStats`；移除 `DemoRelativesTransformer`、`DemoCheck`、`DemoAscend` (业务专属逻辑) |
| `error_handlers.py`  | 保留 `http_error_handler`、`llm_request_error_handler`、`retry_reasoning`、`generate_retry_tag`；移除 `handle_authorization`、`retry_scraping` 中的 业务 特定逻辑                          |
| `runners.py`         | 重命名 `DemoFlow` → `PipelineFlow`；时区改为可配置                                                                                                                            |

### 6.4 import 路径迁移

```
# 示例业务项目 中现有路径
from ..lib.runner_base import FlowRunnerBase
from ..lib.deployment import Deployment
from ..lib.configs import ENVIRONMENT

# 框架中目标路径
from prefect_pipeline.core.runner_base import FlowRunnerBase
from prefect_pipeline.core.deployment import Deployment
from prefect_pipeline.core.configs import ENVIRONMENT

# 示例业务项目 迁移后 (作为框架使用者)
from prefect_pipeline import FlowRunnerBase, Deployment
from prefect_pipeline.runners import PipelineFlow
```

---

## 7. 目录结构设计

```
prefect_pipeline/                 # 框架根目录 (推送到 GitHub)
├── pyproject.toml               # 包定义 + 依赖
├── README.md                    # 快速开始 + 架构说明
├── LICENSE                      # MIT
├── .gitignore
├── docs/
│   ├── PRD.md                   # 本文档
│   └── architecture.md          # 架构详解 (后续补)
├── src/
│   ├── __init__.py              # 公共 API 导出
│   ├── main.py                 # 框架入口: FlowsLoader + prefect.aserve
│   ├── core/                   # 编排层
│   │   ├── __init__.py
│   │   ├── runner_base.py      # FlowRunnerBase + Hook
│   │   ├── deployment.py       # Deployment + Node + 运算符
│   │   ├── orchestration.py    # DAG 分析引擎
│   │   ├── loader.py           # FlowsLoader + EntryPoints
│   │   ├── condition.py        # Condition
│   │   ├── ns_converter.py     # 命名空间转换
│   │   └── configs.py          # 框架配置
│   ├── components/             # 组件层
│   │   ├── __init__.py
│   │   ├── data.py             # DataFetcher + DataTransformer + EmbeddingHandler
│   │   ├── llm.py              # LLMExtractor + Strategy
│   │   ├── vector.py           # EmbeddingModel + AsyncQdrantClient
│   │   └── helper.py           # PipelineHelper + UpdateOp + bulk_write
│   ├── runners/                # Runner 类型层
│   │   ├── __init__.py
│   │   ├── base.py             # PipelineFlow (通用基类)
│   │   ├── transformation.py   # TransformationFlow
│   │   ├── reasoning.py        # ReasoningFlow
│   │   ├── embedding.py        # EmbeddingFlow
│   │   ├── aggregation.py     # AggregationFlow
│   │   └── scraping.py         # WebScrapingFlow
│   ├── infra/                  # 基础设施层
│   │   ├── __init__.py
│   │   ├── db.py               # MongoDB 封装
│   │   ├── exceptions.py       # 自定义异常
│   │   ├── error_handlers.py  # 重试逻辑
│   │   ├── types.py            # 类型定义
│   │   └── utils.py            # 工具函数
│   └── models/                 # 数据模型
│       ├── __init__.py         # BaseItem, Point, ExtraContext, DeploymentContext
│       └── llm.py              # CompletionConfig
└── examples/                   # 示例 (后续补)
    └── simple_pipeline.py
```

---

## 8. 公共 API 设计

### 8.1 顶层导出 (`prefect_pipeline.__init__`)

```python
# 编排核心
from .core.deployment import Deployment, Node
from .core.runner_base import FlowRunnerBase, Hook
from .core.orchestration import Orchestration
from .core.condition import Condition, PeersPolicy
from .core.loader import FlowsLoader, EntryPoints

# Runner 类型
from .runners.base import PipelineFlow
from .runners.transformation import TransformationFlow
from .runners.reasoning import ReasoningFlow
from .runners.embedding import EmbeddingFlow
from .runners.aggregation import AggregationFlow

# 组件
from .components.data import DataFetcher, DataTransformer, EmbeddingHandler
from .components.llm import LLMExtractor, GenericExtractor, LLMExtractionStrategy
from .components.vector import EmbeddingModel, AsyncQdrantClient
from .components.helper import PipelineHelper, UpdateOp, IncOp, bulk_write

# 模型
from .models import BaseItem, Point, ExtraContext, DeploymentContext
from .models.llm import CompletionConfig

# 基础设施
from .infra.db import MongoDB
from .infra.exceptions import (
    BadResponseError, IgnoreRequest, RetryReasoning,
    retriable_exceptions, unretriable_exceptions,
)
```

### 8.2 用户扩展 API

```python
# 自定义 Runner
class MyFlow(PipelineFlow):
    project_name = "my_project"

    async def setup(self, MyComponent, **extra):
        self.component = MyComponent(**extra)

    async def run(self):
        await self.component.run()

    async def clear(self):
        await self.component.close()

# 注册并编排
deployment = MyFlow.deploy(MyComponent, name="my_flow")
pipeline = deployment >> AnotherFlow.deploy(...)
```

### 8.3 配置 API

```python
# 框架配置通过环境变量 + .env
ENVIRONMENT=prod|test
FLOWS_DIRECTORY=./src/flows
PROMPTS_DIRECTORY=./src/prompts
WORKFLOW_POOL=default

# MongoDB
MONGO_USER=...
MONGO_PASSWORD=...
MONGO_HOST=...
WORKFLOW_DB=my_project

# Qdrant
QDRANT_API_URL=...
QDRANT_API_KEY=...

# LLM (由 litellm 统一管理, 各 provider key)
ARK_API_KEY=...
BAILIAN_API_KEY=...
DEEPSEEK_API_KEY=...
```

---

## 9. 技术选型

| 组件         | 技术            | 版本要求           | 理由                 |
| ---------- | ------------- | -------------- | ------------------ |
| 编排引擎       | Prefect       | >=3.0 (v3 API) | 框架构建基础，已深度集成       |
| LLM 网关     | litellm       | >=1.0          | 统一多模型 API，支持 Batch |
| MongoDB 驱动 | motor         | >=3.0          | 异步 MongoDB 客户端     |
| 向量数据库      | qdrant-client | >=1.0          | 高性能向量检索            |
| 嵌入模型       | fastembed     | >=0.3          | 本地 ONNX 嵌入，无需 GPU  |
| 数据校验       | pydantic      | >=2.0          | 数据模型 + JSON Schema |
| 环境管理       | python-dotenv | >=1.0          | .env 加载            |
| 时区处理       | pytz          | >=2024.1       | 时区转换               |
| HTTP 客户端   | httpx         | >=0.27         | 异步 HTTP (爬虫)       |
| 测试框架       | pytest        | >=8.0          | 单元/集成测试            |
| 异步测试       | pytest-asyncio | >=0.23        | async 测试支持         |
| 覆盖率        | pytest-cov    | >=5.0          | 覆盖率统计与门槛          |
| Mock        | pytest-mock / unittest.mock | —      | 外部服务 fake（Mongo/Qdrant/LLM） |

### 9.1 Python 版本

**Python >= 3.12** (框架使用了 3.12 泛型语法 `class Foo[T]`、`type` 语句)

---

## 10. 非功能需求

| 维度       | 要求                                                         |
| -------- | ---------------------------------------------------------- |
| **性能**   | 单次 LLM 推理并发 30 请求；批量推理支持 10000+ 请求                         |
| **可用性**  | 所有 Flow 支持 retries + retry_delay；Reasoning 支持 5 级重试        |
| **可观测性** | 每个 LLM 推理记录 token 消耗 + 耗时到 MongoDB；Prefect 原生日志            |
| **幂等性**  | Backfill 模式幂等 (基于 dt + days 参数)；DataTransformer upsert 幂等  |
| **可扩展性** | 新增 Flow 只需: 1) 创建 Runner + 组件, 2) `deploy()` 注册, 3) DAG 接入 |
| **隔离性**  | 框架代码零业务引用；用户项目通过环境变量配置                                     |

---

## 11. 里程碑计划

> 完整的需求拆分与设计见 [docs/milestones/](./milestones/) 分册；所有功能点/确认点均已挂 GitHub Issue，以 GitHub Milestone `M1`~`M8` 跟踪。

### 总览

| 里程碑 | 名称 | 交付物 | 状态 |
| --- | --- | --- | --- |
| [M1](./milestones/M1-skeleton/) | 框架骨架与工程基线 | 目录结构、pyproject.toml、lint/CI 基线 | 进行中 |
| [M2](./milestones/M2-infra/) | 基础设施层迁移 | models + infra（db/exceptions/error_handlers/utils/types）+ 单元测试 | 未开始 |
| [M3](./milestones/M3-core/) | 编排核心层迁移 | core（deployment/orchestration/runner_base/loader/condition/ns_converter/configs）+ 单元测试 | 未开始 |
| [M4](./milestones/M4-components/) | 组件层迁移 | components（data/llm/vector/helper/batch/spider）+ 单元测试 | 未开始 |
| [M5](./milestones/M5-runners-api/) | Runner 类型层与公共 API | runners/ 子包 + 顶层导出 + 单元测试 | 未开始 |
| [M6](./milestones/M6-testing/) | 框架级测试套件 | 测试基础设施（fakes）、DAG 语义回归、E2E 集成测试、覆盖率门槛 | 未开始 |
| [M7](./milestones/M7-demo-migration/) | 示例业务项目 适配与回归 | 业务 依赖切换 + 业务适配层 + 回归验证 | 未开始 |
| [M8](./milestones/M8-release/) | 示例、文档与开源发布 | examples、architecture.md、LICENSE/CONTRIBUTING、敏感信息审查 | 未开始 |

### 依赖关系

```
M1 ──► M2 ──► M3 ──► M5 ──► M7 ──► M8
        │       │              
        └──► M4 ┘   M4 ──► M5
   (M6 的测试基础设施在 M2 起持续交付，M6 专项收口集成与回归测试)
```

- M2（基础设施层）是所有上层迁移的前置
- M3（编排核心）与 M4（组件）在 M2 完成后可并行
- M5 依赖 M3 + M4；M7 依赖 M5 + M6（回归测试需先就绪）
- 每个 M2~M5 里程碑的功能 Issue 均附带对应单元测试交付（测试与代码同 PR）

### M1: 框架骨架与工程基线 (当前)

- [x] 创建 `prefect_pipeline/` 目录结构
- [x] 更新 `.gitignore`，排除 `示例业务项目/`
- [x] 编写 PRD (本文档)
- [x] 初始化 `README.md`
- [ ] 初始化 `pyproject.toml`
- [ ] 配置 ruff + mypy 工程化基线
- [ ] GitHub Actions lint workflow

---

## 12. 开源准备清单

| 项               | 状态    | 说明                                       |
| --------------- | ----- | ---------------------------------------- |
| LICENSE         | 待添加   | 建议 MIT                                   |
| README.md       | 待编写   | 快速开始 + 架构图 + API 概览                      |
| pyproject.toml  | 待创建   | 包名 `prefect_pipeline`, Python >=3.12     |
| .gitignore      | ✅ 已更新 | 排除 `示例业务项目/`、`.env`、`generated/` 等 |
| CONTRIBUTING.md | 待编写   | 开发流程 + 代码规范                              |
| 敏感信息审查          | 待执行   | 确保无 API key / 私有 host / 内部 URL           |
| GitHub Actions  | 可选    | 后续可加 lint + type check                   |

### 12.1 敏感信息审查要点

迁移代码时必须确认以下内容不被推送到 GitHub:

- `.env` 文件 (已有 .gitignore 覆盖)
- MongoDB 连接串中的真实 host / 密码
- LLM provider API keys
- Qdrant API URL / Key
- 任何 `@volcengine` / `@tencent` 内部域名
- Product Hunt developer token

---

## 13. 风险与约束

### 13.1 技术风险

| 风险                         | 影响        | 缓解措施                        |
| -------------------------- | --------- | --------------------------- |
| import 路径迁移破坏 示例业务项目 | 源项目流水线不可用 | M3 阶段做完整回归测试                |
| Prefect v3 API 变更          | 框架不兼容     | 锁定 Prefect minor 版本         |
| litellm batch API 限制       | 批量推理不可用   | 保留实时推理作为 fallback           |
| `generated/` 代码自动覆盖        | 手动修改丢失    | .gitignore 已排除；框架文档强调禁止手动编辑 |

### 13.2 设计约束

- **DAG 语义不可变**: `>>` (序列) 和 `+` (并行) 的语义在迁移后必须保持不变
- **trigger() 机制不可变**: peer 状态检查 + downstream_signal 去重 + macro_variables 透传的逻辑不能简化
- **Hook 系统不可变**: `@Hook.on(event=...)` 装饰器机制保持原样
- **泛型 Item 注入不可变**: `DataFetcher[Item]`、`LLMExtractor[Item]` 等泛型签名保持原样

### 13.3 开放问题（已全部决策 ✅ 2026-08-22）

| #   | 问题                                                 | 决策                                                                 |
| ---- | -------------------------------------------------- | ------------------------------------------------------------------ |
| Q1  | 框架名称 `prefect_pipeline` 是否合适？                      | ✅ **保持 `prefect_pipeline`**（Issue #1）                             |
| Q2  | `retry_scraping` 完全移到 示例业务项目，还是保留通用重试框架 + 业务适配层？ | ✅ **方案 B：通用重试框架 + 业务适配层**——框架保留通用 retry 装饰器（状态码分类、指数退避），业务专属 403 特殊处理通过参数/子类注入（Issue #3） |
| Q3  | `models/llm.py` 中的 ~50 个 LLM model 实例是否提供框架级默认配置？   | ✅ **方案 A：框架零预置**——仅保留 `CompletionConfig` 配置模型，所有实例由用户项目自定义（Issue #2） |
| Q4  | 框架是否需要提供 Dockerfile 模板？                            | ✅ **暂不提供**（Issue #4）                                               |
| Q5  | `Spider` / `CDPSpider` 组件是否纳入框架？                    | ✅ **方案 A：纳入框架**——提供通用 `SpiderBase`（请求调度、并发控制、重试接入），业务专属解析逻辑留在 示例业务项目（Issue #5） |

> 迁移过程中新产生的确认点以 `confirmation` 标签的 Issue 跟踪，决策后更新本表。

### 13.4 迁移确认点（C 系列，已全部决策 ✅ 2026-08-23）

| #    | 确认点                                                          | 决策                                                                                     |
| ---- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| C2.1 | `db.py` 是否需要支持多数据库实例 / 多集群连接管理？                    | ✅ **不纳入框架**：保留 `MongoDB` 单实例接口，多库由业务侧实例化多个 `MongoDB` 对象实现（Issue #17） |
| C2.2 | `vocabulary.py` 是否纳入框架？                                  | ✅ **不纳入框架**：属业务词汇表，由示例业务项目在其 `models/` 内自行定义（Issue #18）              |
| C3.1 | `FlowsLoader` 代码生成输出路径是否需要可配置？                      | ✅ **保持约定路径**：不暴露可配置项，特殊布局由业务侧子类化 `FlowsLoader` 覆盖（Issue #26）        |
| C4.1 | `batch.py` 的 litellm batch 是否抽象为 provider 接口？            | ✅ **保持简单**：继续直接依赖 litellm，不抽象 provider 接口（Issue #33）                     |
| C6.1 | L4 服务集成测试依赖 docker-compose（真实 Mongo/Qdrant）还是纯 fake？ | ✅ **纯 fake**：fakes 已全覆盖驱动 API 子集，不建设 L4 容器测试（Issue #41）                    |

---

## 14. 验收标准

| #   | 验收项                              | 验证方式                                                                       |
| --- | -------------------------------- | -------------------------------------------------------------------------- |
| AC1 | 框架代码中零 `示例业务项目` / `biz_` 引用 | `grep -ri "示例业务项目\|biz_" prefect_pipeline/src/` 无输出                   |
| AC2 | 框架可独立安装                          | `pip install -e prefect_pipeline/` 成功                                      |
| AC3 | 示例业务项目 迁移后流水线正常            | `cd 示例业务项目 && python -m src.main` 正常启动                               |
| AC4 | 框架推送到 GitHub 后可 clone            | `git clone <github_url>` 成功                                                |
| AC5 | 框架无敏感信息                          | `grep -ri "api_key\|password\|secret" prefect_pipeline/src/` 仅匹配变量名定义，无实际值 |
| AC6 | DAG 运算符语义不变                      | 迁移后 源项目流水线的 `orchestrations.py` 不需修改即可运行                                   |
| AC7 | 测试套件全绿且达标                       | `pytest` 全部通过；`src/core/` 覆盖率 ≥85%，全包 ≥75%                                |
| AC8 | DAG 语义回归通过                       | M6 的 DAG 语义回归测试（序列/并行/混合/peer 检查）全部通过                                  |

---

## 附录 A: 现有源文件 → 框架目标文件映射

```
示例业务项目/src/lib/orchestration.py      → prefect_pipeline/src/core/orchestration.py
示例业务项目/src/lib/deployment.py         → prefect_pipeline/src/core/deployment.py
示例业务项目/src/lib/runner_base.py         → prefect_pipeline/src/core/runner_base.py
示例业务项目/src/lib/condition.py           → prefect_pipeline/src/core/condition.py
示例业务项目/src/lib/ns_converter.py        → prefect_pipeline/src/core/ns_converter.py
示例业务项目/src/lib/configs.py             → prefect_pipeline/src/core/configs.py (泛化)
示例业务项目/src/loader.py                  → prefect_pipeline/src/core/loader.py
示例业务项目/src/main.py                    → prefect_pipeline/src/main.py (泛化)

示例业务项目/src/lib/components/data.py     → prefect_pipeline/src/components/data.py (移除 业务类)
示例业务项目/src/lib/components/llm.py      → prefect_pipeline/src/components/llm.py (移除 DemoItemExtractor)
示例业务项目/src/lib/components/vector.py   → prefect_pipeline/src/components/vector.py
示例业务项目/src/lib/components/helper.py   → prefect_pipeline/src/components/helper.py
示例业务项目/src/lib/components/spider.py   → prefect_pipeline/src/components/spider.py (Q5 已决策纳入, 提供 SpiderBase)
示例业务项目/src/lib/components/batch.py    → prefect_pipeline/src/components/batch.py

示例业务项目/src/lib/runners.py             → prefect_pipeline/src/runners/ (拆分为子包, 泛化)
示例业务项目/src/lib/db.py                  → prefect_pipeline/src/infra/db.py (移除 业务 collections)
示例业务项目/src/lib/exceptions.py          → prefect_pipeline/src/infra/exceptions.py
示例业务项目/src/lib/error_handlers.py      → prefect_pipeline/src/infra/error_handlers.py (泛化)
示例业务项目/src/lib/utils.py               → prefect_pipeline/src/infra/utils.py
示例业务项目/src/lib/types.py               → prefect_pipeline/src/infra/types.py

示例业务项目/src/lib/models/__init__.py    → prefect_pipeline/src/models/__init__.py (移除 业务 模型)
示例业务项目/src/lib/models/llm.py          → prefect_pipeline/src/models/llm.py (移除 业务 model 实例)
示例业务项目/src/lib/models/schemas.py      → prefect_pipeline/src/models/schemas.py (保留通用 SchemaBase)
示例业务项目/src/lib/vocabulary.py          → (评估是否纳入框架)
```

## 附录 B: 不迁移的文件 (保留在 示例业务项目)

```
示例业务项目/src/orchestrations.py          # 业务专属 DAG 定义
示例业务项目/src/flows/                      # 业务 Flow 实现
示例业务项目/src/prompts/                    # 业务专属提示词
示例业务项目/src/setup.py                    # 业务专属启动逻辑 (浏览器)
示例业务项目/Dockerfile                      # 业务 部署
示例业务项目/deploy.sh                       # 业务 部署脚本
示例业务项目/discussion/                     # 设计讨论文档
示例业务项目/notebooks/                      # 探索性 notebook
```
