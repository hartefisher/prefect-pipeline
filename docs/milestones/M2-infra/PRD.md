# M2 — 基础设施层迁移（PRD）

> **状态**: 未开始
> **前置**: M1
> **GitHub Milestone**: `M2`
> **总纲**: [../PRD.md](../PRD.md) · **设计**: [DESIGN.md](./DESIGN.md)

## 1. 目标

将 示例业务项目 中领域无关的基础设施代码（数据模型 + infra 层）迁入框架，并完成 Q2/Q3 决策要求的泛化改造。本层是 M3/M4/M5 全部上层迁移的前置依赖。

## 2. 迁移范围

| 源文件 (示例业务项目) | 目标文件 (框架) | 改造点 |
| --- | --- | --- |
| `src/lib/models/__init__.py` | `src/models/__init__.py` | 移除 业务 业务模型（`Post`、`Product`、`PostNeighbor` 等），保留 `BaseItem`、`Point`、`ExtraContext`、`DeploymentContext` |
| `src/lib/models/llm.py` | `src/models/llm.py` | **Q3-A：零预置**——仅保留 `CompletionConfig` 配置模型与机制，~50 个 model 实例留在 示例业务项目 |
| `src/lib/models/schemas.py` | `src/models/schemas.py` | 保留通用 `SchemaBase` |
| `src/lib/exceptions.py` | `src/infra/exceptions.py` | 原样迁移：可重试/不可重试异常分类 |
| `src/lib/error_handlers.py` | `src/infra/error_handlers.py` | **Q2-B：通用重试框架**——保留 `http_error_handler`、`llm_request_error_handler`、`retry_reasoning`、`generate_retry_tag`；`retry_scraping` 泛化为通用装饰器（状态码分类、指数退避），业务专属 403 逻辑通过参数/子类注入；`handle_authorization` 移出 |
| `src/lib/utils.py` | `src/infra/utils.py` | 原样迁移 |
| `src/lib/types.py` | `src/infra/types.py` | 原样迁移 |
| `src/lib/db.py` | `src/infra/db.py` | 移除 30+ 个 `biz_posts_*` collection 常量；保留 `MongoDB` 连接管理与 `WORKFLOW_DB` 泛化版本 |

## 3. 功能点（全部挂 Issue）

| # | 功能点 | 标签 |
| --- | --- | --- |
| 2.1 | 迁移基础数据模型（BaseItem/Point/ExtraContext/DeploymentContext） | feature |
| 2.2 | 迁移 `models/llm.py` — 仅保留 `CompletionConfig`（Q3-A） | feature |
| 2.3 | 迁移 `models/schemas.py` — 保留 `SchemaBase` | feature |
| 2.4 | 迁移 `infra/exceptions.py` | feature |
| 2.5 | 迁移 `infra/error_handlers.py` — 通用重试框架（Q2-B） | feature |
| 2.6 | 迁移 `infra/utils.py` + `infra/types.py` | feature |
| 2.7 | 迁移 `infra/db.py` — 移除 业务 collections | feature |
| 2.8 | M2 单元测试：异常分类、重试装饰器、utils 纯函数、模型校验 | testing |

## 4. 确认点（挂 Issue，标签 `confirmation`）

| # | 确认点 | 背景 |
| --- | --- | --- |
| C2.1 | `db.py` 是否需要支持多数据库实例/多集群连接管理？ | 当前为单例连接 + `WORKFLOW_DB` 环境变量；若用户项目需跨库聚合需扩展 |
| C2.2 | `vocabulary.py` 是否纳入框架？ | PRD 附录 A 标注"评估"，需确认其是否领域无关 |

## 5. 验收标准

- `grep -ri "示例业务项目\|biz_" src/models/ src/infra/` 无输出（对应总纲 AC1 局部）
- `pytest tests/unit/models tests/unit/infra` 全绿
- `mypy src/models src/infra` 零报错
