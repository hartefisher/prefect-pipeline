# M5 — Runner 类型层与公共 API（PRD）

> **状态**: 未开始
> **前置**: M3 + M4
> **GitHub Milestone**: `M5`
> **总纲**: [../PRD.md](../PRD.md) · **设计**: [DESIGN.md](./DESIGN.md)

## 1. 目标

将 `runners.py` 拆分为 `runners/` 子包并泛化 Runner 类型，完成顶层公共 API 导出——本里程碑结束后框架在 API 层面完整可用。

## 2. 迁移范围

| 原类 (示例业务项目) | 框架类 | 改造点 |
| --- | --- | --- |
| `DemoFlow` | `PipelineFlow` (`runners/base.py`) | 重命名；`project_name` 由子类设置；时区 `Asia/Shanghai` → 可配置，默认 UTC |
| `TransformationFlow` | `runners/transformation.py` | 同名迁移 |
| `ReasoningFlow` | `runners/reasoning.py` | 同名迁移（实时/批量双模式） |
| `EmbeddingFlow` | `runners/embedding.py` | 同名迁移 |
| `AggregationFlow` | `runners/aggregation.py` | 同名迁移 |
| `WebScrapingFlow` | `runners/scraping.py` | 同名迁移，对接 M4 `SpiderBase` |
| `Retry` | `runners/base.py` 内 | 同名迁移（retries=4, retry_delay=60s） |
| 各 `Overall*` 变体 | 各模块内 | 同名迁移（variant="Overall" 跳过时间窗口） |
| — | `src/__init__.py` | 公共 API 全量导出（总纲 §8.1） |

## 3. 功能点（全部挂 Issue）

| # | 功能点 | 标签 |
| --- | --- | --- |
| 5.1 | 泛化 `runners/` 子包（PipelineFlow + 6 类 Runner + Retry + Overall 变体） | feature |
| 5.2 | 公共 API 导出 `src/__init__.py` + 全量 import 路径调整 | feature |
| 5.3 | M5 单元测试：Runner 注册、依赖注入、时区配置、Overall 变体 | testing |

## 4. 确认点

无新增确认点（时区默认 UTC 为总纲已定方案，如需其他默认值在实施 Issue 中提出）。

## 5. 验收标准

- `from prefect_pipeline import Deployment, FlowRunnerBase, PipelineFlow, ...` 按总纲 §8.1 清单全部可用
- `grep -rn "Asia/Shanghai" src/` 无输出（时区已参数化）
- `pytest tests/unit/runners` 全绿
- 一个 toy Flow 能 `deploy()` 注册并通过 `prefect.aserve()` 启动（冒烟验证）
