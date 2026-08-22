# M6 — 框架级测试套件（PRD）

> **状态**: 未开始
> **前置**: M5（测试基础设施自 M2 起持续交付，本里程碑专项收口）
> **GitHub Milestone**: `M6`
> **总纲**: [../PRD.md](../PRD.md) · **设计**: [DESIGN.md](./DESIGN.md)

## 1. 目标

建设框架级测试套件：M2~M5 已随代码交付单元测试，本里程碑补齐**测试基础设施（外部服务 fakes）、DAG 语义回归、E2E 集成测试、覆盖率门槛与 CI test workflow**，使框架达到可开源的质量基线（总纲 G6、AC7、AC8）。

## 2. 测试分层

| 层 | 内容 | 交付于 | 外部依赖 |
| --- | --- | --- | --- |
| L1 单元测试 | 各模块纯逻辑 + mock 外部服务 | M2~M5（已完成） | 无 |
| L2 DAG 语义回归 | 运算符/Hook/trigger 端到端语义固化 | **M6** | mock Prefect API |
| L3 E2E 集成测试 | 真实 Prefect + fakes 组件跑完整流水线 | **M6** | 本地 Prefect（ephemeral） |
| L4 服务集成测试（可选） | 真实 MongoDB/Qdrant 容器 | **M6**（确认点 C6.1） | docker-compose |

## 3. 功能点（全部挂 Issue）

| # | 功能点 | 标签 |
| --- | --- | --- |
| 6.1 | 测试基础设施：`tests/conftest.py` + Mongo/Qdrant/LLM/HTTP fakes + fixtures | testing |
| 6.2 | DAG 语义回归测试：序列/并行/混合/peer 检查/条件触发/Backfill 幂等 | testing |
| 6.3 | E2E 集成测试：mini pipeline（3 Flow DAG）ephemeral Prefect 运行 | testing |
| 6.4 | 覆盖率门槛（pytest-cov：core ≥85%，全包 ≥75%）+ CI test workflow（py3.12/3.13 matrix） | testing |

## 4. 确认点（挂 Issue，标签 `confirmation`）

| # | 确认点 | 背景 |
| --- | --- | --- |
| C6.1 | L4 服务集成测试依赖 docker-compose（真实 Mongo/Qdrant 容器）还是纯 fake？ | docker-compose 更真实但 CI 需容器支持；纯 fake 全平台可跑但覆盖不到驱动行为差异 |

## 5. 验收标准

- `pytest` 单命令全绿（含 L1~L3；L4 若确认建设则通过 `pytest -m integration` 单独触发）
- `pytest --cov=src` 达到门槛且在 CI 强制（fail-under）
- DAG 语义回归覆盖：`A>>B`、`A+B`、`A>>(B+C)>>D`、peer 部分完成不触发、Condition ALL 失败不触发、Backfill 幂等
- GitHub Actions test workflow 在 push/PR 双触发、双 Python 版本绿灯
