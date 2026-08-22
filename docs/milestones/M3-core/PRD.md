# M3 — 编排核心层迁移（PRD）

> **状态**: 未开始
> **前置**: M2（与 M4 可并行）
> **GitHub Milestone**: `M3`
> **总纲**: [../PRD.md](../PRD.md) · **设计**: [DESIGN.md](./DESIGN.md)

## 1. 目标

迁移框架最核心的编排层：DAG 定义运算符、Flow 自动发现注册、Hook 系统、trigger() 触发器。**此层语义不可变**（总纲 §13.2 设计约束），迁移以"原样搬运 + 配置泛化"为原则。

## 2. 迁移范围

| 源文件 | 目标文件 | 改造点 |
| --- | --- | --- |
| `src/lib/deployment.py` | `src/core/deployment.py` | 无（`Deployment` + `Node` + `>>`/`+` 运算符原样迁移） |
| `src/lib/orchestration.py` | `src/core/orchestration.py` | 无（纯 DAG 分析引擎，零业务耦合） |
| `src/lib/runner_base.py` | `src/core/runner_base.py` | 无（Hook 系统 + trigger() 原样迁移） |
| `src/loader.py` | `src/core/loader.py` | 无（FlowsLoader + EntryPoints + 代码生成） |
| `src/lib/condition.py` | `src/core/condition.py` | 无（ALL/ANY 策略） |
| `src/lib/ns_converter.py` | `src/core/ns_converter.py` | 无 |
| `src/lib/configs.py` | `src/core/configs.py` | **泛化**：移除 `MACRO_VARIABLES = {"ph": [...]}` 预置与 `PH_API_URL`/`PH_DEV_TOKEN` 等 PH 专属环境变量 |
| `src/main.py` | `src/main.py` | **泛化**：入口仅做 FlowsLoader.load() + prefect.aserve，无 PH 启动逻辑 |

## 3. 功能点（全部挂 Issue）

| # | 功能点 | 标签 |
| --- | --- | --- |
| 3.1 | 迁移 `core/deployment.py`（Deployment/Node + `>>`/`+` 运算符） | feature |
| 3.2 | 迁移 `core/orchestration.py`（DAG 分析引擎） | feature |
| 3.3 | 迁移 `core/runner_base.py`（Hook 系统 + trigger 触发器） | feature |
| 3.4 | 迁移 `core/loader.py`（FlowsLoader + EntryPoints + 代码生成） | feature |
| 3.5 | 迁移 `core/condition.py` + `core/ns_converter.py` | feature |
| 3.6 | 泛化 `core/configs.py` + `main.py`（去 PH 配置） | feature |
| 3.7 | M3 单元测试：DAG 运算符语义、Condition 策略、ns_converter、Hook 注册 | testing |

## 4. 确认点（挂 Issue，标签 `confirmation`）

| # | 确认点 | 背景 |
| --- | --- | --- |
| C3.1 | FlowsLoader 代码生成输出（`src/generated/deployments.py`）的路径是否需要可配置？ | 当前硬编码 `src/generated/`；用户项目目录结构可能不同 |

## 5. 验收标准

- DAG 三种组合（`A>>B`、`A+B`、`A>>(B+C)>>D`）的 node_map/上下游关系与 product_hunt 行为一致（单元测试断言）
- `MACRO_VARIABLES` 由用户项目注入，框架无 `"ph"` 预置
- `pytest tests/unit/core` 全绿；`mypy src/core` 零报错
