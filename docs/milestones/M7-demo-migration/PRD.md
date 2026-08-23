# M7 — 示例业务项目适配与回归（PRD + 设计）

> **状态**: 未开始
> **前置**: M5 + M6
> **GitHub Milestone**: `M7`
> **总纲**: [../PRD.md](../PRD.md)

## 1. 目标

示例业务项目 从自研 codebase 切换为框架消费者：业务代码全部继承/引用 `prefect_pipeline`，验证总纲 G2（100% 兼容）、AC3、AC6。

## 2. 功能点（全部挂 Issue）

| # | 功能点 | 标签 |
| --- | --- | --- |
| 7.1 | 业务 `pyproject.toml` 添加 `prefect_pipeline` 依赖；全部 import 从 `..lib.*` 改为 `prefect_pipeline.*` | feature |
| 7.2 | 业务适配层：`DemoItemExtractor`、`Demo*Transformer`、业务专属 403 retry 适配、`handle_authorization`、`DemoSpider(HttpSpider[DemoItem])`、LLM model 实例配置、collection 常量——继承/注入框架基类 | feature |
| 7.3 | 业务 回归验证：流水线行为不变（AC3：`python -m src.main` 正常启动；AC6：`orchestrations.py` 零修改运行；时区行为比对） | testing |
| 7.4 | 消灭 `generated/deployments.py`：`version_id` 回归 env，部署清单物化到 MongoDB（替代框架包内生成文件） | feature |

## 3. 设计要点

- **适配层位置**: `示例业务项目/src/lib/` 保留为业务层（只含 业务专属类），框架职责全部删除。
- **Q2-B 适配**: 业务专属 403 处理通过 `retry_scraping(403, ..., on_give_up=biz_auth_refresh)` 注入，`handle_authorization` 留 业务。
- **Q3-A 适配**: ~50 个 LLM model 实例定义移入 `示例业务项目/src/lib/models/llm.py`（业务配置）。
- **Q5-A 适配**: 业务解析逻辑封装为 `DemoSpider.parse()`；浏览器会话管理改用框架 `CDPSpider`。
- **时区**: `class DemoFlow(PipelineFlow): timezone = "Asia/Shanghai"` 保持行为不变。

### 3.1 generated 文件迁移（7.4）

**现状问题**：`FlowsLoader.write_deployment_map()` 把「部署清单 `{ns → {name, flow_name, qualname}}` + `version_id`」写进框架包内 `src/prefect_pipeline/generated/deployments.py`。框架 runtime（`runner_base.get_deployment_context`）仅 import 其中的 `version_id`；业务侧 `DeploymentContextManager` 则 import 其中的 `deployments` 映射。问题：启动写盘（只读 FS 崩溃）、污染框架包目录、`version_id`（配置）与清单（数据）混放、文件可能过期。

**保留的原优化动机**：原设计用「启动时发现一次 → 写文件 → 后续 flow run 进程 import 共享」来避免每个 flow run 重复执行 `iter_files + render_deployment_map`（重复 import 全部 flow 模块）。该优化思路保留，仅更换共享载体。

**迁移（两层）**：

1. **`version_id` 回归 env**：`runner_base.get_deployment_context` 的 `from ..generated.deployments import version_id` 改为 `from .configs import VERSION_ID`。消除框架 runtime 对 generated 的唯一依赖。
2. **部署清单物化到 MongoDB**：`FlowsLoader.load()` 部署时把清单 upsert 进 MongoDB（与「注册到 Prefect」平行的自然副作用，替代 `write_deployment_map`）；`DeploymentContextManager` 改为从 MongoDB 读清单（替代 import `.py`）。载体换成框架已有的 `PREFECT` 状态存储——跨进程/跨机器、只读 FS 安全、生命周期清晰（部署即更新）。

**与 C3.1 的关系**：C3.1 已决策「不暴露可配置输出路径」；本设计在其基础上更进一步——既然 generated 的路径与生命周期都存在根本问题，直接消灭该文件、用 MongoDB 承载，而非为其增加配置项。

## 4. 验收标准（对应总纲）

- AC3: 业务 正常启动并完整跑通一个采集-推理-入库周期
- AC6: `orchestrations.py` 零修改
- 时区比对：迁移前后 `get_current_date()` 逐日一致
- 业务 仓库内 `grep -rn "from ..lib.runner_base\|from ..lib.deployment"` 无输出
- 7.4 验收：框架仓库内无 `generated/` 目录、无 `write_deployment_map`、无 `from ..generated` 引用；`python -m src.main` 启动零写盘（只读 FS 下可启动）；`version_id` 由 env 提供
