# M7 — product_hunt 适配与回归（PRD + 设计）

> **状态**: 未开始
> **前置**: M5 + M6
> **GitHub Milestone**: `M7`
> **总纲**: [../PRD.md](../PRD.md)

## 1. 目标

product_hunt 从自研 codebase 切换为框架消费者：业务代码全部继承/引用 `prefect_pipeline`，验证总纲 G2（100% 兼容）、AC3、AC6。

## 2. 功能点（全部挂 Issue）

| # | 功能点 | 标签 |
| --- | --- | --- |
| 7.1 | PH `pyproject.toml` 添加 `prefect_pipeline` 依赖；全部 import 从 `..lib.*` 改为 `prefect_pipeline.*` | feature |
| 7.2 | PH 业务适配层：`PostExtractor`、`Standardization*Transformer`、PH 403 retry 适配、`handle_authorization`、`PHSpider(HttpSpider[Post])`、LLM model 实例配置、collection 常量——继承/注入框架基类 | feature |
| 7.3 | PH 回归验证：流水线行为不变（AC3：`python -m src.main` 正常启动；AC6：`orchestrations.py` 零修改运行；时区行为比对） | testing |

## 3. 设计要点

- **适配层位置**: `product_hunt/src/lib/` 保留为业务层（只含 PH 专属类），框架职责全部删除。
- **Q2-B 适配**: PH 403 处理通过 `retry_scraping(403, ..., on_give_up=ph_auth_refresh)` 注入，`handle_authorization` 留 PH。
- **Q3-A 适配**: ~50 个 LLM model 实例定义移入 `product_hunt/src/lib/models/llm.py`（业务配置）。
- **Q5-A 适配**: PH 解析逻辑封装为 `PHSpider.parse()`；浏览器会话管理改用框架 `CDPSpider`。
- **时区**: `class ProductHuntFlow(PipelineFlow): timezone = "US/Pacific"` 保持行为不变。

## 4. 验收标准（对应总纲）

- AC3: PH 正常启动并完整跑通一个采集-推理-入库周期
- AC6: `orchestrations.py` 零修改
- 时区比对：迁移前后 `get_current_date()` 逐日一致
- PH 仓库内 `grep -rn "from ..lib.runner_base\|from ..lib.deployment"` 无输出
