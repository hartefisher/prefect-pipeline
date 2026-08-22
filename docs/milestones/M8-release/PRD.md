# M8 — 示例、文档与开源发布（PRD）

> **状态**: 未开始
> **前置**: M7
> **GitHub Milestone**: `M8`
> **总纲**: [../PRD.md](../PRD.md)

## 1. 目标

完成开源发布准备：示例、架构文档、许可证与贡献指南、敏感信息审查。Q4 已决策：**不提供 Dockerfile 模板**。

## 2. 功能点（全部挂 Issue，标签 `docs`）

| # | 功能点 |
| --- | --- |
| 8.1 | `examples/simple_pipeline.py`（最简 DAG）+ `examples/llm_extraction.py`（LLM 提取）+ `examples/batch_inference.py`（批量推理），与 M6 E2E mini pipeline 同构 |
| 8.2 | `docs/architecture.md` 架构详解（分层图、trigger 时序、扩展点指南） |
| 8.3 | LICENSE (MIT) + CONTRIBUTING.md（开发流程 + 代码规范 + 测试要求） |
| 8.4 | 敏感信息审查（总纲 §12.1 清单）+ 发布 tag `v0.1.0` |

## 3. 验收标准

- 三个 example 可 clone 后按 README 步骤直接运行
- `grep -ri "api_key\|password\|secret" src/` 仅匹配变量名（AC5）
- 敏感信息审查 checklist 全部勾选，打 tag 发布
