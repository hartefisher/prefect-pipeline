# M1 — 框架骨架与工程基线（PRD）

> **状态**: ✅ 已完成（2026-08-22，commit 971262e）
> **前置**: 无
> **GitHub Milestone**: `M1`
> **总纲**: [../PRD.md](../PRD.md)

## 1. 目标

搭建 `prefect_pipeline` 仓库骨架与工程化基线，使后续迁移代码有稳定的落点，且 lint/CI 从第一天就生效。

## 2. 功能点（全部挂 Issue，标签 `feature` / `testing`）

| # | 功能点 | 说明 | Issue |
| --- | --- | --- | --- |
| 1.1 | 目录结构创建 | `src/{core,components,runners,infra,models}` 分层目录 | ✅ 已完成 |
| 1.2 | `.gitignore` 更新 | 排除 `product_hunt/`、`.env`、`src/generated/` | ✅ 已完成 |
| 1.3 | PRD 总纲 | v0.2，Q1-Q5 决策落定 | ✅ 已完成 |
| 1.4 | README.md 初版 | 英文，架构图 + DAG 用法示例 | ✅ 已完成 |
| 1.5 | `pyproject.toml` 初始化 | 包定义、依赖清单（含 dev 依赖组）、构建后端 (hatchling)、Python >=3.12 | ✅ #6 |
| 1.6 | ruff + mypy 工程化基线 | lint 规则、类型检查配置、`py.typed` 标记 | ✅ #7 |
| 1.7 | GitHub Actions lint workflow | push/PR 触发 ruff + mypy | ✅ #8 |

## 3. 验收标准

- `pip install -e .` 成功（1.5 完成后）
- `ruff check .` 与 `mypy src/` 在空骨架上零报错
- GitHub Actions lint workflow 在 main 分支绿灯

## 4. 确认点

无新增确认点。
