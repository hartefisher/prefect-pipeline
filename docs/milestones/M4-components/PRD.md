# M4 — 组件层迁移（PRD）

> **状态**: 未开始
> **前置**: M2（与 M3 可并行）
> **GitHub Milestone**: `M4`
> **总纲**: [../PRD.md](../PRD.md) · **设计**: [DESIGN.md](./DESIGN.md)

## 1. 目标

迁移可复用数据处理组件（数据/LLM/向量/爬虫/批量），并落地 Q5-A 决策：`Spider` 组件以通用 `SpiderBase` 形式纳入框架。

## 2. 迁移范围

| 源文件 | 目标文件 | 改造点 |
| --- | --- | --- |
| `src/lib/components/helper.py` | `src/components/helper.py` | 无（PipelineHelper / AutoItemModel / UpdateOp / IncOp / bulk_write 原样） |
| `src/lib/components/data.py` | `src/components/data.py` | 保留 `DataFetcher`、`DataTransformer`、`EmbeddingHandler`、`SimilarityStats`；移除 业务类（`DemoRelativesTransformer`、`DemoCheck`、`DemoAscend`） |
| `src/lib/components/llm.py` | `src/components/llm.py` | 保留 `LLMExtractionStrategy`、`BatchLLMExtractionStrategy`、`LLMExtractor`、`GenericExtractor`；移除 `DemoItemExtractor` |
| `src/lib/components/vector.py` | `src/components/vector.py` | 无（EmbeddingModel / AsyncQdrantClient 原样） |
| `src/lib/components/batch.py` | `src/components/batch.py` | 无（litellm batch 提交/轮询） |
| `src/lib/components/spider.py` | `src/components/spider.py` | **Q5-A**：泛化为 `SpiderBase`（请求调度、并发控制、重试接入），业务解析逻辑留 示例业务项目 |

## 3. 功能点（全部挂 Issue）

| # | 功能点 | 标签 |
| --- | --- | --- |
| 4.1 | 迁移 `components/helper.py` | feature |
| 4.2 | 迁移 `components/data.py`（移除 业务 Transformer） | feature |
| 4.3 | 迁移 `components/llm.py`（移除 DemoItemExtractor，Extractor 基类化） | feature |
| 4.4 | 迁移 `components/vector.py` + `components/batch.py` | feature |
| 4.5 | 迁移 `components/spider.py` — `SpiderBase` 通用化（Q5-A） | feature |
| 4.6 | M4 单元测试：泛型 Item 注入、mock motor/qdrant/litellm/httpx | testing |

## 4. 确认点（挂 Issue，标签 `confirmation`）

| # | 确认点 | 背景 |
| --- | --- | --- |
| C4.1 | `batch.py` 的 litellm batch 提交/轮询是否抽象为 provider 接口？ | 当前直接耦合 litellm batch API；抽象后可扩展其他批量推理 provider，但增加复杂度 |

## 5. 验收标准

- 组件泛型签名不变：`DataFetcher[Item]`、`LLMExtractor[Item]`、`GenericExtractor[Item]` 可用自定义 Item 实例化
- `grep -ri "示例业务项目\|biz_\|DemoItemExtractor" src/components/` 无输出
- `pytest tests/unit/components` 全绿
