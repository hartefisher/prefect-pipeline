# M5 — Runner 类型层与公共 API（设计文档）

> **前置阅读**: [PRD.md](./PRD.md) · 总纲 [../../PRD.md](../../PRD.md) §5.4、§8

## 1. runners/ 子包结构

```
src/runners/
├── __init__.py        # 导出全部 Runner
├── base.py            # PipelineFlow + Retry
├── transformation.py  # TransformationFlow (+ Overall)
├── reasoning.py       # ReasoningFlow (+ Overall)
├── embedding.py       # EmbeddingFlow (+ Overall)
├── aggregation.py     # AggregationFlow (+ Overall)
└── scraping.py        # WebScrapingFlow
```

## 2. PipelineFlow 泛化设计

```python
class PipelineFlow(FlowRunnerBase):
    project_name: str = "default"          # 子类必须设置（原 "ph" 硬编码移除）
    timezone: str = "UTC"                  # 类属性可覆盖；原 get_current_date() 硬编码 US/Pacific

    def get_current_date(self) -> date:
        return datetime.now(ZoneInfo(self.timezone)).date()
```

- `project_name` 用于 collection 命名空间与日志前缀，product_hunt 侧 `class ProductHuntFlow(PipelineFlow): project_name = "ph"`。
- 时区从 pytz 迁移到标准库 `zoneinfo`（Python >=3.12），保留 pytz 兼容字符串（`"US/Pacific"` 直接可用），PH 迁移时设置 `timezone = "US/Pacific"` 保持行为不变。

## 3. Runner 与组件的注入关系

```
PipelineFlow (时间窗口/filter/data_flag)
├── TransformationFlow ── DataTransformer
├── ReasoningFlow ────── GenericExtractor + DataFetcher   (实时 acompletion / 批量 batch API)
├── EmbeddingFlow ────── EmbeddingHandler + DataFetcher
├── AggregationFlow ──── DataTransformer (aggregation pipeline)
└── WebScrapingFlow ──── SpiderBase + DataFetcher
```

各 Runner 的 `setup()` / `run()` / `clear()` 生命周期与 `deploy()` 注册接口保持原签名——这是 PH 流水线零修改迁移（AC6）的关键。

## 4. 公共 API 分层导出

`src/__init__.py` 按总纲 §8.1 组织，分四组：

1. **编排核心**: `Deployment`, `Node`, `FlowRunnerBase`, `Hook`, `Orchestration`, `Condition`, `PeersPolicy`, `FlowsLoader`, `EntryPoints`
2. **Runner 类型**: `PipelineFlow`, `TransformationFlow`, `ReasoningFlow`, `EmbeddingFlow`, `AggregationFlow`
3. **组件**: `DataFetcher`, `DataTransformer`, `EmbeddingHandler`, `LLMExtractor`, `GenericExtractor`, `LLMExtractionStrategy`, `EmbeddingModel`, `AsyncQdrantClient`, `SpiderBase`, `HttpSpider`, `CDPSpider`, `PipelineHelper`, `UpdateOp`, `IncOp`, `bulk_write`
4. **模型与基础设施**: `BaseItem`, `Point`, `ExtraContext`, `DeploymentContext`, `CompletionConfig`, `MongoDB`, 异常清单

`__all__` 显式列出全部导出名，配合 mypy strict 检查无泄漏的深层 import。

## 5. 测试设计（功能点 5.3）

| 测试组 | 覆盖内容 |
| --- | --- |
| Runner 注册 | `deploy()` 生成 Deployment、名字/调度参数正确、`Overall` 变体跳过时间窗口过滤 |
| 依赖注入 | Runner `setup()` 接收注入组件类并实例化（mock 组件） |
| 时区 | `timezone="US/Pacific"` 与 UTC 下 `get_current_date()` 差异断言 |
| API 导出 | `test_public_api.py`：`__all__` 中每个名字可 import、无循环导入 |
| 冒烟 | toy Flow（fake 组件）`deploy()` + `aserve()` 启动即退出（M6 E2E 的雏形） |

## 6. 风险

| 风险 | 缓解 |
| --- | --- |
| `runners.py` 单文件拆包导致 PH 侧 import 大面积改动 | 框架 `runners/__init__.py` 聚合导出，PH 只改一次 import 行 |
| 时区行为差异导致 PH 数据窗口错位 | M7 回归中专门比对 `get_current_date()` 输出 |
