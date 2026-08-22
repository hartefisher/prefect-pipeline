# M4 — 组件层迁移（设计文档）

> **前置阅读**: [PRD.md](./PRD.md) · 总纲 [../../PRD.md](../../PRD.md) §5.2、§6.3

## 1. 组件依赖图

```
helper.py ◄──── data.py ◄──── llm.py
   │              │              │
   │              ▼              ▼
   └────── vector.py        batch.py (litellm)
                  │
              spider.py (httpx / Playwright CDP)
```

实施顺序建议：4.1 helper → 4.2 data → 4.3 llm + 4.4 vector/batch → 4.5 spider。

## 2. SpiderBase 泛化设计（Q5-A 落地）

### 2.1 抽象边界

原 `Spider` / `CDPSpider` 中可泛化的部分：

| 能力 | 归属 |
| --- | --- |
| 请求调度（队列、速率限制） | 框架 `SpiderBase` |
| 并发控制（信号量、批量分组） | 框架 `SpiderBase` |
| 重试接入（对接 M2 `retry_scraping` 通用装饰器） | 框架 `SpiderBase` |
| HTTP 爬虫（httpx 实现） | 框架 `HttpSpider` |
| 浏览器爬虫（Playwright CDP 实现） | 框架 `CDPSpider` |
| URL 构造、响应解析、字段提取 | **用户项目**（业务解析逻辑留 示例业务项目） |

### 2.2 接口草案

```python
class SpiderBase(Generic[Item]):
    concurrency: int = 10
    retry_statuses: tuple[int, ...] = (429, 500, 503)

    async def crawl(self, urls: list[str]) -> AsyncIterator[Item]:
        """调度 + 并发 + 重试，逐条 yield 解析结果"""

    async def parse(self, response) -> AsyncIterator[Item]:
        """子类必须实现：响应 → Item 流"""

class HttpSpider(SpiderBase[Item]):
    """httpx 异步实现"""

class CDPSpider(SpiderBase[Item]):
    """Playwright CDP 实现，复用 示例业务项目 的浏览器会话管理"""
```

- 业务侧（M7）：`class DemoSpider(HttpSpider[DemoItem])`，`parse()` 中保留 业务专属解析与 403 处理注入。
- CDP 浏览器会话管理（原 `src/setup.py` 的启动逻辑）随 `CDPSpider` 进入框架，做成可关闭的可选依赖（`prefect_pipeline[cdp]` extra）。

## 3. 其余组件迁移要点

### 3.1 data.py
- `DataFetcher`: 类属性声明式查询（`collection`/`filter`/`sort`），`AutoItemModel` 泛型推断返回类型——签名不变。
- `DataTransformer`: `process_item()` 异步生成器 + `UpdateOp`/`IncOp` upsert 语义不变。
- `EmbeddingHandler`: 向量入库（Qdrant Point 构造）不变。

### 3.2 llm.py
- `LLMExtractionStrategy`（单次）/ `BatchLLMExtractionStrategy`（批量）依赖 `CompletionConfig`（M2 已就位），零预置实例，model 由用户注入。
- `GenericExtractor`: `schema_model` + `ns` + `version` 三要素接口不变，作为用户继承的主要扩展点。
- `DemoItemExtractor`（含 `https://example.com/{slug}` 硬编码）移出。

### 3.3 batch.py
- litellm batch 生命周期：提交 → 轮询 → 拉取结果 → 解析入库。
- C4.1 决策前保持 litellm 直连；若确认抽象，引入 `BatchProvider` 协议。

## 4. 测试设计（功能点 4.6）

| 测试对象 | 策略 |
| --- | --- |
| 泛型注入 | 自定义 `ToyItem(BaseItem)`，验证 `DataFetcher[ToyItem]`/`GenericExtractor[ToyItem]` 的类型推断与实例化 |
| DataFetcher/DataTransformer | mock motor collection（find/find_one/update_many/bulk_write），断言查询构造与 upsert 语义 |
| LLMExtractor | mock litellm `acompletion`，断言 prompt 组装、XML 解析（复用 M2 utils）、失败重试分类 |
| batch | mock litellm batch API 状态机（pending → completed → results），断言轮询终止条件 |
| vector | mock AsyncQdrantClient（upsert/search）；EmbeddingModel 用 fastembed 小模型真实加载（标记 slow test） |
| SpiderBase | mock httpx（respx 或手写 transport），断言并发上限、重试状态码触发、parse 被调用 |

## 5. 风险

| 风险 | 缓解 |
| --- | --- |
| spider 泛化时破坏 业务 爬虫行为 | M7 回归重点项；SpiderBase 保持窄接口，业务 逻辑全在子类 |
| fastembed 模型下载拖慢 CI | EmbeddingModel 真实加载标记 `slow`，CI 默认跳过 |
| Playwright 为重量级可选依赖 | 通过 extra `prefect_pipeline[cdp]` 隔离，未安装时 import 报错信息友好 |
