# M2 — 基础设施层迁移（设计文档）

> **前置阅读**: [PRD.md](./PRD.md) · 总纲 [../../PRD.md](../../PRD.md) §5.3、§6

## 1. 模块设计

### 1.1 models 包

```
src/models/
├── __init__.py   # BaseItem, Point, ExtraContext, DeploymentContext
├── llm.py        # CompletionConfig (Q3-A: 零预置实例)
└── schemas.py    # SchemaBase
```

- **BaseItem**: 泛型 Item 基类，`DataFetcher[Item]`、`LLMExtractor[Item]` 等泛型注入的目标类型。迁移时保持 pydantic v2 模型签名不变。
- **DeploymentContext**: trigger() 的上下文载体（下游节点 + peer 信息），字段与 product_hunt 现状保持一致，M3 编排层直接复用。
- **CompletionConfig**: LLM 推理配置（model、temperature、max_tokens、batch 支持）。框架只提供"配置形状"，不提供实例——用户项目自行声明 model 实例并注入 `LLMExtractionStrategy`。

### 1.2 infra 包

```
src/infra/
├── db.py              # MongoDB 连接管理
├── exceptions.py      # 异常分类
├── error_handlers.py  # 重试装饰器
├── types.py           # BatchResponse 等类型别名
└── utils.py           # TimeCounter / extract_xml_data / consistent_string_id
```

## 2. 关键设计决策

### 2.1 retry_scraping 通用化（Q2-B 落地）

原 `retry_scraping` 混合了通用 HTTP 重试与 PH 403 反爬处理。拆分方案：

```python
# 框架层：通用重试装饰器（状态码分类 + 指数退避）
def retry_scraping(
    *retry_statuses: int,           # 触发重试的状态码集合，如 429, 500, 503
    max_retries: int = 4,
    backoff_base: float = 60.0,
    on_give_up: Callable | None = None,   # 放弃回调（可注入站点特殊处理）
): ...

# product_hunt 适配层（M7）：子类/回调注入 PH 403 特殊逻辑
ph_retry = retry_scraping(
    403, 429,
    on_give_up=ph_authorization_refresh,   # PH token 刷新
)
```

- `handle_authorization()`（PH GraphQL 逻辑）整体移出框架，留在 product_hunt。
- 状态码 → 异常分类的映射表保留在框架（可重试 vs 不可重试）。

### 2.2 db.py 泛化

- 移除全部 `ph_posts_*` collection 常量（30+ 个）。
- 保留：`MongoDB` 类（motor 异步客户端封装、连接生命周期、`WORKFLOW_DB` 环境变量默认值机制）。
- 用户项目通过 `DB = MongoDB(); coll = DB.my_collection` 直接属性访问，无需框架预置常量。
- 多实例连接管理为确认点 C2.1，默认本期不做。

### 2.3 异常分类机制

`exceptions.py` 维持两组清单不变：

- `retriable_exceptions`: 网络超时、限流、5xx 类
- `unretriable_exceptions`: 参数错误、鉴权失败、schema 不匹配

重试装饰器（error_handlers）与 `RetryReasoning` 五级重试均依赖此分类，迁移时不得改变异常继承结构。

## 3. 测试设计（功能点 2.8）

| 测试对象 | 策略 |
| --- | --- |
| 异常分类 | 断言清单成员的继承关系与装饰器识别行为 |
| retry 装饰器 | mock 被装饰函数按序抛出可重试/不可重试异常，断言重试次数、退避时长、放弃回调触发 |
| utils 纯函数 | `extract_xml_data`（正常/畸形 XML）、`consistent_string_id`（确定性）、`TimeCounter`（耗时记录） |
| pydantic 模型 | `CompletionConfig` 校验规则、`BaseItem` 序列化 |
| db.py | mock motor AsyncIOMotorClient，验证属性访问与连接懒加载 |

不依赖真实 MongoDB——motor 客户端全部 mock（真实服务集成测试归 M6）。

## 4. 风险

| 风险 | 缓解 |
| --- | --- |
| `models/__init__.py` 中 PH 模型与通用模型存在交叉 import | 迁移时逐个检查 `BaseItem` 等通用模型的字段是否引用 PH 类型 |
| 重试装饰器签名变化影响 product_hunt 调用方 | M7 提供适配层；框架保留原函数名 `retry_scraping` |
