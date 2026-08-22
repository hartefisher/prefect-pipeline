# M6 — 框架级测试套件（设计文档）

> **前置阅读**: [PRD.md](./PRD.md) · 总纲 [../../PRD.md](../../PRD.md) §10、§14

## 1. 目录结构

```
tests/
├── conftest.py            # 全局 fixtures + fakes 注册
├── unit/                  # L1（M2~M5 交付，此处归档组织）
│   ├── models/  infra/  core/  components/  runners/
├── regression/            # L2 DAG 语义回归
│   ├── test_dag_semantics.py
│   ├── test_trigger_lifecycle.py
│   └── test_backfill_idempotency.py
├── e2e/                   # L3
│   ├── mini_pipeline/
│   │   ├── flows/         # 3 个 toy Flow（fetch → transform → embed）
│   │   └── test_mini_pipeline.py
├── integration/           # L4（C6.1 确认后建设）
│   ├── docker-compose.yml # mongo + qdrant
│   └── test_services.py
└── markers.ini            # slow / integration 标记注册
```

## 2. 测试基础设施设计（功能点 6.1）

### 2.1 Fakes 清单

| Fake | 替代对象 | 实现要点 |
| --- | --- | --- |
| `FakeMongoCollection` | motor collection | 内存 dict + 异步接口（find/insert_one/update_many/bulk_write/aggregation 子集） |
| `FakeQdrantClient` | AsyncQdrantClient | 内存向量检索（暴力余弦），支持 upsert/search |
| `FakeLLM` | litellm acompletion/batch | 可编程响应序列；XML 输出构造器；按脚本抛异常（测重试） |
| `FakeTransport` | httpx | respx 风格：URL → 响应/异常映射 |

### 2.2 关键 fixtures

```python
@pytest.fixture
def toy_item(): ...                 # 最小 BaseItem 子类

@pytest.fixture
async def fake_db(monkeypatch): ... # 注入 FakeMongoCollection 到 MongoDB

@pytest.fixture
def prefect_ephemeral(): ...        # Prefect ephemeral API（测试内自动启停）
```

## 3. DAG 语义回归设计（功能点 6.2）

回归测试 = 把 示例业务项目 现有编排行为固化为断言，作为迁移正确性的最终裁决：

| 用例 | 断言 |
| --- | --- |
| 序列 `A >> B` | A completion → trigger 恰好发起一次 B（starter_id 透传） |
| 并行 `A + B` | 两者皆 completion 才触发下游（peer 检查），任一失败不触发 |
| 混合 `A >> (B + C) >> D` | B、C peer 集合互指；D 被 downstream_signal 去重为一次 |
| Condition ALL | 任一上游 Failed → check_all 返回 False |
| Hook 分发 | completion/failure 事件分别命中注册函数 |
| Backfill 幂等 | 同 (dt, days) 重复运行不产生重复数据（fake collection 计数断言） |
| disable_trigger | 标志位生效时不触发任何下游 |

## 4. E2E mini pipeline 设计（功能点 6.3）

```
ToyFetch (FakeLLM 产出 3 条 ToyItem)
   >> ToyTransform (FakeMongo upsert 加工)
   >> ToyEmbed (FakeQdrant 入库 3 向量)
```

- Prefect ephemeral 模式启动（无外部 server 依赖）。
- 断言：3 个 Flow 依次 Completed；fake mongo 中 3 条加工记录；fake qdrant 中 3 个点。
- 该用例同时是 README 快速开始的活文档（M8 examples 与其同构）。

## 5. 覆盖率与 CI（功能点 6.4）

```toml
# pyproject.toml
[tool.coverage.report]
fail_under = 75

# 细分门槛通过 CI 脚本二次检查
# pytest --cov=src --cov-report=term; cov core>=85 由 coverage json + jq 断言
```

```yaml
# .github/workflows/test.yml
matrix: python [3.12, 3.13]
steps: install (uv/pip) → ruff → mypy → pytest -m "not integration"
# L4 集成测试：services 容器 + pytest -m integration（C6.1 确认后启用）
```

## 6. 风险

| 风险 | 缓解 |
| --- | --- |
| Prefect ephemeral 模式与部署行为差异 | L3 仅验证 DAG 触发链路；真实调度差异靠 M7 业务 回归兜底 |
| Fake motor 聚合语义与真实 MongoDB 漂移 | Fake 只实现框架用到的聚合子集并文档化；C6.1 的 L4 补真实容器验证 |
| 异步测试不稳定（事件循环泄漏） | pytest-asyncio strict 模式 + fixture 生命周期统一管理 |
