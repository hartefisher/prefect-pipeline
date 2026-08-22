# M3 — 编排核心层迁移（设计文档）

> **前置阅读**: [PRD.md](./PRD.md) · 总纲 [../../PRD.md](../../PRD.md) §5.1、§13.2

## 1. 设计原则：语义不可变

总纲 §13.2 明确四条不可变约束，本层占三条：

1. `>>`（序列）与 `+`（并行）语义不变
2. trigger() 的 peer 状态检查 + downstream_signal 去重 + macro_variables 透传逻辑不简化
3. `@Hook.on(event=...)` 装饰器机制原样

因此 M3 的设计重点不是重构，而是**迁移正确性验证**——通过单元测试将现有语义固化为断言，作为后续所有改动的回归基线（M6 在此基础上扩展为完整 DAG 语义回归）。

## 2. 模块结构

```
src/core/
├── deployment.py      # Deployment + Node, __rshift__, __add__
├── orchestration.py   # DAG 遍历 → node_map + 可视化文本
├── runner_base.py     # FlowRunnerBase + Hook + trigger()
├── loader.py          # FlowsLoader + EntryPoints + 代码生成
├── condition.py       # Condition (ALL/ANY)
├── ns_converter.py    # path ↔ flow_name ↔ entrypoint
└── configs.py         # 环境变量 + 路径（泛化后）
```

## 3. configs.py 泛化设计

### 迁移前（示例业务项目）

```python
MACRO_VARIABLES = {"demo": [...]}          # 业务 预置
EXAMPLE_API_URL = os.getenv("EXAMPLE_API_URL")     # 业务专属
```

### 迁移后（框架）

```python
# 框架只保留机制，不预置内容
MACRO_VARIABLES: dict[str, list] = {}    # 用户项目启动时注入

def register_macro_variables(project: str, variables: list[str]) -> None:
    """用户项目注册自己的宏变量（示例业务项目 在 main.py 中调用 register_macro_variables("demo", [...]))"""
```

- 业务专属环境变量（`EXAMPLE_API_URL`、`EXAMPLE_DEV_TOKEN` 等）整体移除，留在 示例业务项目 的配置模块。
- 框架保留的环境变量：`ENVIRONMENT`、`FLOWS_DIRECTORY`、`PROMPTS_DIRECTORY`、`WORKFLOW_POOL`。

## 4. trigger() 依赖关系

trigger() 是跨模块协作的核心，迁移顺序需保证依赖先就位：

```
runner_base.trigger()
  ├── models.DeploymentContext          (M2 已迁移)
  ├── deployment.Deployment / Node      (3.1)
  ├── condition.Condition.check_all()   (3.5)
  ├── prefect run_deployment API        (外部依赖)
  └── configs.MACRO_VARIABLES           (3.6)
```

Issue 实施顺序建议：3.1 → 3.2 → 3.5 → 3.6 → 3.3 → 3.4（loader 依赖 deployment 与 ns_converter）。

## 5. 测试设计（功能点 3.7）

| 测试组 | 覆盖内容 |
| --- | --- |
| 运算符语义 | `A >> B` 生成上下游边；`A + B` 生成 peer 关系；`(A + B) >> C` 的 downstream 指向与 peer 集合；运算符交换律/结合律边界 |
| orchestration | 给定 DAG 组合，断言 node_map 键值与可视化文本行序 |
| Condition | ALL 策略（任一上游失败 → 不触发）、ANY 策略、空上游 |
| ns_converter | path → flow_name → entrypoint 往返一致性 |
| Hook | `@Hook.on(event=...)` 注册到类、多事件绑定、completion/failure 分发 |
| trigger | mock Prefect API：peer 未齐不触发、peer 齐且条件满足触发一次（downstream_signal 去重）、macro_variables + starter_id 透传 |

> trigger() 测试是整个测试套件价值最高的部分——它把"血缘追踪 + 去重"这两处最容易在迁移中损坏的逻辑锁定。

## 6. 风险

| 风险 | 缓解 |
| --- | --- |
| Prefect v3 内部 API（DeploymentContext、run_deployment 签名）版本差异 | pyproject 锁定 prefect minor 版本；CI 中跑触发器测试 |
| loader 代码生成的模板字符串与框架包名耦合 | 生成模板中的 `src.generated` 引用改为 `prefect_pipeline` 命名空间（C3.1 确认后决定是否参数化） |
