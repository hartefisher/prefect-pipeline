# Prefect Pipeline Framework

A domain-agnostic, orchestrable pipeline framework built on **Prefect v3**, designed for LLM-driven unstructured data processing workflows.

## Key Features

- **DAG via operator overloading** — Define pipelines with `>>` (sequence) and `+` (parallel), no YAML or config files
- **Auto Flow discovery** — Scan `flows/` directory, automatically register and deploy all Flows
- **Unified LLM gateway** — Batch and real-time inference via litellm, supporting 50+ model providers
- **Vector search** — Qdrant integration with fastembed local embeddings
- **Hook system** — `@Hook.on(event="completion")` lifecycle hooks for cross-Flow triggering
- **Fault-tolerant** — Configurable retries, error handlers, and idempotent backfill mode
- **MongoDB native** — Async motor driver with upsert-safe data transformers

## Architecture

```
User Project (flows/, prompts/, orchestrations/)
         │
         ▼
┌────────────────────────────────────────┐
│      Prefect Pipeline Framework         │
│  ┌──────────┐ ┌──────────┐ ┌─────────┐ │
│  │Orchestra-│ │Components│ │   Infra  │ │
│  │tion Layer│ │  Layer   │ │  Layer   │ │
│  │          │ │          │ │          │ │
│  │DAG Engine│ │DataFetch │ │ MongoDB  │ │
│  │Deployment│ │Transform │ │ Qdrant   │ │
│  │RunnerBase│ │LLMExtract│ │LLM Gate- │ │
│  │Loader    │ │Embedding │ │  way     │ │
│  │Condition │ │Batch     │ │Errors    │ │
│  └──────────┘ └──────────┘ └─────────┘ │
└────────────────────────────────────────┘
         │                        │
         ▼                        ▼
   Prefect v3            litellm / motor /
   (scheduling)          qdrant_client
```

## DAG Definition

```python
from prefect_pipeline import Deployment, FlowRunnerBase, Hook
from prefect_pipeline.runners import PipelineFlow

# Define a Flow
class MyFlow(PipelineFlow):
    project_name = "my_project"

    async def setup(self, MyComponent, **extra):
        self.component = MyComponent(**extra)

    async def run(self):
        await self.component.run()

# Register and compose DAG
A = MyFlow.deploy(MyComponent, name="A")
B = MyFlow.deploy(OtherComponent, name="B")
C = MyFlow.deploy(ThirdComponent, name="C")

# A >> B  (sequence: A completes, then B runs)
# A + B   (parallel: A and B run together)
# A >> (B + C)  (A completes, then B and C run in parallel)
pipeline = A >> (B + C)
```

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Orchestration engine | Prefect | >=3.0 |
| LLM gateway | litellm | >=1.0 |
| MongoDB driver | motor | >=3.0 |
| Vector DB | qdrant-client | >=1.0 |
| Embedding model | fastembed | >=0.3 |
| Data validation | pydantic | >=2.0 |
| HTTP client | httpx | >=0.27 |

**Python >= 3.12** (uses 3.12 generic syntax)

## Project Status

This framework is currently in **M1 (skeleton)** phase. See [PRD](docs/PRD.md) for the full design document and roadmap.

## Documentation

- [PRD (v0.1 Draft)](docs/PRD.md) — Full product requirements document with architecture, API design, and migration plan

## License

MIT (pending)
