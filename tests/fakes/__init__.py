"""Reusable in-memory fakes for the `prefect_pipeline` test-suite.

These stand-ins replace external services (MongoDB, Qdrant, LLM gateway,
HTTP transport) so the framework can be exercised end-to-end without any
network access or running containers. They intentionally implement only the
subset of the real APIs the framework actually touches and are documented as
such in ``docs/milestones/M6-testing/DESIGN.md``.
"""

from __future__ import annotations

from .http import FakeTransport
from .llm import FakeLLM
from .mongo import FakeMongoCollection, FakeMongoDB
from .qdrant import FakeQdrantClient

__all__ = [
    "FakeLLM",
    "FakeMongoCollection",
    "FakeMongoDB",
    "FakeQdrantClient",
    "FakeTransport",
]
