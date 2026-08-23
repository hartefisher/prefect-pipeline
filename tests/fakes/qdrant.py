"""In-memory stand-in for ``AsyncQdrantClient``.

Implements cosine-similarity search over an in-memory point store so the
embedding/vector components can be exercised without a running Qdrant server.
Supports the subset the framework uses: ``upsert``, ``query`` (and the legacy
``search`` alias), ``get_collections`` / ``get_collection`` /
``create_collection`` for ``get_or_create_collection``.
"""

from __future__ import annotations

from typing import Any

import numpy as np


def _cosine(a: list[float], b: list[float]) -> float:
    av = np.asarray(a, dtype=float)
    bv = np.asarray(b, dtype=float)
    denom = np.linalg.norm(av) * np.linalg.norm(bv)
    if denom == 0:
        return 0.0
    return float(np.dot(av, bv) / denom)


class _ScoredPoint:
    def __init__(self, id: Any, score: float, payload: dict[str, Any]) -> None:
        self.id = id
        self.score = score
        self.payload = payload


class FakeQdrantClient:
    """Brute-force cosine similarity vector store."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._points: dict[str, list[tuple[Any, list[float], dict[str, Any]]]] = {}
        self._collections: dict[str, bool] = {}

    async def get_collections(self) -> Any:
        names = [type("Coll", (), {"name": n})() for n in self._collections]
        return type("Collections", (), {"collections": names})()

    async def get_collection(self, collection_name: str) -> Any:
        if collection_name not in self._collections:
            raise ValueError(f"Collection {collection_name} does not exist")
        return type("CollectionInfo", (), {"status": "green", "name": collection_name})()

    async def create_collection(self, collection_name: str, *args: Any, **kwargs: Any) -> bool:
        self._collections[collection_name] = True
        self._points.setdefault(collection_name, [])
        return True

    async def get_or_create_collection(self, collection_name: str, *args: Any, **kwargs: Any) -> Any:
        if collection_name not in self._collections:
            await self.create_collection(collection_name)
        return collection_name

    async def upsert(self, collection_name: str, points: list[Any]) -> Any:
        store = self._points.setdefault(collection_name, [])
        for p in points:
            if hasattr(p, "vector"):
                vector = list(p.vector)
                pid = getattr(p, "id", None)
                payload = getattr(p, "payload", None) or {}
            else:
                vector = list(p.get("vector", []))
                pid = p.get("id")
                payload = p.get("payload", {}) or {}
            store.append((pid, vector, dict(payload)))
        return type("UpdateResult", (), {"status": "completed"})()

    async def query(
        self,
        collection_name: str,
        query: list[float],
        limit: int = 10,
        with_payload: bool = True,
        **kwargs: Any,
    ) -> list[_ScoredPoint]:
        store = self._points.get(collection_name, [])
        scored = [
            _ScoredPoint(pid, _cosine(query, vec), payload if with_payload else {}) for pid, vec, payload in store
        ]
        scored.sort(key=lambda s: s.score, reverse=True)
        return scored[:limit]

    async def search(
        self, collection_name: str, query_vector: list[float], limit: int = 10, **kwargs: Any
    ) -> list[_ScoredPoint]:
        return await self.query(collection_name, query_vector, limit=limit, **kwargs)

    async def count(self, collection_name: str, *args: Any, **kwargs: Any) -> Any:
        return type("CountResult", (), {"count": len(self._points.get(collection_name, []))})()

    async def delete(self, collection_name: str, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        self._points.pop(collection_name, None)
