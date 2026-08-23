from typing import Any

from qdrant_client import AsyncQdrantClient as AsyncQdrantClientBase
from qdrant_client.http.models import ExtendedPointId
from qdrant_client.models import Distance, PointStruct, VectorParams

from prefect_pipeline.models import Point


class EmbeddingModel:
    def __init__(
        self,
        model_name: str = "BAAI/bge-base-en-v1.5",
        max_seq_length: int | None = None,
        multi_process: bool | int = False,
    ):
        from fastembed import TextEmbedding

        # 配置初始化参数
        kwargs = {}
        if max_seq_length:
            kwargs["max_length"] = max_seq_length

        # 如果不设置，默认会利用所有 CPU 核心
        if isinstance(multi_process, int) and multi_process > 0:
            kwargs["threads"] = multi_process

        self.model = TextEmbedding(model_name=model_name, **kwargs)

        # 记录多进程参数，用于 embed_batch 的并行处理
        self.parallel = multi_process if isinstance(multi_process, int) else None

    def embed(self, text: str) -> list[float]:
        """对单条文本进行向量化"""
        # fastembed 始终返回生成器。获取第一个元素并转为 Python 原生 list
        embedding_gen = self.model.embed(text)
        return next(iter(embedding_gen)).tolist()  # type: ignore[no-any-return]

    def struct_point(
        self, text: str, id: ExtendedPointId, payload: dict[str, Any]
    ) -> PointStruct:
        """构建单个 Qdrant Point"""
        embedding = self.embed(text)
        return PointStruct(id=id, vector=embedding, payload=payload)

    def embed_batch(self, texts: list[str], batch_size: int = 32) -> list[list[float]]:
        """批量处理文本，利用 fastembed 内置的并行能力"""
        # 使用 parallel 参数替代 sentence-transformers 的 pool
        embeddings_gen = self.model.embed(
            texts, batch_size=batch_size, parallel=self.parallel
        )
        # 将生成的 numpy array 转换为 Python 原生 list
        return [emb.tolist() for emb in embeddings_gen]

    def struct_points(
        self, points: list[Point], batch_size: int = 256
    ) -> list[PointStruct]:
        """构建批量 Qdrant Points"""
        texts = [p.text for p in points]
        # fastembed 处理批量数据非常快，默认 batch_size 可以调大一点 (例如 256)
        embeddings = self.embed_batch(texts, batch_size=batch_size)

        return [
            PointStruct(id=p.id, vector=embedding, payload=p.payload)
            for embedding, p in zip(embeddings, points)
        ]

    def clear(self) -> None:
        """清理资源"""
        # fastembed 及 ONNX Runtime 会通过 Python 垃圾回收机制自动清理。
        # 不再需要手动 stop_multi_process_pool。
        pass


class AsyncQdrantClient(AsyncQdrantClientBase):
    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
    ):
        if url is None:
            import os

            from dotenv import load_dotenv

            load_dotenv()
            url = os.environ.get("QDRANT_API_URL")
            api_key = os.environ.get("QDRANT_API_KEY")
        super().__init__(url=url, api_key=api_key)

    async def get_or_create_collection(self, collection_name: str) -> Any:
        collection: Any
        try:
            collection = await self.get_collection(collection_name)
        except Exception:
            collection = await self.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=768, distance=Distance.COSINE),
            )
        return collection
