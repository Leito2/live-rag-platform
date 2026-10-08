"""Ports: what the application needs from the outside world (ADR-0002).

Signatures are v0 and will be refined by the first adapters (M2: ONNX embedder, Qdrant, pgvector).
A ChatModel port arrives with M4; the provider swap (mock/ollama/haiku) happens inside llm-gateway.
"""
from collections.abc import Sequence
from typing import Protocol

from ragcore.domain.chunks import EmbeddedChunk
from ragcore.domain.search import SearchHit, SearchQuery


class Embedder(Protocol):
    """Same model at ingest and query time (ADR-5). e5 needs different prefixes for each side."""
    dim: int

    def embed_passages(self, texts: Sequence[str]) -> list[list[float]]: ...

    def embed_query(self, text: str) -> list[float]: ...


class VectorIndex(Protocol):
    """Implemented by Qdrant and pgvector with the same vectors, so they compare fairly (ADR-4)."""

    def upsert(self, chunks: Sequence[EmbeddedChunk]) -> None: ...

    def delete_doc(self, doc_id: str, from_chunk_index: int = 0) -> None:
        """Delete chunks with index ≥ from_chunk_index: 0 = whole doc, n = trailing cleanup (ADR-3)."""
        ...

    def search(self, query: SearchQuery) -> list[SearchHit]: ...
