"""Retrieval request/result types shared by every VectorIndex adapter (PLAN.md §5.1)."""
from dataclasses import dataclass

from ragcore.domain.chunks import Chunk


@dataclass(frozen=True, slots=True)
class SearchQuery:
    """`text` feeds the sparse/BM25 side, `vector` the dense side; adapters fuse them (RRF)."""
    text: str
    vector: list[float]
    k: int = 10
    locale: str | None = None
    product: str | None = None


@dataclass(frozen=True, slots=True)
class SearchHit:
    chunk: Chunk
    score: float
