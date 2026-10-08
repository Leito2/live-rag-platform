"""Markdown-aware chunking + deterministic point IDs (PLAN.md §4.3, ADR-2/ADR-3)."""
import re
import uuid
from dataclasses import dataclass
from datetime import datetime

NAMESPACE = uuid.UUID("6f1c2a52-7c3e-4b1a-9d1e-2f6a8c0b4e11")
_HEADING = re.compile(r"^(#{1,3})\s+(.*)$", re.MULTILINE)


def point_id(doc_id: str, chunk_index: int) -> str:
    """Same (doc, chunk) → same ID, so replays overwrite instead of duplicating."""
    return str(uuid.uuid5(NAMESPACE, f"{doc_id}#{chunk_index}"))


def chunk_markdown(title: str, body_md: str, max_chars: int = 1800) -> list[str]:
    """Split on headings first, then by size; prefix each chunk with its title/heading path."""
    sections, last, heading = [], 0, ""
    for m in _HEADING.finditer(body_md):
        if m.start() > last:
            sections.append((heading, body_md[last:m.start()].strip()))
        heading, last = m.group(2).strip(), m.end()
    sections.append((heading, body_md[last:].strip()))
    chunks = []
    for head, text in sections:
        if not text:
            continue
        prefix = f"{title} › {head}".strip(" ›") if head else title
        for i in range(0, len(text), max_chars):
            chunks.append(f"{prefix}\n\n{text[i:i + max_chars]}")
    return chunks


@dataclass(frozen=True, slots=True)
class Chunk:
    """One indexed piece of an article; the same record lives in Qdrant and pgvector (PLAN §2.3)."""
    doc_id: str
    chunk_index: int
    doc_version: int
    title: str
    text: str
    locale: str
    product: str | None
    updated_at: datetime

    @property
    def point_id(self) -> str:
        return point_id(self.doc_id, self.chunk_index)


@dataclass(frozen=True, slots=True)
class EmbeddedChunk:
    chunk: Chunk
    vector: list[float]
