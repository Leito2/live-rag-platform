"""Contracts v1 (PLAN.md §2.3, §5.4)."""
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field


class KBChange(BaseModel):
    """Event on topic kb-changes — same shape from the outbox relay (lite) or Debezium (⏳16GB)."""
    event_id: str
    op: Literal["upsert", "delete"]
    doc_id: str
    doc_version: int = Field(ge=1)
    title: str = ""
    body_md: str = ""
    locale: Literal["es", "en"] = "es"
    product: str | None = None
    updated_at: datetime
    ts_commit_ns: int = Field(ge=0)


class AskRequest(BaseModel):
    question: str = Field(min_length=2, max_length=2000)
    locale: Literal["es", "en"] | None = None
    product: str | None = None


class SSEEvent(StrEnum):
    STATUS = "status"
    TOKEN = "token"
    CITATIONS = "citations"
    RETRACT = "retract"
    DONE = "done"
    ERROR = "error"
