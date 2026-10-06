from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ragcore.contracts import KBChange, SSEEvent


def test_kb_change_rejects_version_zero():
    with pytest.raises(ValidationError):
        KBChange(event_id="e", op="upsert", doc_id="a", doc_version=0,
                 updated_at=datetime.now(UTC), ts_commit_ns=1)


def test_sse_event_names_are_stable():
    assert {e.value for e in SSEEvent} == {"status", "token", "citations", "retract", "done", "error"}
