from ragcore.chunking import chunk_markdown, point_id

DOC = "Intro text.\n\n## Fees\nTransfers cost 2%.\n\n## Limits\nMax 5,000 USD per day."


def test_point_ids_are_deterministic_and_distinct():
    assert point_id("art_1", 0) == point_id("art_1", 0)
    assert point_id("art_1", 0) != point_id("art_1", 1)


def test_chunks_follow_headings_and_carry_context():
    chunks = chunk_markdown("Transfers", DOC)
    assert len(chunks) == 3
    assert chunks[1].startswith("Transfers › Fees") and "2%" in chunks[1]


def test_shrinking_document_produces_fewer_chunks():
    assert len(chunk_markdown("T", "## A\nx")) < len(chunk_markdown("T", DOC))
