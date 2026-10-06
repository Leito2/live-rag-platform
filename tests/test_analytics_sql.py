"""Analytics SQL is written for BigQuery and tested on DuckDB via sqlglot (PLAN §15.4): no GCP in CI."""
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")

SQL_DIR = Path(__file__).resolve().parents[1] / "analytics" / "sql"

FIXTURES = """
CREATE TABLE freshness_events (ts_commit TIMESTAMP, ts_retrievable TIMESTAMP, index_name VARCHAR);
INSERT INTO freshness_events
  SELECT now() - INTERVAL 1 DAY, now() - INTERVAL 1 DAY + to_milliseconds(i * 10), 'qdrant'
  FROM range(1, 101) t(i);
CREATE TABLE qa_events (ts TIMESTAMP, topic VARCHAR, locale VARCHAR, abstained BOOLEAN);
INSERT INTO qa_events
  SELECT now() - INTERVAL 2 DAY, 'fees', 'es', i % 2 = 0 FROM range(10) t(i);
"""


def run(name: str) -> list[tuple]:
    con = duckdb.connect()
    con.execute(FIXTURES)
    sql = sqlglot.transpile((SQL_DIR / name).read_text(encoding="utf-8"), read="bigquery", write="duckdb")[0]
    return con.execute(sql).fetchall()


def test_every_query_transpiles_and_runs():
    for path in SQL_DIR.glob("*.sql"):
        run(path.name)


def test_freshness_p95():
    (day, index_name, changes, p95_ms), = run("freshness_daily.sql")
    assert index_name == "qdrant" and changes == 100 and 900 <= p95_ms <= 1000


def test_abstention_gaps():
    (topic, locale, questions, abstentions, rate), = run("abstention_gaps.sql")
    assert (topic, locale, questions, abstentions) == ("fees", "es", 10, 5) and rate == pytest.approx(0.5)
