# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

Live RAG Platform: RAG over a knowledge base that changes (Postgres outbox/CDC → Redpanda/Kafka → Spark Structured Streaming → Qdrant + pgvector, agentic LangGraph retriever that cites or abstains, SSE token streaming, final test on Cloud Run with Claude Haiku).

The repo is at **M0 bootstrap**. Most directories (`services/*`, `ingest/spark`, `eval`, `bench`, `observability`, `infra/{qdrant,redpanda,debezium}`) contain only `.gitkeep` placeholders. What actually exists: `packages/ragcore`, `infra/postgres/init.sql`, `infra/gcp/*.tf`, `analytics/sql/*.sql`, `docker-compose.yml`, `scripts/doctor.py`, and the tests. Don't assume a service exists because `PLAN.md` or `README.md` describes it.

**`PLAN.md` (Spanish) is the source of truth for design.** Code comments cite it by section (e.g. "PLAN.md §4.3, ADR-2"). Read the relevant section before building a component: ADRs are in §2.2, event/table schemas in §2.3, the milestone list with acceptance criteria in §12 (M0–M10), risks in §13, v2 additions in §14, BigQuery analytics in §15. `README.md` is written in English and is progressive (concepts first, technical detail last); keep new README sections consistent with that skeleton (PLAN §11.2).

## Commands

Python 3.12+, managed with `uv` (workspace). `make` is optional on Windows; each target is a thin wrapper.

```bash
python scripts/doctor.py           # pre-flight checks (stdlib only): uv, docker, RAM, disk, free ports  (make doctor)
uv sync --all-packages --dev       # install everything                                                  (make setup)
uv run pytest -q                   # all tests                                                           (make test)
uv run pytest packages/ragcore/tests/test_chunks.py::test_point_ids_are_deterministic_and_distinct -q   # single test
uv run ruff check .                # lint (CI runs this)                                                 (make lint)
uv run ruff format .               # format                                                              (make fmt)
make up PROFILE=full-lite          # docker compose profile: ingest | serve | full-lite
make down
make tf-plan                       # `terraform init -backend=false && terraform validate` only — never apply
```

CI (`.github/workflows/ci.yml`) runs exactly: `uv sync --all-packages --dev`, `uv run ruff check .`, `uv run pytest -q`. No Docker, GCP, or network services are available in CI. The Makefile targets `kb-seed`, `reindex`, `freshness-demo`, `eval*`, `gcp-final-test` are stubs that exit 1 until their milestone lands.

Ruff: line length 110, rules `E,F,I,UP,B`, `ragcore` is first-party for isort. `.editorconfig`: 4-space Python, 2-space for yml/json/md/sql/tf, LF endings.

## Architecture (big picture)

Two flows share one codebase, and **the shared package `ragcore` is what keeps them consistent**:

- **Ingest path:** Postgres `articles` + `outbox` are written in one transaction (transactional outbox, ADR-1) → relay publishes `KBChange` events to topic `kb-changes` (key `doc_id`) → Spark `foreachBatch` (2s micro-batches) chunks, embeds per batch, and upserts/deletes in **both** Qdrant and pgvector with identical vectors (ADR-4).
- **Query path:** FastAPI SSE → LangGraph agent (hybrid retrieve → grade → rewrite → generate with citations → groundedness check → abstain) → always through the separate `llm-gateway` project (ADR-6; semantic cache, circuit breaker, budget cap). The API never calls Anthropic directly.

Invariants that span multiple files — preserve them:

- **Deterministic point IDs** (`ragcore.domain.chunks.point_id` = `uuid5(NAMESPACE, "doc_id#chunk_index")`; `Chunk.point_id` derives it, never stores it): the same ID is used in Qdrant and `kb_chunks.point_id`, which makes replays idempotent (at-least-once streaming → effectively exactly-once). Changing `NAMESPACE` or the key format invalidates every existing index.
- **Chunking must be identical at ingest and query time**, so it lives only in `ragcore` (`chunk_markdown`: split on `#`–`###` headings, then by `max_chars`, each chunk prefixed with `title › heading`).
- **Versioning/cleanup (ADR-3):** events carry `doc_version`; ingestion ignores versions lower than the indexed one (`indexed_versions` table), deletes leftover chunk indexes when a doc shrinks, and a `delete` op removes all of a doc's points.
- **Event contract** (`ragcore.domain.contracts.KBChange`) is the same whether it comes from the Python outbox relay (8 GB "lite") or Debezium (`⏳16GB`). `SSEEvent` fixes the stream event names: `status, token, citations, retract, done, error`.
- `infra/postgres/init.sql` (`articles`, `outbox`, `kb_chunks` with `vector(384)` + generated `tsv`, `indexed_versions`, `qa_log`) must stay in sync with the contracts and the embedding dimension (e5-small, 384 — marked "verify in M2").

Resource/cost constraints that shape choices: everything must fit in 8 GB (`docker-compose.yml` sets per-service `mem_limit`; profiles `ingest`/`serve`/`full-lite`); spend is **$0 until the M8 final test** (LLM provider is `mock`/`ollama` in dev, Haiku only in the final test); GCP via Terraform is plan/validate only until then. The `llm-gateway` compose service builds from `../llm-gateway` (a sibling repo, not in this one).

### Code architecture: hexagonal-lite ([ADR-0002](docs/adr/0002-hexagonal-lite.md))

The repo root stays organized by deployable unit. Inside `ragcore`, the API and the Spark job, dependencies point inward only:

- `ragcore/domain/`: pure rules and types (chunking, IDs, versioning, wire contracts). stdlib + pydantic only.
- `ragcore/ports.py`: `Protocol`s the application needs (`Embedder`, `VectorIndex`; `ChatModel` arrives in M4).
- `ragcore/application/`: use cases (micro-batch indexing, the RAG graph). Imports domain + ports + orchestration libs like LangGraph.
- `ragcore/adapters/`: port implementations used by 2+ deployables (Qdrant, pgvector, ONNX), with their deps as optional extras.
- Each service's `main.py` is its composition root: the only place that picks and wires adapters.

Placement: code enters `ragcore` only when 2+ deployables need it; a service-only adapter (FastAPI routes, SSE) stays in its service. Small services (`outbox_relay`, `freshness_probe`, `admin`) are plain modules + `main.py`, without ports. Spark's `foreachBatch` stays a thin shell around a pure application function, so ingestion cases are unit-tested without Spark. `packages/ragcore/tests/test_layers.py` enforces the import rules; a failure there means the code belongs in an outer layer.

### Analytics SQL (BigQuery tested on DuckDB)

`analytics/sql/*.sql` is written in **BigQuery dialect**. `tests/test_analytics_sql.py` transpiles each file with `sqlglot` (`read="bigquery", write="duckdb"`) and runs it on a DuckDB fixture, so no GCP is needed in CI. New SQL files are automatically picked up by `test_every_query_transpiles_and_runs`; if you add a table they read, add it to `FIXTURES` in that test, and add a dedicated assertion test. Keep to SQL that sqlglot can transpile.

## Conventions

- `packages/ragcore` uses a src layout with `hatchling`; workspace members are `packages/*`. Each service gets its own directory under `services/` or `ingest/`.
- Tests live in `packages/<pkg>/tests/` (unit) and top-level `tests/` (cross-cutting, `tests/integration/` reserved). `pytest` `testpaths` is configured for both; tests needing optional deps use `pytest.importorskip`.
- Never commit `.env`, generated data, model weights (`*.onnx`, `*.safetensors`), `*.parquet`, `docs/results/*/`, or Terraform state (see `.gitignore`).
