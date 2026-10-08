# ADR-0002: Hexagonal-lite inside a deployable-unit monorepo

- **Status:** accepted
- **Date:** 2026-10-08

## Context
The repo root is organized by deployable unit (`services/`, `ingest/`, `packages/`, `infra/`, `eval/`, `bench/`),
which matches how the pieces ship (Cloud Run API, Spark job, relay). The open question was how code is structured
*inside* them. The core experiments need swappable implementations behind one interface: Qdrant vs pgvector with
the same vectors (ADR-4), one embedding model at ingest and query time (ADR-5). And `ragcore`, shared by ingest,
API, eval and bench, risked becoming a grab-bag that mixes pure rules with Qdrant, ONNX and LangGraph.

## Decision
Keep the deployable-unit layout at the root. Inside `ragcore`, the API and the Spark job, use **hexagonal-lite**:

```
ragcore/domain/       pure rules and types: chunking, point IDs, versioning, wire contracts. stdlib + pydantic only
ragcore/ports.py      Protocols the application needs: Embedder, VectorIndex
ragcore/application/  use cases (index a micro-batch, the RAG graph); domain + ports + orchestration libs
ragcore/adapters/     port implementations used by 2+ deployables (Qdrant, pgvector, ONNX)
<service>/main.py     composition root: reads settings, picks adapters, injects them
```

Dependencies point inward only: domain ← ports ← application ← adapters/composition roots.
`packages/ragcore/tests/test_layers.py` enforces this in CI.

Rules of placement:
- Code goes in `ragcore` only when 2+ deployables need it; otherwise it stays in its service.
- An adapter only one service uses (e.g. FastAPI routes + SSE) lives in that service, not in `ragcore.adapters`.
- Adapter third-party deps go in optional extras (`ragcore[qdrant]`, …), so the Spark image does not pull FastAPI.
- Small single-purpose services (`outbox_relay`, `freshness_probe`, `admin`) stay as plain modules + `main.py`:
  nothing to swap, so ports would be ceremony.

## Considered options
- **Full Clean Architecture** (entities / use cases / interface adapters / frameworks, with DTO mapping at every
  boundary): same dependency rule, but the extra layers and mappers cost more than they return in a solo Python
  project of this size.
- **Flat modules in `ragcore`** (the M0 state): fine at two files, but nothing stops infra imports leaking into
  chunking/ID logic that must stay identical on both sides.

## Consequences
- The two seams that are *not* Python ports: outbox relay vs Debezium meet at the `kb-changes` topic, so their
  shared interface is the `KBChange` contract; mock/ollama/haiku are swapped inside `llm-gateway`. A `ChatModel`
  port arrives in M4, shaped by the streaming/retract decision (D2), so the graph can be tested with a fake.
- `foreachBatch` in the Spark job is a thin shell: it calls a pure application function (dedupe, version check,
  upserts and trailing deletes per ADR-3) and hands the result to the index adapters. The four cases of PLAN §4.1
  become plain unit tests that need no Spark.
- Port signatures are v0 until the first adapters land in M2; changing them then is expected.
