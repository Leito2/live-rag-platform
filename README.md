# 📡 Live RAG Platform

> RAG over a knowledge base that changes: Postgres outbox/CDC → Kafka → Spark Structured Streaming → Qdrant + pgvector,
> an agentic LangGraph retriever that cites or abstains, token streaming over SSE, and a final test on Cloud Run with
> Claude Haiku. **Headline (to be measured):** _index freshness p95 {F}s, faithfulness {G}, total cloud cost < ${C}._

**Status:** 🟡 M0 bootstrap. Full design in [`PLAN.md`](PLAN.md) (Spanish). Courses behind it: Stream engines
(Spark for ML ingestion), LLM Token Streaming, Grafana & latency engineering (Learning vault).

## TL;DR — Results at a Glance
## Part I — The Big Picture
### 1. The Problem: Stale RAG in a Regulated Domain
### 2. Core Concepts Primer
RAG · embeddings & ANN indexes · hybrid search & RRF · CDC & the outbox pattern · micro-batch streaming ·
agentic RAG (grading, rewriting, abstention) · groundedness · SSE & TTFT · semantic caching · cost engineering
### 3. What This Project Demonstrates · 4. Architecture · 5. Design Decisions · 6. Journey of a Change · 7. Journey of a Question
## Part II — Components
## Part III — Live Ingestion · Part IV — Retrieval & Generation · Part V — Proof
## Part VI — Run It Yourself

```bash
python scripts/doctor.py
uv sync --all-packages --dev
uv run pytest -q                    # M0: chunking, deterministic IDs, contracts
make up PROFILE=full-lite           # Postgres+pgvector, Qdrant, Redpanda, Redis, llm-gateway
```

## Part VII — Reflection

## License
MIT
