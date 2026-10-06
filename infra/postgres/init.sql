-- Source of truth + transactional outbox + pgvector index (PLAN.md §2.3, ADR-1).
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS articles (
  doc_id      TEXT PRIMARY KEY,
  doc_version INT  NOT NULL DEFAULT 1,
  title       TEXT NOT NULL,
  body_md     TEXT NOT NULL,
  locale      TEXT NOT NULL CHECK (locale IN ('es','en')),
  product     TEXT,
  deleted     BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Written in the SAME transaction as the article change; the relay publishes and marks sent_at.
CREATE TABLE IF NOT EXISTS outbox (
  id         BIGSERIAL PRIMARY KEY,
  doc_id     TEXT NOT NULL,
  payload    JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (id) WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS kb_chunks (
  point_id    UUID PRIMARY KEY,                 -- uuid5(doc_id#chunk_index), same as Qdrant
  doc_id      TEXT NOT NULL,
  chunk_index INT  NOT NULL,
  doc_version INT  NOT NULL,
  locale      TEXT NOT NULL,
  text        TEXT NOT NULL,
  embedding   vector(384) NOT NULL,             -- multilingual e5-small dimension (verify in M2)
  tsv         tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED
);
CREATE INDEX IF NOT EXISTS kb_chunks_hnsw ON kb_chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS kb_chunks_tsv  ON kb_chunks USING gin (tsv);
CREATE INDEX IF NOT EXISTS kb_chunks_doc  ON kb_chunks (doc_id, chunk_index);

CREATE TABLE IF NOT EXISTS indexed_versions (doc_id TEXT PRIMARY KEY, doc_version INT NOT NULL);

CREATE TABLE IF NOT EXISTS qa_log (
  id          BIGSERIAL PRIMARY KEY,
  question    TEXT NOT NULL,
  answer      TEXT,
  citations   JSONB,
  abstained   BOOLEAN,
  retracted   BOOLEAN,
  cost_usd    NUMERIC(10,6),
  feedback    SMALLINT,                          -- -1 / 0 / +1
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
