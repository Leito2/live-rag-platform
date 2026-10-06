# 📡 P3 — Live RAG Platform

> **Plan de proyecto** (vive en este repo como `PLAN.md`). **Estado:** ✅ Plan completo (13/13 módulos), listo para implementar en la fase F7 del plan de Learning.
> Stack: PostgreSQL (fuente + CDC) · Kafka · Spark Structured Streaming · HF embeddings · Qdrant + pgvector · LangGraph (RAG agéntico) · FastAPI + SSE · Haiku (solo en el test final) vía LLM Gateway (`llm-gateway`, Python) · MLflow + Langfuse · GCP (Cloud Run + Qdrant Cloud free) · Terraform · Docker Compose
> **v2 (§14):** verificador NLI de alucinaciones (ONNX, CPU) · `judgekit` + panel de jueces (Gemma 4 31B) · Langfuse datasets/experiments · KFP v2 → **Vertex AI Pipelines** (prueba final) · caché semántica de P0 invalidada por los eventos del outbox · baselines de chunking y embeddings del proyecto multiagente
> **v3 (§15):** BigQuery (free tier) como capa analítica: load jobs + Storage Write API, tablas particionadas, Looker Studio; DuckDB con el mismo SQL en local y en la CI
> Gasto: **$0 hasta el test final** (mock/Ollama en desarrollo).

## Módulos del plan
| # | Sección | Estado |
|---|---|---|
| 1 | Visión, problema y frase del CV | ✅ |
| 2 | Arquitectura macro y flujo de datos | ✅ |
| 3 | Componentes (uno por pieza, de lo conceptual a lo técnico) | ✅ |
| 4 | Ingesta en vivo: CDC → Spark → embeddings → índices | ✅ |
| 5 | Retrieval y generación: Qdrant vs pgvector, RAG agéntico, SSE | ✅ |
| 6 | Harness de evaluación (faithfulness, relevancia, frescura, TTFT) | ✅ |
| 7 | Estrategia de costo cero hasta el test final | ✅ |
| 8 | Observabilidad | ✅ |
| 9 | Presupuesto de recursos (8 GB / `⏳ 16GB`) | ✅ |
| 10 | Ejecución local, despliegue en GCP (Terraform) y alternativas | ✅ |
| 11 | Estructura del repo y del README | ✅ |
| 12 | Hitos de implementación y criterios de aceptación | ✅ |
| 13 | Riesgos y pendientes | ✅ |
| 14 | **v2 — Absorción de los proyectos del CV** (auditoría de alucinaciones, Vertex AI Pipelines, caché semántica con invalidación por CDC) | ✅ v2 |
| 15 | **v3 — Analítica con BigQuery** (free tier: load jobs, Storage Write API, Looker Studio; DuckDB en local) | ✅ v3 |

## Regla del README
README progresivo: **contexto teórico, conceptual y macro primero**; en cada componente, el detalle técnico al final. Incluye cómo funciona, los pasos para ejecutarlo y las alternativas de ejecución o despliegue (local primero).

---

## 1. Visión, problema y frase del CV

### 1.1 El problema
La mayoría de los sistemas RAG indexan los documentos **una vez** (o cada noche) y responden con lo que quedó guardado. En una fintech eso es peligroso: si a las 10:00 cambia la comisión de una transferencia internacional y el asistente sigue diciendo el valor viejo a las 10:30, **está dando información financiera incorrecta**.

El problema central es la **frescura**: cuánto tarda un cambio en la fuente de verdad en reflejarse en las respuestas. A eso se suman los problemas clásicos de RAG en producción:
- **Alucinación:** responder algo que no está en los documentos.
- **Falta de abstención:** no saber decir "no tengo esa información".
- **Latencia percibida:** el usuario mira una pantalla en blanco mientras el LLM genera.
- **Costo por consulta.**

### 1.2 Qué construimos
Un asistente de conocimiento para una **fintech ficticia** (un centro de ayuda con comisiones, límites, políticas y productos), en el que:
1. Los artículos viven en **PostgreSQL**, que es la fuente de verdad, como lo haría un CMS.
2. Cada cambio se captura con **CDC** (outbox ligero en 8 GB; Debezium en `⏳ 16GB`) y viaja por Kafka (Redpanda).
3. **Spark Structured Streaming** procesa los cambios en micro-lotes: divide en chunks, calcula embeddings **por lote** y actualiza **Qdrant y pgvector** (inserciones, actualizaciones y **borrados**).
4. Un **RAG agéntico con LangGraph** recupera en forma híbrida (denso + BM25), evalúa la relevancia, reescribe la consulta si hace falta, responde **con citas** o **se abstiene**.
5. La respuesta llega **token a token por SSE** (FastAPI), pasando por el **LLM Gateway** propio (`llm-gateway`) con caché semántica.
6. **Haiku** se usa **solo en la prueba final**. En desarrollo se usan el mock y Ollama ($0).
7. La API y el gateway se despliegan en **GCP (Cloud Run)** contra **Qdrant Cloud (free)**, con Terraform, **solo durante la prueba final**, y luego se destruyen.

### 1.3 Por qué Spark aquí (y no Flink)
Aquí no importan los milisegundos: importa **agrupar** cambios para calcular embeddings eficientemente. Los micro-lotes de Spark son una **ventaja**: `foreachBatch` entrega un lote de documentos cambiados, que se procesa con una sola llamada al modelo de embeddings y un upsert masivo. Una frescura de segundos es excelente para un centro de ayuda. Es el contraste perfecto con P1, y el material del curso C2.

### 1.4 Qué demuestra
| Audiencia | Lo que ve |
|---|---|
| Recruiter | RAG en vivo, desplegado en GCP, con métricas de frescura, calidad y costo |
| Entrevistador técnico | CDC + streaming para RAG, borrados y versiones en índices vectoriales, búsqueda híbrida, Qdrant vs pgvector medido, abstención, SSE, control de costos, IaC |
| Tú | La continuación natural de tu Hybrid RAG del CV, ahora en producción y con datos vivos |

### 1.5 Métricas de éxito
| Tipo | Métrica | Meta inicial |
|---|---|---|
| **Frescura** | p95 desde el commit en Postgres hasta que el chunk es recuperable | < 10 s en local |
| **Frescura en respuestas** | % de preguntas de frescura respondidas con el **valor nuevo** tras un cambio | ≥ 95% |
| Retrieval | Recall@5 y MRR sobre los chunks correctos (ground truth) | Se reporta; denso vs híbrido |
| Generación | Faithfulness/groundedness, corrección, **precisión de citas** | Faithfulness ≥ 0.9 (juez validado) |
| Abstención | % de preguntas sin respuesta en la KB que se abstienen correctamente | ≥ 90% |
| UX | **TTFT** (time-to-first-token) p50/p95, latencia total; en frío y en caliente en Cloud Run | TTFT p95 < 1.5 s en caliente (a validar) |
| Costo | Costo por respuesta con Haiku; ahorro de la caché semántica (hit rate × costo evitado) | Prueba final completa < US$2 |
| Comparativa | Qdrant vs pgvector: latencia p95, recall frente a búsqueda exacta, filtros | Tabla con conclusiones |

### 1.6 Alcance
**Dentro:** todo lo anterior, una UI mínima (una página HTML con streaming) y la integración con P2 (el router envía a `/ask`).
**Fuera:** documentos reales de bancos (la KB es ficticia, sin marcas reales), autenticación de usuarios finales, multi-tenant, fine-tuning del LLM y Spark en la nube (Dataproc cobra; la ingesta es local).

### 1.7 Frase del CV (plantilla)
> **Live RAG Platform** · Postgres CDC · Kafka · Spark Structured Streaming · Qdrant/pgvector · LangGraph · FastAPI SSE · Claude Haiku · GCP Cloud Run · Terraform
> Agentic RAG over a live knowledge base: document changes reach the index in **p95 {F} s** via CDC + Spark micro-batch embeddings; hybrid retrieval with cited answers (faithfulness **{G}**, correct abstention **{A}%**), token streaming over SSE (TTFT p95 **{T}** ms), deployed on Cloud Run with Terraform at **{$C} total cost**, with the semantic cache cutting LLM cost **{S}%**.

Versión corta: *"Live RAG: index freshness p95 {F}s, cited answers with {G} faithfulness, deployed on GCP for under ${C}."*

### 1.8 Narrativa para entrevista (30 s)
"La mayoría de los RAG indexan una vez y se desactualizan. Construí uno en el que cada cambio en Postgres llega al índice en unos {F} segundos: CDC hacia Kafka y Spark en micro-lotes, porque agrupar maximiza el throughput de embeddings. Manejé borrados y versiones para que no queden chunks viejos. El agente recupera de forma híbrida, cita o se abstiene, y responde por SSE. Desarrollé todo a costo cero con mocks y modelos locales, y solo en la prueba final usé Haiku en Cloud Run, con un costo total de {$C}."

### 1.9 Aporte al README
**Overview**, **The Problem: Stale RAG**, **What This Demonstrates** y **Results** (con placeholders).

---

## 2. Arquitectura macro y flujo de datos

### 2.1 Vista general
```
 ───────────────────────────── LOCAL (Docker Compose) ─────────────────────────────────────────
 ┌──────────────┐   tx: UPDATE articles + INSERT outbox      ┌──────────────┐ kb-changes  ┌──────────────────────────┐
 │ Admin/CMS    │──────────────────────────────────────────► │ PostgreSQL   │────────────►│ Spark Structured Streaming│
 │ (API + seed) │                                            │ articles     │ Outbox Relay│ foreachBatch (trigger 2s) │
 └──────────────┘                                            │ outbox       │ (o Debezium)│ chunk → embed (lote) →    │
                                                             │ pgvector     │◄────────────│ upsert/delete             │
                                                             └──────┬───────┘   pgvector  └─────────────┬────────────┘
                                                                    │                                   │ Qdrant (local)
                                                                    │                                   │ o Qdrant Cloud
 ┌───────────────── SERVING (local en dev · Cloud Run en la prueba final) ─────────────────────────────▼─────────┐
 │  Cliente (UI HTML / P2) ──POST /ask/stream──► FastAPI (SSE) ──► LangGraph agentic RAG                          │
 │                                                    │            retrieve(híbrido) → grade → [rewrite] →         │
 │                                                    │            generate(citas) → groundedness check → [abstain]│
 │                                                    ▼                                                            │
 │                                           LLM Gateway (`llm-gateway`, Python): caché semántica · breaker · budget           │
 │                                           proveedores: mock │ ollama (dev) │ claude-haiku-4-5 (prueba final)   │
 └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
   Observabilidad: Langfuse Cloud (trazas RAG + costo) · Prometheus/Grafana (frescura, TTFT, cache) · MLflow (evals)
```

### 2.2 Decisiones de diseño (ADRs resumidos)
**ADR-1 · Patrón *transactional outbox* en `lite`.** El artículo y su evento se escriben en **la misma transacción** (`UPDATE articles` + `INSERT INTO outbox`). Un relay en Python publica el outbox en Kafka y lo marca como enviado. Así, si la transacción se confirma, el evento **existe**, sin depender de leer el WAL. Debezium (CDC real sobre el WAL) consume más RAM y queda para `⏳ 16GB`; los dos producen **el mismo** formato de evento.

**ADR-2 · IDs de punto deterministas.** `point_id = uuid5(doc_id, chunk_index)`. Reprocesar un evento **sobrescribe** en lugar de duplicar: sinks idempotentes = efecto exactly-once aunque el streaming sea at-least-once.

**ADR-3 · Versionado y limpieza de chunks.** Cada evento trae `doc_version`. Al procesarlo se insertan o actualizan los chunks nuevos y se **borran los chunks sobrantes** del documento (si el artículo pasó de 8 a 5 chunks, se eliminan los índices 5–7). Se ignoran los eventos con versión menor a la indexada (*out-of-order* seguro). Un `DELETE` del artículo borra todos sus puntos.

**ADR-4 · Dos índices, mismo embedding.** Qdrant y pgvector reciben **los mismos vectores**, así que la comparación es justa. En la nube solo va Qdrant (Cloud SQL cobra en reposo).

**ADR-5 · Embeddings en CPU con ONNX.** Se usa un modelo multilingüe pequeño (familia e5-small) **en ONNX int8**, igual en la ingesta y en las consultas. Ventajas: no consume VRAM (queda para el LLM local), corre en Cloud Run sin GPU y garantiza que la consulta y los documentos usen **exactamente** el mismo modelo. Un modelo más grande (bge-m3) queda como experimento.

**ADR-6 · El LLM siempre detrás del gateway.** La API nunca llama directo a Anthropic. El gateway aporta caché semántica, circuit breaker, fallback local y un **tope de presupuesto**: el control de costos vive en un solo lugar.

**ADR-7 · Abstenerse es una respuesta válida.** Si no hay documentos relevantes o la respuesta no está respaldada por ellos, el sistema dice que no sabe y ofrece pasar con un humano. En finanzas, una respuesta inventada es peor que no responder.

### 2.3 Eventos y esquemas
```jsonc
// kb-changes (el mismo formato con outbox o con Debezium)
{ "event_id": "uuid", "op": "upsert | delete", "doc_id": "art_0123", "doc_version": 7,
  "title": "Comisiones de transferencias internacionales", "body_md": "…",
  "locale": "es", "product": "transfers", "updated_at": "…", "ts_commit_ns": 0 }

// punto en Qdrant / fila en pgvector
{ "id": "uuid5(doc_id, idx)", "vector": [384 floats], "sparse": {…bm25…},
  "payload": { "doc_id": "art_0123", "chunk_index": 2, "doc_version": 7, "text": "…",
               "title": "…", "locale": "es", "product": "transfers", "updated_at": "…",
               "ts_indexed_ns": 0 } }
```
Tablas de Postgres: `articles` (fuente), `outbox`, `kb_chunks` (pgvector + `tsvector` para BM25) y `qa_log` (preguntas, respuestas, citas, costo y feedback).

### 2.4 El viaje de un cambio (frescura)
1. Un editor cambia la comisión: `UPDATE articles … ; INSERT INTO outbox …` (`ts_commit`).
2. El relay lo publica en `kb-changes` (en ≤ 200 ms con poll corto, o con `LISTEN/NOTIFY` para despertar al instante).
3. Spark arma el micro-lote (trigger de 2 s), divide en chunks, calcula embeddings en lote y hace upsert/delete en los dos índices (`ts_indexed`).
4. El **probe de frescura** (§5) busca el marcador hasta encontrarlo: la frescura es `t_found − ts_commit`.

### 2.5 El viaje de una pregunta
1. `POST /ask/stream` → SSE `event: status {"stage": "retrieving"}`.
2. Retrieval híbrido (Qdrant: denso + sparse, fusión RRF) con filtro por `locale`.
3. Evaluación de relevancia de los chunks. Si ninguno es relevante, se reescribe la consulta (máx. 2 veces) o se pasa a la abstención.
4. Generación con citas `[1]`, `[2]` → `event: token` (streaming) → `event: citations`.
5. Revisión de groundedness. Si falla, se reemplaza por una abstención (o se avisa al cliente con `event: retract`).
6. `event: done` con el uso de tokens, el costo, si hubo cache hit y el `trace_id` de Langfuse.

### 2.6 Aporte al README
**Architecture**, **Design Decisions**, **Data Contracts**, **The Journey of a Change** y **The Journey of a Question**.

---

## 3. Componentes

> Patrón: **🧠 Concepto → ⚙️ Cómo funciona aquí → 🔧 Detalle técnico → 🔁 Alternativas.** La ingesta se detalla en §4 y el retrieval y la generación en §5.

### 3.1 KB sintética + Admin API (`kb/`, `services/admin/`)
**🧠** Una base de conocimiento creíble de una fintech ficticia ("NovaBank" o similar, sin marcas reales).
**⚙️** Unos **300–500 artículos ES/EN** generados con el LLM local ($0) a partir de una **ficha de producto estructurada** (comisiones, límites, plazos en YAML). La ficha es la verdad: de ella salen los artículos **y** las respuestas correctas del set de evaluación. La Admin API (`PUT /articles/{id}`, `DELETE …`) simula el CMS y escribe en la transacción de outbox.
**🔧** Generación reproducible con semilla; revisión manual de una muestra; `make kb-seed`.
**🔁** Documentación pública real con licencia abierta (más realista, pero la verdad es menos controlable).

### 3.2 PostgreSQL + Outbox Relay (`infra/postgres`, `services/outbox_relay/`)
**🧠** Postgres es la fuente de verdad. El outbox garantiza que **no se pierda ningún cambio**.
**⚙️** El relay lee el `outbox` pendiente en orden, publica con clave `doc_id` (orden por documento) y lo marca como enviado. Se despierta con `LISTEN/NOTIFY` y hace poll de respaldo cada 1 s.
**🔧** psycopg 3; productor idempotente; limpieza del outbox enviado con más de 24 h. La extensión `pgvector` está en la misma instancia.
**🔁** **Debezium Server o Debezium + Kafka Connect** (`⏳ 16GB`): CDC sobre el WAL con replicación lógica, sin tocar el código de la aplicación.

### 3.3 Redpanda (`infra/redpanda`)
Topic `kb-changes` (3 particiones, clave `doc_id`) y `dlq`. Se usa `--memory 512M`, igual que en P2.

### 3.4 Spark Structured Streaming (`ingest/spark/`) — detalle en §4
**🧠** Micro-lotes: Spark junta los cambios llegados en cada intervalo y los procesa como un lote **con garantías de checkpoint**.
**🔧** Modo `local[2]`, `foreachBatch`, trigger `processingTime='2 seconds'` y checkpoint en un volumen.

### 3.5 Índices: Qdrant + pgvector (`infra/qdrant`, `infra/postgres`) — detalle en §5
Qdrant con una colección de vectores **denso + sparse (BM25)**. pgvector con índice HNSW + `tsvector`. El payload incluye `locale`, `product` y `doc_version` para filtrar.

### 3.6 RAG API (`services/api/`)
**🧠** El punto de entrada: una pregunta entra y sale una respuesta en streaming con citas.
**⚙️** Endpoints:
- `POST /ask/stream`: SSE con eventos `status`, `token`, `citations`, `retract` y `done`.
- `POST /ask`: no streaming; lo usa P2.
- `POST /feedback`: pulgar arriba o abajo, guardado en `qa_log`.
- `GET /health` y `GET /metrics`.
- UI mínima en `/`: una página HTML que consume el stream con `fetch` + `ReadableStream`.

**🔧** FastAPI + LangGraph. Embedding de la consulta con ONNX en CPU dentro del proceso. **Cancelación:** si el cliente se desconecta, se cancela la generación en el gateway (no se pagan tokens que nadie lee).
**🔁** WebSockets (bidireccional, más complejo); gRPC streaming (entre servicios). Se comparan en el curso C5.

### 3.7 Agente RAG con LangGraph (`packages/ragcore/graph.py`) — detalle en §5

### 3.8 LLM Gateway (`llm-gateway`, proyecto P0 en Python)
Proveedores `mock` (stream de tokens determinista para tests), `ollama` (Qwen3 1.7B o Gemma 3 1B en dev) y `anthropic` con **`claude-haiku-4-5`** **solo** en la prueba final. Además aporta la **caché semántica** (se mide su hit rate y el ahorro), el circuit breaker con fallback a Ollama en local y el **tope de presupuesto** (§7).

### 3.9 Observabilidad (§8), evaluación (§6) e infraestructura en GCP (§10)

### 3.10 Aporte al README
**Components** (cada uno con el detalle plegado en `<details>`).

---

## 4. Ingesta en vivo: CDC → Spark → embeddings → índices

### 4.1 Concepto
Indexar en vivo no es solo insertar. Un índice vivo debe manejar **cuatro casos**:

| Caso | Qué pasa en el índice |
|---|---|
| Artículo nuevo | Insertar sus chunks |
| Artículo editado | Actualizar sus chunks **y borrar los sobrantes** |
| Artículo borrado | Borrar todos sus chunks |
| Evento viejo que llega tarde | **Ignorarlo** (por versión) |

La mayoría de los tutoriales de RAG solo cubren el primero. Los índices con chunks huérfanos o versiones viejas son la causa silenciosa de muchas respuestas desactualizadas.

### 4.2 El job de Spark (`foreachBatch`)
```
readStream(kafka: kb-changes) → parse JSON (inválidos → dlq)
  → por micro-lote:
     1. Deduplicar por doc_id quedándose con la mayor doc_version   (varias ediciones en 2 s = 1 sola indexación)
     2. Comparar con la versión indexada (lookup de un estado ligero en Postgres) → descartar las viejas
     3. Deletes → borrar los puntos por filtro doc_id en Qdrant + DELETE en pgvector
     4. Upserts → chunking (por encabezados de Markdown, ~300–500 tokens, solapamiento de ~50)
                → embeddings EN LOTE (ONNX int8, lotes de 64)
                → BM25/sparse
                → upsert en Qdrant + upsert en pgvector (IDs deterministas, ADR-2)
                → borrar los chunks con chunk_index ≥ n_nuevo (ADR-3)
     5. Registrar ts_indexed y la versión indexada; métricas del lote (tamaño, duración, docs/s)
```
- El paso 1 es la **ventaja de los micro-lotes** frente al evento por evento: si alguien guarda un artículo 5 veces en 2 segundos, se embebe **una** vez.
- Garantías: checkpoint de Spark (offsets) + sinks idempotentes = sin pérdidas ni duplicados visibles.

### 4.3 Chunking
Dividido por estructura (encabezados de Markdown) antes que por tamaño. Cada chunk lleva el **título del artículo y la ruta de encabezados** como prefijo de contexto (mejora el retrieval de fragmentos cortos). Los parámetros se registran en MLflow y se evalúan en §6 (tamaño de chunk vs recall).

### 4.4 Re-indexación completa (*backfill*)
`make reindex` recorre `articles` y emite todos los eventos al topic, con el **mismo** camino que los cambios en vivo. Sirve cuando cambia el modelo de embeddings o el chunking: se indexa en una **colección nueva** (`kb_v2`) y se cambia el alias de Qdrant cuando termina (*blue/green* de índices, sin downtime).

### 4.5 Probe de frescura
Un proceso inserta cada 30 s un artículo marcador con un token único (`FRESHNESS-<uuid>`) y mide cuánto tarda en aparecer en **una búsqueda real** contra cada índice. Así se obtiene el p50/p95 de frescura de punta a punta, que es la cifra del CV. También se reportan los tramos: commit → topic, topic → Spark, Spark → índice.

### 4.6 Aporte al README
**Live Ingestion** (los 4 casos, outbox vs Debezium, foreachBatch, versiones y borrados, blue/green) y **Measuring Freshness**.

---

## 5. Retrieval y generación

### 5.1 Retrieval híbrido
- **Denso** (e5-small ONNX): entiende sinónimos y paráfrasis ("¿cuánto me cobran por enviar dinero afuera?").
- **Sparse / BM25:** atrapa términos exactos (nombres de productos, códigos, cifras).
- **Fusión RRF** (*Reciprocal Rank Fusion*), con filtro por `locale` y opcionalmente por `product`.
- **Reranker** (cross-encoder pequeño) **opcional**, medido: cuánto mejora la recall frente a cuánta latencia agrega en CPU.

### 5.2 Qdrant vs pgvector (comparativa medida)
| Dimensión | Cómo se mide |
|---|---|
| Latencia p50/p95 de búsqueda | Carga de consultas fija, top-k = 10, con y sin filtros |
| Recall frente a búsqueda exacta | Fuerza bruta como verdad; varios `ef_search` / `hnsw.ef_search` |
| Híbrido | Qdrant con sparse nativo vs pgvector + `tsvector` con RRF en SQL |
| Frescura | La misma prueba de §4.5 en ambos |
| Operación | RAM, disco, backups, "una base menos" (pgvector) vs funciones especializadas (Qdrant) |

Conclusión esperada (a confirmar con datos): con una KB pequeña, **pgvector es suficiente** y simplifica la operación. Qdrant gana en funciones (sparse nativo, cuantización, filtros a escala). Esto conecta con las notas `10/36/02` y `10/33` del vault.

### 5.3 El grafo de LangGraph
```
          ┌──────────► rewrite_query ──┐ (máx. 2)
          │                            ▼
start → retrieve ──► grade_docs ──► ¿alguno relevante? ──no──► abstain ──► end
                         │ sí
                         ▼
                     generate (stream + citas) ──► check_groundedness ──ok──► end
                                                       │ falla
                                                       └──► abstain / retract
```
- **`grade_docs`:** una sola llamada que puntúa los k chunks en lote (no una llamada por chunk, por costo).
- **`generate`:** prompt con los chunks numerados. Instrucción: responder **solo** con ellos, citar `[n]` y decir explícitamente si falta información. El idioma de la respuesta es el de la pregunta.
- **`check_groundedness`:** verifica que cada afirmación tenga una cita que la respalde. En dev usa el LLM local; en la prueba final, Haiku. Se puede apagar por configuración para medir su costo y beneficio.
- **Respuesta con fecha:** siempre incluye "información actualizada al {updated_at}" según los documentos citados. Así la frescura es visible para el usuario.

### 5.4 SSE (streaming de tokens)
| Evento | Datos |
|---|---|
| `status` | `retrieving`, `grading`, `rewriting`, `generating`, `checking` |
| `token` | Fragmento de texto |
| `citations` | Lista de `{n, doc_id, title, updated_at, url}` |
| `retract` | La revisión de groundedness falló: el cliente reemplaza el texto por la abstención |
| `done` | Tokens in/out, costo estimado, cache hit, `trace_id`, latencias |
| `error` | Error con código (sin stack trace) |

- Heartbeat (comentario `:`) cada 15 s para que los proxies no corten el stream.
- Encabezados `Cache-Control: no-cache` y `X-Accel-Buffering: no`.
- Cancelación al desconectarse el cliente.
- En Cloud Run, verificar el timeout de request y que no haya buffering (curso C5).

**Trade-off documentado:** al hacer streaming, el usuario ve el texto **antes** de que pase la revisión de groundedness. Por eso existe `retract`. La alternativa sin streaming (verificar primero) sube el TTFT. Se miden las dos y se elige con datos.

### 5.5 Aporte al README
**Hybrid Retrieval**, **Qdrant vs pgvector**, **The Agentic RAG Graph**, **Streaming with SSE** y **Why the System Abstains**.

---

## 6. Harness de evaluación

### 6.1 Set de evaluación (~200 preguntas ES/EN)
Generado desde la **ficha de producto** (la verdad estructurada), más una revisión manual:

| Tipo | % | Qué prueba |
|---|---|---|
| Factuales simples | 35% | Retrieval + corrección |
| Multi-hop (dos artículos) | 15% | Retrieval de varios documentos + síntesis |
| Sin respuesta en la KB | 15% | **Abstención** |
| **Frescura** (se preguntan después de un cambio programado) | 15% | Que el valor **nuevo** llegue a la respuesta |
| Paráfrasis, coloquiales, con errores de ortografía | 10% | Robustez del retrieval denso |
| Términos exactos (códigos, nombres de plan) | 10% | Aporte del BM25 |

Cada pregunta tiene: los `doc_id` y chunks relevantes (ground truth de retrieval), la respuesta de referencia y si se debe abstener.

### 6.2 Métricas
| Etapa | Métricas |
|---|---|
| Retrieval | Recall@k, MRR y nDCG para denso, BM25, híbrido y híbrido + reranker |
| Generación | Corrección frente a la referencia, **faithfulness**, **precisión de citas** (la cita respalda la afirmación) y relevancia |
| Abstención | Precisión y recall de abstención (no abstenerse de más ni de menos) |
| Frescura | % de preguntas de frescura con el valor nuevo; tiempo hasta la primera respuesta correcta tras el cambio |
| Sistema | TTFT, latencia total, tokens, costo por respuesta, cache hit rate |

### 6.3 Juez
- En **dev**, un juez local de otra familia (Gemma 3 4B), corrido por separado (VRAM), igual que en P2.
- **Validación con humanos:** 60 respuestas etiquetadas a mano y el kappa juez↔humano. Si es bajo, el juez solo da señales y no conclusiones.
- En la **prueba final**, opcionalmente **Haiku como juez** sobre un subconjunto pequeño (centavos), comparado con el juez local, para medir cuánto difieren.

### 6.4 Ejecución y compuertas
- `make eval` (CI): **solo retrieval** (no necesita LLM) + generación con `mock` (valida la forma: que haya citas, que funcione el formato de abstención). Falla si la Recall@5 cae más de 2 puntos.
- `make eval-full` (local, $0): todo, con Ollama y el juez local.
- `make eval-final` (**prueba final**, el único que gasta): el set completo con Haiku, con tope de presupuesto (§7).
- Todo se registra en **MLflow** (corridas comparables) y en **Langfuse** (traza por pregunta, *datasets*).

### 6.5 Tabla de resultados (plantilla)
| Config | Recall@5 | Faithfulness | Precisión de citas | Abstención (F1) | Frescura OK | TTFT p95 | US$/respuesta |
|---|---|---|---|---|---|---|---|
| Denso + Qwen3 1.7B (local) | | | | | | | $0 |
| Híbrido + Qwen3 1.7B (local) | | | | | | | $0 |
| Híbrido + agente + Qwen3 (local) | | | | | | | $0 |
| **Híbrido + agente + Haiku (Cloud Run)** | | | | | | | ${c} |

### 6.6 Aporte al README
**Evaluation Harness**, **Results** y **Judge Validation**.

---

## 7. Estrategia de costo cero hasta la prueba final

### 7.1 Reglas
1. `LLM_PROVIDER=mock` por defecto; `ollama` para trabajar en serio. **La clave de Anthropic no existe en el `.env` de desarrollo.**
2. La CI nunca usa proveedores reales.
3. GCP: **nada desplegado** hasta M8. `terraform plan` sí, `apply` no. Antes del primer `apply`: **alerta de presupuesto** en la cuenta de facturación.
4. La prueba final es **un solo evento planificado** con presupuesto, checklist y `destroy` al terminar.

### 7.2 Controles de gasto en capas
| Capa | Control |
|---|---|
| Anthropic Console | Límite de gasto mensual de la cuenta (lo configura el usuario) |
| **Gateway** | Tope duro por corrida (`BUDGET_USD=3`): al alcanzarlo, rechaza las llamadas a Anthropic y pasa al fallback |
| Eval | Set cerrado (~200 preguntas), sin bucles abiertos; `max_tokens` acotado |
| Agente | Máximo 2 reescrituras; `grade_docs` en lote; groundedness configurable |
| Caché semántica | Evita pagar dos veces la misma pregunta (se mide) |
| GCP | Cloud Run con `min-instances=0` y `max-instances=2`; sin Cloud SQL ni Dataproc; `terraform destroy` al terminar |

### 7.3 Estimación de la prueba final (a recalcular con precios vigentes)
- **LLM:** ~200 preguntas × ~2–3 llamadas (grade + generate + check) × ~2k tokens de entrada y ~300 de salida ≈ 1.2 M tokens de entrada + 0.18 M de salida. Con un precio de referencia de Haiku 4.5 (~US$1 / MTok de entrada, ~US$5 / MTok de salida; **verificar**), son **≈ US$2 o menos**. El juez Haiku opcional sobre 50 respuestas suma centavos.
- **GCP:** Cloud Run dentro del free tier mensual (requests, vCPU-s y GiB-s), Artifact Registry (pocos cientos de MB), Secret Manager (2–3 secretos) y Cloud Logging dentro de la cuota gratuita. **≈ US$0–1** si se destruye el mismo día.
- **Qdrant Cloud free tier:** $0 (verificar las condiciones vigentes del cluster gratuito, como la suspensión por inactividad).
- **Total esperado: < US$3.** Se reporta el costo **real** en el README.

### 7.4 Aporte al README
**Cost Engineering** (cómo se desarrolló a $0 y el costo real de la prueba final). Conecta con el curso `09/41 - FinOps for ML` del vault.

---

## 8. Observabilidad
| Señal | Herramienta | Qué se ve |
|---|---|---|
| Trazas RAG | **Langfuse Cloud (free)** | Por pregunta: retrieval (chunks + scores), grade, rewrite, generate (tokens, costo, latencia), groundedness; scores del juez |
| Métricas | Prometheus + Grafana (local) | **Frescura** (probe), lag de ingesta, docs/s de Spark, TTFT y latencia, cache hit rate, costo acumulado, tasa de abstención |
| Métricas en GCP | Cloud Monitoring (métricas nativas de Cloud Run: latencia, instancias, cold starts) + las métricas propias exportadas por OTLP | Durante la prueba final |
| Logs | JSON → Cloud Logging en GCP | `trace_id` y `question_id` |

**Dashboards locales:**
- **K1 · Freshness & Ingestion:** frescura p50/p95, lag y tamaño y duración de los micro-lotes.
- **K2 · Answers:** TTFT, latencia, abstención y retracts.
- **K3 · Cost & Cache:** tokens, US$, hit rate y presupuesto restante del gateway.

**Alertas:** frescura p95 > 30 s · lag de Spark creciente · tasa de `retract` alta · presupuesto del gateway > 80%.

**Aporte al README:** **Observability** (capturas de K1, K3 y una traza de Langfuse).

---

## 9. Presupuesto de recursos (8 GB / `⏳ 16GB`)
**Hardware:** i5-10300H (4C/8T), 8 GB de RAM y 4 GB de VRAM. `.wslconfig memory=5GB` (5.5 GB para `full-lite`). Ollama corre nativo.

| Perfil | Servicios | RAM en la VM | VRAM |
|---|---|---|---|
| **`ingest`** | Postgres 300 MB · Redpanda 600 MB · Outbox Relay 100 MB · **Spark local (driver 1 GB + worker Python con ONNX 400 MB)** · Qdrant 300 MB · Probe 80 MB | **≈ 2.8 GB** ✅ | 0 |
| **`serve`** | Postgres · Qdrant · API (ONNX + LangGraph) 450 MB · llm-gateway ~150 MB (caché semántica apagada; ~300 MB con ella) · Prometheus 300 MB | **≈ 1.4 GB** ✅ | Qwen3 1.7B ~1.3 GB (con `ollama`) |
| **`full-lite`** (demo de frescura en vivo) | ingest + serve + Grafana 150 MB | **≈ 4.4 GB** 🟡 | ~1.3 GB |
| **`eval-judge`** | Harness + Gemma 3 4B solo | ~1 GB | ~3 GB |
| **`full`** `⏳ 16GB` | + Debezium (Server o Connect) ~0.7–1 GB + Langfuse self-hosted + reranker y bge-m3 en GPU | +3–4 GB | — |

**Aporte al README:** **Hardware Requirements & Profiles**.

---

## 10. Ejecución local, despliegue en GCP y alternativas

### 10.1 Local (inicio rápido)
```bash
git clone https://github.com/Leito2/<repo-p3> && cd <repo-p3>
cp .env.example .env                 # LLM_PROVIDER=mock
make doctor && make setup
make kb-seed                         # genera la KB (o descarga la versión publicada) y la carga en Postgres
make up PROFILE=full-lite            # ingesta + serving
make reindex                         # primera indexación completa
open http://localhost:8000           # UI: preguntar y ver el streaming
make freshness-demo                  # cambia una comisión y muestra cuándo la respuesta usa el valor nuevo
```

### 10.2 Despliegue en GCP (solo en la prueba final)
**Recursos (Terraform, `infra/gcp/`):**
| Recurso | Configuración | Costo |
|---|---|---|
| Proyecto + APIs habilitadas | `run`, `artifactregistry`, `secretmanager` | $0 |
| **Alerta de presupuesto** | US$5, con avisos al 50/90/100% | $0 (se crea **primero**) |
| Artifact Registry | Imágenes de la API y del gateway | Pocos centavos de almacenamiento |
| Secret Manager | `ANTHROPIC_API_KEY`, `QDRANT_API_KEY`, `LANGFUSE_KEYS` | ~$0 |
| **Cloud Run: `rag-api`** | 1 vCPU, 1 GiB, `min=0`, `max=2`, timeout de 15 min (SSE), concurrencia 20 | Free tier |
| **Cloud Run: `llm-gateway`** | 0.5 vCPU, 256 MiB, `min=0`, `max=2`, **ingress interno** (solo `rag-api` lo llama) | Free tier |
| Cuenta de servicio | Mínimo privilegio (leer secretos e invocar el gateway) | $0 |
| **Qdrant Cloud (free)** | Fuera de Terraform de GCP (o con su provider, si existe); la colección se llena **desde la ingesta local** | $0 |

**Flujo de la prueba final (`make gcp-final-test`):**
1. `terraform apply` (región `us-central1`, verificar la elegibilidad del free tier).
2. Build y push de las imágenes → deploy de las revisiones.
3. La ingesta local apunta a Qdrant Cloud → `make reindex` → demo de frescura **local → cloud**.
4. `make eval-final` contra la URL de Cloud Run, con Haiku y el presupuesto del gateway.
5. Mediciones de **cold start vs warm** (TTFT) y capturas para el README.
6. Exportar resultados y costos → **`terraform destroy`** → verificar en la consola que no quede nada.

**Seguridad mínima:** la API pública protegida con una API key simple (header), rate limit en el gateway y CORS solo para la UI. Ninguna clave va en las imágenes.

### 10.3 Alternativas
| Opción | Uso | Costo |
|---|---|---|
| **Compose local** | Desarrollo y demos | $0 |
| **Cloud Run + Qdrant Cloud** | Prueba final y demo pública temporal | < US$3 |
| Demo pública permanente | Cloud Run con `min=0` (cold starts) + LLM **mock u Ollama no disponible** → solo retrieval + mock | ~$0, pero sin LLM real; opcional |
| GKE / Vertex AI Vector Search | Camino a producción (nota `10/37` del vault) | Cuesta en reposo: **solo documentado** |
| Debezium + Kafka Connect | CDC real | `⏳ 16GB` |
| Spark en Dataproc Serverless | Ingesta gestionada | Cuesta: **solo documentado** |

### 10.4 Aporte al README
**Quickstart**, **Live Freshness Demo**, **Deploying to GCP** (paso a paso, costos, destroy), **Deployment Options** y **Troubleshooting** (SSE detrás de proxies, cold starts, Spark en Windows y Docker, Qdrant Cloud).

---

## 11. Estructura del repo y del README

### 11.1 Repo: `live-rag-platform`
```
live-rag-platform/
├── README.md · LICENSE · Makefile · docker-compose.yml (profiles) · .env.example · pyproject.toml (uv)
├── packages/ragcore/            ← compartido: embeddings (ONNX), chunking, retrieval, graph, prompts, contracts
├── kb/                          ← ficha de producto (YAML) + generador de artículos + KB generada
├── services/
│   ├── admin/ · outbox_relay/ · api/ (con ui/ estática) · freshness_probe/
├── ingest/spark/                ← job de Structured Streaming + Dockerfile de Spark
├── infra/
│   ├── postgres/init.sql (articles, outbox, kb_chunks + pgvector + tsvector, qa_log)
│   ├── redpanda/ · qdrant/
│   ├── debezium/                ← ⏳16GB
│   └── gcp/                     ← Terraform: main.tf, cloud_run.tf, secrets.tf, budget.tf, variables.tf
├── eval/                        ← set de preguntas, métricas, juez, validación del juez, runners (eval, eval-full, eval-final)
├── bench/                       ← Qdrant vs pgvector, TTFT, frescura
├── observability/ (prometheus, grafana K1–K3, alerts)
├── tests/ (unit, contract, integration)
└── docs/ (adr/, results/, eval-reports/, gcp-final-test/, images/)
```

### 11.2 Esqueleto del README (en inglés)
```markdown
# 📡 Live RAG Platform
> headline with real numbers · badges · GIF (freshness demo: change a fee → answer updates live)
## TL;DR — Results at a Glance
## Part I — The Big Picture
  1. The Problem: Stale RAG in a Regulated Domain
  2. Core Concepts Primer: RAG · embeddings & ANN indexes · hybrid search & RRF · CDC & the outbox
     pattern · micro-batch streaming · agentic RAG (grading, rewriting, abstention) · groundedness ·
     SSE & TTFT · semantic caching · cost engineering
  3. What This Project Demonstrates · 4. Architecture · 5. Design Decisions
  6. The Journey of a Change · 7. The Journey of a Question
## Part II — Components (Concept → How → Technical details)
## Part III — Live Ingestion (4 cases, versions & deletes, blue/green reindex, freshness)
## Part IV — Retrieval & Generation (hybrid, Qdrant vs pgvector, the graph, SSE, abstention)
## Part V — Proof (evaluation, judge validation, results, cost engineering, observability)
## Part VI — Run It Yourself (hardware, quickstart, freshness demo, deploying to GCP, options, troubleshooting, structure)
## Part VII — Reflection (lessons, limitations, future work, glossary, references, license)
```

---

## 12. Hitos de implementación y criterios de aceptación
**Definición de terminado:** CI en verde · cabe en 8 GB · README del hito escrito · **$0** (excepto M8) · tag.

| Hito | Objetivo | Criterios de aceptación | README | Curso | Tamaño |
|---|---|---|---|---|---|
| **M0 · Bootstrap** | Repo, Compose, CI, esqueleto del README, `terraform init` + `plan` (sin `apply`) | `make doctor` pasa; `terraform validate` en la CI | Problem, Core Concepts (borrador) | — | S |
| **M1 · KB + outbox** | Ficha de producto, generador de la KB, Postgres, Admin API, outbox + relay → Redpanda | Editar un artículo produce **exactamente un** evento en `kb-changes`; 0 cambios perdidos tras matar el relay | Components (KB, outbox) | C3 (02) | M |
| **M2 · Ingesta Spark** | `foreachBatch` con dedupe, versiones, upsert/delete en Qdrant + pgvector, chunking, reindex blue/green, probe de frescura | Los 4 casos de §4.1 tienen tests de integración en verde; **frescura p95 medida** (primera cifra) | Live Ingestion, Measuring Freshness | C2 (02) | L |
| **M3 · Retrieval** | Híbrido + RRF + reranker opcional; **Qdrant vs pgvector**; eval de retrieval en la CI | Tabla de recall y latencia; compuerta de Recall@5 activa | Hybrid Retrieval, Qdrant vs pgvector | — | M |
| **M4 · API + SSE** | FastAPI SSE, UI mínima, gateway (`mock` → `ollama`), RAG básico, cancelación | Streaming visible en la UI; TTFT medido local; la cancelación detiene la generación | Streaming with SSE, Components (API, gateway) | C5 | M |
| **M5 · Agente + harness** | Grafo completo (grade, rewrite, abstain, groundedness, retract), set de evaluación, juez validado, MLflow + Langfuse | `make eval-full` produce la tabla local; kappa del juez reportado | Agentic RAG Graph, Evaluation, Why the System Abstains | — | L |
| **M6 · Observabilidad + caché** | Métricas, dashboards K1–K3, alertas, medición de la caché semántica | La demo de frescura se ve en K1; hit rate y ahorro calculados | Observability, Cost Engineering (parte local) | C6 | M |
| **M6b · Analítica** (§15) | Exporter Postgres → Parquet → BigQuery, Storage Write API para frescura, dataset en Terraform, SQL de análisis, Looker Studio | SQL probado con DuckDB en la CI; dashboard con frescura, costo y gaps; bytes del mes < 1% del free tier | Analytics with BigQuery | — | M |
| **M7 · Listo para la nube** | Dockerfiles de producción, Terraform completo, secretos, alerta de presupuesto, checklist de la prueba final, simulacro con `mock` **local** | `terraform plan` limpio; checklist revisado; **$0 gastado** | Deploying to GCP (borrador) | — | M |
| **M8 · 💸 PRUEBA FINAL** | `apply` → deploy → reindex a Qdrant Cloud → `eval-final` con Haiku → cold/warm → **`destroy`** | Resultados con Haiku; **costo real < US$3**; consola de GCP sin recursos activos (captura) | Results finales, Cost Engineering, Deploying to GCP | — | S |
| **M9 · Pulido y publicación** | README completo, GIF de frescura, integración con P2, `v1.0` | Una persona ajena lo corre local desde el README | Todo | — | M |
| M10 · `⏳ 16GB` | Debezium, Langfuse self-hosted, bge-m3 + reranker en GPU | Resultados `v1.1` (outbox vs Debezium) | Results | — | M |

**MVP para el CV:** M0 → M5 + M8 (frescura, calidad y la prueba final en la nube).
**Orden de recorte:** reranker → groundedness check (se mide sin él) → K3 (el costo se lee de Langfuse) → demo pública permanente.
**Puntos de decisión:** D1 (M2): ¿Spark en Docker sobre Windows es estable o se usa Spark nativo con `uv`? · D2 (M4): ¿streaming con `retract` o verificar antes de mostrar? · D3 (M7): ¿se repite M8 si algo falla, dentro del mismo presupuesto?

---

## 13. Riesgos y pendientes

### 13.1 Riesgos
| ID | Riesgo | Prob. | Impacto | Mitigación |
|---|---|---|---|---|
| **R1** | Spark en Docker sobre Windows: RAM, imágenes (el catálogo de Bitnami cambió en 2025; usar la imagen oficial `apache/spark`), descarga de `--packages` del conector de Kafka | Media | Medio | Imagen propia con los JARs incluidos y versiones fijadas; plan B: Spark en modo local nativo |
| **R2** | La KB sintética es demasiado "fácil" o repetitiva | Media | Alto | Generación desde la ficha estructurada con estilos variados, artículos que se solapan, versiones contradictorias en el tiempo (para probar la frescura), revisión manual |
| **R3** | El costo de la prueba final se excede | Baja | Medio | Controles en capas (§7.2); tope duro del gateway; presupuesto en la consola |
| **R4** | Se olvida el `destroy` y quedan recursos cobrando | Media | Medio | `make gcp-final-test` termina siempre en `destroy` (con trap ante errores) + verificación por CLI + alerta de presupuesto |
| **R5** | Cambian las condiciones del free tier de GCP o de Qdrant Cloud | Media | Bajo | Verificar justo antes de M8; el costo esperado sin free tier sigue siendo de pocos dólares |
| **R6** | Proxies o Cloud Run hacen buffering del SSE o cortan streams largos | Media | Medio | Heartbeats, encabezados, timeout de Cloud Run; probado en M8 con evidencia |
| **R7** | El cold start de Cloud Run (Python + modelo ONNX) arruina el TTFT | Alta | Bajo | Se reporta frío vs caliente por separado; imagen ligera; modelo int8 pequeño; `min-instances=1` solo documentado (cuesta) |
| **R8** | Sesgo o debilidad del juez local | Media | Medio | Validación con humanos + comparación opcional con el juez Haiku en M8 |
| **R9** | Dependencia de `llm-gateway` (proveedor Anthropic, streaming, presupuesto): sus hitos M3–M7 deben estar listos antes de la prueba final | Media | Alto | Verificar en M0 qué soporta; si falta el tope de presupuesto o el streaming, se agregan al gateway (mejora también tu proyecto del CV) |
| **R10** | Scope creep (es el proyecto más amplio) | Alta | Alto | MVP M0–M5 + M8; orden de recorte |

### 13.2 Verificar al implementar
Precios vigentes de Haiku 4.5 · free tier de Cloud Run y región · condiciones de Qdrant Cloud free · timeout máximo de request y SSE en Cloud Run · disponibilidad de un modelo e5 multilingüe pequeño con export a ONNX (o soporte en FastEmbed) · versión del conector Kafka de Spark compatible con la versión de Spark · provider de Terraform para Qdrant Cloud (si no existe, se crea a mano y se documenta).

### 13.3 Pendientes `⏳ 16GB`
Debezium (Server o Connect) en lugar del outbox, con la comparación outbox vs CDC por WAL · Langfuse self-hosted · bge-m3 + reranker en GPU · todos los perfiles simultáneos con P2.

### 13.4 Preguntas abiertas
1. ~~¿El gateway soporta Anthropic con streaming, cancelación y tope de presupuesto?~~ → **resuelto por diseño** en `llm-gateway` (hitos M4, M5 y M7).
2. ¿Tienes una cuenta de GCP con facturación activa (necesaria incluso para el free tier)? ¿Y una cuenta de Qdrant Cloud?
3. ~~¿Nombre del repo `live-rag-platform`?~~ → **resuelto:** repo público `Leito2/live-rag-platform`.


---

## 14. v2 — Absorción de los proyectos del CV en P3

> **Decisión (2026-10-06):** los tres proyectos del CV quedan aparte, y su contenido se reparte en P0–P4. En P3 (el único proyecto en GCP) entran la **auditoría de alucinaciones** y los **Vertex AI Pipelines** del *LLM Evaluation Suite*, la **caché semántica con invalidación** del gateway en Go (vía P0) y las prácticas de retrieval y observabilidad del *Hybrid RAG Multi-Agent Research System*.

### 14.1 Mapa de absorción
| Origen (CV) | Elemento | Cómo existe en P3 | Hito |
|---|---|---|---|
| Evaluation Suite | **Auditoría de alucinaciones** | Faithfulness **a nivel de afirmación**: la respuesta se divide en afirmaciones y cada una se verifica contra los chunks citados con dos verificadores independientes: (1) un **modelo NLI pequeño en CPU** (clasificador de alucinaciones tipo HHEM o un cross-encoder NLI, en ONNX) y (2) el juez LLM de `judgekit`. Métricas: `hallucination_rate`, `unsupported_claim_rate` y precisión de citas. El nodo `check_groundedness` del grafo usa el verificador NLI en línea (barato) y el juez solo en evaluación | M5 |
| Evaluation Suite | **Vertex AI Pipelines** (reentrenar o re-promptear al degradarse) | Pipeline KFP v2 `rag-quality-loop`: evaluar con `judgekit` → si cae la faithfulness o la Recall@5 → **reindex blue/green** con otra configuración de chunking o **variante de prompt** → evaluar el candidato → compuerta → promover. Corre local con `kfp.local` en desarrollo y **se ejecuta una vez en Vertex AI Pipelines durante la prueba final (M8)**, junto con el del P2 compilado (mismo YAML) | M7, M8 |
| Evaluation Suite | Gemma 4 31B *golden evaluator* | Panel de jueces en la prueba final: juez local (Gemma 3 4B), **Gemma 4 31B** (`judge_golden`, free tier) y Haiku (`smart_paid`, centavos), comparados contra las 60 etiquetas humanas | M5, M8 |
| Evaluation Suite | asyncio, Pandas, HF Evaluate, token matching | Vía `judgekit`: exact match y F1 de tokens contra la respuesta de referencia, IDs de cita, reportes Pandas por tipo de pregunta (factual, multi-hop, sin respuesta, frescura) | M5 |
| Evaluation Suite | Langfuse | Datasets (las ~200 preguntas), experiments por configuración de §6.5, scores por traza, prompt management del generador | M5 |
| Go Edge Gateway | **Caché semántica + invalidación** | P3 es el mayor cliente de la caché semántica de P0 (FAQ con distribución Zipf). Cada respuesta se guarda con `X-Cache-Tags` = los `doc_id` citados. El **relay del outbox** también avisa al gateway (`POST /admin/cache/invalidate` o el consumidor de `kb-changes` de P0), así que **un cambio en la KB invalida las respuestas cacheadas que lo citaban**. El probe de frescura (§4.5) mide además el tiempo hasta que la caché deja de servir el valor viejo | M6 |
| Go Edge Gateway | Circuit breaker, fallback local, headers de diagnóstico | Heredados vía P0; la UI de P3 muestra `X-Cache-Layer` y `X-Provider` en cada respuesta | M4 |
| Multi-Agent Research | `RecursiveCharacterTextSplitter` (512/64) | **Baseline de chunking** frente al chunking por estructura de §4.3, y frente a **encabezados contextuales** (título + ruta de encabezados + resumen determinista del artículo antepuesto a cada chunk). Comparados en Recall@5 y faithfulness | M3 |
| Multi-Agent Research | `bge-small-en-v1.5` (384d) | Segundo embedder en el experimento de embeddings: e5-small multilingüe (por defecto) vs bge-small (inglés) vs bge-m3 (`⏳ 16GB`), por idioma de la pregunta | M3 |
| Multi-Agent Research | MLflow con saneamiento | Las corridas de evaluación **no** registran contenido de documentos: solo métricas, parámetros y hashes SHA-256 de los chunks (política heredada); `metrics.jsonl` como respaldo si MLflow no está disponible | M5 |
| Multi-Agent Research | "Evidencia insuficiente" y *fail-closed* | Ya presentes (abstención y `retract`); se agregan como requisitos EARS en el README con su test | M5 |

### 14.2 Cambios en la prueba final (M8)
- Se suma **una corrida de Vertex AI Pipelines** (`rag-quality-loop`) contra el despliegue en Cloud Run. El costo de Vertex Pipelines es por corrida más el cómputo de sus componentes: se presupuesta en centavos dentro del tope total de **US$3** y se destruye todo con Terraform al final (agregar el bucket de artefactos del pipeline al `destroy`).
- La caché semántica se mide en la nube: hit rate y ahorro reales con Haiku.

### 14.3 Hitos ampliados
| Hito | Cambio |
|---|---|
| M3 | + baseline `RecursiveCharacterTextSplitter` 512/64, encabezados contextuales, bge-small |
| M5 | + verificador NLI en CPU, `judgekit`, panel de jueces, Langfuse datasets/experiments, MLflow saneado |
| M6 | + invalidación por tags conectada al outbox; frescura de la caché medida |
| M7 | + `rag-quality-loop` compilado y probado con `kfp.local`; recursos de Vertex en Terraform |
| M8 | + una corrida en Vertex AI Pipelines; `judge_golden` y Haiku como jueces del subset |

### 14.4 Frase del CV (agregado)
> … claim-level hallucination auditing (NLI + LLM judge panel incl. Gemma 4 31B; **{h}% unsupported claims**), a KFP quality loop executed on **Vertex AI Pipelines**, and a semantic cache invalidated by CDC events so cached answers never outlive the knowledge base (**{s}% cost saved**).


---

## 15. v3 — Analítica con BigQuery (free tier)

> **Decisión (2026-10-06):** el usuario pide implementar BigQuery con el plan gratuito. Va en **P3** porque ya es el proyecto de GCP (cuenta, Terraform y alerta de presupuesto) y porque genera los datos que más vale la pena analizar: preguntas, respuestas, costos, frescura y evaluaciones a lo largo del tiempo.

### 15.1 Concepto
**BigQuery** es el *data warehouse* serverless de Google: SQL sobre almacenamiento columnar, sin servidores que administrar, y se paga por bytes guardados y bytes leídos por consulta. Postgres es la base **transaccional** de P3 (una fila a la vez, consistencia); BigQuery es la base **analítica** (escanea millones de filas para agregar). Separar ambos es el patrón OLTP → OLAP.

### 15.2 Qué es gratis (verificar antes de usar)
| Recurso | Free tier mensual | Uso en P3 |
|---|---|---|
| Almacenamiento activo | 10 GB | Unos pocos MB (logs de QA y eventos) |
| Consultas | 1 TB procesado | Dashboards y análisis; con tablas particionadas, muy por debajo |
| **Load jobs** (cargar archivos) | Gratis | Camino principal: Parquet → BigQuery |
| Storage Write API (streaming) | 2 TiB de ingesta | Eventos de frescura casi en tiempo real |
| Looker Studio | Gratis | Dashboards sobre BigQuery |

**Sandbox vs free tier:** el *sandbox* (sin tarjeta) no permite streaming y borra las tablas a los 60 días. P3 ya necesita facturación activa para la prueba final, así que usa el **free tier normal**, protegido por la alerta de presupuesto (§7) y por límites de bytes por consulta (`maximum_bytes_billed`) en todas las queries. Así, el gasto esperado sigue siendo **$0**.

### 15.3 Cómo funciona aquí
```
Postgres (qa_log, outbox, eval_runs) ──exporter (cada N min)──► Parquet ──load job (gratis)──► BigQuery: rag_analytics.*
Spark foreachBatch ──(eventos de frescura)──► Storage Write API ──────────────────────────────► rag_analytics.freshness_events
make eval / eval-full ──(resultados por pregunta)──► load job ─────────────────────────────────► rag_analytics.eval_results
                                                                                                   │
                                                                     Looker Studio (dashboards) ◄──┘ · SQL de análisis
```
**Dataset `rag_analytics`** (tablas particionadas por día y agrupadas por `locale` o `config`):
| Tabla | Contenido |
|---|---|
| `qa_events` | Pregunta (hash + texto si se permite), idioma, configuración, citas, abstención, TTFT, latencia, tokens, costo, cache hit, feedback |
| `freshness_events` | `doc_id`, versión, `ts_commit`, `ts_indexed`, `ts_retrievable`, índice (Qdrant o pgvector) |
| `eval_results` | Una fila por pregunta y configuración: métricas de retrieval, faithfulness, citas, abstención, juez usado |
| `kb_changes` | Historial de cambios de la KB (desde el outbox) |

**Análisis que habilita** (con SQL versionado en `analytics/sql/`):
- Tendencia de frescura p50/p95 por día y por índice.
- Costo por respuesta y ahorro de la caché semántica de P0 en el tiempo.
- **Preguntas que terminan en abstención agrupadas por tema** → qué falta en la KB (*gap analysis*).
- Calidad por configuración y por idioma a lo largo de las corridas de evaluación (regresiones visibles).

### 15.4 Detalle técnico
- **Desarrollo local sin nube:** el mismo SQL corre sobre **DuckDB** con los mismos Parquet (`ANALYTICS_BACKEND=duckdb|bigquery`). Así la CI prueba las consultas sin GCP y BigQuery solo se usa cuando se quiere.
- **Clientes:** `google-cloud-bigquery` (load jobs y consultas) y `google-cloud-bigquery-storage` (Storage Write API). Autenticación con una cuenta de servicio con permisos mínimos (`bigquery.dataEditor` en el dataset, `bigquery.jobUser` en el proyecto).
- **Terraform:** dataset, tablas con esquema, particionado, expiración de particiones (90 días) y la cuenta de servicio. **El dataset no se destruye con `make gcp-final-test`** (no cobra en reposo dentro del free tier); tiene su propio `terraform destroy -target`.
- **Control de costos:** `maximum_bytes_billed` en cada consulta, `SELECT` solo de columnas necesarias, filtros por partición obligatorios (`require_partition_filter`), y una vista que reporta los bytes procesados del mes (`INFORMATION_SCHEMA.JOBS`).
- **Privacidad:** el texto de las preguntas solo se exporta si `EXPORT_QUESTION_TEXT=true`; si no, va su hash y su tema.
- **Experimento opcional:** `VECTOR_SEARCH` de BigQuery como tercer índice en la comparación de §5.2 (latencia y costo por consulta frente a Qdrant y pgvector), con la KB pequeña para no gastar cuota.

### 15.5 Hito y frase del CV
| Hito | Objetivo | Criterios de aceptación | Tamaño |
|---|---|---|---|
| **M6b · Analítica** (después de M6) | Exporter Postgres → Parquet → BigQuery, Storage Write API para frescura, dataset en Terraform, SQL de análisis con DuckDB en la CI, dashboard en Looker Studio | Las consultas pasan en DuckDB en la CI; con BigQuery, el dashboard muestra frescura, costo y gaps; bytes procesados del mes < 1% del free tier | M |

> … with an analytics layer on **BigQuery** (load jobs + Storage Write API, partitioned tables, Looker Studio) tracking freshness, cost per answer and knowledge-base gaps — within the free tier.
