# BigQuery analytics layer (PLAN §15). Lives inside the free tier (10 GB storage, 1 TB queries/month, free load
# jobs, 2 TiB/month Storage Write API) and does NOT charge at rest, so it is not part of the final-test destroy.
# Remove explicitly with: terraform destroy -target=google_bigquery_dataset.rag_analytics

resource "google_bigquery_dataset" "rag_analytics" {
  project                         = var.project_id
  dataset_id                      = "rag_analytics"
  location                        = "US"
  default_partition_expiration_ms = 90 * 24 * 60 * 60 * 1000 # keep storage tiny
  delete_contents_on_destroy      = true
}

resource "google_bigquery_table" "freshness_events" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.rag_analytics.dataset_id
  table_id                 = "freshness_events"
  deletion_protection      = false
  require_partition_filter = true
  time_partitioning {
    type  = "DAY"
    field = "ts_commit"
  }
  clustering = ["index_name"]
  schema = jsonencode([
    { name = "doc_id", type = "STRING", mode = "REQUIRED" },
    { name = "doc_version", type = "INT64", mode = "REQUIRED" },
    { name = "index_name", type = "STRING", mode = "REQUIRED" },
    { name = "ts_commit", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ts_indexed", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "ts_retrievable", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "qa_events" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.rag_analytics.dataset_id
  table_id                 = "qa_events"
  deletion_protection      = false
  require_partition_filter = true
  time_partitioning {
    type  = "DAY"
    field = "ts"
  }
  clustering = ["locale", "topic"]
  schema = jsonencode([
    { name = "ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "question_hash", type = "STRING", mode = "REQUIRED" },
    { name = "question_text", type = "STRING", mode = "NULLABLE" }, # only if EXPORT_QUESTION_TEXT=true
    { name = "topic", type = "STRING", mode = "NULLABLE" },
    { name = "locale", type = "STRING", mode = "REQUIRED" },
    { name = "config", type = "STRING", mode = "REQUIRED" },
    { name = "abstained", type = "BOOL", mode = "REQUIRED" },
    { name = "n_citations", type = "INT64", mode = "NULLABLE" },
    { name = "ttft_ms", type = "INT64", mode = "NULLABLE" },
    { name = "latency_ms", type = "INT64", mode = "NULLABLE" },
    { name = "tokens_in", type = "INT64", mode = "NULLABLE" },
    { name = "tokens_out", type = "INT64", mode = "NULLABLE" },
    { name = "cost_usd", type = "NUMERIC", mode = "NULLABLE" },
    { name = "cache_layer", type = "STRING", mode = "NULLABLE" },
    { name = "feedback", type = "INT64", mode = "NULLABLE" },
  ])
}

resource "google_service_account" "analytics_exporter" {
  project      = var.project_id
  account_id   = "rag-analytics-exporter"
  display_name = "RAG analytics exporter (BigQuery load jobs + Storage Write API)"
}

resource "google_bigquery_dataset_iam_member" "exporter_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.rag_analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.analytics_exporter.email}"
}

resource "google_project_iam_member" "exporter_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.analytics_exporter.email}"
}
