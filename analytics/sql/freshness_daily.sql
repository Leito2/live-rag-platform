-- Freshness p95 per day and index (PLAN §15). Written in BigQuery SQL; CI transpiles it to DuckDB with sqlglot.
SELECT
  DATE(ts_commit) AS day,
  index_name,
  COUNT(*) AS changes,
  APPROX_QUANTILES(TIMESTAMP_DIFF(ts_retrievable, ts_commit, MILLISECOND), 100)[OFFSET(95)] AS p95_ms
FROM freshness_events
WHERE DATE(ts_commit) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY day, index_name
ORDER BY day, index_name
