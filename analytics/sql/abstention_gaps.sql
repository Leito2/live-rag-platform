-- Knowledge-base gaps: topics where the assistant abstains most (PLAN §15.3).
SELECT
  topic,
  locale,
  COUNT(*) AS questions,
  COUNTIF(abstained) AS abstentions,
  SAFE_DIVIDE(COUNTIF(abstained), COUNT(*)) AS abstention_rate
FROM qa_events
WHERE DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY topic, locale
HAVING COUNT(*) >= 5
ORDER BY abstention_rate DESC, questions DESC
LIMIT 20
