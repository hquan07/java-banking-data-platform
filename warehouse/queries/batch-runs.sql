-- Latest daily finalization attempts and their publication evidence.
SELECT batch_run_id, job_name, business_date, business_timezone, status,
       source_row_count, output_row_count, source_currency_totals,
       output_currency_totals, source_fingerprint, gold_object_key,
       gold_object_sha256, gold_schema_version, dq_checks,
       started_at, finished_at, published_at, error_code, error_message
FROM batch_runs
ORDER BY started_at DESC
LIMIT 100;
