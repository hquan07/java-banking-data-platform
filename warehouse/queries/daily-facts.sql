-- Phase 8 authoritative daily output with the producing batch audit.
SELECT d.calendar_date, a.currency, a.transaction_count,
       a.total_amount, a.failed_count, a.batch_run_id,
       r.source_fingerprint, r.gold_object_key, r.gold_object_sha256,
       r.finished_at, r.published_at
FROM agg_daily_transactions a
JOIN dim_date d ON d.date_key = a.date_key
JOIN batch_runs r ON r.batch_run_id = a.batch_run_id
ORDER BY d.calendar_date DESC, a.currency;
