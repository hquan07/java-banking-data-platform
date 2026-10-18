-- Latest persisted ingestion/reconciliation results, newest first.
SELECT run_id, consumer_name, source_topic, source_offsets,
       raw_count, valid_count, invalid_count, unique_count,
       inserted_count, updated_count, rejected_count,
       duplicate_delivery_count, currency_totals, warehouse_currency_totals,
       amount_deviation, started_at, finished_at
FROM warehouse_reconciliation_runs
ORDER BY finished_at DESC
LIMIT 100;
