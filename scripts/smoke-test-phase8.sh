#!/usr/bin/env bash
set -euo pipefail

readonly RUN_ID="$(date +%s)"
readonly RUN_TOKEN="$(printf '%012d' "$((RUN_ID % 1000000000000))")"
readonly DAY_OFFSET="$((RUN_ID % 10000))"
readonly BATCH_DATE="$(date -u -d "2040-01-01 + ${DAY_OFFSET} days" +%Y-%m-%d)"
readonly DATE_KEY="${BATCH_DATE//-/}"
readonly ACCOUNT_VND="ACC_PHASE8_VND_${RUN_TOKEN}"
readonly ACCOUNT_USD="ACC_PHASE8_USD_${RUN_TOKEN}"

warehouse_sql() {
  docker compose exec -T warehouse-postgres psql \
    -v ON_ERROR_STOP=1 \
    -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
    -d "${POSTGRES_WAREHOUSE_DB:-banking_warehouse}" \
    -tAc "$1"
}

run_batch() {
  local job="$1"
  docker compose --profile batch run --rm --no-deps batch-processor \
    --job="$job" --date="$BATCH_DATE"
}

docker compose up -d --build warehouse-postgres minio minio-init
docker compose --profile batch build batch-processor

run_batch check-daily-source >/dev/null 2>&1 && {
  echo "Expected the source check to fail before synthetic facts are inserted" >&2
  exit 1
}
run_batch daily-transactions >/dev/null 2>&1 && {
  echo "Expected the daily batch to fail and audit an empty source partition" >&2
  exit 1
}

warehouse_sql "
INSERT INTO dim_date (
    date_key, calendar_date, calendar_year, calendar_quarter,
    calendar_month, day_of_month, day_of_week, is_weekend
)
SELECT ${DATE_KEY}, DATE '${BATCH_DATE}', EXTRACT(YEAR FROM DATE '${BATCH_DATE}')::int,
       EXTRACT(QUARTER FROM DATE '${BATCH_DATE}')::int, EXTRACT(MONTH FROM DATE '${BATCH_DATE}')::int,
       EXTRACT(DAY FROM DATE '${BATCH_DATE}')::int, EXTRACT(ISODOW FROM DATE '${BATCH_DATE}')::int,
       EXTRACT(ISODOW FROM DATE '${BATCH_DATE}')::int >= 6
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO dim_account (account_id, customer_key, currency)
SELECT '${ACCOUNT_VND}', customer_key, 'VND' FROM dim_customer WHERE customer_id = 'UNKNOWN'
ON CONFLICT (account_id) DO NOTHING;
INSERT INTO dim_account (account_id, customer_key, currency)
SELECT '${ACCOUNT_USD}', customer_key, 'USD' FROM dim_customer WHERE customer_id = 'UNKNOWN'
ON CONFLICT (account_id) DO NOTHING;

INSERT INTO fact_transactions (
    transaction_id, event_id, account_key, date_key, transaction_type, amount, currency, status,
    occurred_at, produced_at, correlation_id, schema_version,
    source_topic, source_partition, source_offset, source_timestamp
)
SELECT md5('${RUN_TOKEN}-tx-' || seed)::uuid,
       md5('${RUN_TOKEN}-event-' || seed)::uuid,
       account_key,
       ${DATE_KEY},
       'PAYMENT',
       input.amount,
       input.currency,
       input.status,
       (DATE '${BATCH_DATE}' + TIME '12:00:00') AT TIME ZONE 'Asia/Ho_Chi_Minh' + seed * INTERVAL '1 second',
       (DATE '${BATCH_DATE}' + TIME '12:00:01') AT TIME ZONE 'Asia/Ho_Chi_Minh' + seed * INTERVAL '1 second',
       'phase8-smoke-${RUN_TOKEN}',
       1,
       'banking.transactions.validated',
       0,
       ${RUN_ID} + seed,
       (DATE '${BATCH_DATE}' + TIME '12:00:00') AT TIME ZONE 'Asia/Ho_Chi_Minh' + seed * INTERVAL '1 second'
  FROM (
        VALUES
          (1, '${ACCOUNT_VND}', 'VND', 10.00::numeric, 'SUCCESS'),
          (2, '${ACCOUNT_VND}', 'VND', 20.00::numeric, 'FAILED'),
          (3, '${ACCOUNT_USD}', 'USD', 5.00::numeric, 'PENDING')
       ) AS input(seed, account_id, currency, amount, status)
  JOIN dim_account USING (account_id)
ON CONFLICT (transaction_id) DO NOTHING;
" >/dev/null

run_batch check-daily-source >/dev/null
run_batch daily-transactions >/dev/null
FIRST_RESULT="$(warehouse_sql "
SELECT COUNT(*) || '|' || SUM(transaction_count) || '|' || SUM(total_amount) || '|' || SUM(failed_count)
  FROM agg_daily_transactions WHERE date_key = ${DATE_KEY};
" | tr -d '[:space:]')"
FIRST_HASH="$(warehouse_sql "
SELECT source_fingerprint || '|' || gold_object_sha256
  FROM batch_runs
 WHERE business_date = DATE '${BATCH_DATE}' AND status = 'SUCCEEDED'
 ORDER BY finished_at DESC LIMIT 1;
" | tr -d '[:space:]')"

run_batch daily-transactions >/dev/null
run_batch validate-daily-transactions >/dev/null
run_batch publish-report-metadata >/dev/null

SECOND_RESULT="$(warehouse_sql "
SELECT COUNT(*) || '|' || SUM(transaction_count) || '|' || SUM(total_amount) || '|' || SUM(failed_count)
  FROM agg_daily_transactions WHERE date_key = ${DATE_KEY};
" | tr -d '[:space:]')"
SECOND_HASH="$(warehouse_sql "
SELECT source_fingerprint || '|' || gold_object_sha256
  FROM batch_runs
 WHERE business_date = DATE '${BATCH_DATE}' AND status = 'SUCCEEDED'
 ORDER BY finished_at DESC LIMIT 1;
" | tr -d '[:space:]')"

[[ "$FIRST_RESULT" == "2|3|35.00|1" ]]
[[ "$SECOND_RESULT" == "$FIRST_RESULT" ]]
[[ "$SECOND_HASH" == "$FIRST_HASH" ]]
[[ "$(warehouse_sql "SELECT COUNT(*) FROM batch_runs WHERE business_date = DATE '${BATCH_DATE}' AND status = 'SUCCEEDED';" | tr -d '[:space:]')" == "2" ]]
[[ "$(warehouse_sql "SELECT COUNT(*) FROM batch_runs WHERE business_date = DATE '${BATCH_DATE}' AND status = 'FAILED' AND error_code = 'DQ_FAILED';" | tr -d '[:space:]')" == "1" ]]
[[ "$(warehouse_sql "SELECT COUNT(*) FROM batch_runs WHERE business_date = DATE '${BATCH_DATE}' AND published_at IS NOT NULL;" | tr -d '[:space:]')" == "1" ]]

docker compose exec -T minio mc alias set phase8 http://localhost:9000 \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-change-me-root-local-only}" >/dev/null
docker compose exec -T minio mc stat \
  "phase8/${MINIO_BUCKET:-banking-lake}/gold/daily_transactions/report_date=${BATCH_DATE}/daily-transactions-v1.parquet" >/dev/null
docker compose exec -T minio mc stat \
  "phase8/${MINIO_BUCKET:-banking-lake}/gold/daily_transactions/report_date=${BATCH_DATE}/_SUCCESS.json" >/dev/null

python3 -m unittest discover -s orchestration/airflow/tests -p 'test_*.py' >/dev/null

docker compose --profile airflow up -d --build \
  airflow-api-server airflow-scheduler airflow-dag-processor
for _ in $(seq 1 60); do
  if docker compose exec -T airflow-api-server \
    curl --fail --silent http://localhost:8080/api/v2/monitor/health >/dev/null; then
    break
  fi
  sleep 2
done
docker compose exec -T airflow-api-server \
  curl --fail --silent http://localhost:8080/api/v2/monitor/health >/dev/null

AIRFLOW_DAG_LIST="$(docker compose --profile airflow run --rm --no-deps airflow-cli dags list)"
grep -Fq banking_daily_transactions <<< "$AIRFLOW_DAG_LIST"

LOGICAL_DATE_ONE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LOGICAL_DATE_TWO="$(date -u -d '1 second' +%Y-%m-%dT%H:%M:%SZ)"
for logical_date in "$LOGICAL_DATE_ONE" "$LOGICAL_DATE_TWO"; do
  docker compose --profile airflow run --rm --no-deps airflow-cli dags test \
    banking_daily_transactions "$logical_date" --conf "{\"date\":\"${BATCH_DATE}\"}" >/dev/null
done

[[ "$(warehouse_sql "
WITH latest AS (
    SELECT source_fingerprint, gold_object_sha256
      FROM batch_runs
     WHERE business_date = DATE '${BATCH_DATE}' AND status = 'SUCCEEDED'
     ORDER BY finished_at DESC LIMIT 2
)
SELECT COUNT(*) || '|' || COUNT(DISTINCT source_fingerprint) || '|' || COUNT(DISTINCT gold_object_sha256)
  FROM latest;
" | tr -d '[:space:]')" == "2|1|1" ]]

echo "Phase 8 smoke passed for ${BATCH_DATE}: Java and Airflow reruns remained 2 currencies/3 transactions/35.00 total; audit and final Gold publication verified."
