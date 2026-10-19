#!/usr/bin/env bash
set -euo pipefail

readonly RUN_ID="$(date +%s)"
readonly RUN_TOKEN="$(printf '%012d' "$((RUN_ID % 1000000000000))")"
readonly DAY_OFFSET="$((RUN_ID % 100000))"
readonly REPORT_DATE="$(date -u -d "2100-01-01 + ${DAY_OFFSET} days" +%Y-%m-%d)"
readonly NEXT_DATE="$(date -u -d "${REPORT_DATE} + 1 day" +%Y-%m-%d)"
readonly DATE_KEY="${REPORT_DATE//-/}"
readonly ACCOUNT_VND="ACC_PHASE9_VND_${RUN_TOKEN}"
readonly ACCOUNT_USD="ACC_PHASE9_USD_${RUN_TOKEN}"
readonly CORRELATION_ID="phase9-smoke-${RUN_TOKEN}"
readonly BATCH_RUN_ID="$(printf '%s' "phase9-batch-${RUN_TOKEN}" | md5sum | awk '{print $1}' | sed -E 's/(.{8})(.{4})(.{4})(.{4})(.{12})/\1-\2-\3-\4-\5/')"
readonly BASE_URL="http://127.0.0.1:${ANALYTICS_HOST_PORT:-18092}"
readonly MANAGEMENT_URL="http://127.0.0.1:${ANALYTICS_MANAGEMENT_HOST_PORT:-18093}"

warehouse_sql() {
  docker compose exec -T warehouse-postgres psql \
    -v ON_ERROR_STOP=1 \
    -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
    -d "${POSTGRES_WAREHOUSE_DB:-banking_warehouse}" \
    -tAc "$1"
}

cleanup() {
  warehouse_sql "
    DELETE FROM fact_fraud_alerts WHERE source_topic = 'phase9.smoke.${RUN_TOKEN}';
    DELETE FROM fact_transactions WHERE correlation_id = '${CORRELATION_ID}';
    DELETE FROM agg_daily_transactions WHERE batch_run_id = '${BATCH_RUN_ID}'::uuid;
    DELETE FROM batch_runs WHERE batch_run_id = '${BATCH_RUN_ID}'::uuid;
    DELETE FROM dim_account WHERE account_id IN ('${ACCOUNT_VND}', '${ACCOUNT_USD}');
    DELETE FROM dim_date WHERE date_key = ${DATE_KEY}
      AND NOT EXISTS (SELECT 1 FROM fact_transactions WHERE date_key = ${DATE_KEY})
      AND NOT EXISTS (SELECT 1 FROM fact_fraud_alerts WHERE date_key = ${DATE_KEY})
      AND NOT EXISTS (SELECT 1 FROM agg_transaction_windows WHERE date_key = ${DATE_KEY})
      AND NOT EXISTS (SELECT 1 FROM agg_daily_transactions WHERE date_key = ${DATE_KEY});
  " >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose --profile analytics up -d --build warehouse-postgres warehouse-migrate analytics-service

warehouse_sql "
INSERT INTO dim_date (
    date_key, calendar_date, calendar_year, calendar_quarter,
    calendar_month, day_of_month, day_of_week, is_weekend
)
SELECT ${DATE_KEY}, DATE '${REPORT_DATE}', EXTRACT(YEAR FROM DATE '${REPORT_DATE}')::int,
       EXTRACT(QUARTER FROM DATE '${REPORT_DATE}')::int, EXTRACT(MONTH FROM DATE '${REPORT_DATE}')::int,
       EXTRACT(DAY FROM DATE '${REPORT_DATE}')::int, EXTRACT(ISODOW FROM DATE '${REPORT_DATE}')::int,
       EXTRACT(ISODOW FROM DATE '${REPORT_DATE}')::int >= 6
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO dim_account (account_id, customer_key, currency, account_status)
SELECT '${ACCOUNT_VND}', customer_key, 'VND', 'ACTIVE' FROM dim_customer WHERE customer_id = 'UNKNOWN';
INSERT INTO dim_account (account_id, customer_key, currency, account_status)
SELECT '${ACCOUNT_USD}', customer_key, 'USD', 'ACTIVE' FROM dim_customer WHERE customer_id = 'UNKNOWN';

INSERT INTO fact_transactions (
    transaction_id, event_id, account_key, date_key, transaction_type, amount, currency, status,
    occurred_at, produced_at, correlation_id, schema_version,
    source_topic, source_partition, source_offset, source_timestamp
)
SELECT md5('${RUN_TOKEN}-tx-' || input.seed)::uuid,
       md5('${RUN_TOKEN}-event-' || input.seed)::uuid,
       account.account_key,
       ${DATE_KEY},
       'PAYMENT', input.amount, input.currency, input.status,
       (DATE '${REPORT_DATE}' + TIME '12:00:00') AT TIME ZONE 'Asia/Ho_Chi_Minh'
           + input.seed * INTERVAL '1 second',
       (DATE '${REPORT_DATE}' + TIME '12:00:01') AT TIME ZONE 'Asia/Ho_Chi_Minh'
           + input.seed * INTERVAL '1 second',
       '${CORRELATION_ID}', 1, 'phase9.smoke.${RUN_TOKEN}', 0, ${RUN_ID} + input.seed,
       (DATE '${REPORT_DATE}' + TIME '12:00:00') AT TIME ZONE 'Asia/Ho_Chi_Minh'
           + input.seed * INTERVAL '1 second'
  FROM (
        VALUES
          (1, '${ACCOUNT_VND}', 'VND', 10.00::numeric, 'SUCCESS'),
          (2, '${ACCOUNT_VND}', 'VND', 20.00::numeric, 'FAILED'),
          (3, '${ACCOUNT_USD}', 'USD', 5.00::numeric, 'PENDING')
       ) AS input(seed, account_id, currency, amount, status)
  JOIN dim_account account ON account.account_id = input.account_id;

INSERT INTO batch_runs (
    batch_run_id, job_name, business_date, business_timezone, status,
    source_row_count, output_row_count, source_fingerprint, gold_object_key,
    gold_object_sha256, gold_object_size, gold_schema_version, finished_at
) VALUES (
    '${BATCH_RUN_ID}'::uuid, 'daily-transactions', DATE '${REPORT_DATE}', 'Asia/Ho_Chi_Minh',
    'SUCCEEDED', 3, 2, repeat('a', 64), 'gold/phase9-smoke.parquet', repeat('b', 64),
    100, 'gold-daily-final-v1', NOW()
);

INSERT INTO agg_daily_transactions (
    date_key, currency, transaction_count, total_amount, failed_count, batch_run_id
) VALUES
    (${DATE_KEY}, 'USD', 1, 5.00, 0, '${BATCH_RUN_ID}'::uuid),
    (${DATE_KEY}, 'VND', 2, 30.00, 1, '${BATCH_RUN_ID}'::uuid);

INSERT INTO fact_fraud_alerts (
    alert_id, event_id, transaction_id, account_key, date_key, rule_code, rule_version,
    severity, amount, currency, observed_count, window_start, window_end, detected_at,
    schema_version, source_topic, source_partition, source_offset
)
SELECT md5('${RUN_TOKEN}-alert')::uuid,
       md5('${RUN_TOKEN}-event-1')::uuid,
       md5('${RUN_TOKEN}-tx-1')::uuid,
       account_key,
       ${DATE_KEY}, 'HIGH_VALUE', 'v1', 'HIGH', 10.00, 'VND', 1,
       (DATE '${REPORT_DATE}' + TIME '12:00:00') AT TIME ZONE 'Asia/Ho_Chi_Minh',
       (DATE '${REPORT_DATE}' + TIME '12:01:00') AT TIME ZONE 'Asia/Ho_Chi_Minh',
       (DATE '${REPORT_DATE}' + TIME '12:01:00') AT TIME ZONE 'Asia/Ho_Chi_Minh',
       1, 'phase9.smoke.${RUN_TOKEN}', 0, ${RUN_ID}
  FROM dim_account WHERE account_id = '${ACCOUNT_VND}';
" >/dev/null

for _ in $(seq 1 60); do
  if curl --fail --silent "${MANAGEMENT_URL}/actuator/health/readiness" >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent "${MANAGEMENT_URL}/actuator/health/readiness" >/dev/null

DAILY_JSON="$(curl --fail --silent "${BASE_URL}/api/v1/analytics/daily?date=${REPORT_DATE}")"
FRAUD_JSON="$(curl --fail --silent "${BASE_URL}/api/v1/analytics/fraud?from=${REPORT_DATE}T00:00:00Z&to=${NEXT_DATE}T00:00:00Z&page=0&size=10")"
ACCOUNT_JSON="$(curl --fail --silent "${BASE_URL}/api/v1/analytics/accounts/${ACCOUNT_VND}/summary")"

python3 - "$DAILY_JSON" "$FRAUD_JSON" "$ACCOUNT_JSON" "$REPORT_DATE" "$ACCOUNT_VND" <<'PY'
import json
import sys

daily, fraud, account = map(json.loads, sys.argv[1:4])
report_date, account_id = sys.argv[4:6]

assert daily["date"] == report_date
assert daily["transactionCount"] == 3
assert daily["totalAmount"] == "35.00"
assert daily["failedCount"] == 1
assert [row["currency"] for row in daily["currencies"]] == ["USD", "VND"]

assert fraud["totalElements"] == 1
assert fraud["items"][0]["ruleCode"] == "HIGH_VALUE"
assert fraud["items"][0]["accountId"] == account_id

assert account["accountId"] == account_id
assert account["transactionCount"] == 2
assert account["totalAmount"] == "30.00"
assert account["fraudAlertCount"] == 1
PY

[[ "$(warehouse_sql "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank DESC LIMIT 1;" | tr -d '[:space:]')" == "4" ]]

echo "Phase 9 smoke passed for ${REPORT_DATE}: daily, bounded fraud and account summary APIs returned authoritative warehouse values; isolated fixtures will be removed."
