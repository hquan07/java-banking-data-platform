#!/usr/bin/env bash
set -euo pipefail

readonly RUN_ID="$(date +%s)"
readonly RUN_TOKEN="$(printf '%08d' "$((RUN_ID % 100000000))")"
readonly CORRELATION_ID="phase7-smoke-${RUN_ID}"
readonly PAYLOAD_FILE="$(mktemp /tmp/banking-phase7-payloads.XXXXXX)"
readonly STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cleanup() {
  rm -f "$PAYLOAD_FILE"
}
trap cleanup EXIT

export WAREHOUSE_VALIDATED_GROUP_ID="banking-warehouse-validated-smoke-${RUN_ID}"
export WAREHOUSE_FRAUD_GROUP_ID="banking-warehouse-fraud-smoke-${RUN_ID}"
export WAREHOUSE_AGGREGATES_GROUP_ID="banking-warehouse-aggregates-smoke-${RUN_ID}"
export WAREHOUSE_STARTING_OFFSETS=latest

docker compose up -d --build kafka kafka-init warehouse-postgres warehouse-writer

for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:${WAREHOUSE_MANAGEMENT_HOST_PORT:-18089}/actuator/health/readiness >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent http://127.0.0.1:${WAREHOUSE_MANAGEMENT_HOST_PORT:-18089}/actuator/health/readiness >/dev/null

wait_for_group_assignment() {
  local group_id="$1"
  local topic="$2"
  local description
  for _ in $(seq 1 60); do
    description="$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:9092 --group "$group_id" --describe 2>/dev/null || true)"
    if grep -Fq "$topic" <<< "$description"; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for consumer group ${group_id} on ${topic}" >&2
  return 1
}

wait_for_group_assignment "$WAREHOUSE_VALIDATED_GROUP_ID" banking.transactions.validated
wait_for_group_assignment "$WAREHOUSE_FRAUD_GROUP_ID" banking.transactions.fraud-alerts
wait_for_group_assignment "$WAREHOUSE_AGGREGATES_GROUP_ID" banking.transactions.aggregates

OCCURRED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
for index in $(seq 0 999); do
  suffix="$(printf '%s%04d' "$RUN_TOKEN" "$index")"
  printf '{"event_id":"71000000-0000-4000-8000-%s","event_type":"transaction.created","schema_version":1,"transaction_id":"72000000-0000-4000-8000-%s","account_id":"ACC_PHASE7_SMOKE","transaction_type":"PAYMENT","amount":"1.00","currency":"VND","status":"SUCCESS","occurred_at":"%s","produced_at":"%s","correlation_id":"%s"}\n' \
    "$suffix" "$suffix" "$OCCURRED_AT" "$OCCURRED_AT" "$CORRELATION_ID" >> "$PAYLOAD_FILE"
done

produce_file() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic banking.transactions.validated < "$PAYLOAD_FILE"
}

warehouse_sql() {
  docker compose exec -T warehouse-postgres psql \
    -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
    -d "${POSTGRES_WAREHOUSE_DB:-banking_warehouse}" \
    -tAc "$1"
}

wait_for_value() {
  local query="$1"
  local expected="$2"
  for _ in $(seq 1 90); do
    if [[ "$(warehouse_sql "$query" | tr -d '[:space:]')" == "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for warehouse query to return ${expected}: ${query}" >&2
  return 1
}

produce_file
wait_for_value "SELECT COUNT(*) FROM fact_transactions WHERE correlation_id = '${CORRELATION_ID}'" "1000"
FIRST_SUM="$(warehouse_sql "SELECT SUM(amount) FROM fact_transactions WHERE correlation_id = '${CORRELATION_ID}'" | tr -d '[:space:]')"

produce_file
wait_for_value "SELECT COALESCE(SUM(raw_count), 0) >= 2000 FROM warehouse_reconciliation_runs WHERE consumer_name = 'validated' AND started_at >= '${STARTED_AT}'::timestamptz" "t"

SECOND_COUNT="$(warehouse_sql "SELECT COUNT(*) FROM fact_transactions WHERE correlation_id = '${CORRELATION_ID}'" | tr -d '[:space:]')"
SECOND_SUM="$(warehouse_sql "SELECT SUM(amount) FROM fact_transactions WHERE correlation_id = '${CORRELATION_ID}'" | tr -d '[:space:]')"
[[ "$SECOND_COUNT" == "1000" ]]
[[ "$FIRST_SUM" == "1000.00" ]]
[[ "$SECOND_SUM" == "$FIRST_SUM" ]]

ALERT_ID="73000000-0000-4000-8000-${RUN_TOKEN}0000"
AGGREGATE_ID="74000000-0000-4000-8000-${RUN_TOKEN}0000"
EVENT_ID="71000000-0000-4000-8000-${RUN_TOKEN}0000"
WINDOW_START="$(date -u -d '1 minute ago' +%Y-%m-%dT%H:%M:00Z)"
WINDOW_END="$(date -u -d "$WINDOW_START + 1 minute" +%Y-%m-%dT%H:%M:00Z)"

printf '{"alert_id":"%s","rule":"HIGH_VALUE","rule_version":"demo-v1","event_id":"%s","account_id":"ACC_PHASE7_SMOKE","amount":"1.00","currency":"VND","observed_count":1,"window_start":"%s","window_end":"%s","detected_at":"%s","schema_version":1}\n' \
  "$ALERT_ID" "$EVENT_ID" "$WINDOW_START" "$WINDOW_END" "$OCCURRED_AT" | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic banking.transactions.fraud-alerts

printf '{"aggregate_id":"%s","window_key":"%s/VND","window_start":"%s","window_end":"%s","currency":"VND","transaction_count":1000,"total_amount":"1000.00","failed_count":0,"schema_version":1}\n' \
  "$AGGREGATE_ID" "$RUN_ID" "$WINDOW_START" "$WINDOW_END" | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic banking.transactions.aggregates

wait_for_value "SELECT COUNT(*) FROM fact_fraud_alerts WHERE alert_id = '${ALERT_ID}'" "1"
wait_for_value "SELECT COUNT(*) FROM agg_transaction_windows WHERE aggregate_id = '${AGGREGATE_ID}'" "1"

printf '{"malformed":true}\n' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic banking.transactions.validated
wait_for_value "SELECT COUNT(*) FROM warehouse_dlq_outbox WHERE source_topic = 'banking.transactions.validated' AND created_at >= '${STARTED_AT}'::timestamptz AND published_at IS NOT NULL" "1"

echo "Phase 7 smoke passed: 1,000 transactions replayed without count/sum inflation; fraud, aggregate, reconciliation, quarantine and DLQ outbox verified."
