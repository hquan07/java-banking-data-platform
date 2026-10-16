#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose=(docker compose --project-directory "${root_dir}" -f "${root_dir}/docker-compose.yml")
api_url="${TRANSACTION_API_URL:-http://127.0.0.1:${TRANSACTION_HOST_PORT:-18080}}"
api_health_url="${TRANSACTION_HEALTH_URL:-http://127.0.0.1:${TRANSACTION_MANAGEMENT_HOST_PORT:-18081}/actuator/health/readiness}"
publisher_health_url="${PUBLISHER_HEALTH_URL:-http://127.0.0.1:${PUBLISHER_MANAGEMENT_HOST_PORT:-18083}/actuator/health/readiness}"
postgres_user="${POSTGRES_USER:-banking}"
postgres_db="${POSTGRES_OLTP_DB:-banking_oltp}"
topic="banking.transactions.raw"
account_id="ACC_PHASE4"
run_id="$(date -u +%Y%m%d%H%M%S)-$$"
group_id="phase4-smoke-${run_id}"
tmp_dir=$(mktemp -d)
trap 'rm -rf "${tmp_dir}"' EXIT

wait_for_url() {
  local url=$1
  local name=$2
  for attempt in $(seq 1 90); do
    if curl --fail --silent --show-error "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if [[ "${attempt}" -eq 90 ]]; then
      echo "Timed out waiting for ${name}: ${url}" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_init() {
  local container_id
  for attempt in $(seq 1 90); do
    container_id=$("${compose[@]}" ps -aq kafka-init)
    if [[ -n "${container_id}" ]]; then
      state=$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "${container_id}")
      if [[ "${state}" == "exited 0" ]]; then
        return 0
      fi
      if [[ "${state}" == exited\ * ]]; then
        "${compose[@]}" logs kafka-init >&2
        echo "kafka-init failed: ${state}" >&2
        return 1
      fi
    fi
    if [[ "${attempt}" -eq 90 ]]; then
      "${compose[@]}" ps -a >&2
      echo "Timed out waiting for kafka-init" >&2
      return 1
    fi
    sleep 2
  done
}

start_consumer() {
  local output_file=$1
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "${topic}" \
    --group "${group_id}" \
    --timeout-ms 5000 \
    --consumer-property auto.offset.reset=earliest \
    --consumer-property enable.auto.commit=true \
    --consumer-property auto.commit.interval.ms=100 \
    --property print.partition=true \
    --property print.offset=true \
    --property print.key=true \
    --property key.separator=$'\t' >"${output_file}" &
  consumer_pid=$!
  sleep 3
}

post_transaction() {
  local sequence=$1
  local response_file=$2
  curl --fail --silent --show-error \
    -X POST "${api_url}/api/v1/transactions" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: phase4-${run_id}-${sequence}" \
    -H "X-Correlation-ID: phase4-${run_id}-${sequence}" \
    -d "{\"accountId\":\"${account_id}\",\"transactionType\":\"PAYMENT\",\"amount\":\"25.00\",\"currency\":\"VND\"}" \
    >"${response_file}"
  python3 - "${response_file}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
transaction_id = payload.get("transactionId")
if not transaction_id:
    raise SystemExit("Transaction API response has no transactionId")
print(transaction_id)
PY
}

assert_consumed() {
  local output_file=$1
  local transaction_id=$2
  local forbidden_transaction_id=${3:-}
  python3 - "${output_file}" "${transaction_id}" "${account_id}" "${forbidden_transaction_id}" <<'PY'
import json
import re
import sys

path, expected_transaction_id, expected_key, forbidden_transaction_id = sys.argv[1:]
matched_partition = None
with open(path, encoding="utf-8") as handle:
    text = handle.read()
for raw_line in text.splitlines():
    json_start = raw_line.find("{")
    if json_start < 0:
        continue
    prefix = raw_line[:json_start].rstrip("\t")
    try:
        payload = json.loads(raw_line[json_start:])
    except json.JSONDecodeError:
        continue
    if forbidden_transaction_id and payload.get("transaction_id") == forbidden_transaction_id:
        raise SystemExit("Consumer restart replayed an already committed event")
    if payload.get("transaction_id") != expected_transaction_id:
        continue
    if payload.get("account_id") != expected_key:
        raise SystemExit("Payload account_id does not match expected key")
    if not prefix.endswith(expected_key):
        raise SystemExit("Kafka record key does not match account_id")
    partition_match = re.search(r"Partition:\s*([0-9]+)", prefix)
    if not partition_match:
        raise SystemExit("Kafka consumer output has no partition metadata")
    matched_partition = partition_match.group(1)
    break
if matched_partition is None:
    raise SystemExit(f"Expected transaction {expected_transaction_id} was not consumed")
print(matched_partition)
PY
}

echo "Starting Phase 4 stack..."
"${compose[@]}" up -d --build postgres kafka kafka-init transaction-service outbox-publisher
wait_for_init
wait_for_url "${api_health_url}" "transaction service readiness"
wait_for_url "${publisher_health_url}" "outbox publisher readiness"

"${compose[@]}" exec -T postgres psql \
  --set ON_ERROR_STOP=1 \
  --username "${postgres_user}" \
  --dbname "${postgres_db}" <<SQL
INSERT INTO customers (customer_id, status)
VALUES ('CUS_PHASE4', 'ACTIVE')
ON CONFLICT (customer_id) DO UPDATE SET status = EXCLUDED.status;

INSERT INTO accounts (account_id, customer_id, currency, status)
VALUES ('${account_id}', 'CUS_PHASE4', 'VND', 'ACTIVE')
ON CONFLICT (account_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    currency = EXCLUDED.currency,
    status = EXCLUDED.status;
SQL

start_consumer "${tmp_dir}/consumer-1.log"
transaction_1=$(post_transaction 1 "${tmp_dir}/response-1.json")
wait "${consumer_pid}" || true
partition_1=$(assert_consumed "${tmp_dir}/consumer-1.log" "${transaction_1}")

"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group "${group_id}" \
  --describe >"${tmp_dir}/group-after-first.txt"
grep -Fq "${topic}" "${tmp_dir}/group-after-first.txt"

start_consumer "${tmp_dir}/consumer-2.log"
transaction_2=$(post_transaction 2 "${tmp_dir}/response-2.json")
wait "${consumer_pid}" || true
partition_2=$(assert_consumed "${tmp_dir}/consumer-2.log" "${transaction_2}" "${transaction_1}")

if [[ "${partition_1}" != "${partition_2}" ]]; then
  echo "Same account key reached different partitions: ${partition_1} and ${partition_2}" >&2
  exit 1
fi

for attempt in $(seq 1 30); do
  published_count=$("${compose[@]}" exec -T postgres psql \
    --tuples-only --no-align \
    --username "${postgres_user}" \
    --dbname "${postgres_db}" \
    --command "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id IN ('${transaction_1}'::uuid, '${transaction_2}'::uuid) AND published_at IS NOT NULL;")
  if [[ "${published_count}" == "2" ]]; then
    break
  fi
  if [[ "${attempt}" -eq 30 ]]; then
    echo "Outbox rows were not marked published" >&2
    exit 1
  fi
  sleep 1
done

"${compose[@]}" run --rm kafka-init >/dev/null

echo "Phase 4 smoke test passed: transactions=${transaction_1},${transaction_2} partition=${partition_1} group=${group_id}"
