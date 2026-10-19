#!/usr/bin/env bash
set -euo pipefail

readonly PROMETHEUS_URL="http://127.0.0.1:${PROMETHEUS_HOST_PORT:-19090}"
readonly GRAFANA_URL="http://127.0.0.1:${GRAFANA_HOST_PORT:-13000}"
readonly ANALYTICS_MANAGEMENT_URL="http://127.0.0.1:${ANALYTICS_MANAGEMENT_HOST_PORT:-18093}"
readonly GRAFANA_USER="${GRAFANA_ADMIN_USER:-admin}"
readonly GRAFANA_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-change-me-grafana-local-only}"
ANALYTICS_STOPPED=false

recover_analytics() {
  if [[ "$ANALYTICS_STOPPED" == "true" ]]; then
    docker compose --profile analytics start analytics-service >/dev/null 2>&1 || true
  fi
}
trap recover_analytics EXIT

warehouse_counts() {
  docker compose exec -T warehouse-postgres psql \
    -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
    -d "${POSTGRES_WAREHOUSE_DB:-banking_warehouse}" \
    -tAc "SELECT CONCAT(
      (SELECT COUNT(*) FROM fact_transactions), '|',
      (SELECT COUNT(*) FROM fact_fraud_alerts), '|',
      (SELECT COUNT(*) FROM batch_runs)
    );" | tr -d '[:space:]'
}

wait_http() {
  local url="$1"
  local attempts="${2:-90}"
  for _ in $(seq 1 "$attempts"); do
    if curl --fail --silent "$url" >/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${url}" >&2
  return 1
}

wait_prometheus_value() {
  local expression="$1"
  local expected="$2"
  for _ in $(seq 1 30); do
    local response
    response="$(curl --fail --silent --get --data-urlencode "query=${expression}" "${PROMETHEUS_URL}/api/v1/query")"
    if python3 - "$response" "$expected" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
expected = float(sys.argv[2])
results = payload.get("data", {}).get("result", [])
if results and float(results[0]["value"][1]) == expected:
    raise SystemExit(0)
raise SystemExit(1)
PY
    then
      return 0
    fi
    sleep 2
  done
  echo "Prometheus expression did not reach ${expected}: ${expression}" >&2
  return 1
}

wait_prometheus_targets() {
  for _ in $(seq 1 45); do
    local response
    response="$(curl --fail --silent "${PROMETHEUS_URL}/api/v1/targets")"
    if python3 - "$response" <<'PY'
import json
import sys

required = {
    "prometheus", "transaction-service", "outbox-publisher", "lake-writer",
    "warehouse-writer", "analytics-service", "kafka", "postgres-oltp",
    "postgres-warehouse", "flink-jobmanager", "flink-taskmanager", "minio",
}
payload = json.loads(sys.argv[1])
active = payload["data"]["activeTargets"]
healthy = {target["labels"]["job"] for target in active if target["health"] == "up"}
raise SystemExit(0 if required <= healthy else 1)
PY
    then
      return 0
    fi
    sleep 2
  done

  curl --fail --silent "${PROMETHEUS_URL}/api/v1/targets" | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
for target in payload["data"]["activeTargets"]:
    print(target["labels"].get("job"), target["health"], target.get("lastError", ""))
' >&2
  echo "Timed out waiting for all required Prometheus targets" >&2
  return 1
}

docker compose --profile analytics --profile monitoring up -d --build \
  postgres warehouse-postgres kafka kafka-init minio minio-init \
  transaction-service outbox-publisher flink-jobmanager flink-taskmanager flink-submit \
  lake-writer warehouse-writer warehouse-migrate analytics-service \
  kafka-exporter postgres-exporter-oltp postgres-exporter-warehouse prometheus grafana

wait_http "${PROMETHEUS_URL}/-/ready"
wait_http "${GRAFANA_URL}/api/health"
wait_http "${ANALYTICS_MANAGEMENT_URL}/actuator/health/readiness"

docker compose --profile monitoring exec -T prometheus \
  promtool check config /etc/prometheus/prometheus.yml >/dev/null

wait_prometheus_targets

RULES_JSON="$(curl --fail --silent "${PROMETHEUS_URL}/api/v1/rules?type=alert")"
python3 - "$RULES_JSON" <<'PY'
import json
import sys

expected = {
    "BankingTargetDown", "BankingApiSuccessRateLow", "BankingApiP95LatencyHigh",
    "BankingOutboxBacklogHigh", "BankingKafkaConsumerLagGrowing",
    "BankingFlinkCheckpointFailures", "BankingWarehouseWriterFailures",
    "BankingLakeWriterFailures", "BankingWarehouseDataStale",
    "BankingMinioCapacityWarning", "BankingMinioCapacityCritical",
}
payload = json.loads(sys.argv[1])
loaded = {
    rule["name"]
    for group in payload["data"]["groups"]
    for rule in group["rules"]
}
assert expected <= loaded, f"missing alert rules: {sorted(expected - loaded)}"
PY

curl --fail --silent --user "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  "${GRAFANA_URL}/api/datasources/uid/prometheus" >/dev/null
curl --fail --silent --user "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  "${GRAFANA_URL}/api/datasources/uid/warehouse" >/dev/null
DASHBOARD_JSON="$(curl --fail --silent --user "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  "${GRAFANA_URL}/api/dashboards/uid/banking-platform-operations")"
python3 - "$DASHBOARD_JSON" <<'PY'
import json
import sys

dashboard = json.loads(sys.argv[1])["dashboard"]
assert dashboard["title"] == "Banking Platform Operations"
assert len(dashboard["panels"]) >= 14
PY

BEFORE_COUNTS="$(warehouse_counts)"
wait_prometheus_value 'up{job="analytics-service"}' 1

docker compose --profile analytics stop analytics-service >/dev/null
ANALYTICS_STOPPED=true
wait_prometheus_value 'up{job="analytics-service"}' 0

docker compose --profile analytics start analytics-service >/dev/null
ANALYTICS_STOPPED=false
wait_http "${ANALYTICS_MANAGEMENT_URL}/actuator/health/readiness"
wait_prometheus_value 'up{job="analytics-service"}' 1

AFTER_COUNTS="$(warehouse_counts)"
[[ "$BEFORE_COUNTS" == "$AFTER_COUNTS" ]]

echo "Phase 10 smoke passed: 12 scrape jobs healthy, 11 alert rules and 14 dashboard panels provisioned; analytics failure/recovery preserved warehouse counts ${AFTER_COUNTS}."
