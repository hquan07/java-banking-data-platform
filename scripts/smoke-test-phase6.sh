#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose=(docker compose --project-directory "${root_dir}" -f "${root_dir}/docker-compose.yml")
health_url="${LAKE_HEALTH_URL:-http://127.0.0.1:${LAKE_MANAGEMENT_HOST_PORT:-18087}/actuator/health/readiness}"
run_id="$(date -u +%Y%m%d%H%M%S)-$$"
account_id="ACC_PHASE6_${run_id//[^0-9]/}"
raw_group="phase6-smoke-bronze-${run_id}"
curated_group="phase6-smoke-curated-${run_id}"
root_user="${MINIO_ROOT_USER:-minioadmin}"
root_password="${MINIO_ROOT_PASSWORD:-change-me-root-local-only}"
bucket="${MINIO_BUCKET:-banking-lake}"
tmp_dir=$(mktemp -d)
trap 'rm -rf "${tmp_dir}"' EXIT

list_keys() {
  local prefix=$1
  "${compose[@]}" exec -T minio mc find "phase6/${bucket}/${prefix}" 2>/dev/null | sort || true
}

wait_for_url() {
  for attempt in $(seq 1 90); do
    if curl --fail --silent --show-error "${health_url}" >/dev/null 2>&1; then
      return 0
    fi
    if [[ "${attempt}" -eq 90 ]]; then
      "${compose[@]}" logs --tail=120 lake-writer >&2
      echo "Timed out waiting for lake-writer readiness" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_group() {
  local group=$1
  local topic=$2
  for attempt in $(seq 1 60); do
    if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:9092 --group "${group}" --describe --members 2>/dev/null | \
      awk -v expected_group="${group}" '$1 == expected_group && $5 + 0 > 0 { found = 1 } END { exit !found }'; then
      return 0
    fi
    if [[ "${attempt}" -eq 60 ]]; then
      echo "Consumer group ${group} did not subscribe to ${topic}" >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_new_key() {
  local prefix=$1
  local before_file=$2
  local result_file=$3
  for attempt in $(seq 1 60); do
    list_keys "${prefix}" >"${tmp_dir}/current.keys"
    comm -13 "${before_file}" "${tmp_dir}/current.keys" >"${result_file}"
    if [[ -s "${result_file}" ]]; then
      return 0
    fi
    if [[ "${attempt}" -eq 60 ]]; then
      echo "No new object appeared under ${prefix}" >&2
      return 1
    fi
    sleep 2
  done
}

produce_file() {
  local topic=$1
  local input_file=$2
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "${topic}" \
    --sync \
    --reader-property parse.key=true \
    --reader-property key.separator=$'\t' <"${input_file}"
}

echo "Starting Phase 6 MinIO and Kafka dependencies..."
"${compose[@]}" up -d --build kafka kafka-init minio minio-init
"${compose[@]}" exec -T minio mc alias set phase6 http://localhost:9000 "${root_user}" "${root_password}" >/dev/null

list_keys "bronze/transactions/" >"${tmp_dir}/bronze.before"
list_keys "silver/transactions/" >"${tmp_dir}/silver.before"
list_keys "gold/daily_transactions/" >"${tmp_dir}/gold.before"
list_keys "_manifests/bronze/" >"${tmp_dir}/bronze-manifest.before"
list_keys "_manifests/curated/" >"${tmp_dir}/curated-manifest.before"

LAKE_RAW_GROUP_ID="${raw_group}" \
LAKE_CURATED_GROUP_ID="${curated_group}" \
LAKE_STARTING_OFFSETS=latest \
LAKE_MAX_BATCH_RECORDS=3 \
LAKE_FLUSH_INTERVAL=10s \
  "${compose[@]}" up -d --build --force-recreate lake-writer

wait_for_url
wait_for_group "${raw_group}" banking.transactions.raw
wait_for_group "${curated_group}" banking.transactions.validated

python3 - "${tmp_dir}/raw.input" "${tmp_dir}/validated.input" "${account_id}" "${run_id}" <<'PY'
import json
import sys
import time
import uuid

raw_path, validated_path, account, run_id = sys.argv[1:]
with open(raw_path, "w", encoding="utf-8") as raw_file, open(validated_path, "w", encoding="utf-8") as validated_file:
    for index, status in enumerate(("SUCCESS", "FAILED", "SUCCESS")):
        event = {
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"phase6-event-{run_id}-{index}")),
            "event_type": "transaction.created",
            "schema_version": 1,
            "transaction_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"phase6-txn-{run_id}-{index}")),
            "account_id": account,
            "transaction_type": "PAYMENT",
            "amount": f"{100 + index * 25}.00",
            "currency": "VND",
            "status": status,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "produced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "correlation_id": f"phase6-{run_id}",
        }
        payload = json.dumps(event, separators=(",", ":"))
        raw_file.write(f"{account}\t{payload}\n")
        validated_file.write(f"{account}\t{payload}\n")
PY

produce_file banking.transactions.raw "${tmp_dir}/raw.input"
produce_file banking.transactions.validated "${tmp_dir}/validated.input"

wait_for_new_key "bronze/transactions/" "${tmp_dir}/bronze.before" "${tmp_dir}/bronze.new"
wait_for_new_key "silver/transactions/" "${tmp_dir}/silver.before" "${tmp_dir}/silver.new"
wait_for_new_key "gold/daily_transactions/" "${tmp_dir}/gold.before" "${tmp_dir}/gold.new"
wait_for_new_key "_manifests/bronze/" "${tmp_dir}/bronze-manifest.before" "${tmp_dir}/bronze-manifest.new"
wait_for_new_key "_manifests/curated/" "${tmp_dir}/curated-manifest.before" "${tmp_dir}/curated-manifest.new"

for file in bronze.new silver.new gold.new bronze-manifest.new curated-manifest.new; do
  if [[ $(wc -l <"${tmp_dir}/${file}") -ne 1 ]]; then
    echo "Expected exactly one new object in ${file}; micro-batch produced a small-file fanout" >&2
    exit 1
  fi
done

bronze_key=$(<"${tmp_dir}/bronze.new")
silver_key=$(<"${tmp_dir}/silver.new")
gold_key=$(<"${tmp_dir}/gold.new")
bronze_manifest=$(<"${tmp_dir}/bronze-manifest.new")
curated_manifest=$(<"${tmp_dir}/curated-manifest.new")
event_date=$(date -u +%Y-%m-%d)

[[ "${bronze_key}" == *"ingest_date=${event_date}"* ]]
[[ "${silver_key}" == *"event_date=${event_date}"* ]]
[[ "${gold_key}" == *"report_date=${event_date}"* ]]

"${compose[@]}" exec -T minio mc cat "${bronze_key}" | gzip -dc >"${tmp_dir}/bronze.jsonl"
python3 - "${tmp_dir}/bronze.jsonl" "${account_id}" <<'PY'
import json
import sys

path, account = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    rows = [json.loads(line) for line in handle if line.strip()]
if len(rows) != 3:
    raise SystemExit(f"expected 3 Bronze rows, got {len(rows)}")
for row in rows:
    if row["topic"] != "banking.transactions.raw" or row["partition"] < 0 or row["offset"] < 0:
        raise SystemExit("Bronze row is missing Kafka traceability metadata")
    if account not in row["payload_utf8"] or len(row["payload_sha256"]) != 64:
        raise SystemExit("Bronze row did not preserve the raw payload and checksum")
PY

silver_object=${silver_key#phase6/${bucket}/}
verify_log="${tmp_dir}/verify.log"
"${compose[@]}" run --rm --no-deps \
  -e LAKE_WRITER_ENABLED=false \
  -e LAKE_VERIFY_KEY="${silver_object}" \
  lake-writer >"${verify_log}" 2>&1
grep -Fq "LAKE_VERIFY_OK" "${verify_log}"

"${compose[@]}" exec -T minio mc cat "${bronze_manifest}" >"${tmp_dir}/bronze-manifest.json"
"${compose[@]}" exec -T minio mc cat "${curated_manifest}" >"${tmp_dir}/curated-manifest.json"
python3 - "${tmp_dir}/bronze-manifest.json" "${tmp_dir}/curated-manifest.json" <<'PY'
import json
import sys

for path, expected_layer in zip(sys.argv[1:], ("bronze", "curated")):
    with open(path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("status") != "COMMITTED" or manifest.get("layer") != expected_layer:
        raise SystemExit(f"invalid {expected_layer} commit manifest")
    if not manifest.get("sourceOffsets") or not manifest.get("objects"):
        raise SystemExit(f"{expected_layer} manifest is missing offsets or objects")
PY

echo "Phase 6 smoke test passed."
echo "Bronze: ${bronze_key}"
echo "Silver: ${silver_key}"
echo "Gold partial: ${gold_key}"
