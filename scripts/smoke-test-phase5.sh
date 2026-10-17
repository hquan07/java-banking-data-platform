#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose=(docker compose --project-directory "${root_dir}" -f "${root_dir}/docker-compose.yml")
flink_url="${FLINK_URL:-http://127.0.0.1:${FLINK_DASHBOARD_HOST_PORT:-18084}}"
run_id="$(date -u +%Y%m%d%H%M%S)-$$"
account_id="ACC_PHASE5_${run_id//[^0-9]/}"
tmp_dir=$(mktemp -d)
trap 'rm -rf "${tmp_dir}"' EXIT

run_epoch=$(date -u +%s)
base_epoch=$(( run_epoch / 60 * 60 ))
# Keep every rerun unique, including two smoke processes started in the same minute,
# while staying within the 48-bit UUID node field.
id_seed=$(( (run_epoch * 100000 + $$ % 100000) % 281474976710656 ))
high_event_id=$(printf '00000000-0000-4000-8000-%012x' "${id_seed}")
invalid_event_id=$(printf '00000000-0000-4000-8000-%012x' "$((id_seed + 100))")
late_event_id=$(printf '00000000-0000-4000-8000-%012x' "$((id_seed + 200))")

iso_time() {
  date -u -d "@$1" '+%Y-%m-%dT%H:%M:%SZ'
}

append_event() {
  local output_file=$1
  local event_id=$2
  local transaction_id=$3
  local account=$4
  local amount=$5
  local status=$6
  local occurred_epoch=$7
  local schema_version=${8:-1}
  local occurred_at
  local produced_at
  occurred_at=$(iso_time "${occurred_epoch}")
  produced_at=$(iso_time "$((occurred_epoch + 1))")
  printf '%s\t%s\n' "${account}" \
    "{\"event_id\":\"${event_id}\",\"event_type\":\"transaction.created\",\"schema_version\":${schema_version},\"transaction_id\":\"${transaction_id}\",\"account_id\":\"${account}\",\"transaction_type\":\"PAYMENT\",\"amount\":\"${amount}\",\"currency\":\"VND\",\"status\":\"${status}\",\"occurred_at\":\"${occurred_at}\",\"produced_at\":\"${produced_at}\",\"correlation_id\":\"phase5-${run_id}\"}" \
    >>"${output_file}"
}

produce_file() {
  local input_file=$1
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic banking.transactions.raw \
    --reader-property parse.key=true \
    --reader-property key.separator=$'\t' <"${input_file}"
}

produce_marker_file() {
  local input_file=$1
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic banking.transactions.raw \
    --sync \
    --command-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
    --reader-property parse.key=true \
    --reader-property key.separator=$'\t' <"${input_file}"
}

consume_topic() {
  local topic=$1
  local output_file=$2
  local group="phase5-smoke-${run_id}-${topic//./-}"
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "${topic}" \
    --group "${group}" \
    --timeout-ms 5000 \
    --command-property auto.offset.reset=earliest \
    --command-property isolation.level=read_committed \
    --formatter-property print.key=true \
    --formatter-property key.separator=$'\t' >"${output_file}" 2>/dev/null || true
}

job_snapshot() {
  curl --fail --silent --show-error "${flink_url}/jobs/overview"
}

job_id_from_snapshot() {
  python3 -c 'import json,sys; jobs=json.load(sys.stdin).get("jobs", []); print(next((job["jid"] for job in jobs if job.get("name") == "banking-phase5-streaming"), ""))'
}

wait_for_running_job() {
  local snapshot
  for attempt in $(seq 1 90); do
    snapshot=$(job_snapshot 2>/dev/null || true)
    if [[ -n "${snapshot}" ]] && python3 -c 'import json,sys; jobs=json.load(sys.stdin).get("jobs", []); raise SystemExit(0 if any(job.get("name") == "banking-phase5-streaming" and job.get("state") == "RUNNING" for job in jobs) else 1)' <<<"${snapshot}"; then
      return 0
    fi
    if [[ "${attempt}" -eq 90 ]]; then
      "${compose[@]}" ps -a >&2
      "${compose[@]}" logs --tail=100 flink-jobmanager flink-taskmanager flink-submit >&2
      echo "Timed out waiting for the Phase 5 Flink job" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_completed_checkpoint() {
  local minimum=${1:-0}
  local snapshot
  local job_id
  local completed
  for attempt in $(seq 1 60); do
    snapshot=$(job_snapshot)
    job_id=$(job_id_from_snapshot <<<"${snapshot}")
    if [[ -n "${job_id}" ]]; then
      completed=$(curl --fail --silent --show-error "${flink_url}/jobs/${job_id}/checkpoints" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("counts", {}).get("completed", 0))')
      if (( completed > minimum )); then
        return 0
      fi
    fi
    if [[ "${attempt}" -eq 60 ]]; then
      echo "Flink did not complete a checkpoint" >&2
      return 1
    fi
    sleep 2
  done
}

completed_checkpoint_count() {
  local snapshot
  local job_id
  snapshot=$(job_snapshot)
  job_id=$(job_id_from_snapshot <<<"${snapshot}")
  curl --fail --silent --show-error "${flink_url}/jobs/${job_id}/checkpoints" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("counts", {}).get("completed", 0))'
}

assert_outputs() {
  python3 - \
    "${tmp_dir}/validated.log" \
    "${tmp_dir}/invalid.log" \
    "${tmp_dir}/alerts.log" \
    "${tmp_dir}/aggregates.log" \
    "${high_event_id}" \
    "${invalid_event_id}" \
    "${late_event_id}" \
    "${account_id}" <<'PY'
import json
import sys

validated_path, invalid_path, alerts_path, aggregates_path, high_id, invalid_id, late_id, account = sys.argv[1:]

def records(path):
    result = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            start = line.find("{")
            if start < 0:
                continue
            try:
                result.append(json.loads(line[start:]))
            except json.JSONDecodeError:
                continue
    return result

validated = records(validated_path)
invalid = records(invalid_path)
alerts = records(alerts_path)
aggregates = records(aggregates_path)

if sum(item.get("event_id") == high_id for item in validated) != 1:
    raise SystemExit("deduplication failed: high-value event was not emitted exactly once")
if not any(item.get("event_id") == invalid_id and item.get("failure_code") == "UNSUPPORTED_SCHEMA_VERSION" for item in invalid):
    raise SystemExit("invalid schema event was not routed to the invalid topic")
if not any(item.get("event_id") == late_id and item.get("failure_code") == "EVENT_TOO_LATE" for item in invalid):
    raise SystemExit("late event was not routed to the invalid topic")
if not any(item.get("event_id") == high_id and item.get("rule") == "HIGH_VALUE" for item in alerts):
    raise SystemExit("HIGH_VALUE alert was not emitted")
if not any(item.get("account_id") == account and item.get("rule") == "VELOCITY_60S" and item.get("observed_count", 0) > 10 for item in alerts):
    raise SystemExit("VELOCITY_60S alert was not emitted")
if not any(item.get("currency") == "VND" and item.get("transaction_count") == 11 and item.get("total_amount") == "100000100.00" for item in aggregates):
    raise SystemExit("one-minute aggregate does not match the expected count and amount")
PY
}

echo "Starting Phase 5 Flink stack..."
"${compose[@]}" up -d --build kafka kafka-init
"${compose[@]}" up -d --build --force-recreate flink-jobmanager flink-taskmanager
# A short idle timeout makes the multi-partition watermark test deterministic
# without changing the production default in the user's .env file. A unique
# group starting at latest offsets isolates this run from earlier smoke data.
"${compose[@]}" run --rm \
  -e WATERMARK_IDLENESS=PT2S \
  -e STREAMING_GROUP_ID="phase5-smoke-${run_id}" \
  -e STREAMING_STARTING_OFFSETS=LATEST \
  flink-submit
wait_for_running_job

input_file="${tmp_dir}/input.log"
for index in $(seq 0 10); do
  event_id=$(printf '00000000-0000-4000-8000-%012x' "$((id_seed + index))")
  transaction_id=$(printf '10000000-0000-4000-8000-%012x' "$((id_seed + index))")
  amount="10.00"
  status="SUCCESS"
  if [[ "${index}" -eq 0 ]]; then
    event_id="${high_event_id}"
    amount="100000000.00"
  elif [[ "${index}" -eq 5 ]]; then
    status="FAILED"
  fi
  append_event "${input_file}" "${event_id}" "${transaction_id}" "${account_id}" "${amount}" "${status}" "$((base_epoch + index))"
done

# Duplicate the first event before the watermark marker; dedup must suppress it.
head -n 1 "${input_file}" >>"${input_file}"
append_event "${input_file}" "${invalid_event_id}" \
  "$(printf '10000000-0000-4000-8000-%012x' "$((id_seed + 100))")" \
  "${account_id}" "10.00" "SUCCESS" "$((base_epoch + 12))" 2
checkpoint_before_input=$(completed_checkpoint_count)
produce_file "${input_file}"

# Commit the initial batch before advancing watermarks on every raw-topic partition.
wait_for_completed_checkpoint "$((checkpoint_before_input + 1))"
marker_file="${tmp_dir}/markers.log"
for partition in $(seq 0 2); do
  marker_offset=$((id_seed + 300 + partition))
  marker_account="ACC_PHASE5_MARKER_${partition}_${run_id//[^0-9]/}"
  append_event "${marker_file}" \
    "$(printf '00000000-0000-4000-8000-%012x' "${marker_offset}")" \
    "$(printf '10000000-0000-4000-8000-%012x' "${marker_offset}")" \
    "${marker_account}" "1.00" "SUCCESS" "$((base_epoch + 120))"
done
checkpoint_before_marker=$(completed_checkpoint_count)
produce_marker_file "${marker_file}"

# Wait for two newer checkpoints so all three partition watermarks advance and
# their transactional outputs are committed before injecting the late event.
wait_for_completed_checkpoint "$((checkpoint_before_marker + 1))"
late_file="${tmp_dir}/late.log"
append_event "${late_file}" "${late_event_id}" \
  "$(printf '10000000-0000-4000-8000-%012x' "$((id_seed + 200))")" \
  "${account_id}" "10.00" "SUCCESS" "$((base_epoch - 60))"
checkpoint_before_late=$(completed_checkpoint_count)
produce_file "${late_file}"

# Commit the late side output before exercising recovery.
wait_for_completed_checkpoint "${checkpoint_before_late}"
checkpoint_before_restart=$(completed_checkpoint_count)

echo "Restarting TaskManager to verify checkpoint recovery..."
"${compose[@]}" restart flink-taskmanager >/dev/null
wait_for_running_job

# Re-publish the original event after restore; restored dedup state must suppress it.
duplicate_file="${tmp_dir}/duplicate-after-restore.log"
head -n 1 "${input_file}" >"${duplicate_file}"
produce_file "${duplicate_file}"
wait_for_completed_checkpoint "${checkpoint_before_restart}"

consume_topic banking.transactions.validated "${tmp_dir}/validated.log" & validated_pid=$!
consume_topic banking.transactions.invalid "${tmp_dir}/invalid.log" & invalid_pid=$!
consume_topic banking.transactions.fraud-alerts "${tmp_dir}/alerts.log" & alerts_pid=$!
consume_topic banking.transactions.aggregates "${tmp_dir}/aggregates.log" & aggregates_pid=$!
wait "${validated_pid}" "${invalid_pid}" "${alerts_pid}" "${aggregates_pid}"

assert_outputs
echo "Phase 5 smoke test passed: account=${account_id} checkpoint_restore=verified"
