#!/usr/bin/env bash
set -euo pipefail

bootstrap_server="${1:-kafka:9092}"
manifest="${2:-/opt/banking/topics.tsv}"
kafka_bin="${KAFKA_BIN:-/opt/kafka/bin}"

if [[ ! -r "${manifest}" ]]; then
  echo "Topic manifest is not readable: ${manifest}" >&2
  exit 1
fi

for attempt in $(seq 1 60); do
  if "${kafka_bin}/kafka-topics.sh" --bootstrap-server "${bootstrap_server}" --list >/dev/null 2>&1; then
    break
  fi
  if [[ "${attempt}" -eq 60 ]]; then
    echo "Kafka did not become ready at ${bootstrap_server}" >&2
    exit 1
  fi
  sleep 1
done

while IFS=$'\t' read -r topic partitions replication_factor retention_ms cleanup_policy extra; do
  topic="${topic%$'\r'}"
  [[ -z "${topic}" || "${topic}" == \#* ]] && continue
  cleanup_policy="${cleanup_policy%$'\r'}"

  if [[ -n "${extra:-}" \
      || ! "${partitions}" =~ ^[1-9][0-9]*$ \
      || ! "${replication_factor}" =~ ^[1-9][0-9]*$ \
      || ! "${retention_ms}" =~ ^[1-9][0-9]*$ \
      || ! "${cleanup_policy}" =~ ^(delete|compact|compact,delete)$ ]]; then
    echo "Invalid topic manifest row for ${topic}" >&2
    exit 1
  fi

  "${kafka_bin}/kafka-topics.sh" \
    --bootstrap-server "${bootstrap_server}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${replication_factor}"

  description=$("${kafka_bin}/kafka-topics.sh" \
    --bootstrap-server "${bootstrap_server}" \
    --describe \
    --topic "${topic}" | head -n 1)
  actual_partitions=$(sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p' <<<"${description}")
  actual_replication=$(sed -n 's/.*ReplicationFactor: \([0-9][0-9]*\).*/\1/p' <<<"${description}")

  if [[ "${actual_replication}" != "${replication_factor}" ]]; then
    echo "Topic ${topic} has replication factor ${actual_replication}; expected ${replication_factor}" >&2
    exit 1
  fi
  if (( actual_partitions < partitions )); then
    "${kafka_bin}/kafka-topics.sh" \
      --bootstrap-server "${bootstrap_server}" \
      --alter \
      --topic "${topic}" \
      --partitions "${partitions}"
  elif (( actual_partitions > partitions )); then
    echo "Topic ${topic} has ${actual_partitions} partitions; cannot reduce to ${partitions}" >&2
    exit 1
  fi

  "${kafka_bin}/kafka-configs.sh" \
    --bootstrap-server "${bootstrap_server}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --alter \
    --add-config "retention.ms=${retention_ms},cleanup.policy=${cleanup_policy}"

  configs=$("${kafka_bin}/kafka-configs.sh" \
    --bootstrap-server "${bootstrap_server}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --describe)
  grep -Fq "retention.ms=${retention_ms}" <<<"${configs}"
  grep -Fq "cleanup.policy=${cleanup_policy}" <<<"${configs}"
  echo "Topic ready: ${topic} partitions=${partitions} replication=${replication_factor} retention_ms=${retention_ms}"
done < "${manifest}"
