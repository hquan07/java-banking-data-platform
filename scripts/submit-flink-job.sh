#!/usr/bin/env bash
set -euo pipefail

jobmanager="${FLINK_JOBMANAGER:-flink-jobmanager:8081}"
job_name="${FLINK_JOB_NAME:-banking-phase5-streaming}"
job_jar="${FLINK_JOB_JAR:-/opt/flink/usrlib/banking-streaming.jar}"
rest_url="http://${jobmanager}"

for attempt in $(seq 1 60); do
  jobs=$(curl --fail --silent --show-error "${rest_url}/jobs/overview" 2>/dev/null || true)
  if [[ -n "${jobs}" ]]; then
    if grep -Fq "\"name\":\"${job_name}\"" <<<"${jobs}" \
        && grep -Eq '"state":"(RUNNING|RESTARTING|CREATED|INITIALIZING|RECONCILING)"' <<<"${jobs}"; then
      echo "Flink job already active: ${job_name}"
      exit 0
    fi
    break
  fi
  if [[ "${attempt}" -eq 60 ]]; then
    echo "Flink JobManager did not become ready at ${rest_url}" >&2
    exit 1
  fi
  sleep 1
done

if [[ ! -r "${job_jar}" ]]; then
  echo "Flink job jar is not readable: ${job_jar}" >&2
  exit 1
fi

exec /opt/flink/bin/flink run \
  --detached \
  --jobmanager "${jobmanager}" \
  --class io.github.hquan07.banking.streaming.StreamingProcessorJob \
  "${job_jar}"
