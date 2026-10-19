# Benchmarks

No performance baseline has been recorded yet. Target SLO values in the roadmap and alert rules are acceptance objectives, not measured results.

## Reproducible procedure

1. Start from a clean commit and record its SHA, dirty-tree state, image tags/digests, Java/Docker/Compose versions and all non-secret environment overrides.
2. Record CPU model/core allocation, RAM, storage type/free space and host operating system. Do not compare runs with materially different resource limits as if they were equivalent.
3. Register the synthetic or external input in `docs/datasets.md`; use a unique run ID/namespace and fixed generator seed. Preserve the generated input manifest/checksum.
4. Start the normal pipeline plus `monitoring`; wait until all Prometheus targets are healthy and the Flink job has a completed checkpoint.
5. Warm up without recording, then run progressive 10, 50 and 100 TPS stages for a declared duration. Do not increase load after correctness checks fail.
6. Capture generator achieved TPS and HTTP p50/p95/p99, error classes, outbox backlog, Kafka ingress/consumer lag, Flink throughput/backpressure/checkpoint success and duration, lake/warehouse last-success age, PostgreSQL connections, MinIO usage, host/container CPU and memory.
7. Reconcile scoped source offsets and unique `event_id` values through valid, invalid, duplicate, late, lake and warehouse outcomes. Record unexplained loss/duplication as a failed run.
8. Save the report and raw metric export under an ignored benchmark-results directory or external artifact store. Commit only the small reviewed report when it contains no secrets or raw external data.

## Result template

| Field | Measured value |
|---|---|
| Run ID / timestamp | pending |
| Commit SHA / dirty state | pending |
| Host and container limits | pending |
| Dataset ID / checksum / rows | pending |
| Generator seed / requested TPS / duration | pending |
| Achieved TPS / success / error classes | pending |
| HTTP p50 / p95 / p99 | pending |
| Kafka peak/final lag | pending |
| Flink checkpoint success / p95 duration / backpressure | pending |
| Warehouse p95 freshness / final reconciliation | pending |
| Lake object counts / commit age | pending |
| Peak CPU / RAM / disk / MinIO capacity | pending |
| Failure injection and recovery time | pending |
| Evidence artifact location / checksum | pending |

Replace `pending` only with measured values from one reproducible run. Failed runs remain useful evidence and must not be rewritten as successful throughput claims.
