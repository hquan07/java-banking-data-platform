# Runbook

## Transaction service (Phase 2)

Prerequisites: Java 21 and a reachable PostgreSQL database. The configured user must be able to run the two Flyway migrations on startup. Only synthetic test data is permitted.

1. Configure `DB_URL`, `DB_USERNAME`, and `DB_PASSWORD`; optional port/pool settings are listed in `.env.example`.
2. Build with `mvn -B -ntp -pl services/transaction-service -am package`.
3. Start with `java -jar services/transaction-service/target/transaction-service-0.1.0-SNAPSHOT.jar`.
4. Verify `http://127.0.0.1:8081/actuator/health/liveness` and `http://127.0.0.1:8081/actuator/health/readiness` separately.
5. Stop with the process manager's graceful termination signal. The application does not delete database rows on shutdown.

The API requires an active account with the same currency as the request. Until the generator/bootstrap phase exists, insert synthetic customer/account rows through an authenticated PostgreSQL client, for example:

```sql
INSERT INTO customers (customer_id, status) VALUES ('CUS10001', 'ACTIVE');
INSERT INTO accounts (account_id, customer_id, currency, status)
VALUES ('ACC10001', 'CUS10001', 'VND', 'ACTIVE');
```

To diagnose a failed create request, use the returned `X-Correlation-ID` to search application logs, then check database readiness. Never return or paste database exception text into an API response. A client may retry a dependency failure with the exact same `Idempotency-Key` and payload: after an uncertain response, the idempotency lookup determines whether the first attempt committed.

The management listener defaults to `127.0.0.1:8081`. Do not expose it publicly. Compose maps it to host port `18081` by default.

## Transaction generator (Phase 3)

1. Seed every ID in `GENERATOR_ACCOUNT_IDS` as an active account whose currency matches `GENERATOR_CURRENCY`.
2. Start the transaction service and verify its readiness endpoint.
3. Build with `mvn -B -ntp -pl services/transaction-generator -am package`.
4. Set `GENERATOR_ENABLED=true`, TPS, duration, seed and comma-separated account IDs, then run the executable jar.
5. Use the final JSON summary for request counts, outcome classes, p50/p95/p99 and achieved throughput. The generator intentionally includes invalid and idempotent-retry scenarios, so 100% HTTP success is not expected.

Use a fixed seed for reproducible tests. Increase load gradually and keep `GENERATOR_MAX_IN_FLIGHT` bounded. Generator logs intentionally omit request payloads.

## Outbox publisher (Phase 3)

1. Ensure the Phase 2 Flyway schema exists and Kafka is reachable. Until Phase 4 bootstrap is implemented, create `banking.transactions.raw` explicitly with the required partition/replication policy.
2. Build with `mvn -B -ntp -pl services/outbox-publisher -am package`.
3. Set database/Kafka variables and `OUTBOX_PUBLISHER_ENABLED=true`, then run the executable jar.
4. Verify publisher liveness/readiness on `127.0.0.1:8083` when running the jar directly; Compose maps it to host port `18083`. Scrape `/actuator/prometheus` only from the private monitoring network.
5. Monitor unpublished backlog, publish failures/retries and publish latency. A growing backlog with failed readiness normally indicates Kafka or database connectivity trouble.

During a Kafka outage, leave the publisher running or restart it after Kafka recovers. Rows remain in `outbox_events`; backoff schedules the next attempt and expired leases make interrupted claims recoverable. Do not manually set `published_at`, delete rows or clear errors to force progress. Duplicate delivery can occur after ACK but before the database update and must be handled downstream by `event_id`.

## Kafka and Compose runtime (Phase 4)

Start in dependency order with one command:

```bash
docker compose up -d --build postgres kafka kafka-init transaction-service outbox-publisher
docker compose ps -a
```

Expected state: PostgreSQL and Kafka are healthy, the two Java services are running, and `kafka-init` has exited with code 0. Because init is one-shot, inspect it with `docker compose ps -a` rather than waiting for it to remain running. Reapply and verify the manifest with `docker compose run --rm kafka-init`.

Default host mappings are API `18080`, API management `18081`, publisher `18082`, publisher management `18083`, PostgreSQL `55432` and Kafka `9092`. Container-to-container ports remain unchanged.

Run `./scripts/smoke-test.sh` after configuration or image changes. It seeds synthetic `ACC_PHASE4`, creates two transactions, verifies Kafka key/partition and proves a restarted consumer group continues after its committed offset. Generated rows and events are synthetic and may remain in local volumes for inspection.

Useful diagnostics:

```bash
docker compose logs kafka kafka-init transaction-service outbox-publisher
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --all-groups --describe
```

Use `docker compose restart kafka` for a non-destructive restart test. Use `docker compose down` to stop the stack while retaining named volumes. Never use `docker compose down -v` during ordinary troubleshooting because it deletes PostgreSQL and Kafka state.

## Flink streaming runtime (Phase 5)

Build and start the single-host Flink cluster, then submit the job:

```bash
make up-streaming
docker compose ps -a
curl --fail http://127.0.0.1:18084/jobs/overview
```

`flink-submit` is one-shot and must exit with code 0. The submit script does not create a duplicate when `banking-phase5-streaming` is already active. The JobManager and TaskManager remain running; the dashboard is local-only on `127.0.0.1:18084`.

Run `./scripts/smoke-test-phase5.sh` after changing contracts, streaming code, Kafka/Flink versions, checkpoint/watermark settings or Compose. The test checks valid/invalid/late routing, deduplication, both fraud rules, the one-minute aggregate and state recovery after a TaskManager restart. Each run recreates only JobManager/TaskManager, uses a unique consumer group with `STREAMING_STARTING_OFFSETS=LATEST`, sends watermark markers to all three raw-topic partitions and synchronizes with completed checkpoints. This isolates assertions from retained Kafka data without deleting any named volume. It uses only synthetic records and leaves services/volumes running for inspection.

Normal runtime keeps `STREAMING_STARTING_OFFSETS=COMMITTED_OR_EARLIEST`: restore committed group offsets and fall back to the earliest retained record only when no commit exists. `LATEST` is an explicit smoke-test/bootstrap option and can skip retained events, so do not enable it for the regular streaming group.

Useful diagnostics:

```bash
docker compose logs flink-jobmanager flink-taskmanager flink-submit
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group banking-streaming-v1 --describe
curl --fail http://127.0.0.1:18084/jobs/overview
```

If the job is not `RUNNING`, inspect JobManager and TaskManager logs before resubmitting. If checkpoints fail, verify that `flink-checkpoints` is mounted by both Flink services and writable by the `flink` user. Do not delete the volume as a recovery shortcut: it contains the local recovery state. The default checkpoint URI is intentionally local-demo-only and must be replaced with a verified durable object-store configuration before distributed deployment.

For a non-destructive recovery drill, run `docker compose restart flink-taskmanager`, wait for the job to return to `RUNNING`, and confirm a later checkpoint completes. Output consumers must use `isolation.level=read_committed` because the Kafka sinks commit with checkpoints.

## MinIO data lake runtime (Phase 6)

Build and start Kafka, MinIO, the idempotent bootstrap and the lake writer:

```bash
make up-lake
docker compose ps -a
curl --fail http://127.0.0.1:18087/actuator/health/readiness
```

The first MinIO image build compiles the pinned Community Server and `mc` releases from source and can take several minutes. Expected state: Kafka and MinIO are healthy, `kafka-init` and `minio-init` exit with code 0, and `lake-writer` remains running. The S3 API and console are local-only at ports `19000` and `19001`. Re-running `docker compose run --rm minio-init` is safe and reapplies private bucket, versioning, writer policy and lifecycle configuration without deleting objects.

Run the acceptance path after changing the writer, schemas, object layout, MinIO build/policy or Compose configuration:

```bash
make smoke-lake
```

The smoke test uses unique Kafka groups with latest offsets, publishes three synthetic records to each input topic, and expects one new Bronze object, one Silver object, one Gold partial and one manifest for each writer transaction. It validates raw Kafka coordinates and checksum, then starts a no-consumer verification process that reads the live Silver Parquet file. The test preserves named volumes and objects for inspection.

Useful diagnostics:

```bash
docker compose logs minio minio-init lake-writer
docker compose exec -T minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"'
docker compose exec -T minio mc find local/banking-lake
docker compose exec -T minio mc ilm rule ls local/banking-lake
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group banking-lake-bronze-v1 --describe
```

Treat `_manifests/{bronze|curated}/batch_id=...json` with `status=COMMITTED` as the publication marker. A data object without a manifest may be an interrupted attempt; do not delete or promote it manually while the writer is active. Restarting the writer is the first recovery step because deterministic batch IDs make the retry idempotent. If logs report an existing-key checksum mismatch, stop the writer and preserve the object, manifest and source offsets for investigation; do not overwrite the key.

Use `docker compose restart lake-writer` or `docker compose down` for non-destructive operations. Never use `docker compose down -v` during normal troubleshooting: `minio-data` contains the lake. Gold output in this phase is explicitly partial and must not be treated as the authoritative daily report until Phase 8 compaction/finalization is implemented.

## PostgreSQL warehouse runtime (Phase 7)

Start Kafka, the isolated warehouse database and the writer:

```bash
make up-warehouse
docker compose ps -a
curl --fail http://127.0.0.1:18089/actuator/health/readiness
```

Expected state: Kafka and `warehouse-postgres` are healthy, `kafka-init` exits 0, Flyway applies the warehouse migration, and `warehouse-writer` remains running. The database is local-only on `127.0.0.1:55433`; the management endpoint is local-only on `127.0.0.1:18089`.

Run `make smoke-warehouse` after changes to contracts, migration SQL, writer logic, topic configuration or Compose. It creates isolated latest-offset consumer groups, publishes 1,000 canonical validated transactions twice, and proves that `COUNT(*)` and `SUM(amount)` at the transaction grain remain unchanged. It also checks fraud/window facts, persisted reconciliation, malformed quarantine and successful DLQ-outbox publication. The script retains the Kafka and warehouse named volumes.

Useful diagnostics:

```bash
docker compose logs warehouse-postgres warehouse-writer
docker compose exec -T warehouse-postgres psql -U banking_warehouse -d banking_warehouse \
  -c 'SELECT * FROM warehouse_reconciliation_runs ORDER BY finished_at DESC LIMIT 10'
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group banking-warehouse-validated-v1 --describe
```

A database failure intentionally leaves Kafka offsets uncommitted; restore database availability and let the writer retry. A schema/business parse failure is not retried: inspect `warehouse_rejections`, the matching `warehouse_dlq_outbox` row and the DLQ event. Do not manually advance group offsets or mark outbox rows published. Replays are safe at both the source-coordinate and business-ID levels.

Use `docker compose restart warehouse-writer` or `docker compose down` for non-destructive operations. Never use `docker compose down -v` during ordinary troubleshooting because `postgres-warehouse-data` contains facts, offset markers, rejects and reconciliation evidence. Backup/restore is not yet production-certified.

## Daily batch and Airflow runtime (Phase 8)

Build the Java CLI and its dependencies, then run one business date manually:

```bash
make up-batch
make daily-batch DATE=2026-10-18
```

The date is interpreted in `BATCH_BUSINESS_TIMEZONE` (`Asia/Ho_Chi_Minh` by default) and maps to a half-open local-day interval. The job fails closed when the source partition is empty or a DQ/reconciliation check fails. Inspect recent attempts with `warehouse/queries/batch-runs.sql`; do not edit `batch_runs` or `agg_daily_transactions` manually to make a failed run appear successful.

Start the local scheduler:

```bash
make up-airflow
docker compose --profile airflow ps
docker compose --profile airflow run --rm --no-deps airflow-cli dags list
```

The API/UI is bound to `127.0.0.1:18090`. A fresh DAG starts paused and automatic catchup is disabled. After checking the source date, enable normal scheduling explicitly or run a controlled backfill with the UI/CLI `date` configuration. The schedule is `01:00 Asia/Ho_Chi_Minh`; `data_interval_start` supplies the previous business date. Airflow uses a separate `airflow-postgres-data` volume, while task logs use `airflow-logs`.

Expected task order is `check_source_partition → run_java_daily_batch → validate_aggregates → publish_report_metadata`. A successful run leaves both:

```text
gold/daily_transactions/report_date=YYYY-MM-DD/daily-transactions-v1.parquet
gold/daily_transactions/report_date=YYYY-MM-DD/_SUCCESS.json
```

If the Java task fails, inspect the Airflow task log and matching `batch_runs` row. `FAILED/DQ_FAILED` means the partition must be corrected or deliberately skipped; repeated infrastructure errors should retain the same source data and are safe to retry. If validation reports that facts changed after the latest batch, rerun `daily-transactions` before publishing metadata. A checksum mismatch requires preserving the current MinIO object/version and audit row for investigation; do not overwrite it manually.

Run `make smoke-batch` after batch, migration, DAG, timezone, MinIO or Compose changes. It uses synthetic future dates, proves a failed empty partition is audited, runs direct Java and Airflow reruns, and compares counts, sums, fingerprints and object hashes. It preserves all named volumes. Never use `docker compose down -v` during ordinary troubleshooting because that deletes warehouse facts, Gold objects, Airflow metadata and task logs.

## Planned procedures

Backup/restore, operator-driven outbox replay, orphan-object disposition, DLQ disposition, durable multi-host Flink recovery and production warehouse recovery remain planned. Automated outbox retry, the Phase 4 Compose path, local TaskManager/checkpoint recovery, Phase 6 deterministic lake replay and Phase 7 warehouse replay are verified with disposable synthetic data. Do not mark any other recovery procedure verified until it has been executed against disposable test data.
