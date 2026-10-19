# Java Real-Time Banking Data Platform

Local portfolio data platform for synthetic banking transactions, implemented incrementally with reproducible acceptance gates.

## Current status

Phases 0–9 are implemented. The local stack includes separate PostgreSQL 17.11 OLTP and warehouse databases, Kafka 4.3.1 KRaft, the transaction API/outbox publisher, a Flink 2.1.3 streaming job, a MinIO-backed data lake, an idempotent warehouse writer, an Airflow 3.3.2 scheduled Java batch and a read-only analytics API. Phase 8 finalizes authoritative daily Gold Parquet and warehouse aggregates from warehouse facts; Phase 9 exposes bounded daily, fraud and account-summary queries over those warehouse results.

## Repository layout

- `common/`: shared contracts and DTOs.
- `services/`: transaction API, generator, outbox, streaming, lake, warehouse, batch and analytics modules.
- `docs/`: implementation plan and architecture/data/operations documents.
- `infrastructure/`, `warehouse/`, `orchestration/`, `scripts/`, `tests/`: runtime configuration, warehouse queries, Airflow DAGs and acceptance tooling.

## Prerequisites

- JDK 21
- Maven 3.8.7 or newer
- Docker Engine and Docker Compose v2 for the local Kafka/Flink/MinIO/PostgreSQL runtime

## Verify the current implementation

```bash
mvn -B -ntp validate
mvn -B -ntp test
```

If Maven cannot write to the local dependency cache, fix the cache permissions or configure a CI mirror before interpreting the result as a project failure.

## Transaction API

The API expects an existing active account in the OLTP database. Build the executable artifact and start it with the database environment variables shown in `.env.example`:

```bash
mvn -B -ntp -pl services/transaction-service -am package
java -jar services/transaction-service/target/transaction-service-0.1.0-SNAPSHOT.jar
```

Create a synthetic transaction:

```bash
curl -i -X POST http://localhost:8080/api/v1/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-txn-001' \
  -H 'X-Correlation-ID: demo-correlation-001' \
  -d '{"accountId":"ACC10001","transactionType":"TRANSFER","amount":"1500000.00","currency":"VND"}'
```

Actuator health and Prometheus metrics default to the local-only management listener on port `8081`.

## Transaction generator and outbox publisher

The generator only uses account IDs supplied in `GENERATOR_ACCOUNT_IDS`; seed those active accounts first. Build and run a deterministic 60-second load from the repository root:

```bash
mvn -B -ntp -pl services/transaction-generator -am package
GENERATOR_ENABLED=true \
GENERATOR_TPS=10 \
GENERATOR_DURATION=60s \
GENERATOR_SEED=1607 \
GENERATOR_ACCOUNT_IDS=ACC10001 \
java -jar services/transaction-generator/target/transaction-generator-0.1.0-SNAPSHOT.jar
```

Start the publisher after PostgreSQL and Kafka are reachable and the `banking.transactions.raw` topic exists:

```bash
mvn -B -ntp -pl services/outbox-publisher -am package
OUTBOX_PUBLISHER_ENABLED=true \
java -jar services/outbox-publisher/target/outbox-publisher-0.1.0-SNAPSHOT.jar
```

Publisher health and Prometheus metrics default to the local-only listener on port `8083`. The publisher retains failed rows and later retries them; it never deletes outbox events.

## Local Phase 4–9 stack

Start the Kafka/PostgreSQL/API/publisher stack and create all governed topics:

```bash
docker compose up -d --build postgres kafka kafka-init transaction-service outbox-publisher
docker compose ps -a
```

The Compose host endpoints are API `18080`, API management `18081`, publisher `18082`, publisher management `18083`, PostgreSQL `55432` and Kafka `9092`. Override the corresponding `*_HOST_PORT` values when needed.

`kafka-init` is a one-shot service and should show `Exited (0)`. Running it again is safe:

```bash
docker compose run --rm kafka-init
```

Run the complete Phase 4 acceptance test:

```bash
./scripts/smoke-test.sh
```

The smoke test leaves the stack and named volumes running for inspection. Stop services without deleting data using `docker compose down`. Do not add `-v` unless you intentionally want to delete local Kafka and PostgreSQL data.

Start Kafka plus the Phase 5 Flink cluster and submit the job idempotently:

```bash
make up-streaming
docker compose ps -a
```

The Flink dashboard is bound to `127.0.0.1:18084`. Checkpoints use the `flink-checkpoints` named volume and are suitable for this single-host demo only; deleting the volume deletes recovery state.

Run the Phase 5 acceptance test:

```bash
make smoke-streaming
```

The test recreates only the Flink containers, uses an isolated consumer group starting at the current raw-topic end offsets, then publishes valid, duplicate, invalid, high-value, velocity, watermark and late events. It checks all output topics, restarts the TaskManager and verifies that restored deduplication state still suppresses the duplicate. Kafka/PostgreSQL data and named volumes are not deleted.

Start Kafka, MinIO and the Phase 6 lake writer:

```bash
make up-lake
docker compose ps -a
```

MinIO binds its S3 API to `127.0.0.1:19000`, console to `127.0.0.1:19001`, and the lake-writer management endpoint to `127.0.0.1:18087`. The first build compiles the pinned MinIO Community Server release from source and can take several minutes; later builds use Docker cache. `minio-init` creates a private, versioned `banking-lake` bucket, a bucket-scoped writer identity and lifecycle policies for Bronze/Silver/Gold.

Run the Phase 6 acceptance test:

```bash
make smoke-lake
```

The test publishes three synthetic raw and validated records, checks exactly one data object per layer and one commit manifest per batch, verifies Kafka source coordinates/checksums in Bronze, and reads the live Silver Parquet object back through the Java reader. Data is retained in the `minio-data` named volume. These Gold objects are replay-safe partials; only the Phase 8 final object without `part-` in its name is authoritative.

## PostgreSQL warehouse (Phase 7)

Start Kafka, the separate warehouse database and the writer:

```bash
make up-warehouse
docker compose ps -a
curl --fail http://127.0.0.1:18089/actuator/health/readiness
```

The warehouse database is bound to `127.0.0.1:55433` and uses the independent `postgres-warehouse-data` named volume. The writer consumes validated transactions, fraud alerts and one-minute aggregates with separate consumer groups. Each batch commits business upserts, source offset markers, rejects and a reconciliation row in one database transaction before committing Kafka offsets. Malformed records are quarantined and staged in a transactional DLQ outbox.

Run the replay acceptance gate:

```bash
make smoke-warehouse
```

It publishes 1,000 transactions twice and verifies that fact count and monetary sum do not increase, then verifies fraud, window aggregate, persisted reconciliation, quarantine and DLQ publication. It preserves all named volumes.

## Java daily batch and Airflow (Phase 8)

Warehouse facts are the single source of truth for daily finalization. The Java CLI groups one business day by currency, applies data-quality and reconciliation checks, replaces that day's `agg_daily_transactions` partition, writes deterministic final Gold Parquet and records every success or failure in `batch_runs`.

```bash
make up-batch
make daily-batch DATE=2026-10-18
make up-airflow
docker compose --profile airflow ps
```

The Airflow UI is local-only at `http://127.0.0.1:18090`. The local Simple Auth Manager username defaults to `airflow`; Airflow generates its development password in the API-server password file/log. The DAG starts paused, runs at `01:00 Asia/Ho_Chi_Minh`, disables automatic catchup and accepts an explicit `date` only for controlled manual backfills. It executes `check_source_partition → run_java_daily_batch → validate_aggregates → publish_report_metadata` with exponential retry and `max_active_runs=1`.

Run the complete Phase 8 acceptance gate:

```bash
make smoke-batch
```

The smoke test proves a failed empty partition is audited, reruns the Java batch and Airflow DAG for the same synthetic day, verifies stable count/sum/fingerprint/object hash, and checks final Parquet plus `_SUCCESS.json` in MinIO. Named volumes are retained.

## Warehouse analytics API (Phase 9)

Start the isolated warehouse database, apply all warehouse migrations with the one-shot migration job and run the read-only API:

```bash
make up-analytics
docker compose --profile analytics ps -a
curl --fail http://127.0.0.1:18093/actuator/health/readiness
```

Query a finalized daily partition, a bounded fraud interval and one account summary:

```bash
curl --fail 'http://127.0.0.1:18092/api/v1/analytics/daily?date=2026-10-18'
curl --fail 'http://127.0.0.1:18092/api/v1/analytics/fraud?from=2026-10-01T00:00:00Z&to=2026-11-01T00:00:00Z&page=0&size=50'
curl --fail 'http://127.0.0.1:18092/api/v1/analytics/accounts/ACC10001/summary'
```

Fraud ranges use the half-open interval `[from,to)` and cannot exceed 31 days. Pages are zero-based, `page` is capped at 1,000 and `size` at 200. Monetary values are serialized as decimal strings. The daily response is private-cacheable for 30 seconds; all database transactions and pool connections are configured read-only with three-second query/transaction timeouts.

Run the complete Phase 9 acceptance gate:

```bash
make smoke-analytics
```

The smoke test applies migration V4, inserts an isolated synthetic warehouse partition, checks all three contracts and removes only its own fixtures. Warehouse named volumes and existing data are retained.

## Next implementation gate

The next gate is Phase 10: observability, security and operational hardening. The web dashboard remains intentionally deferred until the backend roadmap is complete.

See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan and acceptance gates.
