# Architecture

The target architecture and failure boundaries are described in `docs/IMPLEMENTATION.md`, section 3. The implemented vertical slice now covers the transaction API, outbox publication, Kafka infrastructure, Flink streaming and MinIO data lake through Phase 6.

## Transaction API and transactional outbox

```mermaid
sequenceDiagram
    participant Client
    participant API as Transaction API
    participant DB as PostgreSQL OLTP

    Client->>API: POST /transactions + Idempotency-Key
    API->>DB: BEGIN
    API->>DB: advisory lock for idempotency key
    API->>DB: find transaction by key
    alt same key and same fingerprint
        DB-->>API: existing transaction
        API->>DB: COMMIT
        API-->>Client: 200 existing transaction
    else same key and different fingerprint
        API->>DB: ROLLBACK
        API-->>Client: 409 idempotency conflict
    else new key
        API->>DB: validate active account and currency
        API->>DB: INSERT transactions
        API->>DB: INSERT outbox_events
        alt both writes succeed
            API->>DB: COMMIT
            API-->>Client: 201 Created
        else either write fails
            API->>DB: ROLLBACK both writes
            API-->>Client: safe 503/500 error
        end
    end
```

The advisory lock is transaction-scoped and keyed by a deterministic PostgreSQL hash of the globally scoped idempotency key. The unique constraint on `transactions.idempotency_key` remains the final database invariant.

## Outbox claim, publish and retry

```mermaid
sequenceDiagram
    participant Worker as Outbox Publisher
    participant DB as PostgreSQL OLTP
    participant Kafka

    Worker->>DB: claim due rows with SKIP LOCKED + lease
    DB-->>Worker: committed claimed batch
    Worker->>Kafka: send(event, key=account_id)
    alt Kafka acknowledges
        Kafka-->>Worker: ACK
        Worker->>DB: set published_at if claim still owned
    else send fails or times out
        Kafka--xWorker: error/timeout
        Worker->>DB: retain row, schedule backoff, release claim
    end
```

The database claim transaction ends before network I/O. A claim token prevents a stale worker from completing another worker's row, while an expiring lease makes abandoned work recoverable. Delivery is at least once: a crash after Kafka ACK and before `published_at` can produce a duplicate, so Phase 5 consumers must deduplicate by `event_id`.

Required diagrams:

1. API transaction plus transactional outbox — implemented and integration-tested.
2. Outbox-to-Kafka publish and retry — implemented and integration-tested.
3. Kafka-to-Flink validation/fraud/aggregation — implemented and tested with checkpoint recovery.
4. Lake sink idempotency and replay — implemented and tested; warehouse sink remains planned.
5. Batch reconciliation and report publication — planned.

## Phase 4–6 local runtime topology

```mermaid
flowchart LR
    HOST[Host 127.0.0.1] --> API[transaction-service]
    API --> PG[(PostgreSQL 17.11)]
    PUB[outbox-publisher] --> PG
    PUB --> K[Kafka 4.3.1 KRaft]
    INIT[kafka-init one-shot] --> K
    GEN[generator loadtest profile] --> API
    K --> JM[Flink JobManager 2.1.3]
    JM --> TM[Flink TaskManager]
    TM --> K
    TM --> CP[(checkpoint named volume)]
    K --> LW[lake-writer]
    LW --> M[(MinIO banking-lake)]
    MI[minio-init one-shot] --> M
```

Kafka has separate listeners for the Docker network (`kafka:9092`) and the host (`localhost:9092`). Auto topic creation is disabled; the one-shot init service applies the checked-in topic manifest before producers and Flink start. PostgreSQL, Kafka and Flink checkpoint state use named volumes and are preserved by a normal `docker compose down`.

The Phase 5 application has stable operator UIDs and this order: validate → deduplicate by `event_id` → assign bounded-out-of-order watermarks → route too-late events → validated/fraud/aggregate Kafka sinks. Idle Kafka partitions stop holding back the global watermark after the configurable `WATERMARK_IDLENESS` interval. The normal source mode resumes committed offsets with an earliest fallback; `LATEST` is available only for explicit isolated bootstrap/smoke runs. Validation failures retain only a payload hash and source coordinates. `HIGH_VALUE` applies to VND amounts at or above 100,000,000; `VELOCITY_60S` emits when an account exceeds 10 deduplicated events in a sliding 60-second window. One-minute aggregates remain separated by currency.

Kafka sinks use transactions tied to Flink checkpoints. This covers replay caused by a TaskManager failure inside the streaming job; it does not make later lake or warehouse writes exactly once. Those consumers must still upsert by deterministic IDs in Phases 6–7.

## Phase 6 lake commit flow

The lake writer reads the raw and validated topics with separate consumer groups. Raw batches become compressed JSONL in `bronze/transactions/ingest_date=...`; each row preserves the exact payload bytes plus topic, partition, offset, Kafka timestamp and SHA-256. Validated batches become typed Avro-backed Parquet in `silver/transactions/event_date=...` and daily aggregate partials in `gold/daily_transactions/report_date=...`.

The writer derives a deterministic batch ID from layer, source offsets and payload hashes. It writes and verifies all data objects first, writes `_manifests/{layer}/batch_id=...json` last as the commit marker, and commits Kafka offsets only after storage succeeds. Reprocessing the same records therefore reuses byte-identical objects; an existing key with a different checksum is treated as corruption/collision and fails closed. This protocol provides replay-safe local object publication, not a distributed exactly-once claim. Gold files are deliberately marked partial until Phase 8 performs deterministic daily compaction.
