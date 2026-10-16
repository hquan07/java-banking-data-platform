# Architecture

The target architecture and failure boundaries are described in `docs/IMPLEMENTATION.md`, section 3. The implemented vertical slice now covers the Phase 2 transaction API and the Phase 3 generator/outbox publisher.

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
3. Kafka-to-Flink validation/fraud/aggregation — planned.
4. Sink idempotency and replay — planned.
5. Batch reconciliation and report publication — planned.

## Phase 4 local runtime topology

```mermaid
flowchart LR
    HOST[Host 127.0.0.1] --> API[transaction-service]
    API --> PG[(PostgreSQL 17.11)]
    PUB[outbox-publisher] --> PG
    PUB --> K[Kafka 4.3.1 KRaft]
    INIT[kafka-init one-shot] --> K
    GEN[generator loadtest profile] --> API
```

Kafka has separate listeners for the Docker network (`kafka:9092`) and the host (`localhost:9092`). Auto topic creation is disabled; the one-shot init service applies the checked-in topic manifest before the publisher starts. PostgreSQL and Kafka state use named volumes and are preserved by a normal `docker compose down`.
