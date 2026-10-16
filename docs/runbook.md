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

The management listener defaults to `127.0.0.1:8081`. Do not expose it publicly. A later Compose phase may bind it inside a private monitoring network.

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
4. Verify publisher liveness/readiness on `127.0.0.1:8083`; scrape `/actuator/prometheus` only from the private monitoring network.
5. Monitor unpublished backlog, publish failures/retries and publish latency. A growing backlog with failed readiness normally indicates Kafka or database connectivity trouble.

During a Kafka outage, leave the publisher running or restart it after Kafka recovers. Rows remain in `outbox_events`; backoff schedules the next attempt and expired leases make interrupted claims recoverable. Do not manually set `published_at`, delete rows or clear errors to force progress. Duplicate delivery can occur after ACK but before the database update and must be handled downstream by `event_id`.

## Planned procedures

Backup/restore, operator-driven outbox replay, DLQ handling, Flink recovery, warehouse recovery and data mismatch procedures remain planned. Automated outbox retry through a temporary Kafka outage is verified with disposable test data; the Phase 4 Compose operational procedure is not. Do not mark any other recovery procedure verified until it has been executed against disposable test data.
