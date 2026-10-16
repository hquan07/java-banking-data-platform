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

## Planned procedures

Backup/restore, outbox replay, DLQ handling, Kafka outage, Flink recovery, warehouse recovery and data mismatch procedures remain planned. Do not mark a recovery procedure verified until it has been executed against disposable test data.
