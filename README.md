# Java Real-Time Banking Data Platform

Local portfolio data platform for synthetic banking transactions, implemented incrementally with reproducible acceptance gates.

## Current status

Phases 0–3 are implemented. In addition to the transactional API, the platform now has a deterministic configurable load generator and an at-least-once outbox publisher with claim leases, Kafka acknowledgements, retry/backoff and operational metrics. Phase 3 is integration-tested with disposable PostgreSQL 17.11 and Kafka 4.3.1 containers. Docker Compose infrastructure and full pipeline E2E remain Phase 4 work.

## Repository layout

- `common/`: shared contracts and DTOs.
- `services/`: transaction API, generator, outbox, streaming, lake, warehouse, batch and analytics modules.
- `docs/`: implementation plan and architecture/data/operations documents.
- `infrastructure/`, `warehouse/`, `orchestration/`, `scripts/`, `tests/`: reserved for the corresponding implementation phases.

## Prerequisites

- JDK 21
- Maven 3.8.7 or newer
- Docker Engine for PostgreSQL and Kafka integration tests
- Docker Compose v2 for later runtime phases

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

## Next implementation gate

The next gate is Phase 4: reproducible Docker Compose infrastructure, explicit Kafka topic bootstrap and an end-to-end local smoke test.

See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan and acceptance gates.
