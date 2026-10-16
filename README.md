# Java Real-Time Banking Data Platform

Local portfolio data platform for synthetic banking transactions, implemented incrementally with reproducible acceptance gates.

## Current status

Phases 0–2 are implemented. The transaction service exposes create/read APIs, validates requests, persists a transaction and its outbox event atomically, and provides global idempotency-key handling. Kafka publishing, Docker Compose infrastructure and full pipeline E2E remain planned work; they are not claimed to be implemented yet.

## Repository layout

- `common/`: shared contracts and DTOs.
- `services/`: transaction API, generator, outbox, streaming, lake, warehouse, batch and analytics modules.
- `docs/`: implementation plan and architecture/data/operations documents.
- `infrastructure/`, `warehouse/`, `orchestration/`, `scripts/`, `tests/`: reserved for the corresponding implementation phases.

## Prerequisites

- JDK 21
- Maven 3.8.7 or newer
- Docker Engine for PostgreSQL migration and transaction API integration tests
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

## Next implementation gate

The next gate is Phase 3: a deterministic transaction generator and an at-least-once outbox publisher. Kafka infrastructure itself remains Phase 4.

See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan and acceptance gates.
