# Java Real-Time Banking Data Platform

Portfolio implementation plan and Maven skeleton for a local, synthetic banking transaction data platform.

## Current status

The repository contains the Phase 0 multi-module layout and the Phase 1 domain contracts and OLTP migrations. Runtime services, Docker Compose infrastructure, Kafka topics and end-to-end tests remain planned work; they are not claimed to be implemented yet.

## Repository layout

- `common/`: shared contracts and DTOs.
- `services/`: transaction API, generator, outbox, streaming, lake, warehouse, batch and analytics modules.
- `docs/`: implementation plan and architecture/data/operations documents.
- `infrastructure/`, `warehouse/`, `orchestration/`, `scripts/`, `tests/`: reserved for the corresponding implementation phases.

## Prerequisites

- JDK 21
- Maven 3.8.7 or newer
- Docker Engine for the Phase 1 PostgreSQL migration integration test
- Docker Compose v2 for later runtime phases

## Verify the current implementation

```bash
mvn -B -ntp validate
mvn -B -ntp test
```

If Maven cannot write to the local dependency cache, fix the cache permissions or configure a CI mirror before interpreting the result as a project failure.

## Next implementation gate

Phase 1 contract and migration gates are implemented. The next gate is the Phase 2 transaction API with atomic transaction/outbox persistence and idempotency behavior.

See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan and acceptance gates.
