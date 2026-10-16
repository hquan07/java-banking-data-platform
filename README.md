<<<<<<< HEAD
# Java Real-Time Banking Data Platform

Portfolio implementation plan and Maven skeleton for a local, synthetic banking transaction data platform.

## Current status

The repository currently contains the Phase 0 multi-module layout and design documentation. Runtime services, Docker Compose infrastructure, database migrations, Kafka topics and end-to-end tests are planned work; they are not claimed to be implemented yet.

## Repository layout

- `common/`: shared contracts and DTOs.
- `services/`: transaction API, generator, outbox, streaming, lake, warehouse, batch and analytics modules.
- `docs/`: implementation plan and architecture/data/operations documents.
- `infrastructure/`, `warehouse/`, `orchestration/`, `scripts/`, `tests/`: reserved for the corresponding implementation phases.

## Prerequisites

- JDK 21
- Maven 3.8.7 or newer
- Docker Engine and Docker Compose v2 for later runtime phases

## Build the current skeleton

```bash
mvn -B -ntp validate
mvn -B -ntp test
```

If Maven cannot write to the local dependency cache, fix the cache permissions or configure a CI mirror before interpreting the result as a project failure.

## Next implementation gate

Complete `docs/compatibility-matrix.md`, then implement the Phase 1 migrations and contract tests before adding runtime containers.

See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan and acceptance gates.
=======
# java-banking-data-platform
>>>>>>> origin/main
