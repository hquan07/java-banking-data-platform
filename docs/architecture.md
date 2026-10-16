# Architecture

The target architecture and failure boundaries are described in `docs/IMPLEMENTATION.md`, section 3. Add sequence diagrams here as each vertical slice becomes implemented and verified.

Required diagrams:

1. API transaction plus transactional outbox.
2. Outbox-to-Kafka publish and retry.
3. Kafka-to-Flink validation/fraud/aggregation.
4. Sink idempotency and replay.
5. Batch reconciliation and report publication.
