# Kafka Topic Governance

The executable source of truth is `infrastructure/kafka/topics.tsv`. Local Kafka runs with three partitions, replication factor 1, `cleanup.policy=delete` and explicit retention. Auto topic creation is disabled.

| Topic | Owner | Contract/version | Key | Retention | PII classification | Replay policy |
|---|---|---|---|---:|---|---|
| `banking.transactions.raw` | transaction platform | `TransactionCreatedEvent` v1 | `account_id` | 7 days | Synthetic identifiers only | Replay by a new consumer group; consumers deduplicate `event_id` |
| `banking.transactions.validated` | streaming platform | Canonical validated transaction v1 | `account_id` | 7 days | Synthetic identifiers only | Replay from raw through the versioned validation job |
| `banking.transactions.invalid` | streaming platform | `DeadLetterEvent` v1 for schema/business/late failures | `event_id` or payload hash | 14 days | Synthetic identifiers and sanitized failure metadata | Fix producer/mapping, then replay through controlled tooling |
| `banking.transactions.fraud-alerts` | fraud demo | `FraudAlertEvent` v1 | `account_id` | 30 days | Synthetic identifiers only | Recompute from validated events and versioned rules |
| `banking.transactions.aggregates` | analytics platform | `TransactionAggregateEvent` v1 | `currency:window_start` | 30 days | Aggregated synthetic data | Recompute from validated events for the selected event-time range |
| `banking.transactions.dlq` | platform operations | `DeadLetterEvent` v1 | `event_id` | 30 days | Payload hash and sanitized metadata; no raw payload | Explicit operator review and auditable replay only |

Partition counts may only increase. Reducing partitions or changing key semantics requires a new topic and migration because it changes ordering. Retention changes require a reviewed manifest change and a successful smoke test. Replication factor 1 is local-only; production topology must use multiple brokers, appropriate replication and `min.insync.replicas`.

The Phase 4 smoke test covers the raw topic. The Phase 5 smoke test covers validated, invalid, fraud-alert and aggregate outputs with `read_committed` consumers; infrastructure failures use the DLQ topic. Aggregate and velocity windows can emit deterministic-key corrections during allowed lateness, so downstream storage must upsert by aggregate/alert ID rather than count Kafka records as unique business results.
