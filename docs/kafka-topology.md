# Kafka Topic Governance

The executable source of truth is `infrastructure/kafka/topics.tsv`. Local Kafka runs with three partitions, replication factor 1, `cleanup.policy=delete` and explicit retention. Auto topic creation is disabled.

| Topic | Owner | Contract/version | Key | Retention | PII classification | Replay policy |
|---|---|---|---|---:|---|---|
| `banking.transactions.raw` | transaction platform | `TransactionCreatedEvent` v1 | `account_id` | 7 days | Synthetic identifiers only | Replay by a new consumer group; consumers deduplicate `event_id` |
| `banking.transactions.validated` | streaming platform | Canonical validated transaction v1, reserved for Phase 5 | `account_id` | 7 days | Synthetic identifiers only | Replay from raw through the versioned validation job |
| `banking.transactions.invalid` | streaming platform | Invalid business record envelope v1, reserved for Phase 5 | `event_id` | 14 days | Synthetic identifiers and sanitized failure metadata | Fix producer/mapping, then replay through controlled tooling |
| `banking.transactions.fraud-alerts` | fraud demo | Fraud alert v1, reserved for Phase 5 | `account_id` | 30 days | Synthetic identifiers only | Recompute from validated events and versioned rules |
| `banking.transactions.aggregates` | analytics platform | Window aggregate v1, reserved for Phase 5 | window key | 30 days | Aggregated synthetic data | Recompute from validated events for the selected event-time range |
| `banking.transactions.dlq` | platform operations | `DeadLetterEvent` v1 | `event_id` | 30 days | Payload hash and sanitized metadata; no raw payload | Explicit operator review and auditable replay only |

Partition counts may only increase. Reducing partitions or changing key semantics requires a new topic and migration because it changes ordering. Retention changes require a reviewed manifest change and a successful smoke test. Replication factor 1 is local-only; production topology must use multiple brokers, appropriate replication and `min.insync.replicas`.

The Phase 4 smoke test covers the raw topic only. The remaining topics are provisioned now so Phase 5 cannot depend on broker auto-creation; their routing semantics are not considered implemented until the streaming job tests pass.
