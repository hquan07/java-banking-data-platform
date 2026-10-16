# Data Contracts

Phase 1 defines two versioned JSON Schema 2020-12 contracts and matching immutable Java records in the `banking-common` module.

| Contract | Java type | Machine-readable schema |
|---|---|---|
| Canonical transaction event | `TransactionCreatedEvent` | `common/src/main/resources/contracts/transaction-created-v1.schema.json` |
| Dead-letter envelope | `DeadLetterEvent` | `common/src/main/resources/contracts/dead-letter-event-v1.schema.json` |

## Canonical transaction rules

- `event_type` is exactly `transaction.created`; `schema_version` is exactly `1`.
- `event_id` is the delivery/deduplication identity and `transaction_id` is the business identity.
- `account_id` is the Kafka partition key when ordering by account is required.
- Money is serialized as a decimal string with exactly two fractional digits. Producers and consumers must not use binary floating-point types.
- `occurred_at` is event time; `produced_at` is ingestion/publication time. Both are UTC ISO-8601 timestamps.
- Unknown fields are rejected for v1. Sensitive customer, card or authentication fields are not part of the event.
- `correlation_id` is optional and may be used to join API, outbox and Kafka traces.

## Dead-letter rules

- The envelope records source topic/partition/offset, failure class/code, first failure time and retry count.
- `failure_class` separates non-retryable business errors, producer/schema defects and retryable infrastructure failures.
- The raw failed payload is not copied into the envelope. `payload_hash` is a lowercase SHA-256 digest used for correlation without duplicating sensitive content.
- `replay_status` is one of `PENDING`, `REPLAYED` or `DISCARDED`; replay tooling must update it audibly.
- `failure_message` must be sanitized before construction and is capped at 1024 characters.

## Compatibility policy

1. Existing required fields cannot change meaning or type within v1.
2. A new optional field must have a documented default/absence behavior and contract tests proving an old payload is still accepted.
3. Adding a required field, changing money representation, renaming a field or removing an enum value requires a new schema version and migration window.
4. Consumers must explicitly reject unsupported versions and route them to the DLQ contract.
5. Schema and Java record changes must be committed together with positive and negative tests.

The tests serialize the Java records, validate them against the schemas with format assertions enabled, verify round trips and prove that numeric money and unknown fields are rejected.
