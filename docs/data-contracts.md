# Data Contracts

The shared module defines four versioned JSON Schema 2020-12 contracts and matching immutable Java records.

| Contract | Java type | Machine-readable schema |
|---|---|---|
| Canonical transaction event | `TransactionCreatedEvent` | `common/src/main/resources/contracts/transaction-created-v1.schema.json` |
| Dead-letter envelope | `DeadLetterEvent` | `common/src/main/resources/contracts/dead-letter-event-v1.schema.json` |
| Fraud alert | `FraudAlertEvent` | `common/src/main/resources/contracts/fraud-alert-v1.schema.json` |
| One-minute transaction aggregate | `TransactionAggregateEvent` | `common/src/main/resources/contracts/transaction-aggregate-v1.schema.json` |

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

## Phase 5 output rules

- Fraud alerts carry deterministic `alert_id`, a versioned demo rule code/version and the evaluated event-time window. High-value alerts reference one `event_id`; velocity alerts represent a window and therefore omit it.
- Aggregates use deterministic `aggregate_id` and `window_key`, are keyed by currency plus window start and never combine different currencies.
- Invalid and too-late records reuse `DeadLetterEvent`; infrastructure failures go to the DLQ topic. Neither path embeds the raw payload.

## Transaction API contract

`POST /api/v1/transactions` accepts camel-case JSON with these fields:

| Field | Rule |
|---|---|
| `accountId` | 1–64 characters; starts alphanumeric, followed by alphanumeric, `_` or `-` |
| `transactionType` | `DEPOSIT`, `WITHDRAWAL`, `PAYMENT` or `TRANSFER` |
| `amount` | JSON string, positive, exactly two fractional digits, fits `NUMERIC(19,2)` |
| `currency` | Three uppercase letters and must match the account currency |

Unknown request fields are rejected, and the `amount` field uses strict deserialization so a numeric JSON token is not accepted for its string-based contract.

The required `Idempotency-Key` header contains 1–128 characters from `A-Z`, `a-z`, `0-9`, `.`, `_`, `:`, or `-`. For the MVP it has global scope and no automatic expiry: the transaction row retains the key for the lifetime of that row. Clients must therefore generate a new key for every distinct business request.

- A first successful request returns `201 Created` with a `Location` header.
- A retry with the same key and canonical payload returns `200 OK`, the original `transactionId`, and `idempotentReplay=true`.
- Reusing the key for a different canonical payload returns `409 Conflict`.
- Concurrent requests with the same key are serialized in PostgreSQL before lookup/insert.

The response contains transaction fields plus `correlationId` and `idempotentReplay`; it never exposes the idempotency key, request fingerprint, or outbox payload. `X-Correlation-ID` may be supplied by the caller when it matches the safe 1–128 character format; otherwise the service generates one and always returns it in the response header.

Errors use one shape:

```json
{
  "code": "VALIDATION_ERROR",
  "message": "Request validation failed",
  "correlationId": "demo-correlation-001",
  "details": [
    {"field": "amount", "message": "must be a positive decimal string with exactly two fractional digits"}
  ]
}
```

The implemented status mapping is `400` invalid request, `404` missing account/transaction, `409` idempotency or account-state conflict, `500` unexpected internal error, and `503` database/dependency failure. Internal exception and SQL details are logged but not returned to the client.

## Outbox publication contract

- `outbox_events.payload` is the canonical `TransactionCreatedEvent` JSON and its metadata must match `event_id`, `aggregate_id`, `event_type` and `schema_version` before publishing.
- The Kafka topic defaults to `banking.transactions.raw`; its record key is `account_id` so transactions for one account stay in one partition.
- Producer delivery is at least once with idempotence enabled and `acks=all`. Idempotent production protects Kafka retries within a producer session, but it does not remove the crash-after-ACK duplicate window across database and Kafka.
- A row receives `published_at` only after Kafka acknowledges the send and only while the worker still owns its claim token.
- Failed rows are retained with attempts, a sanitized error and `next_attempt_at`; they are never silently deleted or converted into a different event contract.
- Downstream consumers must deduplicate by `event_id` and explicitly handle unsupported schema versions.
