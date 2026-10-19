# Data Model

The platform uses physically separate OLTP and analytical PostgreSQL databases. Phase 1 owns the OLTP schema; Phase 7 owns the warehouse star schema.

```mermaid
erDiagram
    CUSTOMERS ||--o{ ACCOUNTS : owns
    ACCOUNTS ||--o{ TRANSACTIONS : records
    TRANSACTIONS ||--o{ OUTBOX_EVENTS : emits
    TRANSACTIONS ||--o{ FRAUD_ALERTS : triggers

    CUSTOMERS {
        varchar customer_id PK
        varchar status
        timestamptz created_at
        timestamptz closed_at
    }
    ACCOUNTS {
        varchar account_id PK
        varchar customer_id FK
        char currency
        varchar status
    }
    TRANSACTIONS {
        uuid transaction_id PK
        varchar account_id FK
        numeric amount
        varchar idempotency_key UK
        char request_fingerprint
        timestamptz occurred_at
    }
    OUTBOX_EVENTS {
        uuid event_id PK
        uuid aggregate_id FK
        jsonb payload
        integer attempts
        timestamptz next_attempt_at
        uuid claim_token
    }
    FRAUD_ALERTS {
        uuid alert_id PK
        uuid transaction_id FK
        varchar rule_code UK
        varchar window_id UK
    }
```

`processing_runs` is an independent audit/reconciliation table holding source ranges, input/output counts, optional SHA-256 checksum, amount total, status and timing.

## Migration inventory

| Version | Purpose |
|---|---|
| `V1__create_core_banking_tables.sql` | Customers, accounts, transactions, transactional outbox and polling indexes |
| `V2__create_fraud_and_reconciliation_tables.sql` | Idempotent fraud alerts and processing run evidence |

The migrations live under `services/transaction-service/src/main/resources/db/migration` so Phase 2 can execute them through Flyway without copying schema definitions.

## Invariants

- Monetary values use `NUMERIC(19,2)` and must be greater than zero.
- ISO currency values must be three uppercase letters.
- `idempotency_key` is unique; `request_fingerprint` lets Phase 2 distinguish a safe retry from key reuse with a different payload.
- Outbox workers can only set `claim_token` together with `locked_until`; unpublished events are indexed by retry time.
- A fraud alert is unique by `(transaction_id, rule_code, window_id)` so replay cannot duplicate the same business alert.
- Completed or failed processing runs require `finished_at`; running records must not have one.

## Recovery policy

Migrations are forward-only. Never edit a migration that has run in a shared environment. Correct schema defects with a new migration. Before a production migration, take a database backup and record the Flyway state; recovery is restore-to-new-instance plus application rollback, not an ad-hoc reverse script.

The Phase 1 integration test starts an empty PostgreSQL 17.11 instance, applies both migrations and exercises unique and check constraints. When Docker is unavailable, the test is explicitly skipped rather than reported as passed; the resource-level migration test still runs.

## Warehouse star schema (Phase 7)

```mermaid
erDiagram
    DIM_CUSTOMER ||--o{ DIM_ACCOUNT : owns
    DIM_ACCOUNT ||--o{ FACT_TRANSACTIONS : classifies
    DIM_DATE ||--o{ FACT_TRANSACTIONS : dates
    DIM_ACCOUNT ||--o{ FACT_FRAUD_ALERTS : classifies
    DIM_DATE ||--o{ FACT_FRAUD_ALERTS : dates
    DIM_DATE ||--o{ AGG_TRANSACTION_WINDOWS : dates
    DIM_DATE ||--o{ AGG_DAILY_TRANSACTIONS : dates
```

`fact_transactions` has exactly one row per `transaction_id`; `event_id` is also unique. `fact_fraud_alerts` has one row per `alert_id` and links to a transaction when the referenced event has already arrived. `agg_transaction_windows` stores the streaming one-minute grain keyed by `aggregate_id`/`window_key`. `agg_daily_transactions` has the authoritative final `(date_key, currency)` grain and is replaced transactionally by the Phase 8 batch; every row points to the successful `batch_run_id` that produced it.

`processed_kafka_records` is the delivery ledger. `warehouse_rejections` retains malformed raw bytes, while `warehouse_dlq_outbox` guarantees that quarantine and DLQ intent commit atomically with the consumed offset. `warehouse_reconciliation_runs` persists source ranges, raw/valid/invalid/unique and inserted/updated/rejected counts, duplicate deliveries, source/accepted totals per currency, and amount deviation.

Because the v1 transaction event has `account_id` but no customer attributes, a missing account is inferred and linked to the explicit `UNKNOWN` customer. All contract-valid uppercase three-letter currencies are accepted and measured independently; cross-currency sums are forbidden without an explicit exchange-rate model. Once inferred, an account's currency is immutable in this writer; a conflicting event is quarantined and counted as rejected instead of silently rewriting the dimension.

## Batch audit (Phase 8)

`batch_runs` keeps one row per attempt with job/date/timezone, `RUNNING|SUCCEEDED|FAILED`, source/output counts, per-currency totals, DQ results, source fingerprint, Gold object key/checksum/size/schema, timing, publication time and sanitized failure details. Multiple successful attempts for one date are expected; idempotency is measured by equal business rows, fingerprint and object checksum, not by suppressing audit history. A final Gold partition is publishable only when its latest successful audit still reconciles with current warehouse facts.

## Analytics read model (Phase 9)

Phase 9 adds no duplicate analytics tables. The daily endpoint reads the authoritative `(date_key, currency)` rows from `agg_daily_transactions` and exposes their `batch_run_id` and business timezone. The account endpoint anchors on `dim_account`, then aggregates that account's transaction statuses, amount and fraud history without changing warehouse state. The fraud endpoint filters `fact_fraud_alerts.detected_at` with a half-open interval and a stable alert-ID tie-breaker.

Warehouse migration V4 adds `idx_fact_fraud_detected_alert` on `(detected_at DESC, alert_id DESC)` for time-ordered pagination and `idx_fact_fraud_account_detected` on `(account_key, detected_at DESC)` for account summaries. Existing fact primary/foreign keys and transaction/account indexes remain the source of integrity; API code does not own migrations and does not write repair rows.
