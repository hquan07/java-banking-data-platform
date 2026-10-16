CREATE TABLE customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    CONSTRAINT chk_customers_status
        CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    CONSTRAINT chk_customers_closed_at
        CHECK (closed_at IS NULL OR status = 'CLOSED')
);

CREATE TABLE accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL REFERENCES customers (customer_id),
    currency CHAR(3) NOT NULL,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    CONSTRAINT chk_accounts_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_accounts_status
        CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    CONSTRAINT chk_accounts_closed_at
        CHECK (closed_at IS NULL OR status = 'CLOSED')
);

CREATE INDEX idx_accounts_customer_id ON accounts (customer_id);

CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL REFERENCES accounts (account_id),
    transaction_type VARCHAR(32) NOT NULL,
    amount NUMERIC(19, 2) NOT NULL,
    currency CHAR(3) NOT NULL,
    status VARCHAR(24) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(128) NOT NULL,
    request_fingerprint CHAR(64) NOT NULL,
    correlation_id VARCHAR(128),
    CONSTRAINT uq_transactions_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_transactions_type
        CHECK (transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'PAYMENT', 'TRANSFER')),
    CONSTRAINT chk_transactions_amount
        CHECK (amount > 0),
    CONSTRAINT chk_transactions_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_transactions_status
        CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED', 'REVERSED')),
    CONSTRAINT chk_transactions_request_fingerprint
        CHECK (request_fingerprint ~ '^[a-f0-9]{64}$')
);

CREATE INDEX idx_transactions_account_occurred_at
    ON transactions (account_id, occurred_at DESC);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES transactions (transaction_id),
    event_type VARCHAR(100) NOT NULL,
    schema_version INTEGER NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    locked_until TIMESTAMPTZ,
    claim_token UUID,
    CONSTRAINT chk_outbox_schema_version CHECK (schema_version > 0),
    CONSTRAINT chk_outbox_attempts CHECK (attempts >= 0),
    CONSTRAINT chk_outbox_claim
        CHECK (
            (claim_token IS NULL AND locked_until IS NULL)
            OR (claim_token IS NOT NULL AND locked_until IS NOT NULL)
        )
);

CREATE INDEX idx_outbox_ready_to_publish
    ON outbox_events (next_attempt_at, created_at)
    WHERE published_at IS NULL;

CREATE INDEX idx_outbox_expired_claims
    ON outbox_events (locked_until)
    WHERE published_at IS NULL AND claim_token IS NOT NULL;
