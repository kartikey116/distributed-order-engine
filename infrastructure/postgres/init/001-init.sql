CREATE TABLE orders (
    id UUID PRIMARY KEY,

    user_id UUID NOT NULL,

    amount NUMERIC(12, 2) NOT NULL,

    status VARCHAR(50) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,

    aggregate_type VARCHAR(100) NOT NULL,

    aggregate_id UUID NOT NULL,

    event_type VARCHAR(100) NOT NULL,

    payload JSONB NOT NULL,

    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    published_at TIMESTAMPTZ
);


CREATE INDEX idx_outbox_events_created_at
ON outbox_events(created_at);


CREATE INDEX idx_outbox_events_status
ON outbox_events(status);