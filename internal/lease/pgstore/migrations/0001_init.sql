CREATE TABLE devices (
    id             text PRIMARY KEY,
    labels         jsonb NOT NULL DEFAULT '{}',
    last_heartbeat timestamptz
);

-- Selector matching is labels @> '{"zone":"us-east"}', which this index serves.
CREATE INDEX devices_labels_gin ON devices USING gin (labels);

CREATE TABLE leases (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id       text NOT NULL REFERENCES devices (id),
    holder          text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash    bytea NOT NULL,
    acquired_at     timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    released_at     timestamptz,

    -- A retried request finds its original lease instead of taking another.
    UNIQUE (holder, idempotency_key)
);

-- The exclusivity guarantee. Row locks in Acquire only reduce contention;
-- this index is what makes a double lease impossible. Expired leases are
-- closed (released_at set) before their device is leased again.
CREATE UNIQUE INDEX leases_one_active_per_device ON leases (device_id) WHERE released_at IS NULL;
