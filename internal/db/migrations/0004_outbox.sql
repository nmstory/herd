-- Transactional outbox. Lease changes and journal entries insert their event
-- here in the same transaction, so an event exists if and only if the change
-- committed. A relay publishes rows to Kafka and stamps published_at.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    key          text NOT NULL,
    payload      bytea NOT NULL,
    -- clock_timestamp(), not now(): the time of the write, close to commit,
    -- rather than the start of the transaction. Used to measure relay lag.
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);

CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
