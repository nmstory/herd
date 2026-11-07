-- Device time is priced per minute by the device's model label. The row
-- with model 'default' covers any model without its own price.
CREATE TABLE prices (
    model              text PRIMARY KEY,
    credits_per_minute bigint NOT NULL CHECK (credits_per_minute > 0)
);

-- Snapshotted when the lease is taken, so a later price change doesn't
-- alter what an existing lease costs.
ALTER TABLE leases
    ADD COLUMN credits_per_minute bigint NOT NULL DEFAULT 0,
    ADD COLUMN held               bigint NOT NULL DEFAULT 0;
