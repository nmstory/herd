-- Double-entry ledger. Every movement of credits is a journal entry whose
-- postings sum to zero, so credits are never created or destroyed, only
-- moved between accounts. Accounts are plain names:
--
--   wallet:<holder>  a holder's spendable credits
--   hold:<lease id>  credits reserved for one lease until it settles
--   external         where top-ups come from (goes negative as credits are bought)
--   revenue          credits charged for device time
CREATE TABLE journal_entries (
    id              bigserial PRIMARY KEY,
    kind            text NOT NULL CHECK (kind IN ('topup', 'hold', 'capture', 'expire')),
    holder          text NOT NULL,
    lease_id        uuid REFERENCES leases (id),
    -- Makes each logical movement happen once, however many times it's tried:
    -- topup:<holder>:<key>, hold:<lease>, settle:<lease>.
    idempotency_key text NOT NULL UNIQUE,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE postings (
    entry_id bigint NOT NULL REFERENCES journal_entries (id),
    account  text NOT NULL,
    amount   bigint NOT NULL CHECK (amount <> 0),
    PRIMARY KEY (entry_id, account)
);

CREATE INDEX postings_account ON postings (account, entry_id);

-- Running balances for the accounts that must never go negative (wallets and
-- holds). The CHECK is what makes an overdraft impossible: two concurrent
-- spends serialise on the row lock and the second one fails the check.
-- external and revenue are deliberately not kept here: every entry touches
-- them, so a balance row would make them a lock every transaction queues on.
-- Their balance is SUM(postings).
CREATE TABLE account_balances (
    account text PRIMARY KEY,
    balance bigint NOT NULL CONSTRAINT account_balances_non_negative CHECK (balance >= 0)
);

-- Postings for an entry must sum to zero. Deferred to commit so the postings
-- of one entry can be inserted one at a time.
CREATE FUNCTION check_entry_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    total bigint;
BEGIN
    SELECT sum(amount) INTO total FROM postings WHERE entry_id = NEW.entry_id;
    IF total <> 0 THEN
        RAISE EXCEPTION 'journal entry % is unbalanced by %', NEW.entry_id, total
            USING ERRCODE = 'check_violation', CONSTRAINT = 'postings_balanced';
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER postings_balanced
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_entry_balanced();

-- The ledger is append-only: mistakes are fixed with new entries, never edits.
CREATE FUNCTION forbid_ledger_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

CREATE TRIGGER journal_entries_append_only
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_change();

CREATE TRIGGER postings_append_only
    BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_change();
