-- Double-entry journal for settled trades.
--
-- One trade produces four rows: the buyer receives shares and pays cash, the
-- seller delivers shares and receives cash. Every row is one leg of one
-- movement, and for any (trade, asset) the debits must equal the credits --
-- which is what makes this a ledger rather than a log of balances.

CREATE TABLE IF NOT EXISTS journal_entries (
    id          BIGSERIAL PRIMARY KEY,

    -- (symbol, trade_id) is the venue's trade identity: trade ids are assigned
    -- per symbol, not globally. See ADR-003/004.
    symbol      TEXT   NOT NULL,
    trade_id    BIGINT NOT NULL,

    account_id  TEXT   NOT NULL,

    -- What moved. CASH rows are denominated in price ticks; POSITION rows in
    -- shares of `symbol`.
    asset       TEXT   NOT NULL CHECK (asset IN ('CASH', 'POSITION')),

    -- Which way it moved, as an explicit column rather than a sign on amount
    -- or a pair of mostly-zero debit/credit columns.
    --
    -- It is explicit because it has to be part of the idempotency key below,
    -- and because two columns that encode the same fact can disagree. A single
    -- signed amount would work arithmetically but loses the double-entry
    -- vocabulary an auditor reads.
    direction   TEXT   NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),

    -- Always positive: the direction carries the sign. A zero would be a
    -- meaningless row, so the constraint rejects it rather than storing it.
    amount      BIGINT NOT NULL CHECK (amount > 0),

    settled_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency, by construction rather than by application logic.
--
-- The write-ahead log can re-present a trade that was already settled: replay
-- re-derives trades from the log (ADR-003), and a crash between matching and
-- settling means recovery will offer the same trade again. Rather than having
-- settlement ask "have I seen this?" -- which is a race between the asking and
-- the writing -- the constraint makes a duplicate physically impossible, and
-- INSERT ... ON CONFLICT DO NOTHING makes re-presenting one a no-op.
--
-- `direction` is in the key, and leaving it out was a real bug found under
-- load. One trade writes four rows: two accounts x two assets. But when an
-- account trades with ITSELF -- a self-trade, which nothing here prevents --
-- both sides are the same account_id, so a key of
-- (symbol, trade_id, account_id, asset) collapses the four legs to two. The
-- two counter-legs then hit ON CONFLICT DO NOTHING and vanish silently,
-- leaving a journal where an account received shares and paid nothing. Adding
-- direction keeps all four legs distinct while still making a genuine
-- re-settlement a no-op.
CREATE UNIQUE INDEX IF NOT EXISTS journal_entries_leg_uniq
    ON journal_entries (symbol, trade_id, account_id, asset, direction);

-- Balances are read per account; positions additionally per symbol.
CREATE INDEX IF NOT EXISTS journal_entries_account_idx
    ON journal_entries (account_id, asset, symbol);
