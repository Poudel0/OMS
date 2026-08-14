// Package ledger settles executed trades into a Postgres double-entry journal.
//
// Settlement is idempotent by construction rather than by application logic:
// the journal has a unique index over a trade's legs, so re-presenting a trade
// that was already settled is a no-op at the storage layer. That matters
// because the write-ahead log can offer the same trade twice — replay
// re-derives trades from the log (ADR-003), and a crash between matching and
// settling means recovery will present one again.
package ledger

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Poudel0/OMS/internal/oms"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Asset names the two things a journal row can move, and Direction which way.
const (
	AssetCash     = "CASH"
	AssetPosition = "POSITION"

	DirectionDebit  = "DEBIT"
	DirectionCredit = "CREDIT"
)

// Ledger writes settlement rows to Postgres.
type Ledger struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and returns a Ledger. The caller owns Close.
func Open(ctx context.Context, dsn string) (*Ledger, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("ledger: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ledger: ping: %w", err)
	}
	return &Ledger{pool: pool}, nil
}

// Close releases the connection pool.
func (l *Ledger) Close() { l.pool.Close() }

// Migrate applies the schema. Every statement is guarded with IF NOT EXISTS, so
// running it on an up-to-date database is a no-op.
//
// ponytail: this is a one-file schema applied at startup, not a versioned
// migration tool. It buys correctness for a single table and costs nothing;
// the moment a column has to change shape on a database that already holds
// rows, replace it with a real migration runner rather than editing the file.
func (l *Ledger) Migrate(ctx context.Context) error {
	sql, err := migrations.ReadFile("migrations/0001_journal.sql")
	if err != nil {
		return fmt.Errorf("ledger: read migration: %w", err)
	}
	if _, err := l.pool.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("ledger: apply migration: %w", err)
	}
	return nil
}

// Settle posts the double-entry rows for every trade, all in one transaction.
//
// One transaction for the whole batch, not one per trade: a batch comes from a
// single Submit, and a partially settled batch would leave the journal
// describing an execution that half happened. Postgres gives all-or-nothing
// for free here, so there is no reason to accept less.
func (l *Ledger) Settle(ctx context.Context, trades []oms.Trade) error {
	if len(trades) == 0 {
		return nil
	}

	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ledger: begin: %w", err)
	}
	// Rollback is a no-op once the transaction has committed, so this is safe
	// on the success path and is what covers every early return below.
	defer func() { _ = tx.Rollback(ctx) }()

	batch := &pgx.Batch{}
	for _, t := range trades {
		value, err := tradeValue(t)
		if err != nil {
			return err
		}
		buyer, seller := t.Buyer(), t.Seller()
		if buyer == "" || seller == "" {
			return fmt.Errorf("ledger: trade %s/%d has no buyer or seller — TakerSide unset?", t.Symbol, t.SeqID)
		}

		// The four legs of one trade. Per asset the debits equal the credits,
		// which is the invariant TestLedger_DebitsEqualCredits protects.
		//
		//   buyer:  debit POSITION (gains shares), credit CASH (pays)
		//   seller: debit CASH (receives),         credit POSITION (delivers)
		//
		// All four are written even when buyer == seller (a self-trade). They
		// stay four distinct rows because direction is part of the unique key;
		// see the migration for the bug that taught us that.
		queueEntry(batch, t, buyer, AssetPosition, DirectionDebit, t.Quantity)
		queueEntry(batch, t, buyer, AssetCash, DirectionCredit, value)
		queueEntry(batch, t, seller, AssetCash, DirectionDebit, value)
		queueEntry(batch, t, seller, AssetPosition, DirectionCredit, t.Quantity)
	}

	results := tx.SendBatch(ctx, batch)
	for i := range batch.Len() {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return fmt.Errorf("ledger: settle entry %d: %w", i, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("ledger: close batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ledger: commit: %w", err)
	}
	return nil
}

// ON CONFLICT DO NOTHING is what makes re-settling a trade harmless. The
// alternative — SELECT to check, then INSERT — is a race: two settlers can both
// find nothing and both insert. Letting the unique index arbitrate has no such
// window.
const insertEntry = `
INSERT INTO journal_entries (symbol, trade_id, account_id, asset, direction, amount)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (symbol, trade_id, account_id, asset, direction) DO NOTHING`

func queueEntry(batch *pgx.Batch, t oms.Trade, account, asset, direction string, amount int64) {
	batch.Queue(insertEntry, t.Symbol, int64(t.SeqID), account, asset, direction, amount)
}

// tradeValue is the cash side of a trade, guarded against overflow. Both
// factors originate in client-supplied orders, so a wrap here would post a
// negative cash movement to the journal.
func tradeValue(t oms.Trade) (int64, error) {
	if t.Price < 0 || t.Quantity < 0 {
		return 0, fmt.Errorf("ledger: trade %s/%d has negative price or quantity", t.Symbol, t.SeqID)
	}
	value := int64(t.Price) * t.Quantity
	if t.Quantity != 0 && value/t.Quantity != int64(t.Price) {
		return 0, fmt.Errorf("ledger: trade %s/%d value overflows int64", t.Symbol, t.SeqID)
	}
	return value, nil
}

// Balance sums an account's cash from the journal: debits minus credits.
//
// This is the authoritative balance, as opposed to the in-memory one the
// pre-trade check reads (oms.Accounts). Rebuilding that in-memory view after a
// restart means calling this per account.
func (l *Ledger) Balance(ctx context.Context, account string) (int64, error) {
	var balance int64
	err := l.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount) FILTER (WHERE direction = 'DEBIT'), 0)
		     - COALESCE(SUM(amount) FILTER (WHERE direction = 'CREDIT'), 0)
		FROM journal_entries
		WHERE account_id = $1 AND asset = $2`, account, AssetCash).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("ledger: cash balance for %s: %w", account, err)
	}
	return balance, nil
}

// Position sums an account's holding in one symbol: debits minus credits.
func (l *Ledger) Position(ctx context.Context, account, symbol string) (int64, error) {
	var qty int64
	err := l.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount) FILTER (WHERE direction = 'DEBIT'), 0)
		     - COALESCE(SUM(amount) FILTER (WHERE direction = 'CREDIT'), 0)
		FROM journal_entries
		WHERE account_id = $1 AND asset = $2 AND symbol = $3`, account, AssetPosition, symbol).Scan(&qty)
	if err != nil {
		return 0, fmt.Errorf("ledger: position for %s in %s: %w", account, symbol, err)
	}
	return qty, nil
}

// EntryCount reports how many journal rows exist for one trade. It exists for
// tests and diagnostics: a settled trade has exactly four.
func (l *Ledger) EntryCount(ctx context.Context, symbol string, tradeID oms.SeqID) (int, error) {
	var n int
	err := l.pool.QueryRow(ctx, `
		SELECT count(*) FROM journal_entries WHERE symbol = $1 AND trade_id = $2`,
		symbol, int64(tradeID)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("ledger: count entries for %s/%d: %w", symbol, tradeID, err)
	}
	return n, nil
}

// Imbalance reports any (symbol, trade_id, asset) group whose debits do not
// equal its credits. A correct journal always returns zero rows; anything else
// means value was created or destroyed.
//
// This is the check that caught the self-trade bug (see the migration), so it
// is worth running against a real journal after any load test rather than only
// in unit tests, where accounts are conveniently distinct.
func (l *Ledger) Imbalance(ctx context.Context) (int, error) {
	var n int
	err := l.pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT symbol, trade_id, asset
			FROM journal_entries
			GROUP BY symbol, trade_id, asset
			HAVING COALESCE(SUM(amount) FILTER (WHERE direction = 'DEBIT'), 0)
			    <> COALESCE(SUM(amount) FILTER (WHERE direction = 'CREDIT'), 0)
		) AS unbalanced`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("ledger: imbalance check: %w", err)
	}
	return n, nil
}
