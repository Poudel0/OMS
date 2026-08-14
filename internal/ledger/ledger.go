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
	"sort"
	"strings"

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

// Migrate applies every embedded migration that has not been applied yet, in
// filename order, and records each one.
//
// Each migration runs in its own transaction alongside the insert into
// schema_migrations, so a migration and the record of it either both land or
// neither does — a migration that succeeded but was not recorded would be
// re-run, and a recorded one that failed would be skipped forever.
//
// The advisory lock means two nodes starting together do not both try to
// migrate. It is released when the transaction ends.
//
// This replaces an earlier version that just re-ran one IF NOT EXISTS file
// every boot. That was fine while the schema had never been deployed — and it
// is precisely why fixing the idempotency-key bug in ADR-004 could be a rewrite
// of 0001 rather than a migration. It stops being fine the moment a column has
// to change shape on a database that holds rows, which is now.
func (l *Ledger) Migrate(ctx context.Context) error {
	names, err := migrationNames()
	if err != nil {
		return err
	}

	if _, err := l.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name       TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("ledger: create schema_migrations: %w", err)
	}

	for _, name := range names {
		if err := l.applyMigration(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (l *Ledger) applyMigration(ctx context.Context, name string) error {
	body, err := migrations.ReadFile("migrations/" + name)
	if err != nil {
		return fmt.Errorf("ledger: read migration %s: %w", name, err)
	}

	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ledger: begin migration %s: %w", name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialise migrating nodes against each other. The number is arbitrary but
	// must be stable; it identifies this application's migration lock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("ledger: lock for migration %s: %w", name, err)
	}

	// Re-check inside the lock: another node may have applied it while we waited.
	var applied bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name).Scan(&applied); err != nil {
		return fmt.Errorf("ledger: check migration %s: %w", name, err)
	}
	if applied {
		return nil
	}

	if _, err := tx.Exec(ctx, string(body)); err != nil {
		return fmt.Errorf("ledger: apply migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
		return fmt.Errorf("ledger: record migration %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ledger: commit migration %s: %w", name, err)
	}
	return nil
}

// migrationLockID identifies this application's advisory migration lock. The
// value is arbitrary; all that matters is that it never changes, and that no
// other application sharing the database picks the same one.
const migrationLockID int64 = 8734121

// migrationNames lists the embedded migrations in filename order, which is why
// they are numbered.
func migrationNames() ([]string, error) {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("ledger: list migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
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

// Balances rebuilds every account's cash and per-symbol position by summing the
// whole journal.
//
// This is what makes the in-memory pre-trade store (oms.Accounts) survivable
// across a restart: it is lost on every restart while the journal is not, so a
// recovering node reads authoritative balances from here and then has the
// registry restore holds for whatever orders replay put back in the books.
//
// It scans the entire journal, which is fine at startup and would not be as a
// steady-state query. The bound on it is the same one recovery has generally:
// nothing prunes the journal yet, so both grow together, and snapshots are the
// answer to both.
func (l *Ledger) Balances(ctx context.Context) (cash map[string]int64, positions map[string]map[string]int64, err error) {
	rows, err := l.pool.Query(ctx, `
		SELECT account_id, asset, symbol,
		       COALESCE(SUM(amount) FILTER (WHERE direction = 'DEBIT'), 0)
		     - COALESCE(SUM(amount) FILTER (WHERE direction = 'CREDIT'), 0)
		FROM journal_entries
		GROUP BY account_id, asset, symbol`)
	if err != nil {
		return nil, nil, fmt.Errorf("ledger: read balances: %w", err)
	}
	defer rows.Close()

	cash = make(map[string]int64)
	positions = make(map[string]map[string]int64)
	for rows.Next() {
		var account, asset, symbol string
		var net int64
		if err := rows.Scan(&account, &asset, &symbol, &net); err != nil {
			return nil, nil, fmt.Errorf("ledger: scan balance row: %w", err)
		}
		switch asset {
		case AssetCash:
			// Cash is not per symbol, so the same account accumulates across
			// every symbol it has traded.
			cash[account] += net
		case AssetPosition:
			if positions[account] == nil {
				positions[account] = make(map[string]int64)
			}
			positions[account][symbol] += net
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("ledger: iterate balances: %w", err)
	}
	return cash, positions, nil
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
