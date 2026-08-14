package ledger

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Poudel0/OMS/internal/oms"
)

// testLedger connects to the database named by OMS_TEST_DATABASE_URL and hands
// back a Ledger scoped to this test.
//
// It skips rather than fails when no DSN is set. CI has no Postgres, and a test
// that cannot run is not the same as a test that failed — but a *silently*
// skipped test is how a suite rots, so the skip message says exactly what to
// set to make it run.
//
// Isolation between tests comes from a unique symbol per test rather than a
// fresh database: the journal's identity is (symbol, trade_id, ...), so a
// per-test symbol makes rows from different tests unable to collide.
func testLedger(t *testing.T) (*Ledger, string) {
	t.Helper()
	dsn := os.Getenv("OMS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OMS_TEST_DATABASE_URL to run ledger tests (e.g. postgres:///dhukuti_oms_test)")
	}

	ctx := t.Context()
	l, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(l.Close)
	if err := l.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	// Symbols are uppercase A-Z0-9 per the registry's rules; keep test symbols
	// inside that alphabet so they exercise realistic values.
	symbol := fmt.Sprintf("T%X", len(t.Name())*7919+int(t.Name()[len(t.Name())-1]))
	if len(symbol) > 12 {
		symbol = symbol[:12]
	}
	if _, err := l.pool.Exec(ctx, `DELETE FROM journal_entries WHERE symbol = $1`, symbol); err != nil {
		t.Fatalf("clear prior rows for %s: %v", symbol, err)
	}
	t.Cleanup(func() {
		//nolint:errcheck // best-effort cleanup; a leftover row only affects this symbol
		l.pool.Exec(context.Background(), `DELETE FROM journal_entries WHERE symbol = $1`, symbol)
	})
	return l, symbol
}

func trade(symbol string, id oms.SeqID, price oms.Price, qty int64, takerSide oms.OrderSide) oms.Trade {
	t := oms.Trade{
		SeqID: id, Symbol: symbol, Price: price, Quantity: qty,
		MakerSeqID: 1, TakerSeqID: 2, TakerSide: takerSide,
	}
	if takerSide == oms.Buy {
		t.TakerAccID, t.MakerAccID = "buyer", "seller"
	} else {
		t.TakerAccID, t.MakerAccID = "seller", "buyer"
	}
	return t
}

func TestLedger_SettleWritesFourLegsPerTrade(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	tr := trade(symbol, 1, 500, 10, oms.Buy)
	if err := l.Settle(ctx, []oms.Trade{tr}); err != nil {
		t.Fatalf("Settle() error = %v", err)
	}

	// Two accounts x two assets: the buyer's cash and position, the seller's.
	n, err := l.EntryCount(ctx, symbol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("EntryCount() = %d, want 4", n)
	}

	// 10 shares at 500 = 5000 moving from buyer to seller.
	if got, err := l.Balance(ctx, "buyer"); err != nil {
		t.Fatal(err)
	} else if got != -5000 {
		t.Errorf("buyer cash = %d, want -5000", got)
	}
	if got, err := l.Balance(ctx, "seller"); err != nil {
		t.Fatal(err)
	} else if got != 5000 {
		t.Errorf("seller cash = %d, want 5000", got)
	}
	if got, err := l.Position(ctx, "buyer", symbol); err != nil {
		t.Fatal(err)
	} else if got != 10 {
		t.Errorf("buyer position = %d, want 10", got)
	}
	if got, err := l.Position(ctx, "seller", symbol); err != nil {
		t.Fatal(err)
	} else if got != -10 {
		t.Errorf("seller position = %d, want -10", got)
	}
}

func TestLedger_SettleIsIdempotent(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	tr := trade(symbol, 1, 500, 10, oms.Buy)

	// Settling the same trade repeatedly is not a hypothetical: replay
	// re-derives trades from the write-ahead log, and a crash between matching
	// and settling means recovery presents this trade again.
	for i := range 5 {
		if err := l.Settle(ctx, []oms.Trade{tr}); err != nil {
			t.Fatalf("Settle() attempt %d error = %v", i+1, err)
		}
	}

	n, err := l.EntryCount(ctx, symbol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("EntryCount() after 5 settlements = %d, want 4 — the trade was double-posted", n)
	}
	if got, err := l.Balance(ctx, "buyer"); err != nil {
		t.Fatal(err)
	} else if got != -5000 {
		t.Errorf("buyer cash = %d, want -5000 — balance drifted across re-settlement", got)
	}
}

func TestLedger_DebitsEqualCredits(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	// A mix of taker sides, quantities, and prices, including a re-settlement.
	trades := []oms.Trade{
		trade(symbol, 1, 500, 10, oms.Buy),
		trade(symbol, 2, 501, 3, oms.Sell),
		trade(symbol, 3, 499, 100, oms.Buy),
		trade(symbol, 4, 502, 1, oms.Sell),
	}
	if err := l.Settle(ctx, trades); err != nil {
		t.Fatalf("Settle() error = %v", err)
	}
	if err := l.Settle(ctx, trades); err != nil {
		t.Fatalf("re-Settle() error = %v", err)
	}

	// The invariant that makes this a ledger rather than a log of balances: for
	// every trade and asset, what left one account arrived at another.
	imbalanced, err := l.Imbalance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if imbalanced != 0 {
		t.Errorf("Imbalance() = %d groups where debits != credits, want 0", imbalanced)
	}

	// Cash is conserved across the pair of accounts, and so are shares.
	buyerCash, _ := l.Balance(ctx, "buyer")
	sellerCash, _ := l.Balance(ctx, "seller")
	if buyerCash+sellerCash != 0 {
		t.Errorf("cash created or destroyed: buyer %d + seller %d = %d, want 0",
			buyerCash, sellerCash, buyerCash+sellerCash)
	}
	buyerPos, _ := l.Position(ctx, "buyer", symbol)
	sellerPos, _ := l.Position(ctx, "seller", symbol)
	if buyerPos+sellerPos != 0 {
		t.Errorf("shares created or destroyed: buyer %d + seller %d = %d, want 0",
			buyerPos, sellerPos, buyerPos+sellerPos)
	}
}

func TestLedger_TakerSideDecidesWhoPays(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	// Same two accounts, opposite taker sides. "buyer" must end up paying in
	// both cases — the label follows the economics, not the maker/taker role.
	if err := l.Settle(ctx, []oms.Trade{
		trade(symbol, 1, 500, 10, oms.Buy),  // buyer is the taker
		trade(symbol, 2, 500, 10, oms.Sell), // buyer is the maker
	}); err != nil {
		t.Fatalf("Settle() error = %v", err)
	}

	if got, err := l.Balance(ctx, "buyer"); err != nil {
		t.Fatal(err)
	} else if got != -10_000 {
		t.Errorf("buyer cash = %d, want -10000 (paid for both trades)", got)
	}
	if got, err := l.Position(ctx, "buyer", symbol); err != nil {
		t.Fatal(err)
	} else if got != 20 {
		t.Errorf("buyer position = %d, want 20 (received shares in both trades)", got)
	}
}

func TestLedger_SelfTradeStillWritesAllFourLegs(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	// A self-trade — one account on both sides — is what a load test produced
	// within seconds, because a handful of accounts trading randomly will
	// eventually cross themselves. Nothing in the venue prevents it today.
	//
	// It used to corrupt the journal: with a key of
	// (symbol, trade_id, account_id, asset) the four legs collapsed to two,
	// the counter-legs hit ON CONFLICT DO NOTHING, and the account was left
	// having received shares and paid nothing. 6,133 of 47,935 trades were
	// affected in a 30-second run.
	self := oms.Trade{
		SeqID: 1, Symbol: symbol, Price: 500, Quantity: 10,
		MakerSeqID: 1, TakerSeqID: 2, TakerSide: oms.Buy,
		TakerAccID: "solo", MakerAccID: "solo",
	}
	if err := l.Settle(ctx, []oms.Trade{self}); err != nil {
		t.Fatalf("Settle() error = %v", err)
	}

	n, err := l.EntryCount(ctx, symbol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("EntryCount() for a self-trade = %d, want 4", n)
	}

	// The legs must cancel exactly: the account paid itself and delivered to
	// itself, so it is where it started.
	if imbalanced, err := l.Imbalance(ctx); err != nil {
		t.Fatal(err)
	} else if imbalanced != 0 {
		t.Errorf("Imbalance() = %d, want 0 — a self-trade unbalanced the journal", imbalanced)
	}
	if got, err := l.Balance(ctx, "solo"); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Errorf("self-trader cash = %d, want 0 — value was created or destroyed", got)
	}
	if got, err := l.Position(ctx, "solo", symbol); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Errorf("self-trader position = %d, want 0", got)
	}

	// And it must still be idempotent, which is the property the direction
	// column had to be added without breaking.
	if err := l.Settle(ctx, []oms.Trade{self}); err != nil {
		t.Fatalf("re-Settle() error = %v", err)
	}
	if n, err := l.EntryCount(ctx, symbol, 1); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Errorf("EntryCount() after re-settling a self-trade = %d, want 4", n)
	}
}

func TestLedger_RejectsATradeWithNoSide(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	// TakerSide unset means Buyer() and Seller() both resolve to the maker,
	// which would post a trade with itself. Better to refuse than to write a
	// meaningless journal row.
	bad := oms.Trade{SeqID: 99, Symbol: symbol, Price: 500, Quantity: 10}
	if err := l.Settle(ctx, []oms.Trade{bad}); err == nil {
		t.Fatal("Settle() with an unset TakerSide = nil error, want a refusal")
	}

	n, err := l.EntryCount(ctx, symbol, 99)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("EntryCount() = %d, want 0 — a refused trade wrote rows anyway", n)
	}
}

func TestLedger_BatchIsAllOrNothing(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	// The second trade is invalid. Since the batch shares one transaction,
	// neither trade may end up in the journal — a half-settled batch would
	// describe an execution that only partly happened.
	good := trade(symbol, 1, 500, 10, oms.Buy)
	bad := oms.Trade{SeqID: 2, Symbol: symbol, Price: 500, Quantity: 10} // no TakerSide
	if err := l.Settle(ctx, []oms.Trade{good, bad}); err == nil {
		t.Fatal("Settle() = nil error, want a refusal")
	}

	for _, id := range []oms.SeqID{1, 2} {
		n, err := l.EntryCount(ctx, symbol, id)
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("EntryCount(trade %d) = %d, want 0 — the batch partially committed", id, n)
		}
	}
}

func TestLedger_SettleNothingIsNotAnError(t *testing.T) {
	ctx := t.Context()
	l, _ := testLedger(t)
	if err := l.Settle(ctx, nil); err != nil {
		t.Errorf("Settle(nil) = %v, want nil — an order that crossed nothing is normal", err)
	}
}

func TestLedger_RejectsOverflowingTradeValue(t *testing.T) {
	ctx := t.Context()
	l, symbol := testLedger(t)

	// Both factors come from client-supplied orders. A wrap here would post a
	// negative cash movement, i.e. pay the buyer to take the shares.
	huge := oms.Trade{
		SeqID: 1, Symbol: symbol, Price: oms.Price(1) << 62, Quantity: 1 << 3,
		TakerSide: oms.Buy, TakerAccID: "buyer", MakerAccID: "seller",
	}
	if err := l.Settle(ctx, []oms.Trade{huge}); err == nil {
		t.Fatal("Settle() with an overflowing value = nil error, want a refusal")
	}
}
