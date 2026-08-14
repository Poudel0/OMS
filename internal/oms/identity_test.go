package oms

import (
	"errors"
	"testing"
)

func TestBook_AssignsMonotonicTradeIDs(t *testing.T) {
	book := NewBook()
	if _, err := book.Submit(Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	if _, err := book.Submit(Order{SeqID: 2, Type: Limit, Price: 501, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	trades, err := book.Submit(Order{SeqID: 3, Type: Market, Quantity: 15, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 2 {
		t.Fatalf("trades = %d, want 2", len(trades))
	}
	// types.go documents (Symbol, SeqID) as identifying a trade, and settlement
	// is idempotent on it — a zero here would collapse every trade into one
	// ledger row.
	for i, tr := range trades {
		if tr.SeqID != SeqID(i+1) {
			t.Errorf("trade %d SeqID = %d, want %d", i, tr.SeqID, i+1)
		}
	}

	// Numbering continues across calls rather than restarting per Submit.
	if _, err := book.Submit(Order{SeqID: 4, Type: Limit, Price: 502, Quantity: 5, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	more, err := book.Submit(Order{SeqID: 5, Type: Market, Quantity: 5, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(more) != 1 || more[0].SeqID != 3 {
		t.Errorf("next trade SeqID = %+v, want a single trade with SeqID 3", more)
	}
}

func TestWAL_ReplayReproducesTradeIDsExactly(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}

	// This is the property settlement idempotency depends on. Trades are not
	// logged — they are re-derived by re-running the matching — so if replay
	// numbered them differently, a recovered node would post the same
	// executions to the ledger under new trade IDs and double-count them.
	live := NewBook()
	recs := []Record{
		{Seq: 1, Kind: RecordSubmit, TS: walTS, Order: Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Sell}},
		{Seq: 2, Kind: RecordSubmit, TS: walTS, Order: Order{SeqID: 2, Type: Limit, Price: 501, Quantity: 10, Side: Sell}},
		{Seq: 3, Kind: RecordSubmit, TS: walTS, Order: Order{SeqID: 3, Type: Market, Quantity: 15, Side: Buy}},
		{Seq: 4, Kind: RecordSubmit, TS: walTS, Order: Order{SeqID: 4, Type: Limit, Price: 499, Quantity: 8, Side: Buy}},
		{Seq: 5, Kind: RecordSubmit, TS: walTS, Order: Order{SeqID: 5, Type: Market, Quantity: 8, Side: Sell}},
	}
	var liveTradeIDs []SeqID
	for _, rec := range recs {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
		if rec.Kind == RecordSubmit {
			trades, _ := live.Submit(rec.Order)
			for _, tr := range trades {
				liveTradeIDs = append(liveTradeIDs, tr.SeqID)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if len(liveTradeIDs) == 0 {
		t.Fatal("no trades occurred — test proves nothing")
	}

	replayed := NewBook()
	if _, err := Recover(dir, replayed, 0); err != nil {
		t.Fatal(err)
	}
	if got, want := replayed.nextTradeID, live.nextTradeID; got != want {
		t.Errorf("replayed trade counter = %d, want %d", got, want)
	}

	// The next trade after recovery must continue the original numbering, not
	// collide with an ID the ledger has already settled.
	if _, err := replayed.Submit(Order{SeqID: 6, Type: Limit, Price: 600, Quantity: 1, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	next, err := replayed.Submit(Order{SeqID: 7, Type: Market, Quantity: 1, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 {
		t.Fatalf("trades = %d, want 1", len(next))
	}
	if want := liveTradeIDs[len(liveTradeIDs)-1] + 1; next[0].SeqID != want {
		t.Errorf("first trade ID after recovery = %d, want %d", next[0].SeqID, want)
	}
}

func TestSequencer_AssignsOrderIDWhenCallerLeavesItZero(t *testing.T) {
	ctx := t.Context()
	seq, _, _ := newWALSequencer(t)

	for i := 1; i <= 3; i++ {
		resp, err := seq.Submit(ctx, Order{Placer: "acc-1", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
		if err != nil {
			t.Fatal(err)
		}
		if resp.OrderID != SeqID(i) {
			t.Errorf("assigned order ID = %d, want %d", resp.OrderID, i)
		}
	}
	if err := seq.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSequencer_HonoursAnExplicitOrderID(t *testing.T) {
	ctx := t.Context()
	seq, _, _ := newWALSequencer(t)

	// The benchmarks and the book's own tests supply IDs directly; that path
	// must keep working unchanged.
	resp, err := seq.Submit(ctx, Order{SeqID: 42, Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OrderID != 42 {
		t.Errorf("OrderID = %d, want the caller's 42", resp.OrderID)
	}
}

func TestSequencer_AssignedOrderIDsResumeAboveRecoveredOrders(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	w1, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq1 := NewSequencerWithWAL(ctx, NewBook(), w1)
	var lastID SeqID
	for range 5 {
		resp, err := seq1.Submit(ctx, Order{Placer: "acc-1", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
		if err != nil {
			t.Fatal(err)
		}
		lastID = resp.OrderID
	}
	if err := seq1.Close(); err != nil {
		t.Fatal(err)
	}

	book2 := NewBook()
	if _, err := Recover(dir, book2, 0); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq2 := NewSequencerWithWAL(ctx, book2, w2)
	defer seq2.Close()

	// Reissuing a recovered order's ID would make the new order uncancellable
	// and let a cancel hit the wrong order.
	resp, err := seq2.Submit(ctx, Order{Placer: "acc-1", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OrderID != lastID+1 {
		t.Errorf("first assigned order ID after restart = %d, want %d", resp.OrderID, lastID+1)
	}
}

func TestSequencer_CancelForRefusesAnotherAccountsOrder(t *testing.T) {
	ctx := t.Context()
	seq, book, _ := newWALSequencer(t)

	resp, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}

	// Order IDs are sequential integers, so without this check guessing one is
	// enough to cancel a stranger's order.
	if _, err := seq.CancelFor(ctx, resp.OrderID, "bob"); !errors.Is(err, ErrNotOrderOwner) {
		t.Fatalf("CancelFor() by the wrong account = %v, want ErrNotOrderOwner", err)
	}
	if book.RestingCount() != 1 {
		t.Errorf("RestingCount() = %d, want 1 — the refused cancel removed the order anyway", book.RestingCount())
	}

	if _, err := seq.CancelFor(ctx, resp.OrderID, "alice"); err != nil {
		t.Fatalf("CancelFor() by the owner = %v, want nil", err)
	}
	if book.RestingCount() != 0 {
		t.Errorf("RestingCount() = %d, want 0", book.RestingCount())
	}
}

func TestSequencer_RefusedCancelReplaysAsRefused(t *testing.T) {
	ctx := t.Context()
	seq, live, dir := newWALSequencer(t)

	resp, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seq.CancelFor(ctx, resp.OrderID, "bob"); !errors.Is(err, ErrNotOrderOwner) {
		t.Fatalf("CancelFor() = %v, want ErrNotOrderOwner", err)
	}
	liveState := live.Snapshot()
	if err := seq.Close(); err != nil {
		t.Fatal(err)
	}

	// The cancel is in the log even though it was refused, so replay has to
	// re-evaluate the ownership constraint and refuse it again. If CancelBy
	// were not logged, replay would allow it and lose alice's order.
	replayed := NewBook()
	if _, err := Recover(dir, replayed, 0); err != nil {
		t.Fatal(err)
	}
	if got := replayed.Snapshot(); got != liveState {
		t.Errorf("replayed state = %s, want %s", got, liveState)
	}
	if replayed.RestingCount() != 1 {
		t.Errorf("RestingCount() after replay = %d, want 1 — a refused cancel took effect on replay", replayed.RestingCount())
	}
}

func TestBook_CancelWithoutOwnerStillWorks(t *testing.T) {
	book := NewBook()
	if _, err := book.Submit(Order{SeqID: 1, Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	// An empty owner means "no constraint" — the internal/replay path.
	if err := book.CancelOwned(1, ""); err != nil {
		t.Errorf("CancelOwned(1, \"\") = %v, want nil", err)
	}
}
