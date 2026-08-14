package oms

import (
	"errors"
	"sync"
	"testing"
)

// newFundedVenue builds a registry with a real account store and log, which is
// the only configuration where reservations are active.
func newFundedVenue(t *testing.T) (*Registry, *Accounts, string) {
	t.Helper()
	dir := t.TempDir()
	accounts := NewAccounts()
	reg := NewRegistry(t.Context(), dir, accounts)
	t.Cleanup(func() { reg.Close() })
	return reg, accounts, dir
}

func TestAccounts_RestingBuyHoldsCashAgainstFurtherOrders(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.Deposit("alice", 5_000)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}

	// 10 at 500 = 5,000, exactly the balance.
	if _, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	if got := accounts.AvailableCash("alice"); got != 0 {
		t.Errorf("AvailableCash() = %d, want 0 — the resting order should hold all of it", got)
	}
	if got := accounts.Cash("alice"); got != 5_000 {
		t.Errorf("Cash() = %d, want 5000 — nothing has actually been spent yet", got)
	}

	// This is the bug reservations exist for: the first order has not spent
	// anything, so a check that only read the balance would let this through and
	// the account would owe 10,000 against 5,000.
	_, err = seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("second Submit() = %v, want ErrInsufficientFunds", err)
	}
}

func TestAccounts_RestingSellHoldsShares(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.SetPosition("alice", "NABIL", 100)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 100, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	if got := accounts.AvailablePosition("alice", "NABIL"); got != 0 {
		t.Errorf("AvailablePosition() = %d, want 0", got)
	}
	// Selling the same shares twice must fail: they are already promised.
	_, err = seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 1, Side: Sell})
	if !errors.Is(err, ErrInsufficientShares) {
		t.Errorf("second Submit() = %v, want ErrInsufficientShares", err)
	}
}

func TestAccounts_ConcurrentOrdersCannotOverdrawOneAccount(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)

	// Exactly enough for 10 orders of 500. Fire 100 concurrently across four
	// symbols, so they contend both with each other and across sequencers —
	// cash is shared between symbols even though books are not.
	const affordable = 10
	accounts.Deposit("alice", int64(affordable)*500)

	symbols := []string{"NABIL", "ADBL", "HBL", "NRIC"}
	for _, s := range symbols {
		if _, err := reg.Get(s); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, rejected := 0, 0
	for i := range 100 {
		wg.Go(func() {
			seq, err := reg.Get(symbols[i%len(symbols)])
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			_, err = seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 1, Side: Buy})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrInsufficientFunds):
				rejected++
			default:
				t.Errorf("Submit() unexpected error = %v", err)
			}
		})
	}
	wg.Wait()

	if accepted != affordable {
		t.Errorf("accepted %d orders, want exactly %d — the account was allowed to overdraw", accepted, affordable)
	}
	if rejected != 100-affordable {
		t.Errorf("rejected %d, want %d", rejected, 100-affordable)
	}
	if got := accounts.AvailableCash("alice"); got != 0 {
		t.Errorf("AvailableCash() = %d, want 0", got)
	}
}

func TestAccounts_CancelReleasesTheHold(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.Deposit("alice", 5_000)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if accounts.AvailableCash("alice") != 0 {
		t.Fatal("expected the order to hold the whole balance")
	}

	if _, err := seq.CancelFor(ctx, resp.OrderID, "alice"); err != nil {
		t.Fatalf("CancelFor() error = %v", err)
	}
	if got := accounts.AvailableCash("alice"); got != 5_000 {
		t.Errorf("AvailableCash() after cancel = %d, want 5000 — the hold leaked", got)
	}
	if got := accounts.HoldCount(); got != 0 {
		t.Errorf("HoldCount() = %d, want 0", got)
	}
}

func TestAccounts_FillConvertsHoldIntoActualMovement(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.Deposit("buyer", 10_000)
	accounts.SetPosition("seller", "NABIL", 100)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seq.Submit(ctx, Order{Placer: "seller", Type: Limit, Price: 500, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	if _, err := seq.Submit(ctx, Order{Placer: "buyer", Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}

	// Both orders are fully filled, so nothing should still be held.
	if got := accounts.HoldCount(); got != 0 {
		t.Errorf("HoldCount() = %d, want 0 after both sides filled", got)
	}
	if got := accounts.Cash("buyer"); got != 5_000 {
		t.Errorf("buyer cash = %d, want 5000", got)
	}
	if got := accounts.AvailableCash("buyer"); got != 5_000 {
		t.Errorf("buyer available = %d, want 5000 — a hold survived the fill", got)
	}
	if got := accounts.Position("seller", "NABIL"); got != 90 {
		t.Errorf("seller position = %d, want 90", got)
	}
	if got := accounts.AvailablePosition("seller", "NABIL"); got != 90 {
		t.Errorf("seller available position = %d, want 90", got)
	}
}

func TestAccounts_TakerFillingBelowItsLimitReleasesTheDifference(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.Deposit("buyer", 10_000)
	accounts.SetPosition("seller", "NABIL", 100)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	// Resting ask at 400; the buyer is willing to pay up to 500.
	if _, err := seq.Submit(ctx, Order{Placer: "seller", Type: Limit, Price: 400, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	if _, err := seq.Submit(ctx, Order{Placer: "buyer", Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}

	// The buyer reserved 5,000 but only spent 4,000. The 1,000 difference was
	// never owed and must come back, not stay stranded in a hold.
	if got := accounts.Cash("buyer"); got != 6_000 {
		t.Errorf("buyer cash = %d, want 6000 (paid the maker's 400, not its own 500)", got)
	}
	if got := accounts.AvailableCash("buyer"); got != 6_000 {
		t.Errorf("buyer available = %d, want 6000 — the price improvement stayed reserved", got)
	}
	if got := accounts.HoldCount(); got != 0 {
		t.Errorf("HoldCount() = %d, want 0", got)
	}
}

func TestAccounts_PartialFillKeepsHoldingOnlyTheRemainder(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.Deposit("buyer", 5_000)
	accounts.SetPosition("seller", "NABIL", 100)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seq.Submit(ctx, Order{Placer: "seller", Type: Limit, Price: 500, Quantity: 4, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	// Buy 10 at 500: 4 fill immediately, 6 rest.
	resp, err := seq.Submit(ctx, Order{Placer: "buyer", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if resp.RestingQuantity != 6 {
		t.Fatalf("RestingQuantity = %d, want 6", resp.RestingQuantity)
	}

	// 2,000 spent on the fill, 3,000 still held for the resting 6.
	if got := accounts.Cash("buyer"); got != 3_000 {
		t.Errorf("buyer cash = %d, want 3000", got)
	}
	if got := accounts.ReservedCash("buyer"); got != 3_000 {
		t.Errorf("reserved = %d, want 3000 (6 remaining at 500)", got)
	}
	if got := accounts.AvailableCash("buyer"); got != 0 {
		t.Errorf("available = %d, want 0", got)
	}

	if _, err := seq.CancelFor(ctx, resp.OrderID, "buyer"); err != nil {
		t.Fatal(err)
	}
	if got := accounts.AvailableCash("buyer"); got != 3_000 {
		t.Errorf("available after cancel = %d, want 3000", got)
	}
	if got := accounts.HoldCount(); got != 0 {
		t.Errorf("HoldCount() = %d, want 0", got)
	}
}

func TestAccounts_MarketOrderRemainderDoesNotStayHeld(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.SetPosition("seller", "NABIL", 100)
	accounts.Deposit("buyer", 10_000)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	// A market sell with nothing to cross: the remainder is dropped, never
	// rested, so its hold must not survive.
	if _, err := seq.Submit(ctx, Order{Placer: "seller", Type: Market, Quantity: 50, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	if got := accounts.HoldCount(); got != 0 {
		t.Errorf("HoldCount() = %d, want 0 — a dropped market remainder kept its hold", got)
	}
	if got := accounts.AvailablePosition("seller", "NABIL"); got != 100 {
		t.Errorf("AvailablePosition() = %d, want 100", got)
	}
}

func TestAccounts_RejectedOrderGetsNoLogPositionAndNoHold(t *testing.T) {
	ctx := t.Context()
	reg, accounts, dir := newFundedVenue(t)
	accounts.Deposit("alice", 1_000)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	// Accepted.
	if _, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 2, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	// Rejected: nothing left.
	if _, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 2, Side: Buy}); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("Submit() = %v, want ErrInsufficientFunds", err)
	}
	// Accepted again after a top-up, and it must get position 2, not 3: the
	// rejected order never entered the log.
	accounts.Deposit("alice", 1_000)
	resp, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 2, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 2 {
		t.Errorf("log position = %d, want 2 — a rejected order consumed one", resp.Seq)
	}
	if err := reg.Close(); err != nil {
		t.Fatal(err)
	}

	// And the log itself holds only the two accepted orders. A rejected order in
	// the log would be applied on replay, since replay does not re-run balance
	// checks — the recovered book would then differ from the live one.
	recs := collect(t, dir+"/NABIL")
	if len(recs) != 2 {
		t.Fatalf("logged %d records, want 2 (the rejected order must not be logged)", len(recs))
	}
}

func TestAccounts_HoldsAreRestoredAfterRecovery(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	accounts1 := NewAccounts()
	accounts1.Deposit("alice", 5_000)
	reg1 := NewRegistry(ctx, dir, accounts1)
	seq1, err := reg1.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seq1.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	if err := reg1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart with the same balance but a fresh account store, as a node
	// rebuilding from the journal would.
	accounts2 := NewAccounts()
	accounts2.Deposit("alice", 5_000)
	reg2 := NewRegistry(ctx, dir, accounts2)
	defer reg2.Close()
	seq2, err := reg2.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}

	// The recovered order is still live in the book, so its cash must still be
	// held. Without restoring holds, available balance would be overstated by
	// exactly what alice's own resting order has already committed — and she
	// could double-spend it.
	if got := accounts2.ReservedCash("alice"); got != 5_000 {
		t.Errorf("ReservedCash() after recovery = %d, want 5000", got)
	}
	if got := accounts2.AvailableCash("alice"); got != 0 {
		t.Errorf("AvailableCash() after recovery = %d, want 0", got)
	}
	if _, err := seq2.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy}); !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("Submit() after recovery = %v, want ErrInsufficientFunds", err)
	}
}

func TestAccounts_LoadBalancesRefusesWithLiveHolds(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.Deposit("alice", 5_000)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 1, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	// Overwriting balances underneath a live order would leave its reservation
	// describing an amount that no longer relates to anything.
	if err := accounts.LoadBalances(map[string]int64{"alice": 1}, nil); err == nil {
		t.Error("LoadBalances() with a live hold = nil error, want a refusal")
	}
}

func TestBook_SelfTradePreventionCancelsTheRestingSide(t *testing.T) {
	book := NewBook()
	if _, err := book.Submit(Order{SeqID: 1, Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}

	// Alice crossing her own ask must not print a trade against herself.
	res, err := book.SubmitOrder(Order{SeqID: 2, Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Trades) != 0 {
		t.Errorf("Trades = %+v, want none — a wash trade printed", res.Trades)
	}
	if len(res.SelfPrevented) != 1 || res.SelfPrevented[0] != 1 {
		t.Errorf("SelfPrevented = %v, want [1]", res.SelfPrevented)
	}
	// The incoming order rests, and the book must not be crossed.
	if res.RestingQuantity != 10 {
		t.Errorf("RestingQuantity = %d, want 10", res.RestingQuantity)
	}
	bid, hasBid := book.BestBid()
	if !hasBid || bid != 500 {
		t.Errorf("BestBid() = %d,%v, want 500,true", bid, hasBid)
	}
	if ask, hasAsk := book.BestAsk(); hasAsk {
		t.Errorf("BestAsk() = %d, want none — the resting ask should be cancelled", ask)
	}
}

func TestBook_SelfTradePreventionStillMatchesOtherAccounts(t *testing.T) {
	book := NewBook()
	// alice's ask sits in front of bob's at the same price, so the incoming
	// order meets alice first and must skip past her to reach bob.
	if _, err := book.Submit(Order{SeqID: 1, Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	if _, err := book.Submit(Order{SeqID: 2, Placer: "bob", Type: Limit, Price: 500, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}

	res, err := book.SubmitOrder(Order{SeqID: 3, Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("Trades = %+v, want one (against bob)", res.Trades)
	}
	if res.Trades[0].MakerAccID != "bob" {
		t.Errorf("maker = %q, want bob", res.Trades[0].MakerAccID)
	}
	if len(res.SelfPrevented) != 1 || res.SelfPrevented[0] != 1 {
		t.Errorf("SelfPrevented = %v, want [1] (alice's own ask)", res.SelfPrevented)
	}
}

func TestBook_EmptyPlacerNeverSelfMatches(t *testing.T) {
	book := NewBook()
	// The internal and test path: no account means no self-trade prevention, or
	// every order in the package's own tests would cancel itself.
	if _, err := book.Submit(Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	res, err := book.SubmitOrder(Order{SeqID: 2, Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Trades) != 1 {
		t.Errorf("Trades = %+v, want one", res.Trades)
	}
	if len(res.SelfPrevented) != 0 {
		t.Errorf("SelfPrevented = %v, want none", res.SelfPrevented)
	}
}

func TestAccounts_SelfPreventedOrderReleasesItsHold(t *testing.T) {
	ctx := t.Context()
	reg, accounts, _ := newFundedVenue(t)
	accounts.Deposit("alice", 10_000)
	accounts.SetPosition("alice", "NABIL", 100)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	sell, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Sell})
	if err != nil {
		t.Fatal(err)
	}
	if got := accounts.ReservedPosition("alice", "NABIL"); got != 10 {
		t.Fatalf("ReservedPosition() = %d, want 10", got)
	}

	buy, err := seq.Submit(ctx, Order{Placer: "alice", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(buy.SelfPrevented) != 1 || buy.SelfPrevented[0] != sell.OrderID {
		t.Fatalf("SelfPrevented = %v, want [%d]", buy.SelfPrevented, sell.OrderID)
	}

	// The cancelled sell's shares must be free again; the surviving buy still
	// holds its cash.
	if got := accounts.ReservedPosition("alice", "NABIL"); got != 0 {
		t.Errorf("ReservedPosition() = %d, want 0 — the cancelled order's hold leaked", got)
	}
	if got := accounts.AvailablePosition("alice", "NABIL"); got != 100 {
		t.Errorf("AvailablePosition() = %d, want 100", got)
	}
	if got := accounts.ReservedCash("alice"); got != 5_000 {
		t.Errorf("ReservedCash() = %d, want 5000 (the resting buy)", got)
	}
	if got := accounts.HoldCount(); got != 1 {
		t.Errorf("HoldCount() = %d, want 1 (only the resting buy)", got)
	}
}
