package oms

import (
	"encoding/json"
	"testing"
)

func restingQty(t *testing.T, b *Book, side OrderSide, price Price) []int64 {
	t.Helper()
	lvl, ok := b.levels[price]
	if !ok {
		return nil
	}
	if lvl.side != side {
		t.Fatalf("level at %d belongs to side %v, not %v", price, lvl.side, side)
	}
	var qtys []int64
	for e := lvl.orders.Front(); e != nil; e = e.Next() {
		qtys = append(qtys, e.Value.(Order).Quantity)
	}
	return qtys
}

func sumFills(trades []Trade) int64 {
	var total int64
	for _, tr := range trades {
		total += tr.Quantity
	}
	return total
}

func TestSubmit_UnknownSideOrType(t *testing.T) {
	b := NewBook()
	_, err := b.Submit(Order{SeqID: 1, Side: UnknownSide, Type: Limit, Price: 100, Quantity: 10})
	if err == nil {
		t.Fatal("Submit() with UnknownSide = nil error, want error")
	}
	_, err = b.Submit(Order{SeqID: 2, Side: Buy, Type: UnknownType, Price: 100, Quantity: 10})
	if err == nil {
		t.Fatal("Submit() with UnknownType = nil error, want error")
	}
}

func TestSubmit_NoMatchRests(t *testing.T) {
	b := NewBook()
	trades, err := b.Submit(Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 50})
	if err != nil {
		t.Fatalf("Submit() error = %v, want nil", err)
	}
	if len(trades) != 0 {
		t.Fatalf("Submit() trades = %v, want none (empty book)", trades)
	}
	if got, ok := b.BestBid(); !ok || got != 100 {
		t.Fatalf("BestBid() = (%d, %v), want (100, true)", got, ok)
	}
	if _, ok := b.index[1]; !ok {
		t.Error("index missing entry for resting order SeqID 1")
	}
}

func TestSubmit_NoCrossPriceTooFar(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 110, Quantity: 50})
	trades := mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 50})
	if len(trades) != 0 {
		t.Fatalf("Submit() trades = %v, want none (100 < 110, no cross)", trades)
	}
	if got, _ := b.BestBid(); got != 100 {
		t.Errorf("BestBid() = %d, want 100", got)
	}
	if got, _ := b.BestAsk(); got != 110 {
		t.Errorf("BestAsk() = %d, want 110", got)
	}
}

func TestSubmit_FullFillExactQuantity(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 50})
	trades := mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 50})

	if len(trades) != 1 {
		t.Fatalf("len(trades) = %d, want 1", len(trades))
	}
	tr := trades[0]
	if tr.Price != 100 || tr.Quantity != 50 || tr.MakerSeqID != 1 || tr.TakerSeqID != 2 {
		t.Errorf("trade = %+v, want price 100, qty 50, maker 1, taker 2", tr)
	}
	if _, ok := b.BestAsk(); ok {
		t.Error("BestAsk() ok = true, want the level fully consumed and removed")
	}
	if _, ok := b.index[1]; ok {
		t.Error("index still has fully-filled maker SeqID 1")
	}
	if _, ok := b.index[2]; ok {
		t.Error("index has fully-filled taker SeqID 2, taker never rests")
	}
}

func TestSubmit_PartialFillOfMaker(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 200})
	trades := mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 50})

	if len(trades) != 1 || trades[0].Quantity != 50 {
		t.Fatalf("trades = %+v, want one trade of qty 50", trades)
	}
	if got, ok := b.BestAsk(); !ok || got != 100 {
		t.Fatalf("BestAsk() = (%d, %v), want (100, true) — maker still resting", got, ok)
	}
	qtys := restingQty(t, b, Sell, 100)
	if len(qtys) != 1 || qtys[0] != 150 {
		t.Errorf("resting qtys at 100 = %v, want [150]", qtys)
	}
	ref, ok := b.index[1]
	if !ok {
		t.Fatal("index missing maker SeqID 1 after partial fill")
	}
	if ref.element.Value.(Order).Quantity != 150 {
		t.Errorf("indexed element qty = %d, want 150", ref.element.Value.(Order).Quantity)
	}
}

func TestSubmit_PartialFillOfTaker_RemainderRests(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 30})
	trades := mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 50})

	if len(trades) != 1 || trades[0].Quantity != 30 {
		t.Fatalf("trades = %+v, want one trade of qty 30", trades)
	}
	if _, ok := b.BestAsk(); ok {
		t.Error("BestAsk() ok = true, want ask side fully drained")
	}
	qtys := restingQty(t, b, Buy, 100)
	if len(qtys) != 1 || qtys[0] != 20 {
		t.Errorf("resting qtys at bid 100 = %v, want [20] (50-30 leftover)", qtys)
	}
	if _, ok := b.index[2]; !ok {
		t.Error("index missing taker SeqID 2's resting remainder")
	}
}

func TestSubmit_MultiLevelCross(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 30})
	mustSubmit(t, b, Order{SeqID: 2, Side: Sell, Type: Limit, Price: 105, Quantity: 30})

	trades := mustSubmit(t, b, Order{SeqID: 3, Side: Buy, Type: Limit, Price: 105, Quantity: 50})

	if len(trades) != 2 {
		t.Fatalf("len(trades) = %d, want 2 (crossed two levels)", len(trades))
	}
	if trades[0].Price != 100 || trades[0].Quantity != 30 {
		t.Errorf("trades[0] = %+v, want price 100 qty 30 (best price consumed first)", trades[0])
	}
	if trades[1].Price != 105 || trades[1].Quantity != 20 {
		t.Errorf("trades[1] = %+v, want price 105 qty 20 (remainder from second level)", trades[1])
	}
	if got, ok := b.BestAsk(); !ok || got != 105 {
		t.Fatalf("BestAsk() = (%d, %v), want (105, true) — 10 shares left resting", got, ok)
	}
	qtys := restingQty(t, b, Sell, 105)
	if len(qtys) != 1 || qtys[0] != 10 {
		t.Errorf("resting qtys at ask 105 = %v, want [10]", qtys)
	}
}

func TestSubmit_FIFOWithinLevel(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 20})
	mustSubmit(t, b, Order{SeqID: 2, Side: Sell, Type: Limit, Price: 100, Quantity: 20})

	trades := mustSubmit(t, b, Order{SeqID: 3, Side: Buy, Type: Limit, Price: 100, Quantity: 20})

	if len(trades) != 1 {
		t.Fatalf("len(trades) = %d, want 1", len(trades))
	}
	if trades[0].MakerSeqID != 1 {
		t.Errorf("MakerSeqID = %d, want 1 (first-in, first matched)", trades[0].MakerSeqID)
	}
	if _, ok := b.index[2]; !ok {
		t.Error("SeqID 2 should still be resting, untouched")
	}
}

func TestSubmit_MarketOrder_SweepsAndDropsRemainder(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 60})

	trades := mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Market, Quantity: 100})

	if len(trades) != 1 || trades[0].Quantity != 60 {
		t.Fatalf("trades = %+v, want one trade of qty 60 (only liquidity available)", trades)
	}
	if _, ok := b.BestAsk(); ok {
		t.Error("BestAsk() ok = true, want book empty after sweep")
	}
	if _, ok := b.index[2]; ok {
		t.Error("index has market order's unfilled remainder, it must never rest")
	}
	if _, ok := b.BestBid(); ok {
		t.Error("BestBid() ok = true, market order's leftover 40 qty must not rest")
	}
}

func TestSubmit_MarketOrder_NoLiquidityNoOp(t *testing.T) {
	b := NewBook()
	trades, err := b.Submit(Order{SeqID: 1, Side: Buy, Type: Market, Quantity: 100})
	if err != nil {
		t.Fatalf("Submit() error = %v, want nil", err)
	}
	if len(trades) != 0 {
		t.Fatalf("trades = %v, want none (empty book)", trades)
	}
	if _, ok := b.BestBid(); ok {
		t.Error("BestBid() ok = true, market order must never rest")
	}
}

func TestCancel_UnknownOrder(t *testing.T) {
	b := NewBook()
	if err := b.Cancel(999); err == nil {
		t.Fatal("Cancel() on unknown SeqID = nil error, want error")
	}
}

func TestCancel_RestingOrder(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 50})

	if err := b.Cancel(1); err != nil {
		t.Fatalf("Cancel() error = %v, want nil", err)
	}
	if _, ok := b.BestBid(); ok {
		t.Error("BestBid() ok = true, want level removed once its only order is cancelled")
	}
	if _, ok := b.index[1]; ok {
		t.Error("index still has entry for cancelled order")
	}
	if _, ok := b.levels[100]; ok {
		t.Error("levels still has entry for price 100 after its only order was cancelled")
	}
}

func TestCancel_DoubleCancelFails(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 50})
	mustCancel(t, b, 1)
	if err := b.Cancel(1); err == nil {
		t.Fatal("second Cancel() of the same order = nil error, want error")
	}
}

func TestCancel_LeavesSiblingLevelIntact(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 50})
	mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 30})

	mustCancel(t, b, 1)

	if got, ok := b.BestBid(); !ok || got != 100 {
		t.Fatalf("BestBid() = (%d, %v), want (100, true) — sibling order still resting", got, ok)
	}
	qtys := restingQty(t, b, Buy, 100)
	if len(qtys) != 1 || qtys[0] != 30 {
		t.Errorf("resting qtys at 100 = %v, want [30]", qtys)
	}
}

func TestSnapshot_AggregatesQuantityPerLevel(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 30})
	mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 20})
	mustSubmit(t, b, Order{SeqID: 3, Side: Sell, Type: Limit, Price: 105, Quantity: 40})

	var got struct {
		Bids []struct {
			Price    Price `json:"price"`
			Quantity int64 `json:"quantity"`
		} `json:"bids"`
		Asks []struct {
			Price    Price `json:"price"`
			Quantity int64 `json:"quantity"`
		} `json:"asks"`
	}
	if err := json.Unmarshal([]byte(b.Snapshot()), &got); err != nil {
		t.Fatalf("Snapshot() produced invalid JSON: %v", err)
	}

	if len(got.Bids) != 1 || got.Bids[0].Price != 100 || got.Bids[0].Quantity != 50 {
		t.Errorf("Snapshot() bids = %+v, want one level at 100 with quantity 50 (30+20 aggregated)", got.Bids)
	}
	if len(got.Asks) != 1 || got.Asks[0].Price != 105 || got.Asks[0].Quantity != 40 {
		t.Errorf("Snapshot() asks = %+v, want one level at 105 with quantity 40", got.Asks)
	}
}

func TestCancel_AfterPartialFillCancelsRemainder(t *testing.T) {
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 200})
	mustSubmit(t, b, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 50})

	if err := b.Cancel(1); err != nil {
		t.Fatalf("Cancel() of partially-filled maker error = %v, want nil", err)
	}
	if _, ok := b.BestAsk(); ok {
		t.Error("BestAsk() ok = true, want the 150 remaining shares gone after cancel")
	}
}

func TestCancel_AfterRequeueOnPartialFill_IndexStaysCorrect(t *testing.T) {
	// Regression test: matchAgainstBids used to skip updating b.index on
	// pushFront/popFront, leaving stale backrefs pointing at dead list
	// elements once a resting Buy order was partially filled.
	b := NewBook()
	mustSubmit(t, b, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 200})
	mustSubmit(t, b, Order{SeqID: 2, Side: Sell, Type: Limit, Price: 100, Quantity: 50})

	if err := b.Cancel(1); err != nil {
		t.Fatalf("Cancel() after partial fill on the bid side error = %v, want nil", err)
	}
	if _, ok := b.BestBid(); ok {
		t.Error("BestBid() ok = true, want the remaining 150 shares gone after cancel")
	}
}

func mustSubmit(t *testing.T, b *Book, o Order) []Trade {
	t.Helper()
	trades, err := b.Submit(o)
	if err != nil {
		t.Fatalf("Submit(%+v) error = %v, want nil", o, err)
	}
	return trades
}

func mustCancel(t *testing.T, b *Book, id SeqID) {
	t.Helper()
	if err := b.Cancel(id); err != nil {
		t.Fatalf("Cancel(%d) error = %v, want nil", id, err)
	}
}
