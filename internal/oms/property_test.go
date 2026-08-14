package oms

import (
	"testing"

	"pgregory.net/rapid"
)

// TestSubmit_Properties runs random sequences of Submit/Cancel calls against
// a Book and checks, after every single step, invariants that must hold no
// matter what sequence of orders arrived:
//
//   - no resting order ever has non-positive quantity
//   - sum of a Submit call's returned trade quantities never exceeds what
//     was submitted, and the leftover matches exactly what ends up resting
//     (or, for Market orders, that nothing rests at all)
//   - price-time priority: within any single price level, orders are always
//     in strictly increasing SeqID order (first in, first matched)
//   - the bid/ask ladders stay sorted, non-crossing, and in exact
//     correspondence with the non-empty levels in the book
func TestSubmit_Properties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		b := NewBook()
		var nextSeqID SeqID = 1
		var liveIDs []SeqID // orders we believe are still resting, for Cancel to pick from

		rt.Repeat(map[string]func(*rapid.T){
			"submit": func(rt *rapid.T) {
				side := Buy
				if rapid.Bool().Draw(rt, "isSell") {
					side = Sell
				}
				orderType := Limit
				if rapid.IntRange(0, 9).Draw(rt, "typeRoll") == 0 {
					orderType = Market
				}
				price := Price(rapid.IntRange(90, 110).Draw(rt, "price"))
				qty := int64(rapid.IntRange(1, 100).Draw(rt, "qty"))

				o := Order{
					SeqID:    nextSeqID,
					Symbol:   "TEST",
					Side:     side,
					Type:     orderType,
					Price:    price,
					Quantity: qty,
				}
				nextSeqID++

				trades, err := b.Submit(o)
				if err != nil {
					rt.Fatalf("Submit(%+v) error = %v, want nil", o, err)
				}

				filled := sumFills(trades)
				if filled > o.Quantity {
					rt.Fatalf("sum(fills) = %d, exceeds submitted quantity %d", filled, o.Quantity)
				}
				for _, tr := range trades {
					if tr.Quantity <= 0 {
						rt.Fatalf("trade %+v has non-positive quantity", tr)
					}
				}

				ref, resting := b.index[o.SeqID]
				switch {
				case orderType == Market:
					if resting {
						rt.Fatalf("market order SeqID %d rested, must never rest", o.SeqID)
					}
				case filled == o.Quantity:
					if resting {
						rt.Fatalf("fully-filled order SeqID %d still rests", o.SeqID)
					}
				default:
					if !resting {
						rt.Fatalf("order SeqID %d filled %d/%d but has no resting remainder", o.SeqID, filled, o.Quantity)
					}
					remaining := ref.element.Value.(Order).Quantity
					if remaining != o.Quantity-filled {
						rt.Fatalf("resting remainder = %d, want %d (submitted %d - filled %d)", remaining, o.Quantity-filled, o.Quantity, filled)
					}
					if remaining <= 0 {
						rt.Fatalf("resting order SeqID %d has non-positive quantity %d", o.SeqID, remaining)
					}
					liveIDs = append(liveIDs, o.SeqID)
				}

				assertBookInvariants(rt, b)
			},
			"cancel": func(rt *rapid.T) {
				if len(liveIDs) == 0 {
					return
				}
				i := rapid.IntRange(0, len(liveIDs)-1).Draw(rt, "idx")
				id := liveIDs[i]
				liveIDs = append(liveIDs[:i], liveIDs[i+1:]...)

				// The order may have been fully consumed by a later match
				// since we recorded it — Cancel correctly errors in that
				// case, that's not itself an invariant violation.
				_ = b.Cancel(id)
				assertBookInvariants(rt, b)
			},
		})
	})
}

func assertBookInvariants(rt *rapid.T, b *Book) {
	rt.Helper()

	checkLadder(rt, b, b.bids, Buy, func(a, c Price) bool { return a > c })  // strictly descending
	checkLadder(rt, b, b.asks, Sell, func(a, c Price) bool { return a < c }) // strictly ascending

	if bid, ok := b.BestBid(); ok {
		if ask, ok := b.BestAsk(); ok && bid >= ask {
			rt.Fatalf("book crossed: BestBid()=%d >= BestAsk()=%d", bid, ask)
		}
	}

	if len(b.levels) != len(b.bids)+len(b.asks) {
		rt.Fatalf("levels map has %d entries, want %d (bids %d + asks %d)", len(b.levels), len(b.bids)+len(b.asks), len(b.bids), len(b.asks))
	}
}

func checkLadder(rt *rapid.T, b *Book, ladder []Price, side OrderSide, ordered func(a, c Price) bool) {
	rt.Helper()
	for i, price := range ladder {
		if i > 0 && !ordered(ladder[i-1], price) {
			rt.Fatalf("%v ladder not sorted at index %d: %v", side, i, ladder)
		}
		lvl, ok := b.levels[price]
		if !ok {
			rt.Fatalf("%v ladder has price %d with no matching level", side, price)
		}
		if lvl.side != side {
			rt.Fatalf("level at price %d has side %v, want %v", price, lvl.side, side)
		}
		if lvl.orders.Len() == 0 {
			rt.Fatalf("%v ladder has price %d whose level is empty — should've been removed", side, price)
		}

		var lastSeqID SeqID = -1
		for e := lvl.orders.Front(); e != nil; e = e.Next() {
			o := e.Value.(Order)
			if o.Quantity <= 0 {
				rt.Fatalf("resting order SeqID %d at price %d has non-positive quantity %d", o.SeqID, price, o.Quantity)
			}
			if o.SeqID <= lastSeqID {
				rt.Fatalf("price-time priority violated at price %d: SeqID %d appears after SeqID %d", price, o.SeqID, lastSeqID)
			}
			lastSeqID = o.SeqID
		}
	}
}
