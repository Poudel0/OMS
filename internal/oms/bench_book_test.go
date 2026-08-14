package oms

import (
	"fmt"
	"math/rand"
	"testing"
)

const (
	benchBasePrice   Price = 1000
	benchPriceSpread       = 50
	benchClampRange  Price = 200
	benchTargetDepth       = 2_000
)

// BenchmarkSubmit measures single-threaded Submit() throughput under a
// realistic mixed limit+market workload. Order prices are anchored to the
// book's own last trade (not a fixed constant) and clamped to a bounded
// range, and once resting depth exceeds benchTargetDepth new orders are
// priced to guarantee a cross instead of drawn purely at random — without
// that control, independent random buy/sell submission has no force
// balancing arrivals against removals, and total resting depth (and
// therefore live heap size and GC pressure) grows without bound as b.N
// scales into the millions, contaminating the measurement with degrading
// GC behavior instead of steady-state matching cost. See the identical
// issue and fix in cmd/bench/main.go.
func BenchmarkSubmit(b *testing.B) {
	rng := rand.New(rand.NewSource(42))

	// Precomputed so fmt.Sprintf never runs inside the timed loop.
	placers := make([]string, 50)
	for i := range placers {
		placers[i] = fmt.Sprintf("trader-%d", i)
	}

	book := NewBook()
	lastPrice := benchBasePrice
	var seq SeqID

	for b.Loop() {
		seq++
		o := benchOrder(rng, seq, book, placers, lastPrice)
		trades, err := book.Submit(o)
		if err != nil {
			b.Fatalf("Submit(%+v) error = %v", o, err)
		}
		if len(trades) > 0 {
			lastPrice = benchClamp(trades[len(trades)-1].Price)
		}
	}
}

func benchClamp(p Price) Price {
	if p > benchBasePrice+benchClampRange {
		return benchBasePrice + benchClampRange
	}
	if p < benchBasePrice-benchClampRange {
		return benchBasePrice - benchClampRange
	}
	return p
}

func benchOrder(rng *rand.Rand, seq SeqID, book *Book, placers []string, lastPrice Price) Order {
	side := Buy
	if rng.Intn(2) == 0 {
		side = Sell
	}
	orderType := Limit
	if rng.Intn(20) == 0 { // ~5% market orders
		orderType = Market
	}

	price := lastPrice + Price(rng.Intn(2*benchPriceSpread+1)-benchPriceSpread)
	if book.RestingCount() > benchTargetDepth {
		if side == Buy {
			if ask, ok := book.BestAsk(); ok {
				price = ask // guaranteed to cross, shrinks the book instead of growing it
			}
		} else if bid, ok := book.BestBid(); ok {
			price = bid
		}
	}

	return Order{
		SeqID:    seq,
		Symbol:   "NABIL",
		Placer:   placers[rng.Intn(len(placers))],
		Type:     orderType,
		Price:    benchClamp(price),
		Quantity: int64(rng.Intn(500) + 1),
		Side:     side,
	}
}
