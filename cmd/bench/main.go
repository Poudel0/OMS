package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Poudel0/OMS/internal/oms"
)

const (
	duration    = 2 * time.Hour
	reportEvery = 10 * time.Second

	basePrice   = oms.Price(1000)
	priceSpread = 50  // orders land within lastPrice +/- this — keeps buys and sells overlapping
	clampRange  = 200 // lastPrice is kept within basePrice +/- this — bounds the total price universe so no resting order becomes permanently unreachable
	maxQty      = 500

	targetDepth = 2_000 // resting orders the book hovers around — see genOrder

	maxHeapMB = 1024 // circuit breaker: bail out instead of eating all your RAM if something's actually broken
)

// lastPrice anchors new order prices to the book's own last trade instead of
// a fixed constant. A matching book always keeps every resting bid below
// every resting ask, so the bid/ask boundary drifts over time; anchoring to
// a fixed price lets that drift pile resting orders one-sidedly against the
// edge of a static range. Tracking the last trade keeps the workload
// centered on wherever the book actually is.
//
// lastPrice is also clamped to stay within basePrice +/- clampRange. Left
// unclamped it's an unbounded random walk: as it wanders away from where
// earlier orders rested, those orders can never be reached by future orders
// again and pile up as permanently stranded liquidity — same unbounded-growth
// failure as the fixed-anchor bug, just via a different mechanism. Clamping
// keeps the whole simulation inside one bounded, continuously-revisited price
// universe.
var lastPrice = basePrice

func clamp(p oms.Price) oms.Price {
	if p > basePrice+clampRange {
		return basePrice + clampRange
	}
	if p < basePrice-clampRange {
		return basePrice - clampRange
	}
	return p
}

// genOrder generates the next order to submit. Pure random independent
// buy/sell submission has no force balancing arrivals against removals — a
// small persistent asymmetry between how much quantity trades away per order
// versus how much rests compounds into unbounded linear growth over a long
// run, regardless of how the price is anchored (clamping lastPrice only
// bounded the *price* dimension, not resting *quantity*). So once the book
// gets deeper than targetDepth, price the order to guarantee it crosses
// (at-or-through the current best opposite price) instead of drawing a random
// offset — that's a real negative-feedback control loop, not a hopeful
// assumption that randomness self-balances.
func genOrder(rng *rand.Rand, seq int64, book *oms.Book) oms.Order {
	side := oms.Buy
	if rng.Intn(2) == 0 {
		side = oms.Sell
	}
	orderType := oms.Limit
	if rng.Intn(20) == 0 { // ~5% market orders, to exercise that path too
		orderType = oms.Market
	}

	price := lastPrice + oms.Price(rng.Intn(2*priceSpread+1)-priceSpread)
	if book.RestingCount() > targetDepth {
		if side == oms.Buy {
			if ask, ok := book.BestAsk(); ok {
				price = ask // guaranteed to cross, shrinks the book instead of growing it
			}
		} else {
			if bid, ok := book.BestBid(); ok {
				price = bid
			}
		}
	}

	return oms.Order{
		SeqID:     oms.SeqID(seq),
		Symbol:    "NABIL",
		Placer:    fmt.Sprintf("trader-%d", rng.Intn(50)),
		Type:      orderType,
		Price:     clamp(price),
		Quantity:  int64(rng.Intn(maxQty) + 1),
		Side:      side,
		TimeStamp: time.Now(),
	}
}

func main() {
	rng := rand.New(rand.NewSource(42)) // fixed seed: reproducible run
	book := oms.NewBook()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	deadline := time.After(duration)
	ticker := time.NewTicker(reportEvery)
	defer ticker.Stop()

	start := time.Now()
	var orders, trades int64
	var seq int64

	fmt.Printf("running for up to %s, reporting every %s (Ctrl+C to stop early and still get a summary)\n", duration, reportEvery)

loop:
	for {
		select {
		case <-deadline:
			fmt.Println("reached time limit")
			break loop
		case <-stop:
			fmt.Println("interrupted")
			break loop
		case <-ticker.C:
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			elapsed := time.Since(start)
			heapMB := float64(mem.HeapAlloc) / 1e6
			fmt.Printf("[%8s] orders=%d trades=%d resting=%d orders/sec=%.0f heapMB=%.1f\n",
				elapsed.Round(time.Second), orders, trades, book.RestingCount(),
				float64(orders)/elapsed.Seconds(), heapMB)

			if heapMB > maxHeapMB {
				fmt.Printf("heap exceeded %dMB — bailing out, this smells like unbounded book growth, not normal churn\n", maxHeapMB)
				break loop
			}
		default:
			seq++
			ts, err := book.Submit(genOrder(rng, seq, book))
			if err != nil {
				fmt.Println("submit error:", err)
				continue
			}
			if len(ts) > 0 {
				lastPrice = clamp(ts[len(ts)-1].Price)
			}
			orders++
			trades += int64(len(ts))
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\ndone: %s elapsed, %d orders, %d trades, %.0f orders/sec\n",
		elapsed.Round(time.Second), orders, trades, float64(orders)/elapsed.Seconds())
}
