package oms

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

// runConcurrent drives submit from exactly `producers` goroutines, splitting
// b.N submissions roughly evenly across them, and is shared by both the
// channel-sequencer and mutex-baseline benchmarks below so they see the
// identical workload shape.
//
// Order prices are anchored to a shared atomic lastPrice (updated from each
// producer's own trade results) and clamped, same technique as
// BenchmarkSubmit — but unlike that single-threaded benchmark, this one
// deliberately skips the depth-forced-crossing control: querying live
// resting depth safely here would mean routing every generated order through
// an extra synchronized round trip, which would add a second serialization
// point and contaminate the very thing this benchmark measures (channel vs
// mutex synchronization overhead). Over the short duration a concurrency
// benchmark actually runs for (seconds, not the multi-hour case that
// motivated the depth control in cmd/bench), residual book growth is small
// and — more importantly — identical for both implementations, so the
// relative comparison this benchmark exists to produce stays fair even
// though the absolute ns/op numbers carry a little more overhead than
// BenchmarkSubmit's.
func runConcurrent(b *testing.B, producers int, submit func(o Order) ([]Trade, error)) {
	b.Helper()

	var lastPrice atomic.Int64
	lastPrice.Store(int64(benchBasePrice))

	placers := make([]string, 50)
	for i := range placers {
		placers[i] = fmt.Sprintf("trader-%d", i)
	}

	var seqCounter atomic.Int64
	perProducer := (b.N + producers - 1) / producers

	b.ResetTimer()
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(p) + 1))
			for range perProducer {
				side := Buy
				if rng.Intn(2) == 0 {
					side = Sell
				}
				orderType := Limit
				if rng.Intn(20) == 0 {
					orderType = Market
				}
				base := Price(lastPrice.Load())
				price := benchClamp(base + Price(rng.Intn(2*benchPriceSpread+1)-benchPriceSpread))

				o := Order{
					SeqID:    SeqID(seqCounter.Add(1)),
					Symbol:   "NABIL",
					Placer:   placers[rng.Intn(len(placers))],
					Type:     orderType,
					Price:    price,
					Quantity: int64(rng.Intn(500) + 1),
					Side:     side,
				}

				trades, err := submit(o)
				if err != nil {
					b.Error(err) // safe from any goroutine, unlike Fatal
					return
				}
				if len(trades) > 0 {
					lastPrice.Store(int64(benchClamp(trades[len(trades)-1].Price)))
				}
			}
		}(p)
	}
	wg.Wait()
}

func benchmarkSequencer(b *testing.B, producers int) {
	ctx := b.Context()
	seq := NewSequencer(ctx, NewBook())
	runConcurrent(b, producers, func(o Order) ([]Trade, error) {
		resp, err := seq.Submit(ctx, o)
		return resp.Trades, err
	})
}

func BenchmarkSequencer_1(b *testing.B)  { benchmarkSequencer(b, 1) }
func BenchmarkSequencer_4(b *testing.B)  { benchmarkSequencer(b, 4) }
func BenchmarkSequencer_16(b *testing.B) { benchmarkSequencer(b, 16) }
func BenchmarkSequencer_64(b *testing.B) { benchmarkSequencer(b, 64) }

func benchmarkMutex(b *testing.B, producers int) {
	mb := NewMutexBook()
	runConcurrent(b, producers, mb.Submit)
}

func BenchmarkMutex_1(b *testing.B)  { benchmarkMutex(b, 1) }
func BenchmarkMutex_4(b *testing.B)  { benchmarkMutex(b, 4) }
func BenchmarkMutex_16(b *testing.B) { benchmarkMutex(b, 16) }
func BenchmarkMutex_64(b *testing.B) { benchmarkMutex(b, 64) }
