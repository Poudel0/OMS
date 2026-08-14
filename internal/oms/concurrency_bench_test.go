package oms

import (
	"fmt"
	"math/rand"
	"os"
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

// benchmarkSequencerWAL measures the same workload with durability turned on:
// every batch is appended and fsynced before any of it is matched. The gap
// against BenchmarkSequencer_* is the price of crash recovery, and it should
// *narrow* as producers rise — more concurrent producers mean more requests
// waiting in the channel, which means bigger group commits and fewer fsyncs
// per order. See ADR-003.
//
// This one writes to a real file, so unlike every other benchmark here its
// result depends on the storage underneath — see benchWALDir.
func benchmarkSequencerWAL(b *testing.B, producers int) {
	ctx := b.Context()
	w, err := OpenWriter(benchWALDir(b))
	if err != nil {
		b.Fatalf("OpenWriter() error = %v", err)
	}
	seq := NewSequencerWithWAL(ctx, NewBook(), w)
	runConcurrent(b, producers, func(o Order) ([]Trade, error) {
		resp, err := seq.Submit(ctx, o)
		return resp.Trades, err
	})
	b.StopTimer()
	if err := seq.Close(); err != nil {
		b.Fatalf("Close() error = %v", err)
	}
	// Report the mean group-commit size: how many orders each fsync served.
	// This is the mechanism the ns/op figures above are explained by, so it
	// belongs in the same output rather than in a separate hand-waved claim.
	if commits, requests := seq.CommitStats(); commits > 0 {
		b.ReportMetric(float64(requests)/float64(commits), "orders/fsync")
	}
}

// benchWALDir picks where the WAL benchmarks write.
//
// The default, b.TempDir(), lands under $TMPDIR — which on most Linux desktops
// (this one included) is a tmpfs, i.e. RAM. fsync against tmpfs barely costs
// anything, so that configuration does NOT measure the price of durability.
// It is still worth having as a control: it isolates the WAL's own encoding
// and syscall overhead from whatever the storage device charges to flush.
//
// For the number that actually means "what crash recovery costs", point
// OMS_BENCH_WAL_DIR at a directory on real storage:
//
//	OMS_BENCH_WAL_DIR=$HOME/.cache go test -bench=SequencerWAL -benchtime=2s ./internal/oms/
//
// Report which of the two any published figure came from. A tmpfs fsync
// number presented as a durability number is an invented number.
func benchWALDir(b *testing.B) string {
	parent := os.Getenv("OMS_BENCH_WAL_DIR")
	if parent == "" {
		return b.TempDir()
	}
	dir, err := os.MkdirTemp(parent, "oms-bench-wal-")
	if err != nil {
		b.Fatalf("create wal bench dir under %s: %v", parent, err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func BenchmarkSequencerWAL_1(b *testing.B)  { benchmarkSequencerWAL(b, 1) }
func BenchmarkSequencerWAL_4(b *testing.B)  { benchmarkSequencerWAL(b, 4) }
func BenchmarkSequencerWAL_16(b *testing.B) { benchmarkSequencerWAL(b, 16) }
func BenchmarkSequencerWAL_64(b *testing.B) { benchmarkSequencerWAL(b, 64) }

func benchmarkMutex(b *testing.B, producers int) {
	mb := NewMutexBook()
	runConcurrent(b, producers, mb.Submit)
}

func BenchmarkMutex_1(b *testing.B)  { benchmarkMutex(b, 1) }
func BenchmarkMutex_4(b *testing.B)  { benchmarkMutex(b, 4) }
func BenchmarkMutex_16(b *testing.B) { benchmarkMutex(b, 16) }
func BenchmarkMutex_64(b *testing.B) { benchmarkMutex(b, 64) }
