package oms

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkRecover measures cold-start replay: how long a node takes to rebuild
// a book from a log of N records.
//
// This is the number that bounds a restart, and it is the one that makes the
// unbounded-log problem concrete rather than theoretical: nothing prunes a log,
// so this time grows for the life of the node. Snapshots are the fix (ADR-003),
// and this benchmark is what would demonstrate they were needed.
//
// The log is built once per size and reused across iterations, because building
// it is dominated by fsyncs and would otherwise swamp what is being measured.
// Only the replay is timed.
func BenchmarkRecover(b *testing.B) {
	for _, records := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			dir := buildLog(b, records)

			// Report per-record cost too: replay should be linear, and a
			// ns/record that climbs with size would mean it is not.
			b.ResetTimer()
			for b.Loop() {
				book := NewBook()
				last, err := Recover(dir, book, 0)
				if err != nil {
					b.Fatalf("Recover() error = %v", err)
				}
				if last == 0 {
					b.Fatal("Recover() replayed nothing")
				}
			}
			b.StopTimer()

			elapsed := b.Elapsed().Seconds() / float64(b.N)
			b.ReportMetric(elapsed*1e9/float64(records), "ns/record")
			b.ReportMetric(float64(records)/elapsed, "records/sec")
		})
	}
}

// buildLog writes a realistic mixed workload — crossing limits, markets, and
// cancels — to a temp directory outside $TMPDIR when OMS_BENCH_WAL_DIR is set.
//
// Building it is deliberately not timed. It pays one fsync per batch and would
// otherwise dominate the measurement entirely (ADR-003: one fsync is ~3.4ms on
// real storage, so 100k records of write cost minutes while replay costs
// milliseconds).
func buildLog(b *testing.B, records int) string {
	b.Helper()

	parent := os.Getenv("OMS_BENCH_WAL_DIR")
	var dir string
	if parent == "" {
		dir = b.TempDir()
	} else {
		var err error
		dir, err = os.MkdirTemp(parent, "oms-recover-")
		if err != nil {
			b.Fatalf("create log dir under %s: %v", parent, err)
		}
		b.Cleanup(func() { os.RemoveAll(dir) })
	}

	w, err := OpenWriter(filepath.Join(dir, "NABIL"))
	if err != nil {
		b.Fatalf("OpenWriter() error = %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	ts := time.Unix(1_700_000_000, 0).UTC()
	var resting []SeqID

	for i := 1; i <= records; i++ {
		seq := int64(i)
		if len(resting) > 0 && rng.Intn(6) == 0 {
			victim := resting[rng.Intn(len(resting))]
			if err := w.Append(Record{Seq: seq, Kind: RecordCancel, TS: ts, CancelID: victim}); err != nil {
				b.Fatal(err)
			}
			continue
		}
		side := Buy
		if rng.Intn(2) == 0 {
			side = Sell
		}
		orderType := Limit
		if rng.Intn(25) == 0 {
			orderType = Market
		}
		o := Order{
			SeqID: SeqID(i), Symbol: "NABIL", Placer: fmt.Sprintf("acct-%d", rng.Intn(8)),
			Type: orderType, Price: Price(495 + rng.Intn(11)),
			Quantity: int64(1 + rng.Intn(100)), Side: side, TimeStamp: ts,
		}
		if err := w.Append(Record{Seq: seq, Kind: RecordSubmit, TS: ts, Order: o}); err != nil {
			b.Fatal(err)
		}
		if orderType == Limit {
			resting = append(resting, o.SeqID)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatalf("Close() error = %v", err)
	}
	return filepath.Join(dir, "NABIL")
}
