package oms

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
)

// newWALSequencer wires a fresh book to a fresh log in a temp dir and returns
// all three, so tests can assert against the live book and then replay the
// same log into a second one.
func newWALSequencer(t *testing.T) (*Sequencer, *Book, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	book := NewBook()
	return NewSequencerWithWAL(t.Context(), book, w), book, dir
}

func TestSequencer_AssignsMonotonicLogPositions(t *testing.T) {
	ctx := t.Context()
	seq, _, _ := newWALSequencer(t)

	for i := 1; i <= 5; i++ {
		resp, err := seq.Submit(ctx, Order{SeqID: SeqID(i), Type: Limit, Price: 500, Quantity: 10, Side: Buy})
		if err != nil {
			t.Fatalf("Submit(%d) error = %v", i, err)
		}
		if resp.Seq != int64(i) {
			t.Errorf("Submit(%d) log position = %d, want %d", i, resp.Seq, i)
		}
	}
	if err := seq.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestSequencer_WALlessSequencerReportsNoLogPosition(t *testing.T) {
	ctx := t.Context()
	seq := NewSequencer(ctx, NewBook())

	resp, err := seq.Submit(ctx, Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if resp.Seq != 0 {
		t.Errorf("Seq = %d, want 0 — there is no log to have a position in", resp.Seq)
	}
}

func TestSequencer_LogsCancelsSoTheyDoNotResurrectOnReplay(t *testing.T) {
	ctx := t.Context()
	seq, live, dir := newWALSequencer(t)

	if _, err := seq.Submit(ctx, Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	if _, err := seq.Submit(ctx, Order{SeqID: 2, Type: Limit, Price: 499, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	if err := seq.Cancel(ctx, 1); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if err := seq.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed := NewBook()
	if _, err := Recover(dir, replayed, 0); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if got, want := replayed.Snapshot(), live.Snapshot(); got != want {
		t.Errorf("replayed state = %s, want %s", got, want)
	}
	if bid, _ := replayed.BestBid(); bid != 499 {
		t.Errorf("BestBid() after replay = %d, want 499 — the cancelled order came back", bid)
	}
}

func TestSequencer_ConcurrentWorkloadReplaysToTheSameBook(t *testing.T) {
	ctx := t.Context()
	seq, live, dir := newWALSequencer(t)

	// Producers race, so the order mutations land in is not predictable from
	// the outside. That is exactly the point: the log records the order the
	// sequencer actually applied, so replay has to reproduce the live book
	// whatever interleaving happened to occur.
	const producers = 8
	const perProducer = 400

	var wg sync.WaitGroup
	var mu sync.Mutex
	var acked []SeqID

	for p := range producers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(p)))
			for i := range perProducer {
				id := SeqID(p*perProducer + i + 1)
				side := Buy
				if (p+i)%2 == 0 {
					side = Sell
				}
				o := Order{
					SeqID: id, Symbol: "NABIL", Placer: "acc", Type: Limit,
					Price: Price(495 + rng.Intn(11)), Quantity: int64(1 + rng.Intn(50)), Side: side,
				}
				if _, err := seq.Submit(ctx, o); err != nil {
					t.Errorf("Submit() error = %v", err)
					return
				}
				mu.Lock()
				acked = append(acked, id)
				mu.Unlock()

				// Occasionally cancel something already acknowledged, so the
				// log has to interleave both mutation kinds correctly.
				if rng.Intn(6) == 0 {
					mu.Lock()
					victim := acked[rng.Intn(len(acked))]
					mu.Unlock()
					if err := seq.Cancel(ctx, victim); err != nil && !errors.Is(err, ErrSequencerClosed) {
						continue // already filled or already cancelled; both fine
					}
				}
			}
		}(p)
	}
	wg.Wait()

	liveState := live.Snapshot()
	liveCount := live.RestingCount()
	if err := seq.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if liveCount == 0 {
		t.Fatal("live book ended empty — nothing rested, so replay proves nothing")
	}

	replayed := NewBook()
	if _, err := Recover(dir, replayed, 0); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if got := replayed.Snapshot(); got != liveState {
		t.Errorf("replayed book diverged from live\n got: %s\nwant: %s", got, liveState)
	}
	if got := replayed.RestingCount(); got != liveCount {
		t.Errorf("replayed RestingCount() = %d, want %d", got, liveCount)
	}
}

func TestSequencer_ResumesLogPositionsAfterRestart(t *testing.T) {
	dir := t.TempDir()

	w1, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq1 := NewSequencerWithWAL(t.Context(), NewBook(), w1)
	for i := 1; i <= 3; i++ {
		if _, err := seq1.Submit(t.Context(), Order{SeqID: SeqID(i), Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
			t.Fatal(err)
		}
	}
	if err := seq1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Restart: recover the book from the log, then keep writing to the same
	// log. Positions must continue, not restart at 1 — a reused position
	// would make the log ambiguous about which record came first.
	book2 := NewBook()
	lastSeq, err := Recover(dir, book2, 0)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if lastSeq != 3 {
		t.Fatalf("Recover() lastSeq = %d, want 3", lastSeq)
	}
	if book2.RestingCount() != 3 {
		t.Fatalf("recovered RestingCount() = %d, want 3", book2.RestingCount())
	}

	w2, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq2 := NewSequencerWithWAL(t.Context(), book2, w2)
	resp, err := seq2.Submit(t.Context(), Order{SeqID: 4, Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 4 {
		t.Errorf("first log position after restart = %d, want 4", resp.Seq)
	}
	if err := seq2.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// And the whole log, across both runs, still replays into the same state.
	book3 := NewBook()
	if _, err := Recover(dir, book3, 0); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if got, want := book3.Snapshot(), book2.Snapshot(); got != want {
		t.Errorf("replay across a restart = %s, want %s", got, want)
	}
}

func TestSequencer_WALFailureRejectsWithoutTouchingBook(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	book := NewBook()
	seq := NewSequencerWithWAL(ctx, book, w)

	// Pull the file out from under the writer. Append still buffers happily;
	// the failure surfaces at the fsync, which is the moment durability was
	// supposed to be established.
	if err := w.f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := seq.Submit(ctx, Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err == nil {
		t.Fatal("Submit() with a broken log = nil error, want the write failure surfaced to the caller")
	}
	// The whole point of write-ahead ordering: an order that could not be
	// made durable must not have been matched either.
	if book.RestingCount() != 0 {
		t.Errorf("RestingCount() = %d, want 0 — the book was mutated despite the log failing", book.RestingCount())
	}
	if _, ok := book.BestBid(); ok {
		t.Error("BestBid() ok = true, want the rejected order absent from the book")
	}

	// The failure is sticky: a log in an unknown state cannot be trusted for
	// subsequent orders either.
	if _, err := seq.Submit(ctx, Order{SeqID: 2, Type: Limit, Price: 501, Quantity: 10, Side: Buy}); err == nil {
		t.Error("second Submit() = nil error, want the sequencer to stay failed")
	}
	if book.RestingCount() != 0 {
		t.Errorf("RestingCount() = %d, want 0", book.RestingCount())
	}
}

func TestSequencer_GroupCommitKeepsRequestsInChannelOrder(t *testing.T) {
	ctx := t.Context()
	seq, _, dir := newWALSequencer(t)

	// Batching must not reorder anything. A price-crossing sequence is the
	// sharpest check available: reordering these would change the trades, and
	// a replay of a reordered log would diverge from the live book.
	orders := []Order{
		{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Sell},
		{SeqID: 2, Type: Limit, Price: 501, Quantity: 10, Side: Sell},
		{SeqID: 3, Type: Market, Quantity: 15, Side: Buy},
	}
	for _, o := range orders {
		if _, err := seq.Submit(ctx, o); err != nil {
			t.Fatalf("Submit(%d) error = %v", o.SeqID, err)
		}
	}
	if err := seq.Close(); err != nil {
		t.Fatal(err)
	}

	recs := collect(t, dir)
	if len(recs) != len(orders) {
		t.Fatalf("logged %d records, want %d", len(recs), len(orders))
	}
	for i, rec := range recs {
		if rec.Seq != int64(i+1) {
			t.Errorf("record %d log position = %d, want %d", i, rec.Seq, i+1)
		}
		if rec.Order.SeqID != orders[i].SeqID {
			t.Errorf("record %d is order %d, want %d — the batch reordered requests",
				i, rec.Order.SeqID, orders[i].SeqID)
		}
	}
}

func TestSequencer_CloseIsIdempotentAndReportsFlushErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq := NewSequencerWithWAL(ctx, NewBook(), w)

	if _, err := seq.Submit(ctx, Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	if err := seq.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := seq.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil (idempotent)", err)
	}
	if _, err := seq.Submit(ctx, Order{SeqID: 2, Type: Limit, Price: 500, Quantity: 10, Side: Buy}); !errors.Is(err, ErrSequencerClosed) {
		t.Errorf("Submit() after Close() = %v, want ErrSequencerClosed", err)
	}
}
