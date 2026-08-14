package oms

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSequencer_SubmitMatchesDirectBook(t *testing.T) {
	ctx := t.Context()
	book := NewBook()
	seq := NewSequencer(ctx, book)

	resp, err := seq.Submit(ctx, Order{SeqID: 1, Side: Sell, Type: Limit, Price: 100, Quantity: 50})
	if err != nil {
		t.Fatalf("Submit() error = %v, want nil", err)
	}
	if len(resp.Trades) != 0 {
		t.Fatalf("Trades = %v, want none (nothing to cross yet)", resp.Trades)
	}

	resp, err = seq.Submit(ctx, Order{SeqID: 2, Side: Buy, Type: Limit, Price: 100, Quantity: 50})
	if err != nil {
		t.Fatalf("Submit() error = %v, want nil", err)
	}
	if len(resp.Trades) != 1 || resp.Trades[0].Quantity != 50 {
		t.Fatalf("Trades = %+v, want one trade of qty 50", resp.Trades)
	}
	if resp.TotalLatency <= 0 {
		t.Error("TotalLatency = 0, want a positive measured duration")
	}
	if resp.QueueLatency < 0 {
		t.Error("QueueLatency < 0, impossible")
	}
}

func TestSequencer_Cancel(t *testing.T) {
	ctx := t.Context()
	book := NewBook()
	seq := NewSequencer(ctx, book)

	if _, err := seq.Submit(ctx, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 50}); err != nil {
		t.Fatalf("Submit() error = %v, want nil", err)
	}
	if err := seq.Cancel(ctx, 1); err != nil {
		t.Fatalf("Cancel() error = %v, want nil", err)
	}
	if _, ok := book.BestBid(); ok {
		t.Error("BestBid() ok = true, want the cancelled order gone")
	}
	if err := seq.Cancel(ctx, 1); err == nil {
		t.Error("second Cancel() = nil error, want error (already cancelled)")
	}
}

func TestSequencer_ContextCancellationDoesNotHang(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	book := NewBook()
	seq := NewSequencer(ctx, book)

	// cancel() only *requests* shutdown; it does not wait for it. Submitting
	// straight after it would race the sequencer goroutine, which is sitting
	// in a select where both the incoming request and ctx.Done() are ready —
	// and Go picks among ready select cases at random, so it would serve one
	// last order about half the time. Wait for the goroutine to actually be
	// gone, which is what Done() reports, before asserting that a closed
	// sequencer rejects work.
	cancel()
	select {
	case <-seq.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() never closed after context cancellation")
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
	defer callCancel()

	_, err := seq.Submit(callCtx, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 50})
	if err != ErrSequencerClosed {
		t.Fatalf("Submit() after sequencer shutdown = %v, want ErrSequencerClosed", err)
	}
}

func TestSequencer_SubmitTimesOutIndependentlyOfSequencer(t *testing.T) {
	ctx := t.Context()
	book := NewBook()
	seq := NewSequencer(ctx, book)

	// An already-expired deadline must make Submit return promptly with
	// ctx.Err(), without needing the sequencer itself to be unhealthy.
	callCtx, callCancel := context.WithTimeout(context.Background(), 0)
	defer callCancel()

	_, err := seq.Submit(callCtx, Order{SeqID: 1, Side: Buy, Type: Limit, Price: 100, Quantity: 50})
	if err != context.DeadlineExceeded {
		t.Fatalf("Submit() with expired deadline = %v, want context.DeadlineExceeded", err)
	}
}

func TestSequencer_ConcurrentSubmitsAreSerialized(t *testing.T) {
	ctx := t.Context()
	book := NewBook()
	seq := NewSequencer(ctx, book)

	const producers = 16
	const perProducer = 200

	var wg sync.WaitGroup
	var totalTrades int64
	var mu sync.Mutex

	for p := range producers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := range perProducer {
				side := Buy
				if (p+i)%2 == 0 {
					side = Sell
				}
				o := Order{
					SeqID:    SeqID(p*perProducer + i + 1),
					Side:     side,
					Type:     Limit,
					Price:    Price(95 + (i % 10)),
					Quantity: int64(1 + i%50),
				}
				resp, err := seq.Submit(ctx, o)
				if err != nil {
					t.Errorf("Submit() error = %v, want nil", err)
					return
				}
				mu.Lock()
				totalTrades += int64(len(resp.Trades))
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	// The point of this test is that -race finds nothing: every book
	// mutation goes through the single sequencer goroutine no matter how
	// many producer goroutines call Submit concurrently. As a basic sanity
	// check, confirm the book is still internally consistent afterward.
	if bid, ok := book.BestBid(); ok {
		if ask, ok := book.BestAsk(); ok && bid >= ask {
			t.Errorf("book crossed after concurrent submits: BestBid()=%d >= BestAsk()=%d", bid, ask)
		}
	}
	if totalTrades == 0 {
		t.Error("no trades occurred across 3200 overlapping-price orders — matching likely broken")
	}
}
