package oms

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrSequencerClosed is returned by Submit/Cancel once the sequencer has
// been shut down via Close.
var ErrSequencerClosed = errors.New("oms: sequencer closed")

// OrderResponse is what a Sequencer hands back for a successful Submit.
type OrderResponse struct {
	Trades []Trade
	// QueueLatency is how long the request waited before the sequencer
	// goroutine started processing it — time spent behind other requests.
	QueueLatency time.Duration
	// TotalLatency is the full round trip: from the caller invoking Submit
	// to the reply being received back.
	TotalLatency time.Duration
}

type submitRequest struct {
	order  Order
	sentAt time.Time
	reply  chan submitResult
}

type submitResult struct {
	trades      []Trade
	err         error
	pickedUpAt  time.Time
	respondedAt time.Time
}

type cancelRequest struct {
	orderID SeqID
	reply   chan error
}

// Reply channels are the hottest allocation in Submit/Cancel — profiling
// showed them dominating allocated objects under concurrent load (see
// docs/adr/0002-single-writer-sequencer.md). Pooled here instead.
//
// Only pool a channel once it's provably empty: either the request was
// never handed to the sequencer goroutine (done/ctx.Done fired first), or
// we already drained the one value the goroutine will ever send. If a
// caller's ctx is cancelled AFTER the request reached the goroutine, that
// goroutine may still write to the channel later — such a channel is never
// returned to the pool, so a stale value can never leak into a future Get().
var submitReplyPool = sync.Pool{New: func() any { return make(chan submitResult, 1) }}
var cancelReplyPool = sync.Pool{New: func() any { return make(chan error, 1) }}

// Sequencer serializes all access to one symbol's Book through a single
// goroutine — see ADR-002. Producers call Submit/Cancel from any number of
// goroutines; every request is funneled through unbuffered channels to the
// one goroutine that actually touches the book, so the book itself never
// needs a lock.
type Sequencer struct {
	book    *Book
	submits chan submitRequest
	cancels chan cancelRequest
	done    chan struct{}
}

// NewSequencer starts the sequencer's goroutine and returns immediately.
// The goroutine runs until ctx is cancelled or Close is called.
func NewSequencer(ctx context.Context, book *Book) *Sequencer {
	s := &Sequencer{
		book:    book,
		submits: make(chan submitRequest),
		cancels: make(chan cancelRequest),
		done:    make(chan struct{}),
	}
	go s.run(ctx)
	return s
}

func (s *Sequencer) run(ctx context.Context) {
	defer close(s.done)
	for {
		select {
		case req := <-s.submits:
			pickedUpAt := time.Now()
			trades, err := s.book.Submit(req.order)
			req.reply <- submitResult{trades: trades, err: err, pickedUpAt: pickedUpAt, respondedAt: time.Now()}
		case req := <-s.cancels:
			req.reply <- s.book.Cancel(req.orderID)
		case <-ctx.Done():
			return
		}
	}
}

// Submit hands o to the sequencer goroutine and blocks for its response, or
// until ctx is cancelled — whichever comes first. Safe to call from any
// number of goroutines concurrently; the book itself is only ever touched
// by the one sequencer goroutine.
func (s *Sequencer) Submit(ctx context.Context, o Order) (OrderResponse, error) {
	sentAt := time.Now()
	reply := submitReplyPool.Get().(chan submitResult)

	select {
	case s.submits <- submitRequest{order: o, sentAt: sentAt, reply: reply}:
	case <-s.done:
		submitReplyPool.Put(reply) // never sent — provably empty
		return OrderResponse{}, ErrSequencerClosed
	case <-ctx.Done():
		submitReplyPool.Put(reply) // never sent — provably empty
		return OrderResponse{}, ctx.Err()
	}

	select {
	case res := <-reply:
		submitReplyPool.Put(reply) // just drained the one value — provably empty
		return OrderResponse{
			Trades:       res.trades,
			QueueLatency: res.pickedUpAt.Sub(sentAt),
			TotalLatency: res.respondedAt.Sub(sentAt),
		}, res.err
	case <-ctx.Done():
		// The request may already be in flight to run(), which could still
		// write to reply later — do not pool it.
		return OrderResponse{}, ctx.Err()
	}
}

// Cancel removes a resting order by SeqID, same blocking/cancellation
// semantics as Submit.
func (s *Sequencer) Cancel(ctx context.Context, orderID SeqID) error {
	reply := cancelReplyPool.Get().(chan error)

	select {
	case s.cancels <- cancelRequest{orderID: orderID, reply: reply}:
	case <-s.done:
		cancelReplyPool.Put(reply)
		return ErrSequencerClosed
	case <-ctx.Done():
		cancelReplyPool.Put(reply)
		return ctx.Err()
	}

	select {
	case err := <-reply:
		cancelReplyPool.Put(reply)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done returns a channel that's closed once the sequencer's goroutine has
// exited (its context was cancelled). Useful for waiting out a clean
// shutdown.
func (s *Sequencer) Done() <-chan struct{} {
	return s.done
}
