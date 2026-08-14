package oms

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrSequencerClosed is returned by Submit/Cancel once the sequencer has
// stopped accepting work — either its context was cancelled or Close was
// called.
var ErrSequencerClosed = errors.New("oms: sequencer closed")

// maxBatch caps how many requests a single group commit covers. It bounds the
// worst-case latency the last request in a batch can inherit from the first,
// and bounds the batch slice's memory. Nothing measured picked this number;
// it is a cap, not a tuning parameter, and under a WAL-less sequencer it has
// no effect at all.
const maxBatch = 256

// OrderResponse is what a Sequencer hands back for a successful Submit.
type OrderResponse struct {
	// Seq is the write-ahead log position assigned to this request, or 0 if
	// the sequencer has no WAL. It is a log position, not an order ID — see
	// Record.
	Seq    int64
	Trades []Trade
	// QueueLatency is how long the request waited before the sequencer
	// goroutine started processing its batch — time spent behind other
	// requests.
	QueueLatency time.Duration
	// TotalLatency is the full round trip: from the caller invoking Submit
	// to the reply being received back. With a WAL attached this includes
	// the batch's fsync.
	TotalLatency time.Duration
}

// request is one unit of work for the sequencer goroutine. Submits and
// cancels share a single struct and a single channel on purpose: the WAL has
// to record mutations in exactly the order the book applies them, and one
// channel gets that from the channel's own FIFO guarantee. Two channels
// selected over would let a cancel sent later overtake a submit sent
// earlier, because a Go select among ready cases picks at random.
type request struct {
	kind     RecordKind
	order    Order // kind == RecordSubmit
	cancelID SeqID // kind == RecordCancel
	seq      int64 // assigned by the sequencer at commit time
	reply    chan result
}

type result struct {
	seq         int64
	trades      []Trade
	err         error
	pickedUpAt  time.Time
	respondedAt time.Time
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
var replyPool = sync.Pool{New: func() any { return make(chan result, 1) }}

// Sequencer serializes all access to one symbol's Book through a single
// goroutine — see ADR-002. Producers call Submit/Cancel from any number of
// goroutines; every request is funneled through one unbuffered channel to the
// one goroutine that actually touches the book, so the book itself never
// needs a lock.
//
// With a WAL attached (NewSequencerWithWAL) that goroutine is also the single
// point where durability happens: it drains whatever requests are already
// queued, appends them all to the log, fsyncs once, and only then applies any
// of them to the book. See ADR-003.
type Sequencer struct {
	book *Book
	wal  *Writer
	reqs chan request
	done chan struct{}
	quit chan struct{}
	once sync.Once

	// Owned exclusively by the sequencer goroutine.
	seq     int64
	walErr  error
	commits int64 // group commits performed
	batched int64 // requests those commits covered

	// shutdownErr is written before close(done) and only read after a
	// receive from done, which is what makes that unsynchronized access safe.
	shutdownErr error
}

// NewSequencer starts a sequencer with no write-ahead log: crash-unsafe, but
// the configuration the ADR-002 benchmarks measure. The goroutine runs until
// ctx is cancelled or Close is called.
func NewSequencer(ctx context.Context, book *Book) *Sequencer {
	return NewSequencerWithWAL(ctx, book, nil)
}

// NewSequencerWithWAL starts a sequencer that logs every mutation durably
// before applying it. wal may be nil, which is exactly NewSequencer.
//
// Log positions resume from wal.LastSeq(), so a restart after recovery never
// reissues a position an earlier run already wrote. The caller is expected to
// have already replayed the log into book (see Recover) — this constructor
// does not, because whether recovery should happen, and from which snapshot
// position, is not the sequencer's decision to make.
func NewSequencerWithWAL(ctx context.Context, book *Book, wal *Writer) *Sequencer {
	s := &Sequencer{
		book: book,
		wal:  wal,
		reqs: make(chan request),
		done: make(chan struct{}),
		quit: make(chan struct{}),
	}
	if wal != nil {
		s.seq = wal.LastSeq()
	}
	go s.run(ctx)
	return s
}

func (s *Sequencer) run(ctx context.Context) {
	defer close(s.done)
	batch := make([]request, 0, maxBatch)
	for {
		select {
		case req := <-s.reqs:
			batch = append(batch[:0], req)
			s.drain(&batch)
			s.commit(batch)
		case <-s.quit:
			s.shutdownErr = s.closeWAL()
			return
		case <-ctx.Done():
			s.shutdownErr = s.closeWAL()
			return
		}
	}
}

// drain pulls every request already queued behind the first one, without
// blocking. This is what makes one fsync serve many requests: under load the
// channel always has work waiting, so batches grow and the per-request fsync
// cost falls; when idle, a lone request is committed immediately rather than
// waiting for company.
func (s *Sequencer) drain(batch *[]request) {
	for len(*batch) < maxBatch {
		select {
		case req := <-s.reqs:
			*batch = append(*batch, req)
		default:
			return
		}
	}
}

// commit logs the whole batch durably, then applies it to the book.
//
// The ordering is the entire point of a write-ahead log and is not negotiable:
// if the append or the fsync fails, the book is not touched at all and every
// caller in the batch gets the error. A trade is never acknowledged unless it
// is already recoverable.
func (s *Sequencer) commit(batch []request) {
	pickedUpAt := time.Now()
	s.commits++
	s.batched += int64(len(batch))

	if s.wal != nil && s.walErr == nil {
		startSeq := s.seq
		if err := s.logBatch(batch, pickedUpAt); err != nil {
			// Sticky. A log that failed a write is in an unknown state, and
			// its bufio.Writer latches the error anyway; continuing to match
			// orders against a book we can no longer recover would be the
			// one genuinely unrecoverable mistake available here.
			s.seq = startSeq
			s.walErr = err
		}
	}

	for i := range batch {
		req := batch[i]
		res := result{seq: req.seq, pickedUpAt: pickedUpAt}
		switch {
		case s.walErr != nil:
			res.err = s.walErr
		case req.kind == RecordCancel:
			res.err = s.book.Cancel(req.cancelID)
		default:
			res.trades, res.err = s.book.Submit(req.order)
		}
		res.respondedAt = time.Now()
		req.reply <- res
	}
}

func (s *Sequencer) logBatch(batch []request, ts time.Time) error {
	for i := range batch {
		s.seq++
		batch[i].seq = s.seq
		rec := Record{Seq: s.seq, Kind: batch[i].kind, TS: ts}
		if batch[i].kind == RecordCancel {
			rec.CancelID = batch[i].cancelID
		} else {
			rec.Order = batch[i].order
		}
		if err := s.wal.Append(rec); err != nil {
			return err
		}
	}
	return s.wal.Sync()
}

func (s *Sequencer) closeWAL() error {
	if s.wal == nil {
		return nil
	}
	return s.wal.Close()
}

// do hands req to the sequencer goroutine and waits for its reply, giving up
// early if ctx is cancelled or the sequencer has stopped.
func (s *Sequencer) do(ctx context.Context, req request) (result, error) {
	req.reply = replyPool.Get().(chan result)

	select {
	case s.reqs <- req:
	case <-s.done:
		replyPool.Put(req.reply) // never sent — provably empty
		return result{}, ErrSequencerClosed
	case <-ctx.Done():
		replyPool.Put(req.reply) // never sent — provably empty
		return result{}, ctx.Err()
	}

	select {
	case res := <-req.reply:
		replyPool.Put(req.reply) // just drained the one value — provably empty
		return res, res.err
	case <-ctx.Done():
		// The request is already in flight and the goroutine will still
		// write to reply later — do not pool it.
		return result{}, ctx.Err()
	}
}

// Submit hands o to the sequencer goroutine and blocks for its response, or
// until ctx is cancelled — whichever comes first. Safe to call from any
// number of goroutines concurrently; the book itself is only ever touched
// by the one sequencer goroutine.
func (s *Sequencer) Submit(ctx context.Context, o Order) (OrderResponse, error) {
	sentAt := time.Now()
	res, err := s.do(ctx, request{kind: RecordSubmit, order: o})
	if err != nil {
		return OrderResponse{}, err
	}
	return OrderResponse{
		Seq:          res.seq,
		Trades:       res.trades,
		QueueLatency: res.pickedUpAt.Sub(sentAt),
		TotalLatency: res.respondedAt.Sub(sentAt),
	}, nil
}

// Cancel removes a resting order by SeqID, same blocking/cancellation
// semantics as Submit.
func (s *Sequencer) Cancel(ctx context.Context, orderID SeqID) error {
	_, err := s.do(ctx, request{kind: RecordCancel, cancelID: orderID})
	return err
}

// Close stops the sequencer, waits for its goroutine to exit, and closes the
// write-ahead log — returning any error from that final flush, which is the
// last chance to notice that a shutdown lost data. Idempotent; safe to call
// alongside context cancellation.
func (s *Sequencer) Close() error {
	s.once.Do(func() { close(s.quit) })
	<-s.done
	return s.shutdownErr
}

// CommitStats reports how many group commits the sequencer performed and how
// many requests they covered. requests/commits is the mean batch size, i.e.
// how many orders each fsync paid for — the number ADR-003's whole argument
// rests on.
//
// Only call this once the goroutine has exited (after Close, or after a
// receive from Done). The counters are written without synchronization
// because the sequencer goroutine is their only writer; waiting for it to
// finish is what makes reading them safe, and it is why these are not
// exposed as a live metric.
func (s *Sequencer) CommitStats() (commits, requests int64) {
	return s.commits, s.batched
}

// Done returns a channel that's closed once the sequencer's goroutine has
// exited (its context was cancelled, or Close was called). Useful for waiting
// out a clean shutdown.
func (s *Sequencer) Done() <-chan struct{} {
	return s.done
}
