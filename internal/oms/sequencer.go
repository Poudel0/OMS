package oms

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	Seq int64
	// OrderID is the order's identifier: either the one the caller supplied,
	// or the one the sequencer assigned if the caller left it zero.
	OrderID SeqID
	Trades  []Trade
	// SelfPrevented lists this account's own resting orders that were cancelled
	// because the incoming order would otherwise have traded with itself. See
	// Book.SubmitOrder.
	SelfPrevented []SeqID
	// RestingQuantity is how much of the order stayed in the book.
	RestingQuantity int64
	// QueueLatency is how long the request waited before the sequencer
	// goroutine started processing its batch — time spent behind other
	// requests.
	QueueLatency time.Duration
	// TotalLatency is the full round trip: from the caller invoking Submit
	// to the reply being received back. With a WAL attached this includes
	// the batch's fsync.
	TotalLatency time.Duration
}

// reqKind is what a request asks the sequencer to do. It is deliberately not
// RecordKind: a snapshot is a request but never a log record, and conflating the
// two would put a value in the log format's enum that the log can never hold.
type reqKind uint8

const (
	reqSubmit reqKind = iota + 1
	reqCancel
	reqSnapshot
)

// request is one unit of work for the sequencer goroutine. Submits and cancels
// share a single struct and a single channel on purpose: the WAL has to record
// mutations in exactly the order the book applies them, and one channel gets
// that from the channel's own FIFO guarantee. Two channels selected over would
// let a cancel sent later overtake a submit sent earlier, because a Go select
// among ready cases picks at random.
type request struct {
	kind     reqKind
	order    Order  // kind == reqSubmit
	cancelID SeqID  // kind == reqCancel
	cancelBy string // kind == reqCancel; "" means skip the ownership check
	seq      int64  // assigned by the sequencer at commit time
	orderID  SeqID  // assigned by the sequencer at commit time, if it assigns
	// preErr is set when the order was rejected before it reached the log —
	// today only by the pre-trade check. Such an order is one the venue never
	// accepted, so it gets no log position and never touches the book.
	preErr error
	reply  chan result
}

type result struct {
	state         BookState
	seq           int64
	orderID       SeqID
	trades        []Trade
	selfPrevented []SeqID
	restingQty    int64
	err           error
	pickedUpAt    time.Time
	respondedAt   time.Time
}

// ErrNotOrderOwner reports a cancel for an order placed by a different
// account.
//
// The account making the claim is not authenticated in v1 (see ADR-005), so
// this check is only as strong as the account_id the caller sent. It is still
// worth having now rather than later: order IDs are sequential integers, so
// without it any client could cancel any other client's orders by guessing,
// and adding authentication later makes this check real without changing the
// logic around it.
var ErrNotOrderOwner = errors.New("oms: order belongs to another account")

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

	// Set once by attach, before the sequencer is published to any caller, and
	// never written again. They need no lock: the goroutine only reads them
	// while handling a request, which necessarily happens after the send that
	// delivered it, which happens after the write in attach.
	symbol   string
	accounts *Accounts
	onTrades func([]Trade)

	// Owned exclusively by the sequencer goroutine.
	seq         int64
	nextOrderID SeqID
	walErr      error

	// Live counters. Written only by the sequencer goroutine, but atomically,
	// so a metrics scrape can read them without a round trip through the request
	// channel. ADR-003 deferred this as "needs atomics or a report-from-inside
	// channel"; two atomic adds per group commit is nothing beside the fsync the
	// same commit pays for, and it is the difference between a live gauge and a
	// number only available after shutdown.
	commits atomic.Int64 // group commits performed
	batched atomic.Int64 // requests those commits covered
	resting atomic.Int64 // orders currently in the book
	lastPos atomic.Int64 // highest log position assigned

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
	// Resume order-ID assignment above every order the book already holds, so
	// a restart cannot hand a new order the ID of one still resting.
	s.nextOrderID = book.MaxOrderID()
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
	s.commits.Add(1)
	s.batched.Add(int64(len(batch)))

	// Order IDs are assigned before the log is written, never after: the log
	// has to carry the same ID the caller was told about, or replay would
	// resurrect the order under a different identity and no cancel would ever
	// find it again.
	for i := range batch {
		if batch[i].kind != reqSubmit {
			continue
		}
		if batch[i].order.SeqID == 0 {
			s.nextOrderID++
			batch[i].order.SeqID = s.nextOrderID
		}
		batch[i].orderID = batch[i].order.SeqID
		// One sequencer owns exactly one symbol, so normalise rather than trust
		// the caller's field. Reservations are keyed by symbol, and a mismatch
		// here would strand a hold under a key nothing releases.
		if s.symbol != "" {
			batch[i].order.Symbol = s.symbol
		}
	}

	// Pre-trade check-and-reserve, before anything is logged. A rejected order
	// is one the venue never accepted, so it must get no log position and never
	// reach the book — replay does not re-run balance checks, so a rejected
	// order in the log would be applied on recovery and the recovered book would
	// diverge from the one that was lost.
	if s.accounts != nil {
		for i := range batch {
			if batch[i].kind != reqSubmit {
				continue
			}
			if err := s.accounts.Reserve(batch[i].order); err != nil {
				batch[i].preErr = err
			}
		}
	}

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
		res := result{seq: req.seq, orderID: req.orderID, pickedUpAt: pickedUpAt}
		switch {
		case req.kind == reqSnapshot:
			// A read, so it is never logged and cannot fail. Serving it from
			// this goroutine is what makes it race-free: the book has exactly
			// one reader-writer, and s.seq is owned by this goroutine too.
			res.state = s.book.Snapshot()
			res.seq = s.seq
		case req.preErr != nil:
			// Rejected before the log. Reserve failed, so there is no hold to
			// release.
			res.err = req.preErr
		case s.walErr != nil:
			res.err = s.walErr
			// Reserved, then the log refused it. The order is not accepted, so
			// the hold must go back.
			s.release(req)
		case req.kind == reqCancel:
			res.err = s.book.CancelOwned(req.cancelID, req.cancelBy)
			if res.err == nil && s.accounts != nil {
				s.accounts.Release(s.symbol, req.cancelID)
			}
		default:
			mr, err := s.book.SubmitOrder(req.order)
			res.trades, res.selfPrevented, res.restingQty, res.err = mr.Trades, mr.SelfPrevented, mr.RestingQuantity, err
			if s.accounts != nil {
				if err != nil {
					s.release(req)
				} else {
					// One atomic step: move the value, shrink both sides' holds
					// by what actually moved, free the taker's remainder unless
					// it is still resting, and drop the holds of any resting
					// orders self-trade prevention cancelled.
					s.accounts.Complete(s.symbol, req.orderID, mr.Trades, mr.RestingQuantity, mr.SelfPrevented)
				}
			}
			if len(res.trades) > 0 && s.onTrades != nil {
				s.onTrades(res.trades)
			}
		}
		res.respondedAt = time.Now()
		req.reply <- res
	}

	// One map-length read per batch rather than per request, inside the
	// goroutine that owns the book.
	s.resting.Store(int64(s.book.RestingCount()))
	s.lastPos.Store(s.seq)
}

// release drops a submit's reservation. Cancels never hold one of their own.
func (s *Sequencer) release(req request) {
	if s.accounts != nil && req.kind == reqSubmit {
		s.accounts.Release(s.symbol, req.orderID)
	}
}

// attach wires the per-symbol collaborators a Registry owns. It must be called
// before the sequencer is handed to any caller, which is what makes writing
// these fields without a lock safe — see the field comments.
func (s *Sequencer) attach(symbol string, accounts *Accounts, onTrades func([]Trade)) {
	s.symbol = symbol
	s.accounts = accounts
	s.onTrades = onTrades
}

func (s *Sequencer) logBatch(batch []request, ts time.Time) error {
	for i := range batch {
		if batch[i].preErr != nil {
			continue // never accepted, so it consumes no log position
		}
		if batch[i].kind == reqSnapshot {
			continue // a read changes nothing, so there is nothing to log
		}
		s.seq++
		batch[i].seq = s.seq
		rec := Record{Seq: s.seq, Kind: RecordSubmit, TS: ts}
		if batch[i].kind == reqCancel {
			rec.Kind = RecordCancel
			rec.CancelID = batch[i].cancelID
			rec.CancelBy = batch[i].cancelBy
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
//
// If o.SeqID is zero the sequencer assigns the order ID itself and reports it
// as OrderResponse.OrderID. That is how anything reachable from the network
// should submit: order IDs are sequential, so letting a client choose its own
// would let it claim an identifier another account's order already holds. A
// non-zero o.SeqID is honoured as-is, which is what the benchmarks and the
// book's own tests rely on.
func (s *Sequencer) Submit(ctx context.Context, o Order) (OrderResponse, error) {
	sentAt := time.Now()
	res, err := s.do(ctx, request{kind: reqSubmit, order: o})
	if err != nil {
		return OrderResponse{}, err
	}
	return OrderResponse{
		Seq:             res.seq,
		OrderID:         res.orderID,
		Trades:          res.trades,
		SelfPrevented:   res.selfPrevented,
		RestingQuantity: res.restingQty,
		QueueLatency:    res.pickedUpAt.Sub(sentAt),
		TotalLatency:    res.respondedAt.Sub(sentAt),
	}, nil
}

// Cancel removes a resting order by ID, with no ownership check. Use
// CancelFor for anything that came off the network.
func (s *Sequencer) Cancel(ctx context.Context, orderID SeqID) error {
	_, err := s.do(ctx, request{kind: reqCancel, cancelID: orderID})
	return err
}

// CancelFor removes a resting order only if account placed it, failing with
// ErrNotOrderOwner otherwise. Same blocking and cancellation semantics as
// Submit.
//
// The check happens inside the sequencer goroutine, so the lookup of the
// order's owner and its removal cannot be separated by another mutation —
// there is no window in which the order could be filled and its ID reused
// between the two.
func (s *Sequencer) CancelFor(ctx context.Context, orderID SeqID, account string) (int64, error) {
	res, err := s.do(ctx, request{kind: reqCancel, cancelID: orderID, cancelBy: account})
	return res.seq, err
}

// Snapshot returns the symbol's current book depth, served from inside the
// sequencer goroutine so that it cannot race a mutation. It is a read: nothing
// is logged and no position is consumed.
// It also reports the log position the snapshot was taken at, which is what
// makes two nodes' snapshots comparable after a failover.
func (s *Sequencer) Snapshot(ctx context.Context) (BookState, int64, error) {
	res, err := s.do(ctx, request{kind: reqSnapshot})
	if err != nil {
		return "", 0, err
	}
	return res.state, res.seq, nil
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
// Safe to call at any time, from any goroutine.
func (s *Sequencer) CommitStats() (commits, requests int64) {
	return s.commits.Load(), s.batched.Load()
}

// RestingOrders reports how many orders are currently in the book, as of the
// last completed batch. Safe to call at any time; a moment stale by design,
// which is what a gauge wants.
func (s *Sequencer) RestingOrders() int64 { return s.resting.Load() }

// LastPosition reports the highest log position assigned, or 0 without a log.
func (s *Sequencer) LastPosition() int64 { return s.lastPos.Load() }

// Done returns a channel that's closed once the sequencer's goroutine has
// exited (its context was cancelled, or Close was called). Useful for waiting
// out a clean shutdown.
func (s *Sequencer) Done() <-chan struct{} {
	return s.done
}
