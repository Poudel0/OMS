// Package replica is the follower side of WAL shipping: it streams a primary's
// write-ahead log and replays it into its own books and its own log, so that it
// can be promoted by hand if the primary is lost. See ADR-006.
package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Poudel0/OMS/internal/api"
	"github.com/Poudel0/OMS/internal/oms"
	"github.com/Poudel0/OMS/internal/pb"
)

const (
	// reconnectDelay is how long to wait before redialling a primary that went
	// away. A primary restart is expected, not exceptional, so this is short.
	reconnectDelay = 250 * time.Millisecond

	// progressInterval is how often the follower tells the primary where it has
	// got to. Purely advisory, so it does not need to be frequent.
	progressInterval = time.Second

	// symbolRefreshInterval is how often the follower re-asks which symbols
	// exist, picking up instruments that started trading after it connected.
	//
	// This is also the worst-case delay before a *brand-new* symbol starts
	// replicating at all, which is what makes it short. It was 5s until the
	// replication tests made the cost obvious: every one of them sat idle waiting
	// for the second discovery pass, because at connect time the symbol had not
	// traded yet and so did not exist. A venue listing a new scrip should not wait
	// seconds for its first order to be protected.
	//
	// The cost is one small RPC twice a second per follower, which is nothing next
	// to the stream it is already carrying.
	symbolRefreshInterval = 500 * time.Millisecond
)

// Replica holds one follower's state: per symbol, a book and a log of its own.
//
// The follower writes received records into its own write-ahead log **under the
// primary's positions**, never renumbered. Two things fall out of that, and they
// are the whole reason it is done this way:
//
//   - Restart needs no separate checkpoint file. The follower's own log tells it
//     where it got to, because its last position IS the primary's position for
//     that record.
//   - Promotion is nearly free. The follower's log is byte-format-identical to a
//     primary's, so promoting it is just pointing `cmd/server -wal` at the same
//     directory. There is no conversion step to get wrong under pressure.
type Replica struct {
	dir string
	log *slog.Logger

	mu      sync.RWMutex
	symbols map[string]*symbolState
}

type symbolState struct {
	book *oms.Book
	wal  *oms.Writer

	// applied guards the book and the log against the one goroutine that feeds
	// them. Each symbol has its own stream goroutine, so this is per symbol, not
	// global.
	mu sync.Mutex

	position   int64
	positionTS time.Time
}

// New returns a follower storing its logs under dir.
func New(dir string, log *slog.Logger) *Replica {
	if log == nil {
		log = slog.Default()
	}
	return &Replica{dir: dir, log: log, symbols: make(map[string]*symbolState)}
}

// Close closes every symbol's log, joining failures rather than returning the
// first.
func (r *Replica) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for symbol, st := range r.symbols {
		st.mu.Lock()
		err := st.wal.Close()
		st.mu.Unlock()
		if err != nil {
			errs = append(errs, fmt.Errorf("symbol %s: %w", symbol, err))
		}
	}
	r.symbols = make(map[string]*symbolState)
	return errors.Join(errs...)
}

// Snapshot returns a symbol's current book depth, for comparing against the
// primary.
func (r *Replica) Snapshot(symbol string) (oms.BookState, bool) {
	st, ok := r.state(symbol)
	if !ok {
		return "", false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.book.Snapshot(), true
}

// Position reports how far a symbol has been applied.
func (r *Replica) Position(symbol string) int64 {
	st, ok := r.state(symbol)
	if !ok {
		return 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.position
}

// Symbols lists the symbols this follower is tracking.
func (r *Replica) Symbols() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.symbols))
	for symbol := range r.symbols {
		out = append(out, symbol)
	}
	return out
}

// RestingCount reports how many orders rest in a symbol's book.
func (r *Replica) RestingCount(symbol string) int {
	st, ok := r.state(symbol)
	if !ok {
		return 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.book.RestingCount()
}

func (r *Replica) state(symbol string) (*symbolState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.symbols[symbol]
	return st, ok
}

// openSymbol prepares a symbol, recovering its book and position from the
// follower's own log. Recovery is what makes a follower restart cheap: it needs
// no state from the primary to know where to resume.
func (r *Replica) openSymbol(symbol string) (*symbolState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.symbols[symbol]; ok {
		return st, nil
	}

	dir := filepath.Join(r.dir, symbol)
	book := oms.NewBook()
	position, err := oms.Recover(dir, book, 0)
	if err != nil {
		return nil, fmt.Errorf("replica: recover %s: %w", symbol, err)
	}
	w, err := oms.OpenWriter(dir)
	if err != nil {
		return nil, fmt.Errorf("replica: open log for %s: %w", symbol, err)
	}

	st := &symbolState{book: book, wal: w, position: position}
	r.symbols[symbol] = st
	return st, nil
}

// apply makes a batch durable and then replays it into the book, in that order —
// the same write-ahead discipline the primary uses, and for the same reason: a
// follower that applied a record it had not stored would forget it on restart and
// resume from a position it had already passed.
//
// Records at or below the current position are skipped rather than rejected. A
// reconnect asks for everything after the last applied position, but a primary is
// free to resend, and idempotence here is cheaper than a protocol that forbids it.
func (st *symbolState) apply(records []oms.Record) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	fresh := make([]oms.Record, 0, len(records))
	for _, rec := range records {
		if rec.Seq <= st.position {
			continue
		}
		fresh = append(fresh, rec)
	}
	if len(fresh) == 0 {
		return nil
	}

	for _, rec := range fresh {
		if err := st.wal.Append(rec); err != nil {
			return fmt.Errorf("replica: append: %w", err)
		}
	}
	// One fsync for the batch, same group-commit reasoning as the primary
	// (ADR-003). Nothing is applied to the book until it is durable.
	if err := st.wal.Sync(); err != nil {
		return fmt.Errorf("replica: sync: %w", err)
	}

	for _, rec := range fresh {
		rec.Apply(st.book)
		st.position = rec.Seq
		st.positionTS = rec.TS
	}
	return nil
}

// Run streams from the primary until ctx ends, reconnecting as needed. It
// returns only when ctx is done, or on an error a retry cannot fix.
func (r *Replica) Run(ctx context.Context, conn pb.ReplicationServiceClient) error {
	// Each symbol gets its own stream goroutine, because the logs are per symbol
	// (ADR-005). No global ordering is needed between symbols, and one lagging
	// symbol cannot stall another.
	var wg sync.WaitGroup
	running := make(map[string]struct{})
	var mu sync.Mutex

	ticker := time.NewTicker(symbolRefreshInterval)
	defer ticker.Stop()

	for {
		symbols, err := r.discover(ctx, conn)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			r.log.WarnContext(ctx, "cannot list symbols on the primary; will retry", "err", err)
		}
		for _, symbol := range symbols {
			mu.Lock()
			_, already := running[symbol]
			if !already {
				running[symbol] = struct{}{}
			}
			mu.Unlock()
			if already {
				continue
			}
			wg.Go(func() { r.followSymbol(ctx, conn, symbol) })
		}

		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
		}
	}
	wg.Wait()
	return ctx.Err()
}

func (r *Replica) discover(ctx context.Context, conn pb.ReplicationServiceClient) ([]string, error) {
	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := conn.ListSymbols(listCtx, &pb.ListSymbolsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetSymbols(), nil
}

// followSymbol keeps one symbol replicated, reconnecting after the primary goes
// away. A primary restart is an expected event, not an error: the follower simply
// redials and resumes from its own position.
func (r *Replica) followSymbol(ctx context.Context, conn pb.ReplicationServiceClient, symbol string) {
	st, err := r.openSymbol(symbol)
	if err != nil {
		r.log.ErrorContext(ctx, "cannot open symbol locally", "symbol", symbol, "err", err)
		return
	}
	go r.reportProgress(ctx, conn, symbol, st)

	for ctx.Err() == nil {
		if err := r.streamOnce(ctx, conn, symbol, st); err != nil {
			if ctx.Err() != nil {
				return
			}
			// DataLoss means the primary's log is damaged. Retrying cannot help
			// and continuing would risk applying records from beyond a hole, so
			// stop this symbol and leave the evidence.
			if status.Code(err) == codes.DataLoss {
				r.log.ErrorContext(ctx, "primary reports a damaged log; stopping this symbol",
					"symbol", symbol, "err", err)
				return
			}
			r.log.WarnContext(ctx, "replication stream ended; reconnecting",
				"symbol", symbol, "position", st.currentPosition(), "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (r *Replica) streamOnce(ctx context.Context, conn pb.ReplicationServiceClient, symbol string, st *symbolState) error {
	stream, err := conn.StreamWAL(ctx, &pb.StreamWALRequest{
		Symbol:        symbol,
		AfterPosition: st.currentPosition(),
	})
	if err != nil {
		return err
	}
	for {
		batch, err := stream.Recv()
		if err != nil {
			return err
		}
		records := make([]oms.Record, 0, len(batch.GetRecords()))
		for _, in := range batch.GetRecords() {
			rec, err := api.RecordFromPB(in)
			if err != nil {
				return fmt.Errorf("replica: decode record at %d: %w", in.GetPosition(), err)
			}
			records = append(records, rec)
		}
		if err := st.apply(records); err != nil {
			return err
		}
	}
}

func (st *symbolState) currentPosition() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.position
}

func (st *symbolState) currentProgress() (int64, time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.position, st.positionTS
}

// reportProgress tells the primary where this symbol has got to. It is advisory:
// failures are logged at debug and never affect replication, because a follower
// that cannot report is still a working follower.
func (r *Replica) reportProgress(ctx context.Context, conn pb.ReplicationServiceClient, symbol string, st *symbolState) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		position, ts := st.currentProgress()
		reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := conn.ReportProgress(reportCtx, &pb.ReportProgressRequest{
			Symbol:                        symbol,
			AppliedPosition:               position,
			AppliedRecordLoggedAtUnixNano: ts.UnixNano(),
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			r.log.DebugContext(ctx, "progress report failed", "symbol", symbol, "err", err)
		}
	}
}
