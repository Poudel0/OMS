package oms

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
)

// ErrInvalidSymbol reports a symbol that does not match the accepted format.
// This is a trust-boundary rejection, not a style preference: a symbol
// becomes a directory name under the WAL root, so "../../etc" has to be
// impossible rather than merely discouraged.
var ErrInvalidSymbol = errors.New("oms: invalid symbol")

// ErrTooManySymbols reports that the registry is already tracking MaxSymbols
// instruments and will not lazily create another.
var ErrTooManySymbols = errors.New("oms: too many symbols")

const (
	// maxSymbolLen bounds a symbol at a length no real NEPSE scrip exceeds
	// (the longest are promoter-share tickers like NABILP).
	maxSymbolLen = 12

	// MaxSymbols caps how many instruments one registry will lazily create.
	//
	// This is a resource bound at a trust boundary, not a tuning knob: every
	// new symbol costs a goroutine, a directory, and an fsync, and symbols
	// arrive from network clients. Without a cap, a client sending AAAAA,
	// AAAAB, AAAAC... spawns unbounded goroutines and files.
	//
	// ponytail: a flat cap stands in for the real answer, which is to seed the
	// registry from the exchange's listed-scrip table and refuse anything not
	// on it. NEPSE lists a few hundred scrips, so 512 is comfortably above any
	// legitimate workload while still being a ceiling. Replace this with the
	// scrip list once there is one to read.
	MaxSymbols = 512
)

// validSymbol reports whether s is an acceptable instrument symbol: 1 to
// maxSymbolLen characters, uppercase ASCII letters and digits only.
//
// The allowlist is deliberately narrower than "reject path separators". A
// denylist of dangerous characters is a bet that the list is complete; an
// allowlist of safe ones is not, and the set of characters a real ticker
// needs is tiny.
func validSymbol(s string) bool {
	if len(s) == 0 || len(s) > maxSymbolLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			continue
		}
		if c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

// Registry maps a symbol to the one Sequencer that owns its book, creating
// each lazily on first use.
//
// This is the multi-symbol partitioning story from ADR-005: N symbols run on N
// independent sequencer goroutines with N independent write-ahead logs, so
// unrelated instruments never contend with each other. It is the payoff the
// single-writer design in ADR-002 was chosen for.
type Registry struct {
	// OnTrades, if set, is called with each symbol's trades as they execute,
	// from inside that symbol's sequencer goroutine. See Sequencer.OnTrades: it
	// must not block. Set it before the first Get.
	OnTrades func(symbol string, trades []Trade)

	ctx    context.Context
	walDir string

	mu   sync.RWMutex
	seqs map[string]*Sequencer
}

// NewRegistry returns a registry that stores each symbol's log under
// walDir/<symbol>/. Every sequencer it creates inherits ctx, so cancelling
// ctx shuts all of them down.
//
// An empty walDir means no durability at all — useful for tests and for the
// ADR-002 benchmark configuration, and a footgun anywhere else.
func NewRegistry(ctx context.Context, walDir string) *Registry {
	return &Registry{ctx: ctx, walDir: walDir, seqs: make(map[string]*Sequencer)}
}

// Get returns the sequencer for symbol, creating it — and replaying its log
// into a fresh book — on first use.
//
// Safe for concurrent use. The common case takes only a read lock; the write
// lock is held only while a symbol is created for the first time, and is
// re-checked under it because two callers can both miss the read-locked
// lookup for the same new symbol.
//
// ponytail: creation does its WAL recovery while holding the write lock, so a
// first-ever order in one symbol briefly blocks a first-ever order in another.
// It does not block trading in any symbol already created, which is the case
// that matters. Move to a per-symbol creation lock if cold-start latency
// across many symbols ever shows up as a problem.
func (r *Registry) Get(symbol string) (*Sequencer, error) {
	if !validSymbol(symbol) {
		return nil, fmt.Errorf("%w: %q must be 1-%d chars of A-Z0-9", ErrInvalidSymbol, symbol, maxSymbolLen)
	}

	r.mu.RLock()
	s, ok := r.seqs[symbol]
	r.mu.RUnlock()
	if ok {
		return s, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.seqs[symbol]; ok {
		return s, nil // lost the race to another caller; theirs is fine
	}
	if len(r.seqs) >= MaxSymbols {
		return nil, fmt.Errorf("%w: already tracking %d", ErrTooManySymbols, len(r.seqs))
	}

	s, err := r.newSequencer(symbol)
	if err != nil {
		return nil, err
	}
	r.seqs[symbol] = s
	return s, nil
}

// withFeed attaches the registry's trade callback to a freshly created
// sequencer. It is set here, under the registry's write lock and before the
// sequencer is published to any caller, so no request can be in flight yet.
func (r *Registry) withFeed(symbol string, s *Sequencer) *Sequencer {
	if r.OnTrades != nil {
		s.OnTrades = func(trades []Trade) { r.OnTrades(symbol, trades) }
	}
	return s
}

func (r *Registry) newSequencer(symbol string) (*Sequencer, error) {
	book := NewBook()
	if r.walDir == "" {
		return r.withFeed(symbol, NewSequencer(r.ctx, book)), nil
	}

	// validSymbol has already established that symbol cannot escape walDir.
	dir := filepath.Join(r.walDir, symbol)

	// Recover before opening the writer, not after: OpenWriter truncates a
	// torn tail away, and replay has to see the same records the writer is
	// about to continue from. Recovering an absent directory is not an error
	// — that is what a symbol's first ever order looks like.
	if _, err := Recover(dir, book, 0); err != nil {
		return nil, fmt.Errorf("oms: recover symbol %s: %w", symbol, err)
	}
	w, err := OpenWriter(dir)
	if err != nil {
		return nil, fmt.Errorf("oms: open log for symbol %s: %w", symbol, err)
	}
	return r.withFeed(symbol, NewSequencerWithWAL(r.ctx, book, w)), nil
}

// Symbols returns the symbols this registry has created, in no particular
// order.
func (r *Registry) Symbols() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.seqs))
	for symbol := range r.seqs {
		out = append(out, symbol)
	}
	return out
}

// Close shuts down every sequencer and closes every log, returning all
// failures joined rather than the first one — a shutdown that lost data in
// two symbols should report both.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for symbol, s := range r.seqs {
		if err := s.Close(); err != nil {
			errs = append(errs, fmt.Errorf("symbol %s: %w", symbol, err))
		}
	}
	r.seqs = make(map[string]*Sequencer)
	return errors.Join(errs...)
}
