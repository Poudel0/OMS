package oms

import "sync"

// MutexBook wraps a Book with a single mutex guarding every call. It exists
// purely as a comparison baseline for ADR-002 — see docs/adr/0002-single-writer-sequencer.md
// and BenchmarkMutex_* / BenchmarkSequencer_* in concurrency_bench_test.go.
type MutexBook struct {
	mu   sync.Mutex
	book *Book
}

// NewMutexBook returns an empty, mutex-guarded order book.
func NewMutexBook() *MutexBook {
	return &MutexBook{book: NewBook()}
}

func (m *MutexBook) Submit(o Order) ([]Trade, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.book.Submit(o)
}

func (m *MutexBook) Cancel(orderID SeqID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.book.Cancel(orderID)
}

func (m *MutexBook) Snapshot() BookState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.book.Snapshot()
}

func (m *MutexBook) RestingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.book.RestingCount()
}

var _ OrderBook = (*MutexBook)(nil)
