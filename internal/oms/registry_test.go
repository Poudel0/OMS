package oms

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func TestRegistry_RejectsMalformedSymbols(t *testing.T) {
	reg := NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()

	bad := []struct {
		symbol string
		why    string
	}{
		{"", "empty"},
		{"../etc", "path traversal"},
		{"../../../etc/passwd", "deep path traversal"},
		{"NABIL/../ADBL", "embedded traversal"},
		{"nabil", "lowercase"},
		{"NA BIL", "space"},
		{"NABIL.", "punctuation"},
		{"NAB-IL", "hyphen"},
		{"..", "parent dir"},
		{".", "current dir"},
		{"NABIL\x00X", "null byte"},
		{"TOOLONGSYMBOLNAME", "over length cap"},
	}
	for _, tc := range bad {
		if _, err := reg.Get(tc.symbol); !errors.Is(err, ErrInvalidSymbol) {
			t.Errorf("Get(%q) [%s] = %v, want ErrInvalidSymbol", tc.symbol, tc.why, err)
		}
	}
	if got := reg.Symbols(); len(got) != 0 {
		t.Errorf("Symbols() = %v, want none created", got)
	}
}

func TestRegistry_RejectedSymbolCreatesNothingOnDisk(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	reg := NewRegistry(t.Context(), walDir, nil)
	defer reg.Close()

	// The decisive check for the traversal guard: a rejected symbol must not
	// have written anywhere, least of all outside the WAL root.
	if _, err := reg.Get("../escaped"); !errors.Is(err, ErrInvalidSymbol) {
		t.Fatalf("Get() = %v, want ErrInvalidSymbol", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "wal" {
			t.Errorf("unexpected entry %q created outside the wal dir", e.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(root, "escaped")); !os.IsNotExist(err) {
		t.Error("a directory escaped the wal root")
	}
}

func TestRegistry_AcceptsRealisticSymbols(t *testing.T) {
	reg := NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()

	for _, symbol := range []string{"NABIL", "ADBL", "NRIC", "NABILP", "HBL", "UPPER30"} {
		if _, err := reg.Get(symbol); err != nil {
			t.Errorf("Get(%q) error = %v, want nil", symbol, err)
		}
	}
}

func TestRegistry_SameSymbolReturnsSameSequencer(t *testing.T) {
	reg := NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()

	first, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	second, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Error("Get() returned two different sequencers for one symbol — the book would be split in two")
	}
}

func TestRegistry_SymbolsAreIsolatedFromEachOther(t *testing.T) {
	ctx := t.Context()
	reg := NewRegistry(ctx, t.TempDir(), nil)
	defer reg.Close()

	nabil, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	adbl, err := reg.Get("ADBL")
	if err != nil {
		t.Fatal(err)
	}

	// Crossing prices in one symbol must not match against the other.
	if _, err := nabil.Submit(ctx, Order{SeqID: 1, Symbol: "NABIL", Type: Limit, Price: 500, Quantity: 10, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	resp, err := adbl.Submit(ctx, Order{SeqID: 1, Symbol: "ADBL", Type: Limit, Price: 500, Quantity: 10, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Trades) != 0 {
		t.Errorf("Trades = %+v, want none — an ADBL buy matched a NABIL sell", resp.Trades)
	}

	// And each keeps its own log position sequence, rather than sharing one.
	if resp.Seq != 1 {
		t.Errorf("ADBL first log position = %d, want 1 (positions are per-symbol)", resp.Seq)
	}
}

func TestRegistry_ConcurrentGetOfTheSameSymbolCreatesOne(t *testing.T) {
	reg := NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()

	// Two callers can both miss the read-locked lookup for a new symbol; the
	// re-check under the write lock is what stops that becoming two books.
	const callers = 32
	var wg sync.WaitGroup
	got := make([]*Sequencer, callers)
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := reg.Get("NABIL")
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			got[i] = s
		}(i)
	}
	wg.Wait()

	for i := 1; i < callers; i++ {
		if got[i] != got[0] {
			t.Fatalf("caller %d got a different sequencer — the symbol was created more than once", i)
		}
	}
	if symbols := reg.Symbols(); len(symbols) != 1 {
		t.Errorf("Symbols() = %v, want exactly one", symbols)
	}
}

func TestRegistry_EnforcesSymbolCap(t *testing.T) {
	reg := NewRegistry(t.Context(), "", nil) // no WAL: this test is about the cap, not durability
	defer reg.Close()

	for i := range MaxSymbols {
		symbol := "S" + strconv.Itoa(i)
		if _, err := reg.Get(symbol); err != nil {
			t.Fatalf("Get(%q) error = %v, want nil below the cap", symbol, err)
		}
	}
	if _, err := reg.Get("ONEMORE"); !errors.Is(err, ErrTooManySymbols) {
		t.Errorf("Get() past the cap = %v, want ErrTooManySymbols", err)
	}
	// An already-created symbol must still work once the cap is reached —
	// the cap bounds new instruments, not trading.
	if _, err := reg.Get("S0"); err != nil {
		t.Errorf("Get() of an existing symbol at the cap = %v, want nil", err)
	}
}

func TestRegistry_RecoversEachSymbolIndependentlyAcrossRestart(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	reg1 := NewRegistry(ctx, dir, nil)
	nabil, err := reg1.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	adbl, err := reg1.Get("ADBL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nabil.Submit(ctx, Order{SeqID: 1, Symbol: "NABIL", Type: Limit, Price: 500, Quantity: 10, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	if _, err := nabil.Submit(ctx, Order{SeqID: 2, Symbol: "NABIL", Type: Limit, Price: 499, Quantity: 20, Side: Buy}); err != nil {
		t.Fatal(err)
	}
	if _, err := adbl.Submit(ctx, Order{SeqID: 1, Symbol: "ADBL", Type: Limit, Price: 300, Quantity: 5, Side: Sell}); err != nil {
		t.Fatal(err)
	}
	if err := reg1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Restart: a fresh registry over the same directory must rebuild each
	// symbol's book from that symbol's own log, and resume its positions.
	reg2 := NewRegistry(ctx, dir, nil)
	defer reg2.Close()

	nabil2, err := reg2.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	if got := nabil2.book.RestingCount(); got != 2 {
		t.Errorf("NABIL RestingCount() after restart = %d, want 2", got)
	}
	if bid, _ := nabil2.book.BestBid(); bid != 500 {
		t.Errorf("NABIL BestBid() after restart = %d, want 500", bid)
	}

	adbl2, err := reg2.Get("ADBL")
	if err != nil {
		t.Fatal(err)
	}
	if got := adbl2.book.RestingCount(); got != 1 {
		t.Errorf("ADBL RestingCount() after restart = %d, want 1", got)
	}
	if ask, _ := adbl2.book.BestAsk(); ask != 300 {
		t.Errorf("ADBL BestAsk() after restart = %d, want 300", ask)
	}

	// NABIL wrote two records, so its next position is 3; ADBL wrote one, so
	// its next is 2. Sharing a counter would show up right here.
	resp, err := nabil2.Submit(ctx, Order{SeqID: 3, Symbol: "NABIL", Type: Limit, Price: 498, Quantity: 1, Side: Buy})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 3 {
		t.Errorf("NABIL next log position = %d, want 3", resp.Seq)
	}
	resp, err = adbl2.Submit(ctx, Order{SeqID: 2, Symbol: "ADBL", Type: Limit, Price: 301, Quantity: 1, Side: Sell})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 2 {
		t.Errorf("ADBL next log position = %d, want 2", resp.Seq)
	}
}

func TestRegistry_EachSymbolGetsItsOwnLogDirectory(t *testing.T) {
	dir := t.TempDir()
	reg := NewRegistry(t.Context(), dir, nil)
	for _, symbol := range []string{"NABIL", "ADBL"} {
		if _, err := reg.Get(symbol); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.Close(); err != nil {
		t.Fatal(err)
	}

	for _, symbol := range []string{"NABIL", "ADBL"} {
		segs, err := segmentPaths(filepath.Join(dir, symbol))
		if err != nil {
			t.Fatal(err)
		}
		if len(segs) == 0 {
			t.Errorf("symbol %s has no log segment of its own", symbol)
		}
	}
}
