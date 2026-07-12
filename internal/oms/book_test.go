package oms

import (
	"container/list"
	"reflect"
	"testing"
)

func TestBook(t *testing.T) {
	lvls := []struct {
		name   string
		pushes []Order
		want   []Order
	}{
		{
			name:   "pushes_nil",
			pushes: nil,
			want:   nil,
		},
		{
			name:   "pushes_nil",
			pushes: []Order{},
			want:   nil,
		},
		{
			name: "fifo_orders_preserved",
			pushes: []Order{
				{SeqID: 1, Quantity: 100},
				{SeqID: 2, Quantity: 200},
				{SeqID: 3, Quantity: 300},
			},
			want: []Order{
				{SeqID: 1, Quantity: 100},
				{SeqID: 2, Quantity: 200},
				{SeqID: 3, Quantity: 300},
			},
		},
		{
			name: "pushes_single",
			pushes: []Order{
				{SeqID: 2, Quantity: 50},
			},
			want: []Order{
				{SeqID: 2, Quantity: 50},
			},
		},
	}

	for _, tt := range lvls {
		t.Run(tt.name, func(t *testing.T) {
			lvl := &priceLevel{orders: list.New()}
			for _, o := range tt.pushes {
				lvl.push(o)
			}

			for i, want := range tt.want {
				got, ok := lvl.popFront()
				if !ok {
					t.Fatalf("popFront() #%d: ok = false, want order %+v", i, want)
				}
				if got != want {
					t.Errorf("popFront() #%d = %+v, want %+v", i, got, want)
				}
			}

			if _, ok := lvl.popFront(); ok {
				t.Errorf("popFront() after exhaustion: ok = true, want false")
			}
		})
	}

}

// --- priceLevel: push / front / popFront (FIFO behavior) ---

func TestPriceLevel_EmptyFrontAndPop(t *testing.T) {
	p := &priceLevel{price: 100, orders: list.New()}

	if _, ok := p.front(); ok {
		t.Errorf("front() on empty level = ok true, want false")
	}
	if _, ok := p.popFront(); ok {
		t.Errorf("popFront() on empty level = ok true, want false")
	}
}

func TestPriceLevel_FIFO(t *testing.T) {
	p := &priceLevel{price: 100, orders: list.New()}

	o1 := Order{SeqID: 1}
	o2 := Order{SeqID: 2}
	o3 := Order{SeqID: 3}

	p.push(o1)
	p.push(o2)
	p.push(o3)

	// front() should peek the oldest without removing it.
	if got, ok := p.front(); !ok || got.SeqID != o1.SeqID {
		t.Fatalf("front() = (%v, %v), want (%v, true)", got, ok, o1)
	}
	if got, ok := p.front(); !ok || got.SeqID != o1.SeqID {
		t.Fatalf("front() called twice changed the head: got %v", got)
	}

	// popFront() drains in insertion order.
	want := []Order{o1, o2, o3}
	for i, w := range want {
		got, ok := p.popFront()
		if !ok {
			t.Fatalf("popFront() #%d = ok false, want an order", i)
		}
		if got.SeqID != w.SeqID {
			t.Errorf("popFront() #%d = %v, want %v", i, got, w)
		}
	}

	// Now empty again.
	if _, ok := p.popFront(); ok {
		t.Errorf("popFront() after draining = ok true, want false")
	}
}

// --- getOrCreateLevel: fast path (level already exists) ---

func TestGetOrCreateLevel_FastPathReturnsSame(t *testing.T) {
	b := NewBook()

	first := b.getOrCreateLevel(Buy, 100)
	second := b.getOrCreateLevel(Buy, 100)

	if first != second {
		t.Errorf("second call created a new level; want the same *priceLevel")
	}
	// SSeqIDe shouldn't matter for an existing price — same level comes back.
	third := b.getOrCreateLevel(Sell, 100)
	if third != first {
		t.Errorf("calling with a different sSeqIDe for an existing price returned a different level")
	}

	if len(b.bids) != 1 {
		t.Errorf("bids ladder = %v, want exactly one entry", b.bids)
	}
}

// --- getOrCreateLevel: bids stay sorted DESCENDING ---

func TestGetOrCreateLevel_bidsDescending(t *testing.T) {
	b := NewBook()

	// Insert out of order; ladder must end up descending.
	for _, price := range []Price{90, 100, 70, 110, 80} {
		b.getOrCreateLevel(Buy, price)
	}

	want := []Price{110, 100, 90, 80, 70}
	if !reflect.DeepEqual(b.bids, want) {
		t.Errorf("bids = %v, want %v", b.bids, want)
	}
	if len(b.asks) != 0 {
		t.Errorf("asks = %v, want empty", b.asks)
	}
	// Every inserted price must be indexed in levels.
	for _, price := range want {
		if _, ok := b.levels[price]; !ok {
			t.Errorf("levels missing price %d", price)
		}
	}
}

// --- getOrCreateLevel: asks stay sorted ASCENDING ---

func TestGetOrCreateLevel_AsksAscending(t *testing.T) {
	b := NewBook()

	for _, price := range []Price{90, 100, 70, 110, 80} {
		b.getOrCreateLevel(Sell, price)
	}

	want := []Price{70, 80, 90, 100, 110}
	if !reflect.DeepEqual(b.asks, want) {
		t.Errorf("asks = %v, want %v", b.asks, want)
	}
	if len(b.bids) != 0 {
		t.Errorf("bids = %v, want empty", b.bids)
	}
}

// --- getOrCreateLevel: table-driven insertion-position check ---

func TestGetOrCreateLevel_InsertionOrder(t *testing.T) {
	tests := []struct {
		name    string
		sSeqIDe OrderSide
		inserts []Price
		want    []Price // expected ladder contents in order
	}{
		{
			name:    "bids single",
			sSeqIDe: Buy,
			inserts: []Price{100},
			want:    []Price{100},
		},
		{
			name:    "bids new highest goes first",
			sSeqIDe: Buy,
			inserts: []Price{100, 90, 120},
			want:    []Price{120, 100, 90},
		},
		{
			name:    "bids new lowest goes last",
			sSeqIDe: Buy,
			inserts: []Price{100, 90, 50},
			want:    []Price{100, 90, 50},
		},
		{
			name:    "asks new lowest goes first",
			sSeqIDe: Sell,
			inserts: []Price{100, 110, 90},
			want:    []Price{90, 100, 110},
		},
		{
			name:    "asks new highest goes last",
			sSeqIDe: Sell,
			inserts: []Price{100, 110, 130},
			want:    []Price{100, 110, 130},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBook()
			for _, p := range tt.inserts {
				b.getOrCreateLevel(tt.sSeqIDe, p)
			}

			got := b.bids
			if tt.sSeqIDe == Sell {
				got = b.asks
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ladder = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- BestBSeqID / BestAsk ---

func TestBestBSeqID(t *testing.T) {
	b := NewBook()

	if _, ok := b.BestBid(); ok {
		t.Errorf("BestBSeqID() on empty book = ok true, want false")
	}

	b.getOrCreateLevel(Buy, 90)
	b.getOrCreateLevel(Buy, 110)
	b.getOrCreateLevel(Buy, 100)

	got, ok := b.BestBid()
	if !ok {
		t.Fatalf("BestBSeqID() = ok false, want a price")
	}
	if got != 110 {
		t.Errorf("BestBSeqID() = %d, want 110 (highest bSeqID)", got)
	}
}

func TestBestAsk(t *testing.T) {
	b := NewBook()

	if _, ok := b.BestAsk(); ok {
		t.Errorf("BestAsk() on empty book = ok true, want false")
	}

	b.getOrCreateLevel(Sell, 110)
	b.getOrCreateLevel(Sell, 90)
	b.getOrCreateLevel(Sell, 100)

	got, ok := b.BestAsk()
	if !ok {
		t.Fatalf("BestAsk() = ok false, want a price")
	}
	if got != 90 {
		t.Errorf("BestAsk() = %d, want 90 (lowest ask)", got)
	}
}
