// Package oms implements a single-symbol L3 limit order book: price-time
// priority matching for LIMIT and MARKET orders, O(1) cancel via an
// orderID backref index, and depth snapshots for market data.
package oms

import "time"

// Price is an integer tick — NEPSE instruments trade in discrete price
// increments, so prices are represented as ticks rather than floats to keep
// comparisons and arithmetic exact.
type Price int64

// OrderType is either Limit or Market. The zero value, UnknownType, is
// intentionally invalid so a zero-valued Order is rejected by Submit rather
// than silently treated as one type or the other.
type OrderType int8

// OrderSide is either Buy or Sell. The zero value, UnknownSide, is
// intentionally invalid for the same reason as UnknownType.
type OrderSide int8

// SeqID uniquely identifies an order (and, on Trade, the trade itself)
// within a symbol. It is assigned by the caller in submission order — the
// book relies on ascending SeqIDs to prove price-time priority in tests.
type SeqID int64

const (
	UnknownSide OrderSide = iota
	Sell
	Buy
)
const (
	UnknownType OrderType = iota
	Limit
	Market
)

// Scrip describes a tradeable instrument on the exchange.
type Scrip struct {
	Name            string
	Symbol          string
	LTP             Price // last traded price
	MaxShares       int64
	TradeableShares int64
}

// Trade is one execution resulting from a Submit call. MakerSeqID always
// identifies the order that was already resting in the book; TakerSeqID
// identifies the incoming order that crossed it — the trade always executes
// at the maker's price, never the taker's.
type Trade struct {
	SeqID      SeqID // (Symbol, SeqID) uniquely identifies this trade
	Symbol     string
	Price      Price
	MakerSeqID SeqID
	TakerSeqID SeqID
	MakerAccID string
	TakerAccID string
	Quantity   int64
	TimeStamp  time.Time // metric only, not used in matching
}

// Order is a request to buy or sell Quantity shares of Symbol, either at a
// specific Price (Limit) or at whatever price is currently available
// (Market, where Price is ignored).
type Order struct {
	SeqID     SeqID
	Symbol    string
	Placer    string // account placing the order
	Type      OrderType
	Price     Price
	Quantity  int64
	Side      OrderSide
	TimeStamp time.Time // metric only, not used in matching
}

// OrderBook is the matching engine's public contract for one symbol.
type OrderBook interface {
	// Submit matches o against the resting book and returns every trade it
	// produced. A Limit order's unfilled remainder rests in the book; a
	// Market order's unfilled remainder is dropped.
	Submit(o Order) (trades []Trade, err error)
	// Cancel removes a resting order by SeqID. It returns an error both when
	// the SeqID never existed and when it already reached a terminal state
	// (fully filled or previously cancelled) — the book does not distinguish
	// those two cases.
	Cancel(orderID SeqID) error
	// Snapshot returns the current book depth, for market data / diagnostics.
	Snapshot() BookState
}

// BookState is a serialized depth snapshot returned by OrderBook.Snapshot.
type BookState string
