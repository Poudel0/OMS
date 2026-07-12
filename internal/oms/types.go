package oms

import "time"

type Price int64
type OrderType int8
type OrderSide int8
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

type Scrip struct {
	Name            string
	Symbol          string
	LTP             Price
	MaxShares       int64
	TradeableShares int64
}

type Trade struct {
	SeqID      SeqID //(symbol,seqid) is unique
	Symbol     string
	Price      Price
	MakerSeqID SeqID //Always the restingf order, taker matches againt this not other way around
	TakerSeqID SeqID
	MakerAccID string
	TakerAccID string
	Quantity   int64
	TimeStamp  time.Time //metric only no matching
}

type Order struct {
	SeqID     SeqID
	Symbol    string
	Placer    string
	Type      OrderType
	Price     Price
	Quantity  int64
	Side      OrderSide
	TimeStamp time.Time //metric only no matching
}

type OrderBook interface {
	Submit(o Order) (trades []Trade, err error)
	Cancel(orderID SeqID) error
	Snapshot() BookState
}

type BookState string
