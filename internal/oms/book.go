package oms

import (
	"container/list"
	"errors"
	"sort"
	"time"
)

type priceLevel struct {
	price  Price
	side   OrderSide
	orders *list.List
}

type OrderRef struct {
	level   *priceLevel
	element *list.Element
}

func (p *priceLevel) push(o Order) *list.Element {
	return p.orders.PushBack(o)
}
func (p *priceLevel) pushFront(o Order) *list.Element {
	return p.orders.PushFront(o)
}

func (p *priceLevel) front() (Order, bool) {
	e := p.orders.Front()
	if e == nil {
		return Order{}, false
	}
	return e.Value.(Order), true
}

func (p *priceLevel) popFront() (Order, bool) {
	e := p.orders.Front()
	if e == nil {
		return Order{}, false
	}
	return p.orders.Remove(e).(Order), true
}

type Book struct {
	bids   []Price // decending best bid first
	asks   []Price // ascending best ask first
	index  map[SeqID]OrderRef
	levels map[Price]*priceLevel
}

func NewBook() *Book {
	return &Book{levels: make(map[Price]*priceLevel), index: make(map[SeqID]OrderRef)}
}

func (b *Book) getOrCreateLevel(side OrderSide, price Price) *priceLevel {
	// 1. Fast path: level already exists.
	if lvl, ok := b.levels[price]; ok {
		return lvl
	}

	// 2. Slow path: pick the ladder for this side.
	//    Work on a *pointer to* the slice field so the write-back in
	//    step 6 hits the actual Book field, not a copy.
	var ladder *[]Price
	var i int
	if side == Buy {
		ladder = &b.bids
		// 3. bids descending → first i where bids[i] <= price.
		i = sort.Search(len(b.bids), func(j int) bool {
			return b.bids[j] <= price
		})
	} else {
		ladder = &b.asks
		// 3. asks ascending → first i where asks[i] >= price.
		i = sort.Search(len(b.asks), func(j int) bool {
			return b.asks[j] >= price
		})
	}

	lvl := &priceLevel{price: price, side: side, orders: list.New()}

	// 5 + 6. Insert price into the ladder at index i, write back.
	s := *ladder
	s = append(s, 0)     // grow by one
	copy(s[i+1:], s[i:]) // shift right from i
	s[i] = price         // drop into the gap
	*ladder = s          // reassign grown/shifted slice onto the field

	b.levels[price] = lvl

	return lvl
}

func (b *Book) removeLevel(side OrderSide, price Price) bool {
	if _, ok := b.levels[price]; !ok {
		return false
	}
	var ladder *[]Price
	var i int
	if side == Buy {
		ladder = &b.bids
		i = sort.Search(len(b.bids), func(j int) bool {
			return b.bids[j] <= price
		})
	} else {
		ladder = &b.asks
		i = sort.Search(len(b.asks), func(j int) bool {
			return b.asks[j] >= price
		})
	}
	// ladder[i] == price is guaranteed since caller only removes existing levels
	*ladder = append((*ladder)[:i], (*ladder)[i+1:]...)
	delete(b.levels, price)
	return true
}

func (b *Book) BestBid() (Price, bool) {
	if len(b.bids) != 0 {
		return b.bids[0], true
	}
	return 0, false
}

func (b *Book) BestAsk() (Price, bool) {
	if len(b.asks) != 0 {
		return b.asks[0], true
	}
	return 0, false
}

func (b *Book) Submit(o Order) (trades []Trade, err error) {
	if o.Side == UnknownSide || o.Type == UnknownType {
		return nil, errors.New("Unknown Side or Type")
	}
	switch o.Side {
	case Buy:
		trades, o, err = b.matchAgainstAsks(o)
	case Sell:
		trades, o, err = b.matchAgainstBids(o)
	}
	if err != nil {
		return trades, err
	}
	if o.Type == Limit && o.Quantity > 0 {
		lvl := b.getOrCreateLevel(o.Side, o.Price) // leftover rests
		e := lvl.push(o)
		b.index[o.SeqID] = OrderRef{
			level:   lvl,
			element: e,
		}

		// Market leftover: do nothing, it just evaporates
		// ponytail: unfilled market remainder is dropped, not logged/persisted — revisit if audit trail becomes a requirement
	}
	return trades, nil
}

// taker is buy order
func (b *Book) matchAgainstAsks(taker Order) (trades []Trade, remainder Order, err error) {
	for taker.Quantity > 0 {
		// var askingPrice Price
		askingPrice, ok := b.BestAsk()
		if !ok {
			break
		}
		if taker.Type == Limit && askingPrice > taker.Price {
			break
		}
		lvl := b.levels[askingPrice]
		maker, _ := lvl.popFront()
		delete(b.index, maker.SeqID)

		fill := min(taker.Quantity, maker.Quantity)

		trades = append(trades, Trade{
			Symbol:     taker.Symbol,
			Price:      askingPrice, // maker's price wins
			MakerSeqID: maker.SeqID,
			TakerSeqID: taker.SeqID,
			MakerAccID: maker.Placer,
			TakerAccID: taker.Placer,
			Quantity:   fill,
			TimeStamp:  time.Now(),
		})

		taker.Quantity -= fill
		maker.Quantity -= fill

		if maker.Quantity > 0 {
			e := lvl.pushFront(maker) // maker not done, stays first in line
			b.index[maker.SeqID] = OrderRef{level: lvl, element: e}
		}
		if lvl.orders.Len() == 0 {
			b.removeLevel(Sell, askingPrice)
		}

	}
	return trades, taker, nil
}

// taker is sell order
func (b *Book) matchAgainstBids(taker Order) (trades []Trade, remainder Order, err error) {
	for taker.Quantity > 0 {
		// var askingPrice Price
		biddingPrice, ok := b.BestBid()
		if !ok {
			break
		}
		if taker.Type == Limit && biddingPrice < taker.Price {
			break
		}
		lvl := b.levels[biddingPrice]
		maker, _ := lvl.popFront()
		delete(b.index, maker.SeqID)

		fill := min(taker.Quantity, maker.Quantity)

		trades = append(trades, Trade{
			Symbol:     taker.Symbol,
			Price:      biddingPrice, // maker's price wins
			MakerSeqID: maker.SeqID,
			TakerSeqID: taker.SeqID,
			MakerAccID: maker.Placer,
			TakerAccID: taker.Placer,
			Quantity:   fill,
			TimeStamp:  time.Now(),
		})

		taker.Quantity -= fill
		maker.Quantity -= fill

		if maker.Quantity > 0 {
			e := lvl.pushFront(maker) // maker not done, stays first in line
			b.index[maker.SeqID] = OrderRef{level: lvl, element: e}
		}
		if lvl.orders.Len() == 0 {
			b.removeLevel(Buy, biddingPrice)
		}

	}
	return trades, taker, nil
}

func (b *Book) Cancel(orderID SeqID) error {
	ref, ok := b.index[orderID]
	if !ok {
		return errors.New("order not found") // need to handle wrong seqid entered vs cancel reject— they just lost a race against the matching engine, which is completely normal and expected in a live trading system
	}
	ref.level.orders.Remove(ref.element)
	delete(b.index, orderID)
	if ref.level.orders.Len() == 0 {
		b.removeLevel(ref.level.side, ref.level.price)
	}
	return nil
}
