package oms

import (
	"container/list"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// priceLevel is a FIFO queue of orders resting at one price. Orders are
// consumed from the front (oldest first) and, on partial fill, an unfinished
// maker is pushed back to the front — it keeps its place in line rather than
// losing priority to orders that arrived after it.
type priceLevel struct {
	price  Price
	side   OrderSide
	orders *list.List
}

// OrderRef is a backref into exactly where a resting order lives, so Cancel
// can remove it in O(1) instead of scanning every level. Both fields are
// required: list.Element.Remove needs the *list.List that owns the element,
// and the level's side/price are needed to clean up an emptied price rung.
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

// Book is an L3 limit order book for a single symbol: price-time priority
// matching, O(1) best-price lookup, and O(1) cancel by SeqID.
//
// bids/asks hold only the distinct prices with resting orders, kept sorted
// (bids descending, asks ascending) so the best price is always index 0.
// levels maps a price to its FIFO queue; index maps a resting order's SeqID
// to exactly where it lives, for Cancel.
type Book struct {
	bids   []Price // descending, best bid first
	asks   []Price // ascending, best ask first
	index  map[SeqID]OrderRef
	levels map[Price]*priceLevel
}

var _ OrderBook = (*Book)(nil)

// NewBook returns an empty order book ready to accept orders.
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

// BestBid returns the highest resting buy price, or ok=false if no bids rest.
func (b *Book) BestBid() (Price, bool) {
	if len(b.bids) != 0 {
		return b.bids[0], true
	}
	return 0, false
}

// BestAsk returns the lowest resting sell price, or ok=false if no asks rest.
func (b *Book) BestAsk() (Price, bool) {
	if len(b.asks) != 0 {
		return b.asks[0], true
	}
	return 0, false
}

// RestingCount reports how many orders are currently resting in the book
// (both sides combined). Useful for depth-based load shaping in load
// generators/benchmarks, not used by matching itself.
func (b *Book) RestingCount() int {
	return len(b.index)
}

// depthLevel is one row of an aggregated depth snapshot: total resting
// quantity at a price, without exposing individual order identities.
type depthLevel struct {
	Price    Price `json:"price"`
	Quantity int64 `json:"quantity"`
}

// Snapshot returns the book's current depth (L2: aggregated quantity per
// price, not individual order queue position) as JSON.
func (b *Book) Snapshot() BookState {
	bids := make([]depthLevel, 0, len(b.bids))
	for _, price := range b.bids {
		bids = append(bids, depthLevel{Price: price, Quantity: levelQuantity(b.levels[price])})
	}
	asks := make([]depthLevel, 0, len(b.asks))
	for _, price := range b.asks {
		asks = append(asks, depthLevel{Price: price, Quantity: levelQuantity(b.levels[price])})
	}

	out, err := json.Marshal(struct {
		Bids []depthLevel `json:"bids"`
		Asks []depthLevel `json:"asks"`
	}{Bids: bids, Asks: asks})
	if err != nil {
		// json.Marshal only fails on unsupported types (channels, funcs,
		// cyclic structures) — depthLevel is a flat struct of Price/int64,
		// so this is unreachable in practice.
		return "{}"
	}
	return BookState(out)
}

func levelQuantity(lvl *priceLevel) int64 {
	var total int64
	for e := lvl.orders.Front(); e != nil; e = e.Next() {
		total += e.Value.(Order).Quantity
	}
	return total
}

// Submit matches o against the resting book and returns every trade it
// produced. MARKET and LIMIT orders share one matching loop — the only
// difference is that a LIMIT order stops matching once the best opposite
// price is no longer acceptable, while a MARKET order matches until either
// its quantity is exhausted or the book runs out of liquidity. A LIMIT
// order's unfilled remainder rests in the book; a MARKET order's unfilled
// remainder is dropped — there is no price at which it would wait.
func (b *Book) Submit(o Order) (trades []Trade, err error) {
	if o.Side == UnknownSide || o.Type == UnknownType {
		return nil, errors.New("oms: order has unknown side or type")
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
		lvl := b.getOrCreateLevel(o.Side, o.Price)
		e := lvl.push(o)
		b.index[o.SeqID] = OrderRef{level: lvl, element: e}
	}
	// ponytail: unfilled MARKET remainder is dropped, not logged/persisted — revisit if an audit trail becomes a requirement
	return trades, nil
}

// matchAgainstAsks matches a Buy taker against resting Sell orders.
func (b *Book) matchAgainstAsks(taker Order) (trades []Trade, remainder Order, err error) {
	for taker.Quantity > 0 {
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

// matchAgainstBids matches a Sell taker against resting Buy orders.
func (b *Book) matchAgainstBids(taker Order) (trades []Trade, remainder Order, err error) {
	for taker.Quantity > 0 {
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

// Cancel removes a resting order by SeqID in O(1) via the index backref.
//
// ponytail: an unknown SeqID and a SeqID that already reached a terminal
// state (fully filled, or already cancelled) are indistinguishable here —
// both simply have no index entry. A real venue might want to tell a
// cancel-reject ("too late, it already traded") apart from a bad ID, which
// would need tracking terminal orders separately. Not needed yet.
func (b *Book) Cancel(orderID SeqID) error {
	ref, ok := b.index[orderID]
	if !ok {
		return errors.New("oms: order not found")
	}
	ref.level.orders.Remove(ref.element)
	delete(b.index, orderID)
	if ref.level.orders.Len() == 0 {
		b.removeLevel(ref.level.side, ref.level.price)
	}
	return nil
}
