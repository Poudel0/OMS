package oms

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"sync"
)

// ErrInsufficientFunds and ErrInsufficientShares are the two pre-trade
// rejections: a buyer without the cash to pay, and a seller without the shares
// to deliver.
var (
	ErrInsufficientFunds  = errors.New("oms: insufficient funds")
	ErrInsufficientShares = errors.New("oms: insufficient shares")
	ErrUnknownAccount     = errors.New("oms: unknown account")
)

// Accounts records what each account owns and what is currently committed to
// live orders. It backs the venue's one inline pre-trade check, and it is
// deliberately not a risk engine.
//
// # Why reservations exist
//
// A balance check that only reads the balance is wrong for two reasons, and the
// second is the one that bites:
//
//   - Two concurrent orders from one account can both pass a check that only
//     one of them could afford, because nothing is held between the check and
//     the match.
//   - Worse, a *resting* order has not spent anything yet. An account with 5,000
//     can rest ten buys worth 5,000 each and pass every check, because no cash
//     moves until they fill. Serializing the checks would not help at all here.
//
// So the check is a check-and-reserve: it atomically verifies that
// `owned - reserved` covers the order and then holds that amount against the
// order's ID. The hold survives while the order rests, shrinks as the order
// fills (the value having actually moved), and is released when the order is
// cancelled, rejected, or otherwise finished.
//
// Every method takes the same lock, so a check-and-reserve is atomic against
// every other account operation — including ones from a different symbol's
// sequencer, which matters because cash is shared across symbols even though
// books are not.
//
// ponytail: in-memory, so holds and balances are lost on restart while the
// journal survives. LoadBalances rebuilds the balances; holds are rebuilt by
// the registry re-reserving each recovered resting order as it replays. What is
// still missing is any *durable* record of an account opening or a deposit —
// those live only here and in whatever seeded them.
type Accounts struct {
	mu        sync.Mutex
	cash      map[string]int64
	positions map[string]map[string]int64

	// What is committed to live orders and therefore unavailable.
	reservedCash   map[string]int64
	reservedShares map[string]map[string]int64

	// holds tracks each live order's outstanding reservation so it can be
	// reduced on fill and released on cancel.
	holds map[holdKey]*hold
}

type holdKey struct {
	symbol  string
	orderID SeqID
}

// hold is one order's outstanding reservation.
type hold struct {
	account string
	symbol  string
	side    OrderSide
	// price is what the reservation was computed at, which for a buy is the
	// order's own limit price — not the price it eventually trades at. A taker
	// that fills below its limit releases more than it spends, which is correct:
	// the difference was never owed.
	price Price
	qty   int64 // quantity still reserved
}

// NewAccounts returns an empty account store.
func NewAccounts() *Accounts {
	return &Accounts{
		cash:           make(map[string]int64),
		positions:      make(map[string]map[string]int64),
		reservedCash:   make(map[string]int64),
		reservedShares: make(map[string]map[string]int64),
		holds:          make(map[holdKey]*hold),
	}
}

// Deposit adds cash (in the same integer ticks as prices) to an account,
// creating it if it does not exist.
func (a *Accounts) Deposit(account string, amount int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cash[account] += amount
}

// SetPosition sets an account's holding in one symbol, creating the account if
// it does not exist.
func (a *Accounts) SetPosition(account, symbol string, qty int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.setPositionLocked(account, symbol, qty)
}

func (a *Accounts) setPositionLocked(account, symbol string, qty int64) {
	if a.positions[account] == nil {
		a.positions[account] = make(map[string]int64)
	}
	a.positions[account][symbol] = qty
}

// Cash reports an account's total balance, including amounts reserved against
// live orders. Use AvailableCash for what a new order can actually spend.
func (a *Accounts) Cash(account string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cash[account]
}

// Position reports an account's total holding in one symbol, including shares
// committed to live sell orders.
func (a *Accounts) Position(account, symbol string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.positions[account][symbol]
}

// AvailableCash is cash not already committed to a live order.
func (a *Accounts) AvailableCash(account string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cash[account] - a.reservedCash[account]
}

// AvailablePosition is shares not already committed to a live sell order.
func (a *Accounts) AvailablePosition(account, symbol string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.positions[account][symbol] - a.reservedShares[account][symbol]
}

// ReservedCash and ReservedPosition expose the holds, for diagnostics and for
// tests that need to prove a reservation was released rather than leaked.
func (a *Accounts) ReservedCash(account string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reservedCash[account]
}

func (a *Accounts) ReservedPosition(account, symbol string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reservedShares[account][symbol]
}

// Exists reports whether the account has ever been funded or given a position.
func (a *Accounts) Exists(account string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.existsLocked(account)
}

func (a *Accounts) existsLocked(account string) bool {
	if _, ok := a.cash[account]; ok {
		return true
	}
	_, ok := a.positions[account]
	return ok
}

// notional returns price*quantity, refusing to wrap. An order's cost is
// attacker-influenced (both factors arrive over the network), and a silent
// int64 overflow here would turn a huge buy into a negative cost that passes
// any balance check.
func notional(price Price, quantity int64) (int64, error) {
	if price < 0 || quantity < 0 {
		return 0, fmt.Errorf("oms: negative price %d or quantity %d", price, quantity)
	}
	if quantity != 0 && int64(price) > math.MaxInt64/quantity {
		return 0, fmt.Errorf("oms: order value %d x %d overflows", price, quantity)
	}
	return int64(price) * quantity, nil
}

// Reserve is the inline pre-trade check: it atomically verifies that o is
// affordable out of what is not already committed, and holds that amount
// against o.SeqID.
//
// It must be called with o.SeqID already assigned, and before the order is
// written to the write-ahead log — a rejected order is one the venue never
// accepted, so it has no business in the log. Every caller must eventually
// pair it with Complete (the order was processed) or Release (it was not).
//
// A MARKET buy is the one case with no bound to reserve against: it has no
// price, so its cost is unknown until it fills. NEPSE's daily circuit limits
// would bound it — worst case is the upper band times quantity — but those are
// not modelled here, so a MARKET buy reserves nothing and is checked only for a
// positive available balance. It can therefore overdraw. That is a named gap,
// and it is the reason MARKET buys are the only order type this function cannot
// make a hard promise about.
func (a *Accounts) Reserve(o Order) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.existsLocked(o.Placer) {
		return fmt.Errorf("%w: %q", ErrUnknownAccount, o.Placer)
	}
	if o.Quantity <= 0 {
		return fmt.Errorf("oms: order quantity %d must be positive", o.Quantity)
	}
	key := holdKey{symbol: o.Symbol, orderID: o.SeqID}
	if _, exists := a.holds[key]; exists {
		return fmt.Errorf("oms: order %s/%d already has a reservation", o.Symbol, o.SeqID)
	}

	if o.Side == Sell {
		available := a.positions[o.Placer][o.Symbol] - a.reservedShares[o.Placer][o.Symbol]
		if available < o.Quantity {
			return fmt.Errorf("%w: %s has %d %s available, needs %d",
				ErrInsufficientShares, o.Placer, available, o.Symbol, o.Quantity)
		}
		a.addReservedSharesLocked(o.Placer, o.Symbol, o.Quantity)
		a.holds[key] = &hold{account: o.Placer, symbol: o.Symbol, side: Sell, price: o.Price, qty: o.Quantity}
		return nil
	}

	available := a.cash[o.Placer] - a.reservedCash[o.Placer]
	if o.Type == Market {
		if available <= 0 {
			return fmt.Errorf("%w: %s has no uncommitted cash for a market buy", ErrInsufficientFunds, o.Placer)
		}
		// Nothing to hold — see the doc comment. The hold is still recorded so
		// that Complete/Release have something to resolve, keeping every order's
		// lifecycle identical.
		a.holds[key] = &hold{account: o.Placer, symbol: o.Symbol, side: Buy, price: 0, qty: o.Quantity}
		return nil
	}

	cost, err := notional(o.Price, o.Quantity)
	if err != nil {
		return err
	}
	if available < cost {
		return fmt.Errorf("%w: %s has %d available, needs %d", ErrInsufficientFunds, o.Placer, available, cost)
	}
	a.reservedCash[o.Placer] += cost
	a.holds[key] = &hold{account: o.Placer, symbol: o.Symbol, side: Buy, price: o.Price, qty: o.Quantity}
	return nil
}

// RestoreHold re-establishes a hold for an order recovered from the log, without
// checking affordability.
//
// It deliberately does not check. The order was already accepted and is already
// resting in a recovered book; refusing its hold would silently overstate the
// account's available balance by exactly the amount that order has committed,
// which is the bug reservations exist to prevent. If the journal-derived balance
// no longer covers it, that is a reconciliation problem for an operator, not a
// reason to pretend the order is not there.
func (a *Accounts) RestoreHold(symbol string, o Order) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := holdKey{symbol: symbol, orderID: o.SeqID}
	if _, exists := a.holds[key]; exists {
		return
	}
	if o.Side == Sell {
		a.addReservedSharesLocked(o.Placer, symbol, o.Quantity)
		a.holds[key] = &hold{account: o.Placer, symbol: symbol, side: Sell, price: o.Price, qty: o.Quantity}
		return
	}
	// Only LIMIT orders rest, so a recovered buy always has a real price to
	// reserve against — the unbounded MARKET-buy case cannot occur here.
	if cost, err := notional(o.Price, o.Quantity); err == nil {
		a.reservedCash[o.Placer] += cost
	}
	a.holds[key] = &hold{account: o.Placer, symbol: symbol, side: Buy, price: o.Price, qty: o.Quantity}
}

// Release frees an order's entire outstanding reservation and forgets it. Use it
// when an order left the book without trading the rest: cancelled, rejected
// after reservation, or a MARKET remainder that was dropped.
//
// Releasing an order that has no hold is not an error. Cancels arrive for orders
// that already filled, and replay re-presents attempts that failed the first
// time; both are normal.
func (a *Accounts) Release(symbol string, orderID SeqID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.releaseLocked(holdKey{symbol: symbol, orderID: orderID})
}

func (a *Accounts) releaseLocked(key holdKey) {
	h, ok := a.holds[key]
	if !ok {
		return
	}
	a.shrinkHoldLocked(h, h.qty)
	delete(a.holds, key)
}

// shrinkHoldLocked reduces a hold by qty, freeing the corresponding reservation.
func (a *Accounts) shrinkHoldLocked(h *hold, qty int64) {
	if qty <= 0 {
		return
	}
	if qty > h.qty {
		qty = h.qty
	}
	h.qty -= qty

	if h.side == Sell {
		a.addReservedSharesLocked(h.account, h.symbol, -qty)
		return
	}
	if h.price == 0 {
		return // market buy: nothing was reserved
	}
	freed, err := notional(h.price, qty)
	if err != nil {
		// Unreachable: this product was already computed at Reserve time.
		return
	}
	a.reservedCash[h.account] -= freed
	if a.reservedCash[h.account] == 0 {
		delete(a.reservedCash, h.account)
	}
}

func (a *Accounts) addReservedSharesLocked(account, symbol string, delta int64) {
	if a.reservedShares[account] == nil {
		a.reservedShares[account] = make(map[string]int64)
	}
	a.reservedShares[account][symbol] += delta
	if a.reservedShares[account][symbol] == 0 {
		delete(a.reservedShares[account], symbol)
	}
}

// Complete applies an order's outcome atomically: it moves cash and shares for
// every trade, shrinks both sides' reservations by what those trades consumed,
// and releases the taker's remaining hold unless restingQuantity says the order
// is still live in the book.
//
// One call, one lock acquisition, for the whole outcome. Splitting the balance
// movement from the reservation reconciliation would leave a window in which
// `owned - reserved` was wrong, which is exactly the class of bug reservations
// exist to close.
//
// selfPrevented lists resting orders cancelled by self-trade prevention; their
// holds are released, because those orders are gone.
func (a *Accounts) Complete(symbol string, orderID SeqID, trades []Trade, restingQuantity int64, selfPrevented []SeqID) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, t := range trades {
		value, err := notional(t.Price, t.Quantity)
		if err != nil {
			// Unreachable for a trade the book produced: both factors came from
			// orders that already passed notional() at reservation time.
			continue
		}
		buyer, seller := t.Buyer(), t.Seller()

		// Actual value movement.
		a.cash[buyer] -= value
		a.cash[seller] += value
		a.addPositionLocked(buyer, t.Symbol, t.Quantity)
		a.addPositionLocked(seller, t.Symbol, -t.Quantity)

		// Both sides had this quantity reserved; it has now genuinely moved, so
		// the hold shrinks by the same amount it stopped being a hold for.
		a.shrinkTradedLocked(symbol, t.MakerSeqID, t.Quantity)
		a.shrinkTradedLocked(symbol, t.TakerSeqID, t.Quantity)
	}

	for _, id := range selfPrevented {
		a.releaseLocked(holdKey{symbol: symbol, orderID: id})
	}

	// The taker's hold survives only for what is still resting in the book.
	if restingQuantity <= 0 {
		a.releaseLocked(holdKey{symbol: symbol, orderID: orderID})
	}
}

// shrinkTradedLocked reduces one order's hold by a filled quantity, dropping the
// hold once nothing is left of it.
func (a *Accounts) shrinkTradedLocked(symbol string, orderID SeqID, qty int64) {
	key := holdKey{symbol: symbol, orderID: orderID}
	h, ok := a.holds[key]
	if !ok {
		return
	}
	a.shrinkHoldLocked(h, qty)
	if h.qty == 0 {
		delete(a.holds, key)
	}
}

func (a *Accounts) addPositionLocked(account, symbol string, delta int64) {
	if a.positions[account] == nil {
		a.positions[account] = make(map[string]int64)
	}
	a.positions[account][symbol] += delta
}

// LoadBalances replaces every balance with the supplied ones, discarding
// reservations. It is for rebuilding from the durable journal at startup, before
// any order has been accepted — see ledger.Ledger.Balances.
//
// It refuses to run once holds exist, because overwriting balances underneath a
// live order would leave the reservations describing amounts that no longer
// relate to anything.
func (a *Accounts) LoadBalances(cash map[string]int64, positions map[string]map[string]int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.holds) != 0 {
		return fmt.Errorf("oms: cannot load balances with %d live reservations", len(a.holds))
	}
	a.cash = make(map[string]int64, len(cash))
	maps.Copy(a.cash, cash)
	a.positions = make(map[string]map[string]int64, len(positions))
	for account, bySymbol := range positions {
		for symbol, qty := range bySymbol {
			a.setPositionLocked(account, symbol, qty)
		}
	}
	a.reservedCash = make(map[string]int64)
	a.reservedShares = make(map[string]map[string]int64)
	return nil
}

// HoldCount reports how many live reservations exist. A venue with no resting
// orders must report zero; anything else is a leak.
func (a *Accounts) HoldCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.holds)
}
