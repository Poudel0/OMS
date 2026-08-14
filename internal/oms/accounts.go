package oms

import (
	"errors"
	"fmt"
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

// Accounts is an in-memory record of what each account can spend and deliver.
// It backs the one inline pre-trade check the venue performs, and it is
// deliberately not a risk engine.
//
// ponytail: in-memory, so balances are lost on restart while the ledger
// (ADR-004) survives. Rebuilding them means summing the journal at boot, which
// is the right fix and needs the ledger to exist first. Until then a restarted
// node must be re-seeded.
//
// Two known ceilings, both named rather than hidden:
//
//   - The check and the order submission are not atomic. Two concurrent orders
//     from one account can both pass a check that only one of them could
//     afford, because nothing is reserved between checking and matching. The
//     fix is to reserve at check time and release on reject; that needs the
//     reservation to live behind the same serialization point as the book,
//     which is a larger change than this check is worth today.
//   - A MARKET buy's cost cannot be bounded from the order alone, since it has
//     no price. NEPSE's daily circuit limits would bound it (worst case is the
//     upper band times quantity), but those are not modelled here, so a MARKET
//     buy is checked only for a positive balance and can overdraw.
type Accounts struct {
	mu        sync.Mutex
	cash      map[string]int64
	positions map[string]map[string]int64
}

// NewAccounts returns an empty account store.
func NewAccounts() *Accounts {
	return &Accounts{cash: make(map[string]int64), positions: make(map[string]map[string]int64)}
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
	if a.positions[account] == nil {
		a.positions[account] = make(map[string]int64)
	}
	a.positions[account][symbol] = qty
}

// Cash reports an account's spendable balance.
func (a *Accounts) Cash(account string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cash[account]
}

// Position reports an account's holding in one symbol.
func (a *Accounts) Position(account, symbol string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.positions[account][symbol]
}

// Exists reports whether the account has ever been funded or given a position.
func (a *Accounts) Exists(account string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
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

// Check is the inline pre-trade check: it reports whether account can afford to
// place o, without reserving anything.
//
// See the type comment for the two cases this deliberately does not cover
// (concurrent orders from one account, and MARKET buy cost).
func (a *Accounts) Check(o Order) error {
	if !a.Exists(o.Placer) {
		return fmt.Errorf("%w: %q", ErrUnknownAccount, o.Placer)
	}

	if o.Side == Sell {
		if held := a.Position(o.Placer, o.Symbol); held < o.Quantity {
			return fmt.Errorf("%w: %s holds %d %s, needs %d",
				ErrInsufficientShares, o.Placer, held, o.Symbol, o.Quantity)
		}
		return nil
	}

	balance := a.Cash(o.Placer)
	if o.Type == Market {
		// No price, so no bound. See the type comment.
		if balance <= 0 {
			return fmt.Errorf("%w: %s has no cash for a market buy", ErrInsufficientFunds, o.Placer)
		}
		return nil
	}

	cost, err := notional(o.Price, o.Quantity)
	if err != nil {
		return err
	}
	if balance < cost {
		return fmt.Errorf("%w: %s has %d, needs %d", ErrInsufficientFunds, o.Placer, balance, cost)
	}
	return nil
}

// Settle moves cash and shares for executed trades. It is the in-memory mirror
// of the durable journal the ledger writes (ADR-004), not a replacement for it:
// this is what the next pre-trade check reads, while the ledger is what an
// auditor reads.
//
// Balances are allowed to go negative here rather than being refused. By the
// time a trade exists the execution has already happened and is already
// durable in the log; refusing to record its cash movement would make the
// in-memory view disagree with the book. A negative balance is a debit that
// wants collecting, which is a business problem, not a data-integrity one.
func (a *Accounts) Settle(trades []Trade) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, t := range trades {
		value, err := notional(t.Price, t.Quantity)
		if err != nil {
			// Unreachable for a trade the book produced: both factors came
			// from orders that already passed notional() at check time.
			continue
		}
		buyer, seller := t.Buyer(), t.Seller()
		a.cash[buyer] -= value
		a.cash[seller] += value
		a.addPositionLocked(buyer, t.Symbol, t.Quantity)
		a.addPositionLocked(seller, t.Symbol, -t.Quantity)
	}
}

func (a *Accounts) addPositionLocked(account, symbol string, delta int64) {
	if a.positions[account] == nil {
		a.positions[account] = make(map[string]int64)
	}
	a.positions[account][symbol] += delta
}
