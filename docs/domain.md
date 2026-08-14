# The domain: how a stock exchange actually works

This document explains the *business* this code implements, for a reader who
knows software but not markets. Every term the codebase uses is defined here, and
where the model deviates from a real venue, it says so.

If you only read one thing: an exchange's job is to **match buyers with sellers
fairly, and to be able to prove afterwards what happened.** Matching is the easy
half.

---

## 1. What is being traded

**NEPSE** is the Nepal Stock Exchange. This project models a broker/exchange
engine for it, which shapes several decisions that would be different elsewhere.

**Scrip** — Nepali/South Asian market usage for a listed security. Interchangeable
with "symbol", "ticker", or "instrument". Examples used throughout this codebase:
`NABIL` (Nabil Bank), `ADBL` (Agricultural Development Bank), `HBL` (Himalayan
Bank), `NRIC` (Nepal Reinsurance).

**Promoter shares** get a `P` suffix — `NABILP`. Promoter shares are held by
founders/institutional backers, trade under restrictions, and are a separate line
from ordinary shares. This matters here for exactly one reason: it is why
`maxSymbolLen` is 12 rather than 6. Real symbols can be longer than the obvious
four characters.

**Prices are integers, not floats.** NEPSE instruments trade in discrete **ticks**,
so a price is a whole number of the smallest tradeable increment. This is not a
micro-optimisation — it is a correctness requirement. `0.1 + 0.2 != 0.3` in
binary floating point, and an exchange that computes a trade's value slightly
differently than its counterparty is a broken exchange. Every price and quantity
in this codebase is `int64`.

The code says `Price int64` and never names a currency unit. Real NEPSE prices
are in paisa (1/100 of a rupee); the engine does not care, as long as everyone
uses the same unit.

---

## 2. Orders

An **order** is an instruction to buy or sell a quantity of one scrip.

**Side** — `Buy` or `Sell`. Note the zero value in this codebase is
`UnknownSide`, deliberately invalid, so a caller that forgets to set the field is
rejected rather than silently having its order treated as a buy.

**Two order types**, which is all this project builds:

| Type | Means | Behaviour when it cannot fully fill |
|---|---|---|
| **LIMIT** | "Buy at most 500, or better" | The unfilled remainder **rests** in the book, waiting |
| **MARKET** | "Buy at whatever is available" | The unfilled remainder is **dropped** — there is no price at which it would wait |

"Or better" means *better for you*: a buy limit at 500 will happily fill at 496.
A sell limit at 500 will happily fill at 504. It never fills worse than its limit.

**Cancel** withdraws a resting order. It can fail, and this codebase deliberately
does not distinguish why: an order that never existed, one that already filled,
and one already cancelled all return "not found". A real venue often wants to
tell a client "too late, it already traded" separately from "bad ID"; that needs
tracking terminal orders, which this does not do.

### What is deliberately NOT built

No **stop** orders (trigger when price crosses a level), no **iceberg**/hidden
orders (show only part of your size), no **fill-or-kill**, no **immediate-or-cancel**,
no **good-till-date**. LIMIT + MARKET + CANCEL is the smallest set that exercises
every interesting mechanism in a matching engine: resting, crossing, partial
fills, price levels, and priority.

---

## 3. The order book

The **order book** is every resting order for one scrip, organised so the engine
can find the best available price instantly.

```
        BIDS (buyers)            ASKS (sellers)
        ───────────────          ───────────────
  best  501 × 300                504 × 150   ← best ask (lowest sell)
        500 × 1,200              505 × 800
        496 × 50                 510 × 2,000
         ↑                        ↑
     descending                ascending
```

**Best bid** = highest price a buyer will pay. **Best ask** (or "best offer") =
lowest price a seller will accept. Bids are sorted descending and asks ascending,
so in both cases the best price is at index 0.

**The spread** is the gap between them (504 − 501 = 3 here).

**A crossed book** — best bid ≥ best ask — must never happen. If someone is
willing to pay 505 and someone else is willing to sell at 504, they should
already have traded. Every invariant test in this codebase checks this, and it is
the reason self-trade prevention cancels the resting order rather than refusing
to match (see §7).

### Price-time priority

The matching rule. Two resting orders compete on:

1. **Price first** — a buyer offering 501 is served before one offering 500.
2. **Time second** — among orders at the *same* price, the one that arrived
   first is filled first. FIFO.

This is why the book is a map of price → **queue**, not price → set. Queue
position is a real thing an order owns, and the engine must not shuffle it.

One consequence that surprises people: when a resting order is **partially**
filled, its remainder goes back to the **front** of its queue, not the back. It
was there first; a partial fill is not a demotion. `priceLevel.pushFront` exists
for exactly this.

### L2 vs L3

- **L3** — every individual order, with identity and queue position. This is what
  the engine holds internally, because matching needs it.
- **L2** — total quantity per price, identities discarded. This is what
  `Book.Snapshot()` and `GetBookSnapshot` return.

The distinction is a privacy and fairness one: publishing L3 would let anyone see
exactly where in the queue each participant sits.

---

## 4. Matching: maker, taker, and whose price wins

When an incoming order can trade against the book:

- **Taker** — the incoming order. It *takes* liquidity that was on offer.
- **Maker** — the resting order it trades against. It *made* liquidity available
  by waiting.

**The trade executes at the maker's price, always.** This is not arbitrary. The
maker published a price and committed to it by waiting; the taker crossed a price
that was already on offer. If a buy limit at 500 hits a resting ask at 496, the
trade is at **496** and the buyer saves 4 per share.

That "price improvement" has a direct consequence in this codebase: a taker
reserves cash at its own limit price but may spend less, so
`Accounts.Complete` releases the difference. It was never owed.

Real venues also charge makers and takers different fees (usually rebating makers
for providing liquidity). No fee model here at all.

**Sweeping / walking the book** — a large order can consume several price levels
in one submission, producing multiple trades at successively worse prices. A
market buy for 25 against asks of 10@500, 10@501, 10@502 produces three trades
and pays an average worse than 500. `TestPlaceOrder_MarketOrderCrossesMultipleLevels`
covers exactly this.

---

## 5. Accounts, positions, and settlement

**Account** — a party that trades. This codebase treats an account as an opaque
string and does not model customers, KYC, or brokers-of-record.

**Position** — how many shares of one scrip an account holds. Can be negative in
the journal's arithmetic; a real venue would have views about short selling that
this does not model.

**Cash** — spendable balance, in the same integer ticks as prices.

**Settlement** is the transfer of value after a trade: the buyer's cash goes to
the seller, the seller's shares go to the buyer. Matching says a trade *happened*;
settlement makes it *true*.

### T+0 vs T+2

Real equity markets settle on **T+2**: the trade happens today, the actual
exchange of cash and shares completes two business days later, via a clearing
house that guarantees both sides. That delay exists because moving money and
registering share ownership are slow, and it creates counterparty risk in the
meantime — which is the clearing house's whole reason to exist.

**This project settles T+0**, synchronously, in the same process, immediately.
That is a deliberate simplification: T+0 exercises the double-entry mechanics
without adding a clearing house, margin, novation, or a settlement calendar. The
sprint plan puts it plainly: "T+0 shows the pattern. Strip the extra days."

### Double-entry bookkeeping

The journal is **double-entry**, the 700-year-old accounting discipline: every
movement of value is recorded twice, once as a **debit** (into somewhere) and
once as a **credit** (out of somewhere), and the two must be equal.

One trade produces **four** rows:

| Account | Asset | Direction | Meaning |
|---|---|---|---|
| buyer | POSITION | DEBIT | receives shares |
| buyer | CASH | CREDIT | pays |
| seller | CASH | DEBIT | receives |
| seller | POSITION | CREDIT | delivers |

For any `(trade, asset)` the debits equal the credits. That is what makes the
journal *auditable* rather than merely a record of balances: you can prove no
value was created or destroyed, by summing a column. `Ledger.Imbalance()` runs
exactly that check, and it is what caught the self-trade bug described in §7.

Note the shape: two non-negative columns and an explicit direction, rather than
one signed amount. That is the conventional accounting form, and it makes
"debits equal credits" a sum over rows instead of a sign convention nobody
remembers at 3am.

### Two balance stores, on purpose

| | `oms.Accounts` (memory) | `journal_entries` (Postgres) |
|---|---|---|
| Authority | pre-trade checks | audit, truth |
| Speed | nanoseconds | milliseconds |
| Survives restart | **no** | yes |
| Knows about reservations | yes | no |

The in-memory store is what the *next order* is checked against, so it must be
fast and local. The journal is what an auditor reads. They are reconciled at
startup: `Ledger.Balances()` sums the journal and seeds the in-memory store.

---

## 6. Risk: the one check this venue does

Before an order is accepted:

- A **buy** must have the cash to pay for it.
- A **sell** must have the shares to deliver.

That is the entire risk model. No margin, no leverage, no position limits, no
concentration limits, no credit lines. The sprint plan calls a full risk engine
out of scope: "one inline balance check is enough."

The subtlety that makes even this one check non-trivial is **exposure vs
spending**. A resting order has not spent anything yet — no cash moves until it
fills — so an account with 5,000 could rest ten buy orders worth 5,000 each and
pass a naive balance check every time. The fix is **reservations**: money is held
against a live order from the moment it is accepted until it fills or is
cancelled. See §7 and [ADR-004](adr/0004-synchronous-settlement.md).

### Circuit limits (modelled: no)

NEPSE applies **daily circuit breakers** — a scrip's price cannot move more than
a fixed percentage from its previous close (historically ±10%, narrower for some
instruments). Trading in that scrip halts if it hits the band.

This is not modelled, and its absence has one concrete consequence: **a MARKET
buy's cost cannot be bounded.** A limit order's worst case is `price × quantity`.
A market order has no price, so without a circuit limit there is no worst case to
reserve against. The code therefore checks a market buy only for a positive
balance and documents that it can overdraw. With circuit limits, the worst case
would be `upper_band × quantity` and the check could be exact.

---

## 7. Market abuse: wash trading and self-trade prevention

A **wash trade** is a trade where the same beneficial owner is on both sides. No
ownership changes hands, but a trade prints — creating fake volume and a fake
price. It is market manipulation, and real exchanges police it.

With only a handful of accounts trading randomly, an account crosses *itself*
within seconds. So this is not a theoretical concern; it showed up in the first
load test.

**Self-trade prevention (STP)** is the control. Real venues offer several
policies; CME, for instance, lets you choose cancel-resting, cancel-incoming, or
cancel-both. This codebase implements **cancel-resting**: when an incoming order
would match its own account, the resting order is cancelled and matching
continues past it.

Why cancel the resting side rather than reject the incoming order? Because
rejecting the taker and letting it rest anyway would leave a **crossed book** —
a bid at or above an ask — which breaks the invariant everything else relies on.
Cancelling the resting order keeps the book consistent and prints no wash trade.

The cancelled order IDs are **reported to the client**. An order silently
vanishing from the book is worse than the problem being solved.

What is *not* built: surveillance for the subtler patterns (layering, spoofing,
coordinated accounts under one owner, marking the close). Those need a
surveillance system, not a matching-engine check.

---

## 8. Durability, and what an exchange owes its clients

The core promise: **if the venue told you your order was accepted, that order
survives the venue crashing.**

This is what a **write-ahead log** (WAL) buys. Before the order book is touched,
the order is written to a file and the file is `fsync`'d to physical storage.
Only then does matching happen and only then is the client told anything. If the
process dies a microsecond later, the order is on disk and replay rebuilds it.

The vocabulary:

- **fsync** — the syscall that forces the OS to actually put bytes on the device
  rather than leaving them in a cache. It is the expensive part: ~3.4ms on this
  project's hardware, versus microseconds for everything else.
- **Group commit** — batching many orders into one fsync. If ten orders arrive
  together, append all ten and fsync once. This is *the* throughput lever, and
  it is why contention improves per-order throughput here.
- **Replay** — rebuilding state by re-running the log from the beginning.
- **Log position** (Postgres calls it an LSN) — a monotonic counter identifying a
  record's place in the log. Distinct from an order ID: cancels get positions too.

**Idempotence** is the property that doing something twice has the same effect as
doing it once. It matters here because replay can present the same trade to
settlement more than once, so the journal must be able to absorb a duplicate
without double-counting. This codebase gets it *by construction* — a unique
database index — rather than by checking first, because check-then-insert is a
race.

---

## 9. Replication and failover

**Primary** — the node taking client traffic. **Follower** (also "replica",
"standby", "secondary") — a node that copies the primary's state but serves no
clients.

**WAL shipping** is how: the follower streams the primary's log and replays it.
Postgres streaming replication works the same way, which is where the vocabulary
comes from.

**Asynchronous** replication means the primary acknowledges an order once *its
own* fsync returns, without waiting for the follower. The trade-off is exact and
worth stating in domain terms: client latency does not depend on a second
machine, but **a primary lost while the follower is N records behind loses those
N acknowledged orders.** For a venue, that is a real obligation to real clients.
Synchronous replication would close the window and put a network round trip in
every order's latency.

**Replication lag** is how wide that window currently is, and it is the number an
operator should watch. This codebase reports it in two units, because both
matter: records behind (how many orders) and milliseconds behind (how long).

**Failover** is promoting the follower when the primary is lost. Here it is
**manual** — a human decides. No consensus protocol, no automatic election, no
quorum.

**Split brain** is the failure mode that makes automatic failover hard: two nodes
both believing they are primary, both accepting orders, both writing the same
log. There is no clean recovery — the same log position means different things on
the two nodes, and there is no merge. This design prevents it *procedurally* (the
runbook says never run two primaries) rather than mechanically (no fencing, no
lease, no lock). That is the honest limit of a manual-failover design.

---

## 10. Glossary

| Term | Meaning |
|---|---|
| Ask / Offer | A resting sell order, or its price |
| Bid | A resting buy order, or its price |
| Book | All resting orders for one scrip |
| Circuit limit | Daily price band beyond which a scrip cannot trade (not modelled) |
| Crossed book | Best bid ≥ best ask; must never happen |
| Double-entry | Every value movement recorded as equal debit and credit |
| Fill | An execution, whole or partial, of an order |
| fsync | Syscall forcing data to physical storage |
| Group commit | Batching many log appends into one fsync |
| L2 / L3 | Aggregated depth per price / individual orders with identity |
| Log position (LSN) | Monotonic counter identifying a record in the WAL |
| Maker | The resting order in a trade; its price is the trade price |
| Market order | Buy/sell at whatever is available; never rests |
| Limit order | Buy/sell at a specified price or better; rests if unfilled |
| NEPSE | Nepal Stock Exchange |
| Position | Shares of a scrip an account holds |
| Price-time priority | Better price first, then earlier arrival |
| Promoter shares | Founder/institutional share class, `P` suffix |
| Replication lag | How far behind a follower is, in records and in time |
| Resting order | An order sitting in the book waiting to be matched |
| Scrip | A listed security; symbol, ticker, instrument |
| Settlement | Actually moving cash and shares after a trade |
| Split brain | Two nodes both acting as primary; unrecoverable |
| Spread | Best ask minus best bid |
| Sweep | One order consuming several price levels |
| T+0 / T+2 | Settlement same day / two business days later |
| Taker | The incoming order in a trade; crosses the spread |
| Tick | The smallest price increment |
| WAL | Write-ahead log; durability before mutation |
| Wash trade | Same owner on both sides; market manipulation |

---

## 11. Where this model departs from a real venue

Collected so no reader mistakes the model for the real thing:

| Real venue | This project | Why |
|---|---|---|
| T+2 settlement via a clearing house | T+0, in-process | Shows the double-entry pattern without a clearing house |
| Full risk engine, margin, limits | One cash/shares check | Explicitly out of scope |
| Daily circuit limits | Not modelled | Consequence: MARKET buy cost is unbounded |
| Fee schedules, maker rebates | None | No revenue model |
| Stop, iceberg, FOK, IOC orders | LIMIT + MARKET + CANCEL | Smallest set exercising every mechanism |
| Surveillance for layering, spoofing | Self-trade prevention only | Needs a surveillance system |
| Authenticated participants | Account is a trusted string | Auth explicitly out of scope |
| Opening/closing auctions, halts | Continuous trading only | Not needed to show matching |
| Short-sale rules, borrow | Positions may go negative | Not modelled |
| Corporate actions (splits, dividends) | None | Not a matching-engine concern |
| Multiple venues, order routing | Single venue | No SOR/FIX |

The scope was chosen so one engineer could finish it and explain every line,
rather than producing a shallow layer over tools that do the interesting work
invisibly. See [`docs/architecture.md`](architecture.md) for what that produced.
