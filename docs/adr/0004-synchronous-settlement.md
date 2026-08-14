# ADR-004: Synchronous in-process settlement, idempotent on trade identity

- **Status:** Accepted
- **Week:** 4
- **Linked code:** [`internal/ledger/ledger.go`](../../internal/ledger/ledger.go), [`internal/ledger/migrations/0001_journal.sql`](../../internal/ledger/migrations/0001_journal.sql), [`internal/api/server.go`](../../internal/api/server.go), [`internal/oms/accounts.go`](../../internal/oms/accounts.go)

## Context

A matched trade moves value: the buyer owes cash and is owed shares, the seller
the reverse. The write-ahead log (ADR-003) makes the *book* recoverable, but it
says nothing about who owns what — and it deliberately does not log trades at
all, re-deriving them from replay instead. Something durable has to record the
money.

## Decision

Settlement is a **synchronous function call in the same process**, not a
service, a queue, or a job. On each trade the ledger posts double-entry rows to
Postgres in one transaction, keyed so that posting the same trade twice is a
no-op.

There is no Kafka, no outbox worker, no eventual-consistency window between
matching and settling. The venue is one process that owns one book per symbol;
introducing a network hop to talk to itself would add failure modes without
removing any.

### The journal

```
journal_entries(symbol, trade_id, account_id, asset, direction, amount, settled_at)
```

One trade writes four rows:

| account | asset | direction | meaning |
|---|---|---|---|
| buyer  | POSITION | DEBIT  | receives shares |
| buyer  | CASH     | CREDIT | pays |
| seller | CASH     | DEBIT  | receives |
| seller | POSITION | CREDIT | delivers |

For any `(symbol, trade_id, asset)` the debits equal the credits. That is the
invariant that makes this a ledger rather than a log of balances, and
`Ledger.Imbalance` exists to assert it against a real database.

`direction` is an explicit column rather than a sign on `amount` or a pair of
mostly-zero `debit`/`credit` columns. It has to be explicit because it is part
of the idempotency key — see below — and two columns encoding the same fact can
disagree with each other.

### Idempotency by construction, not by checking

```sql
CREATE UNIQUE INDEX ON journal_entries (symbol, trade_id, account_id, asset, direction);
INSERT ... ON CONFLICT (...) DO NOTHING
```

The alternative — `SELECT` to see whether a trade was settled, then `INSERT` —
is a race: two settlers can both find nothing and both insert. Letting the
unique index arbitrate has no such window. "Idempotent by construction" is a
stronger claim than "we handle duplicates", and it is the claim this schema
earns.

Duplicates are not hypothetical. Trades are re-derived from the log on replay,
and a crash between matching and settling means recovery will present the same
trade again. This is also why `Book.nextTradeID` lives on the *Book* rather than
the Sequencer: trade IDs have to come out of replay identical, or a recovered
node would post the same executions under new IDs and double-count them
(`TestWAL_ReplayReproducesTradeIDsExactly`).

## The bug that shaped the key, and how it was found

The key was originally `(symbol, trade_id, account_id, asset)` — four legs, two
accounts times two assets. Unit tests passed, because unit tests use accounts
called "buyer" and "seller".

Then a 30-second gRPC load test with 8 accounts ran, and the journal came back
with **12,266 rows missing and net cash of -40,013,771** where it should have
been 0. 6,133 of 47,935 trades had only two legs.

The cause was **self-trades**: with a handful of accounts trading randomly, an
account eventually crosses itself. When buyer and seller are the same
`account_id`, the four legs collapse to two keys — `(acct, POSITION)` and
`(acct, CASH)` — so the two counter-legs hit `ON CONFLICT DO NOTHING` and
vanished. The journal was left recording that an account received shares and
paid nothing. Silently.

Adding `direction` to the key keeps all four legs distinct while still making a
genuine re-settlement a no-op, and
`TestLedger_SelfTradeStillWritesAllFourLegs` now covers it.

Two lessons worth writing down rather than quietly fixing:

- **`ON CONFLICT DO NOTHING` is a loaded gun.** It cannot distinguish "this is
  the retry I designed for" from "my key is wrong". An `ON CONFLICT` clause is
  a claim that every colliding row is genuinely the same row, and that claim
  needs a test per way two rows can collide.
- **The load test found what the unit tests could not**, because the unit tests
  chose their own inputs and chose them tidily. Self-trading was not on the
  list of things to test; it was on the list of things a random workload does
  within seconds. Running the real thing under real load is not a
  nice-to-have.

Nothing in the venue prevents a self-trade today. That is a market-abuse
control (wash trading) and NEPSE would police it; it is out of scope for a
stripped v1 risk story, and it is recorded as a follow-up. The ledger being
correct regardless is not optional, which is why the fix went here.

## Two balance stores, and why

There are deliberately two records of who owns what, at different
authority levels:

- **`oms.Accounts`** — in memory, guards the one inline pre-trade check, updated
  from inside the sequencer goroutine as trades execute. It is what the *next*
  order is checked against, so it must be fast and local.
- **`journal_entries`** — durable, in Postgres, the auditable record.
  `Ledger.Balance` and `Ledger.Position` derive from it.

The in-memory view is lost on restart while the journal survives. Rebuilding it
means summing the journal per account at boot; that is the right fix and it
needs the ledger to exist first, which it now does. Until then a restarted node
must be re-seeded, and that is a named gap rather than a hidden one.

## Where the trade-offs actually bite

**Settlement is on the request path; the in-memory update is not.** The balance
update happens in `Sequencer.OnTrades`, inside the sequencer goroutine, because
the next order's check must see it. A Postgres round trip there would put an
unbounded stall between the matcher and its next order, so the durable write
happens afterwards, in the gRPC handler.

**Settlement runs on a context stripped of the caller's cancellation.** This
was also found by the load test: every in-flight order at shutdown logged
"trades are durable but unsettled", because settlement inherited the request
context. A trade that has executed cannot be un-executed, so a client hanging
up must not be able to abandon its settlement — that would leave the journal
permanently disagreeing with the book. `context.WithoutCancel` plus its own
30-second timeout, covered by
`TestPlaceOrder_SettlementSurvivesClientCancellation`.

**A settlement failure is reported, not swallowed.** The trade is already
durable in the log, so a failure here is not a lost execution — it is an
unsettled one, and replay will re-present it. The client gets `Internal` and
the operator gets a log line naming the order and trade count. Returning
success would be the worse lie.

**The pre-trade check is not atomic with the order.** Two concurrent orders from
one account can both pass a check only one of them could afford, because
nothing is reserved between checking and matching. The fix is to reserve at
check time and release on reject, which means the reservation has to live behind
the same serialization point as the book. Named in `oms.Accounts`, not built.

**A MARKET buy's cost cannot be bounded from the order alone**, since it has no
price. NEPSE's daily circuit limits would bound it — worst case is the upper
band times quantity — but those are not modelled, so a MARKET buy is checked
only for a positive balance and can overdraw.

## Measured cost

Full stack, 64 concurrent gRPC clients across 4 symbols, 30 seconds
(`docs/bench/week4-grpc-loadtest.txt`):

| Configuration | Throughput | p50 | p99 |
|---|---|---|---|
| WAL + Postgres settlement | 1,973 orders/sec | 28.9 ms | 71.3 ms |
| WAL, no ledger | 3,910 orders/sec | 14.8 ms | 32.9 ms |

**Durable settlement roughly halves throughput and doubles median latency.**
That is the price of the journal, measured rather than assumed, and it is worth
stating plainly: two thirds of the way down this stack, the interesting
engineering (the lock-free-ish single writer, the group commit) is being paid
for by two fsyncs — one in the WAL, one in Postgres's own commit.

The obvious next lever is batching settlement the way the WAL already batches
appends: one transaction per group commit rather than per order. That is a real
optimisation with a real design question attached (what happens to the client
that is still waiting), and it is not made here.

## Consequences

- The `Ledger` interface exists so the server is constructible without a
  database — CI has no Postgres, and a nil ledger means trades match and log but
  are never journalled. That is correct for tests and wrong anywhere money is at
  stake, so `cmd/server` warns loudly when it starts that way.
- Ledger tests skip rather than fail without `OMS_TEST_DATABASE_URL`, and the
  skip message says exactly what to set. CI runs them against a Postgres
  service container so the skip is not permanent.
- The schema is applied at startup with `IF NOT EXISTS` guards rather than by a
  migration tool. That is honest for one table with no production rows and stops
  being honest the moment a column has to change shape on a database that holds
  data — at which point this needs a real migration runner. Note that the
  `direction` fix above was a *rewrite* of `0001`, which was only acceptable
  because nothing had been deployed.
- ADR-006's follower inherits settlement as a question, not an answer: a
  passive follower replaying the WAL will re-derive the same trades with the
  same IDs, so it must not also settle them, or the same execution lands in the
  journal from two nodes. The unique key makes that survivable; deciding which
  node settles is still owed.
