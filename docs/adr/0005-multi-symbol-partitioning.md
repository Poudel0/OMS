# ADR-005: Multi-symbol partitioning via a per-symbol sequencer registry

- **Status:** Accepted
- **Week:** 4
- **Linked code:** [`internal/oms/registry.go`](../../internal/oms/registry.go), [`internal/api/server.go`](../../internal/api/server.go)

## Context

ADR-002 chose one goroutine per symbol as the only writer to that symbol's book,
and said the payoff would arrive with multiple symbols: N instruments running on
N independent goroutines with no cross-symbol contention. This is that
payoff, and the machinery is smaller than the argument for it.

## Decision

A `Registry` maps a symbol to the one `Sequencer` that owns its book, creating
each on first use. Each symbol gets its own goroutine, its own book, its own
write-ahead log directory, and its own log-position and trade-ID sequences.

Partitioning is by symbol because symbol is the natural shard key: matching only
ever compares orders in the same instrument, so two symbols share nothing that
would need coordinating. There is no cross-symbol transaction to make this
awkward — a trade is always within one instrument.

**Lazy creation.** A symbol's sequencer is created on its first order rather
than seeded from an instrument list, because there is no instrument list to read
yet. The common path takes a read lock; the write lock is held only while a
symbol is created for the first time, and the map is re-checked under it because
two callers can both miss the read-locked lookup for the same new symbol
(`TestRegistry_ConcurrentGetOfTheSameSymbolCreatesOne`).

**Recovery is per symbol and happens at creation.** `Recover` runs before
`OpenWriter`, so replay sees the same records the writer is about to continue
from — `OpenWriter` truncates a torn tail, and doing that first would hide
records from replay. Recovering a directory that does not exist is not an error;
that is what a symbol's first ever order looks like.

## Symbols are a trust boundary

A symbol arrives from a network client and becomes a **directory name** under
the WAL root. `"../../etc"` therefore has to be impossible, not merely
discouraged.

`validSymbol` is an allowlist: 1–12 characters of `A-Z0-9`, nothing else. An
allowlist rather than a denylist of dangerous characters, because a denylist is
a bet that the list is complete, and the set of characters a real ticker needs
is tiny. `TestRegistry_RejectsMalformedSymbols` covers traversal, embedded
traversal, lowercase, spaces, punctuation, and NUL, and
`TestRegistry_RejectedSymbolCreatesNothingOnDisk` asserts the stronger property:
a rejected symbol leaves nothing on the filesystem at all.

`MaxSymbols` (512) caps lazily created instruments. Every new symbol costs a
goroutine, a directory, and an fsync, and symbols arrive from the network — so
without a cap a client sending `AAAAA`, `AAAAB`, `AAAAC`… spawns unbounded
goroutines and files. The cap bounds *new instruments*, not trading: an existing
symbol keeps working once the cap is reached. NEPSE lists a few hundred scrips,
so 512 is comfortably above any legitimate workload.

The real answer is to seed the registry from the exchange's listed-scrip table
and refuse anything not on it. The cap is a stand-in with a named upgrade path,
not a design.

## Identity is per symbol, and that is deliberate

Three sequences are per symbol, not global:

| Sequence | Owner | Why per-symbol |
|---|---|---|
| Log position (`Record.Seq`) | Sequencer | Each symbol has its own log; a shared counter would need coordination between otherwise independent writers. |
| Trade ID (`Book.nextTradeID`) | **Book** | It has to survive replay identically, because settlement is idempotent on it (ADR-004). Only the book is rebuilt by replay. |
| Order ID | Sequencer | Assigned above `Book.MaxOrderID()` after recovery, so a restart cannot reissue the ID of an order still resting. |

So a trade is identified by `(symbol, trade_id)`, which is exactly the journal's
key. `TestRegistry_RecoversEachSymbolIndependentlyAcrossRestart` asserts that two
symbols resume their own positions rather than sharing one — a shared counter
would show up right there.

Order IDs are assigned by the venue, never by the client. They are sequential
integers, so a client-chosen ID could name an order another account holds. That
in turn is why `CancelFor` checks ownership: guessing an ID is trivial, and
without the check any client could cancel any other client's orders. The account
making the claim is **not authenticated** in v1 — multi-tenant auth is
explicitly out of scope — so the check is only as strong as the `account_id` the
caller sent. It is still worth having now: adding authentication later makes it
real without changing the logic around it.

## Measured — and partitioning makes durable throughput *worse*

The same total load (64 gRPC clients, 20 s) spread across a rising number of
symbols (`docs/bench/week4-symbol-scaling.txt`):

| Symbols | With WAL | p99 | Without WAL | p99 |
|---|---|---|---|---|
| 1 | **7,012 orders/sec** | 14.2 ms | 44,954 orders/sec | 3.8 ms |
| 2 | 4,626 | 28.8 ms | 41,401 | 4.6 ms |
| 4 | 3,690 | 38.2 ms | 41,541 | 4.7 ms |
| 8 | **3,134** | 37.0 ms | 43,091 | 5.7 ms |

Durable throughput **falls 2.2× as symbols rise**, and p99 nearly triples. The
expectation going in was that it would rise, or at worst flatten. It does the
opposite.

The no-WAL column is what identifies the cause. Without durability, symbol count
is flat within noise (41–45k/sec): matching genuinely does not care how many
symbols it is spread over, which is the partitioning claim holding exactly as
designed. Every bit of the loss is in durability.

The mechanism is **group-commit fragmentation**, and it is ADR-003's finding
pointing the other way. Group commit works by amortising one fsync across every
request queued behind the first. With one symbol, all 64 clients queue into *one*
sequencer, so each fsync serves a large batch. With eight symbols, each sequencer
sees ~8 clients, so each fsync serves roughly an eighth as many orders — and
there are now eight independent fsync streams contending for one device that
serialises them anyway. More partitions, smaller batches, same total device
capacity, less throughput.

So the honest statement of what partitioning buys:

- **Matching parallelism: real.** Confirmed by the flat no-WAL sweep.
- **Isolation: real.** A symbol's book, log, and identity sequences are its own,
  and one symbol's load cannot touch another's correctness.
- **Durable throughput: negative, on shared storage.** Partitioning divides the
  batching that durability depends on.

Which means the operational conclusion is the reverse of the intuitive one:
**on a single device, fewer symbols per node is better for durable throughput.**
Scaling out is a matter of more devices or more nodes — a per-symbol WAL on its
own spindle, or symbols split across machines — not of more goroutines. That is
worth knowing before Week 6 quotes a headline number, and it is the sort of thing
only measuring the sweep would have surfaced.

Per-symbol batch sizes were not instrumented in this run. `Sequencer.CommitStats`
reports them but is not exposed over gRPC, so the no-WAL control is what pins the
cause to fsync rather than to lock contention or scheduling. Wiring that ratio
into the Week 6 metrics endpoint would turn the explanation above from
well-supported into directly observed.

For reference, the end-to-end number with settlement included, 4 symbols, 30 s
(`docs/bench/week4-grpc-loadtest.txt`): 1,973 orders/sec durable, 3,910 without
the ledger, with a journal that balances to the row.

## Consequences

- **The trade feed is per symbol and published from the sequencer goroutine**
  (`Sequencer.OnTrades` → `Registry.OnTrades` → `api.tradeFeed`). That placement
  is the point: it is the only place trade order is total, so a market-data feed
  cannot publish trades out of the order they matched in. The cost is that the
  callback runs on the critical path and must not block, which is why a slow
  subscriber has its trades dropped rather than being waited for.
- **Registry creation holds the write lock across WAL recovery.** A first-ever
  order in one symbol therefore briefly blocks a first-ever order in another. It
  does not block trading in any symbol already created, which is the case that
  matters. A per-symbol creation lock is the fix if cold-start latency across
  many symbols ever shows up as a problem.
- **`Registry.Close` joins every symbol's error** rather than returning the
  first. A shutdown that lost data in two symbols should report both.
- **Nothing evicts an idle symbol.** A symbol created once holds its goroutine
  and its book for the process lifetime. With a bounded instrument list that is
  fine and arguably correct — a listed scrip should be ready to trade — but it
  means memory is a function of symbols touched, not symbols active.
- **ADR-006's follower inherits this shape directly.** Replication is per symbol
  because the logs are per symbol, so a follower is N independent replay streams
  rather than one ordered stream. That removes the need for any global ordering
  between symbols, and it means a single lagging symbol cannot stall the others.
