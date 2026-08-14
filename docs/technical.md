# Technical reference

Code map, data flow, invariants, wire and disk formats, concurrency rules, and
every tunable constant. For *why* any of it is shaped this way, see the
[ADRs](adr/); for the domain vocabulary, see [`domain.md`](domain.md).

Go 1.25.5. ~5,300 lines of non-test code, ~5,400 lines of tests, 141 tests.

---

## 1. Package map

```
cmd/
  server/      the venue: gRPC + registry + ledger + metrics + tracing   249 loc
  follower/    passive replica; serves no client traffic                  95 loc
  loadgen/     gRPC load generator, reports p50/p95/p99/p99.9            172 loc
  bench/       multi-hour Book-level stress simulation                   157 loc

internal/
  oms/         the engine. No network, no SQL, no logging.
    types.go       Order, Trade, Price, SeqID, OrderBook iface           115 loc
    book.go        L3 book: matching, price levels, cancel, snapshot     453 loc
    sequencer.go   single-writer goroutine; the 5-step commit            510 loc
    wal.go         write-ahead log: format, Writer, Reader, Recover      585 loc
    tail.go        Tailer: follows a log across segments as it grows     194 loc
    registry.go    symbol -> sequencer, lazy, capped, validated          213 loc
    accounts.go    balances + reservations (the pre-trade check)          447 loc
    book_mutex.go  mutex baseline kept for ADR-002 comparison             42 loc

  api/         gRPC surface + the trust boundary
    server.go      OrderService: validate, route, settle, observe        551 loc
    replication.go ReplicationService: stream the log to followers       338 loc
    feed.go        per-symbol trade fan-out for StreamTrades              94 loc

  ledger/      Postgres double-entry settlement                          361 loc
  replica/     follower engine: stream, apply, own log                   379 loc
  metrics/     Prometheus collectors                                     233 loc
  tracing/     OpenTelemetry setup                                       104 loc
  pb/          generated gRPC stubs (do not edit; `make proto`)
```

**Dependency direction.** `oms` depends on nothing in this repo. `api` depends on
`oms`, `pb`, and `metrics` (for the `FollowerLag` shape). `replica` depends on
`api` (for the record codec), `oms`, and `pb`. `ledger` depends on `oms` only for
the `Trade` type. Nothing depends on `cmd`.

`api` defines the `Ledger` and `Observer` interfaces it needs rather than
importing the implementations, so the gRPC layer is constructible with neither a
database nor a metrics registry — which is what CI does.

---

## 2. Request flow

```
PlaceOrder
  │
  ├─ orderFromRequest()          ← THE TRUST BOUNDARY. Nothing downstream re-checks.
  │    symbol non-empty · account printable ASCII ≤64 · side/type not UNSPECIFIED
  │    quantity 1..2^32 · price 1..2^40 for LIMIT, exactly 0 for MARKET
  │    SeqID left ZERO on purpose → the venue assigns the order ID
  │
  ├─ Registry.Get(symbol)        ← symbol allowlist (A-Z0-9, ≤12), MaxSymbols cap
  │    on first use: Recover() the book from its log, then OpenWriter()
  │
  ├─ Sequencer.Submit(ctx, order)
  │    │  request goes down ONE channel; caller blocks on a pooled reply channel
  │    ▼
  │  ┌──────────── sequencer goroutine, one per symbol ──────────────┐
  │  │ drain()   pull every request already queued behind the first  │
  │  │                                                              │
  │  │ 1 assign order IDs (if caller left them 0), normalise symbol  │
  │  │ 2 Accounts.Reserve()   atomic; failure ⇒ preErr, NOT logged   │
  │  │ 3 wal.Append() × N     buffered only                         │
  │  │ 4 wal.Sync()           ONE fsync for the whole batch         │
  │  │ 5 Book.SubmitOrder()   only now is the book touched          │
  │  │ 6 Accounts.Complete()  move value + reconcile holds, atomic  │
  │  │ 7 onTrades()           publish to the feed, in match order   │
  │  └──────────────────────────────────────────────────────────────┘
  │
  ├─ Ledger.Settle()             ← context.WithoutCancel + own 30s timeout
  │                                4 journal rows per trade, one transaction
  └─ response: order_id, log_position, trades, resting_quantity,
               self_prevented_order_ids
```

**Step order is the design.** Reserve before log (a rejected order must never
enter the log, because replay does not re-run balance checks). Log and fsync
before match (nothing acknowledged that is not recoverable). Value movement and
hold reconciliation in one step (any gap between them makes
`owned − reserved` briefly wrong).

---

## 3. Invariants

Things that must always hold. Each names what enforces it.

**Book**

| Invariant | Enforced by |
|---|---|
| Never crossed: best bid < best ask | matching loop; self-trade prevention cancels rather than refusing |
| Bids descending, asks ascending, index 0 is best | `getOrCreateLevel` binary-insert |
| Price-time priority: better price, then FIFO | price → `list.List` queue |
| A partially filled maker keeps its queue position | `pushFront`, not `pushBack` |
| An empty price level is removed | `removeLevel` after every pop |
| `index` maps exactly the resting orders | maintained on push/pop/cancel |
| MARKET never rests | `SubmitOrder` rests only `Limit` |
| Trade price is always the maker's | match loops use the level's price |
| Trade IDs are dense, per symbol, from 1 | `Book.nextTradeID` — on the *Book*, so replay reproduces it |

**Log**

| Invariant | Enforced by |
|---|---|
| Positions are gap-free and monotonic per symbol | `s.seq++` inside the goroutine; rolled back on WAL failure |
| Log order == apply order | ONE request channel (a `select` over two would reorder) |
| Nothing acknowledged before its fsync returns | steps 3–5 above |
| A rejected order has no position and never reaches the book | `preErr` skips both |
| Replay reaches byte-identical book state | `Snapshot()` equality tests, incl. 8 concurrent producers |
| Segment filename order == log order | `%019d.wal`, zero-padded |

**Money**

| Invariant | Enforced by |
|---|---|
| `available = owned − reserved`, never negative at accept time | `Accounts.Reserve` under one lock |
| Every hold is eventually released | `Complete` / `Release` on every path; `HoldCount()` asserts zero |
| Per `(trade, asset)`: debits == credits | 4-leg write; `Ledger.Imbalance()` |
| Settling a trade twice is a no-op | unique index + `ON CONFLICT DO NOTHING` |
| `price × quantity` never wraps | `notional()` overflow guard |

**Replication**

| Invariant | Enforced by |
|---|---|
| A follower cannot backpressure matching | it reads log *files*; nothing waits for it |
| Follower stores the *primary's* positions | never renumbered ⇒ its log is a valid primary log |
| Follower writes before it applies | same discipline as the primary |
| Replication stops at damage, never skips it | `ErrCorruptWAL` → gRPC `DataLoss` |

---

## 4. Concurrency model

**One goroutine owns one book.** That is the whole model. `Book` has no
synchronisation of any kind and needs none.

Who may touch what:

| State | Owner | How others read it |
|---|---|---|
| `Book` (all fields) | its sequencer goroutine, exclusively | `Sequencer.Snapshot()` — a request through the channel |
| `Sequencer.seq`, `nextOrderID`, `walErr` | its goroutine, exclusively | not exposed |
| `Sequencer.commits/batched/resting/lastPos` | its goroutine writes | `atomic` loads, for metrics |
| `Sequencer.symbol/accounts/onTrades` | written once by `attach()` before publication | plain reads, ordered by the channel send |
| `Accounts` (everything) | any goroutine | one `sync.Mutex`; cash is shared across symbols even though books are not |
| `Registry.seqs` | any goroutine | `sync.RWMutex`, double-checked on create |
| `Writer` | its sequencer goroutine, exclusively | — |
| `tradeFeed.subs` | any goroutine | `sync.Mutex` |
| `ReplicationServer.progress` | any goroutine | `sync.Mutex` |

**Three subtle rules worth knowing before editing:**

1. **`select` picks randomly among ready cases.** This bit twice. It is why
   submits and cancels share ONE channel (two channels would let a cancel
   overtake an earlier submit, making log order ≠ apply order), and it is why the
   shutdown test must wait on `Done()` rather than assuming `cancel()` took
   effect.

2. **`attach()` writes without a lock, safely.** The sequencer goroutine only
   reads `symbol`/`accounts`/`onTrades` while handling a request, which
   happens-after the channel send that delivered it, which happens-after
   `attach`. The registry calls `attach` under its write lock before publishing
   the sequencer, so no request can be in flight. Do not move it.

3. **`OnTrades` runs on the critical path and must not block.** It fires inside
   the sequencer goroutine because that is the only place trade order is total.
   The cost of that guarantee is that a slow subscriber must be *dropped*, not
   waited for.

**Reply channel pooling.** `sync.Pool` of `chan result`. A channel returns to the
pool only when provably empty: either the request never reached the goroutine, or
its one value was drained. If a caller's context is cancelled *after* the request
is in flight, the goroutine may still write to that channel, so it is
deliberately never pooled — leaking one channel is correct; a stale value
surfacing in a later request is not.

---

## 5. On-disk format: the write-ahead log

One directory per symbol under the WAL root: `<walDir>/<SYMBOL>/`.
Segments are named for the first log position they contain, zero-padded to the
full width of an `int64`: `0000000000000000001.wal`. That padding is what makes
lexical filename order identical to log order, which is why `segmentPaths` can
just sort strings.

```
segment := header record*

header  := magic[4]="DWAL"  version:uint32          (8 bytes)

record  := len:uint32                               ┐
           seq:int64                                │ 20-byte fixed header
           tsUnixNano:int64                         ┘
           payload[len]                             JSON
           crc32:uint32                             IEEE, over header+payload
```

All integers little-endian.

**Why this order.** The length prefix is first because a reader cannot check
anything until it knows how far the payload runs. The CRC is last because it
cannot be computed until the payload exists. The CRC covers the fixed header
*and* the payload, so a torn write anywhere in the record is caught.

**Payload is JSON** (`{"k":kind,"o":{order},"c":cancelID,"cb":cancelBy}`). A
packed binary encoding would be ~⅓ the size, but an append is dominated by the
fsync that follows it by three orders of magnitude, and JSON keeps a segment
readable with `xxd` when a replay goes wrong. `walVersion` exists so the swap is
possible later.

**Corruption policy — two cases, treated differently on purpose:**

| Case | Meaning | Behaviour |
|---|---|---|
| Short read (fewer bytes than needed) | Truncated tail. The ordinary crash outcome; these bytes were never acknowledged. | Stop the scan, **no error**. `OpenWriter` truncates them away before appending. |
| CRC mismatch, or `len` > 1 MiB cap | Bytes are *present* but wrong. Storage lied. | `ErrCorruptWAL`. Replay halts; `OpenWriter` refuses to append; `StreamWAL` returns `DataLoss`. |

The length cap earns its place independently: without it, a torn write leaving
garbage in the length prefix would have the reader allocate gigabytes before
reaching the CRC that would have rejected the record.

The honest edge: a torn write damaging the length prefix at the very end of a
segment is indistinguishable from a truncated tail, and is reported as
corruption. That is the safe direction to be wrong in.

**Directory fsync** after creating a segment, so the *filename* survives a crash
too — otherwise a segment's contents can be on disk while its directory entry is
not.

---

## 6. Database schema

`internal/ledger/migrations/0001_journal.sql`, applied by a runner that records
each migration in `schema_migrations` in the *same transaction* as the migration
itself, under a Postgres advisory lock (`migrationLockID = 8734121`) so two nodes
starting together do not both migrate.

```sql
journal_entries(
  id          BIGSERIAL PRIMARY KEY,
  symbol      TEXT   NOT NULL,
  trade_id    BIGINT NOT NULL,      -- (symbol, trade_id) is trade identity
  account_id  TEXT   NOT NULL,
  asset       TEXT   NOT NULL CHECK (asset IN ('CASH','POSITION')),
  direction   TEXT   NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
  amount      BIGINT NOT NULL CHECK (amount > 0),
  settled_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)

UNIQUE (symbol, trade_id, account_id, asset, direction)   -- idempotency key
INDEX  (account_id, asset, symbol)                        -- balance reads
```

**`direction` is in the unique key, and leaving it out was a real bug.** One
trade writes four rows: two accounts × two assets. But when an account trades
with *itself*, both sides are the same `account_id`, so a key of
`(symbol, trade_id, account_id, asset)` collapses four legs into two — and the
counter-legs hit `ON CONFLICT DO NOTHING` and vanish silently. Measured impact
before the fix: 6,133 of 47,935 trades corrupted, 12,266 rows missing, net cash
−40,013,771 where it should have been 0.

Balances are derived, never stored:
`SUM(amount) FILTER (direction='DEBIT') − SUM(amount) FILTER (direction='CREDIT')`.

---

## 7. gRPC API

`proto/dhukuti/oms/v1/oms.proto`, package `dhukuti.oms.v1`. Regenerate with
`make proto` — plugin versions are pinned by `tool` directives in `go.mod`, so it
reproduces anywhere and does not disturb a globally installed `protoc-gen-go`. CI
regenerates and fails on any diff.

### OrderService

| RPC | Notes |
|---|---|
| `PlaceOrder` | Synchronous: returns once durable **and** matched. Order ID assigned by the venue. |
| `CancelOrder` | Checks the requesting account owns the order. |
| `StreamTrades` | Live feed from subscription onward. Not replayable; drops for slow subscribers. |
| `GetBookSnapshot` | L2 depth + the log position it was taken at. Served from inside the sequencer goroutine, so consistent rather than torn. |

### ReplicationService

Separate service on purpose: a follower is not a client, the log is more
sensitive than the order API, and a deployment should be able to expose one
without the other.

| RPC | Notes |
|---|---|
| `ListSymbols` | Discovery, so a follower needs no configured symbol list. |
| `StreamWAL` | Server-stream of records after a position; keeps streaming as new ones land. One stream per symbol. |
| `ReportProgress` | Advisory. The primary streams whether or not anyone reports. |
| `ReplicationStatus` | Per-symbol records-behind, millis-behind, and `follower_seen`. |

### Status code mapping

| Code | Means |
|---|---|
| `InvalidArgument` | Malformed request; bad symbol, price, quantity, account |
| `NotFound` | Unknown account, or no such resting order |
| `FailedPrecondition` | Insufficient funds or shares |
| `PermissionDenied` | Cancel for an order owned by another account |
| `ResourceExhausted` | `MaxSymbols` reached |
| `Unavailable` | Shutting down |
| `Internal` | WAL failure, or settlement failed (trades durable but unsettled) |
| `DataLoss` | The log is damaged; replication refuses to continue |

`cancel_requested_by` travels in the WAL record on the wire. It has to: the
follower re-evaluates the ownership constraint, so a log that omitted it would
let a cancel the primary *refused* succeed on the follower, and the two books
would diverge.

---

## 8. Observability

**Metrics** (`:9091/metrics`). Request-path counters and histograms are
incremented inline; everything derivable from live state is computed **at scrape
time** by a custom collector — no background goroutine, and no gauge that can
drift from what it describes.

| Metric | Type | Labels |
|---|---|---|
| `oms_orders_total` | counter | symbol, side, type, outcome |
| `oms_cancels_total` | counter | symbol, outcome |
| `oms_trades_total` | counter | symbol |
| `oms_order_duration_seconds` | histogram | symbol |
| `oms_settlements_total` | counter | outcome |
| `oms_settlement_duration_seconds` | histogram | — |
| `oms_trade_feed_dropped_total` | counter | symbol |
| `oms_resting_orders` | gauge (scrape) | symbol |
| `oms_log_position` | gauge (scrape) | symbol |
| `oms_group_commits_total` | counter (scrape) | symbol |
| `oms_group_commit_requests_total` | counter (scrape) | symbol |
| `oms_replication_{primary,follower}_position` | gauge (scrape) | symbol |
| `oms_replication_records_behind` | gauge (scrape) | symbol |
| `oms_replication_millis_behind` | gauge (scrape) | symbol |
| `oms_replication_follower_seen` | gauge (scrape) | symbol |
| `go_*`, `process_*` | — | includes `go_gc_duration_seconds` |

Outcome label values are a small closed set on purpose. An unbounded label (an
error string) creates a time series per distinct failure and eventually takes the
monitoring system down.

**The query that matters:**
```promql
rate(oms_group_commit_requests_total[1m]) / rate(oms_group_commits_total[1m])
```
Mean orders per fsync. [ADR-003](adr/0003-wal-design.md)'s entire argument rests
on that number; it measured **6.91** during the final 60s run.

**Tracing.** Spans: gRPC handler (via `otelgrpc` stats handler) →
`sequencer.submit` → `ledger.settle`. Opt-in; with no endpoint OTel's default is
a no-op.

**The group commit is deliberately not a span.** One commit serves many
concurrent requests, so it belongs to many traces at once — and a span has one
parent. Attaching it to one arbitrary trace would misattribute the whole cost to
one request. Tracing and batching are in genuine tension; the batch is measured
as a metric instead, where a fan-in fits naturally.

---

## 9. Every tunable constant

| Constant | Value | Where | What it bounds |
|---|---|---|---|
| `maxSymbolLen` | 12 | registry.go | Symbol length (promoter tickers are longer than 4) |
| `MaxSymbols` | 512 | registry.go | Lazily created symbols. **Resource bound at a trust boundary** |
| `maxAccountIDLen` | 64 | api/server.go | Account ID length |
| `maxPrice` | 2^40 | api/server.go | Price, so `price × quantity` cannot be absurd |
| `maxQuantity` | 2^32 | api/server.go | Quantity, same reason |
| `maxRecordPayload` | 1 MiB | wal.go | Reader allocation from a garbage length prefix |
| `DefaultSegmentBytes` | 64 MiB | wal.go | Segment rotation threshold |
| `maxBatch` | 256 | sequencer.go | Group commit size. **A cap, not a tuning parameter** — never reached |
| `feedBuffer` | 256 | api/feed.go | How far a trade subscriber may fall behind before dropping |
| `walBatchCap` | 512 | api/replication.go | Records per `StreamWAL` message |
| `TailPollInterval` | 5 ms | tail.go | Latency replication adds. Below one fsync, so not the lag source |
| `reconnectDelay` | 250 ms | replica.go | Redial after the primary goes away |
| `progressInterval` | 1 s | replica.go | Advisory lag reporting |
| `symbolRefreshInterval` | 500 ms | replica.go | **Also the worst-case delay before a new symbol is protected at all** — was 5s until the tests exposed the cost |
| `settleTimeout` | 30 s | api/server.go | Ledger write once the trade has already executed |
| `migrationLockID` | 8734121 | ledger.go | Advisory lock identity; arbitrary but must never change |
| `walVersion` | 1 | wal.go | On-disk format version |

Nothing measured picked `maxBatch`; it bounds worst-case latency inheritance and
batch memory, and the final run showed ~6.9 orders/fsync — nowhere near it.

---

## 10. Testing strategy

141 tests. What each layer is *for*:

| Kind | Where | Catches |
|---|---|---|
| Unit | `book_test`, `submit_test` | Matching mechanics: fills, FIFO, multi-level, cancel interactions |
| Property (`rapid`, 2000+ cases) | `property_test` | Invariants under generated input: no negative quantity, priority holds, `sum(fills) == filled` |
| Integration | `sequencer_wal_test`, `registry_test` | Durability, recovery, per-symbol isolation, restart continuity |
| Adversarial | `wal_test` | Corruption, truncation, bad magic, future version, oversized length |
| Crash | `chaos_test` | **Subprocess** panic mid-stream, then recovery |
| E2E | `api/server_test` | Real gRPC over `bufconn`: marshalling, streaming, status codes |
| Replication | `replica/replica_test` | Eventual equality, restart both ways, promotion, damaged log |
| Observability | `metrics/metrics_test` | Rendered `/metrics` output, and scraping while the venue is busy |
| Benchmark | `bench_book_test`, `concurrency_bench_test`, `recovery_bench_test` | Throughput, group-commit ratio, replay time |

**Two techniques worth reusing:**

**The crash test must be a subprocess.** A same-process "crash" cannot escape
Go's deferred cleanup, so it would flush and close the log on the way out —
testing the graceful path while claiming to test the crash path. The child
registers no cleanup at all, deliberately, and prints a marker before panicking
so the parent can tell a planned crash from a setup failure.

**Ledger tests skip without `OMS_TEST_DATABASE_URL`**, and the skip message says
exactly what to set. CI supplies a Postgres service container so the skip does
not become permanent — a silently-skipped test is how a suite rots.

```sh
go test ./... -race
OMS_TEST_DATABASE_URL=postgres:///dhukuti_oms_test go test ./...   # + ledger tests
OMS_BENCH_WAL_DIR=$HOME/.cache go test -bench=SequencerWAL ./internal/oms/
```

`OMS_BENCH_WAL_DIR` is not optional for a number you intend to quote: without it
the WAL benchmarks write to `$TMPDIR`, which is usually a tmpfs, where fsync is
nearly free and the result measures encoding overhead rather than durability.

---

## 11. Extension points

Where to add things, and what each change would touch.

| To add | Start at | Also touches |
|---|---|---|
| A new order type (stop, IOC) | `Book.SubmitOrder` matching loops | proto enum, `orderFromRequest`, WAL payload is already version-tagged |
| Snapshots (bound recovery time) | `Recover`'s `afterSeq` — already the hook | a snapshot writer; `Registry.newSequencer` to load one first |
| Batched settlement | `Sequencer.commit` step 6/7 | `Ledger.Settle` signature; a decision about the waiting client |
| Fencing (prevent split brain) | `Registry`/`OpenWriter` | a lock file or lease; failover runbook |
| Real auth | `validateAccountID` | gRPC interceptor; `CancelFor`'s check becomes enforceable |
| Circuit limits | `Accounts.Reserve` MARKET branch | a per-scrip band store; makes MARKET cost bounded |
| Re-settle after promotion | add `log_position` to `journal_entries` | a migration (runner exists); a promotion step |
| Per-symbol WAL devices | `Registry.walDir` → per-symbol paths | deployment; would recover the ADR-005 fragmentation loss |

The proto is versioned (`dhukuti.oms.v1`) and the WAL carries a `walVersion`, so
both wire and disk formats can change without guesswork.
