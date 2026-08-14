# Dhukuti OMS

A NEPSE-style order matching engine in Go: L3 price-time-priority order
book, single-writer-per-symbol concurrency, WAL-backed crash recovery, a
Postgres double-entry ledger, gRPC API, and async replication to a passive
follower.

## Status

Actively built in stages; each stage lands with tests and a benchmark
before the next one starts. Current state:

- [x] Order book: LIMIT + MARKET matching, price-time priority, O(1) cancel
- [x] Single-writer sequencer per symbol (channel-fed goroutine)
- [x] Write-ahead log + crash recovery (group-commit fsync, CRC'd records,
      segment rotation, subprocess crash test)
- [x] Multi-symbol registry (one goroutine, book, and log per symbol)
- [x] gRPC API (PlaceOrder / CancelOrder / StreamTrades)
- [x] Postgres double-entry settlement, idempotent on trade identity
- [x] WAL shipping to a passive follower, manual failover
- [ ] Tracing (OpenTelemetry) + metrics (Prometheus)

See [`docs/adr/`](docs/adr/) for the design decisions behind each piece as
they land, and [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for measured
numbers — nothing in that file is a target, only what was actually measured.

## Architecture

```
Client
  |  PlaceOrder / CancelOrder / StreamTrades (gRPC)
  v
[gRPC handler] -- inline balance check
  |  routes to per-symbol sequencer via registry
  v
[Sequencer per symbol] -- single goroutine, monotonic SeqID, WAL append+fsync
  |  channel-fed; reply channel returns trades to caller
  v
[Order Book per symbol] -- L3, price-time priority, in-memory
  |  emits []Trade
  v
[Settlement] -- synchronous function call, posts double-entry journal to Postgres
  v
[Postgres Ledger] -- idempotent on trade ID

Parallel: [WAL Shipper] -- streams WAL records to a follower node (gRPC server-stream)
Follower replays the WAL to maintain hot-standby book state.
```

## Order book

[`internal/oms`](internal/oms) implements the matching engine core:

- `Book.Submit(Order) ([]Trade, error)` — matches an incoming LIMIT or
  MARKET order against the opposite side, price-time priority, and rests
  any unfilled LIMIT remainder. MARKET orders never rest — see
  [ADR-001](docs/adr/0001-order-book-data-structure.md).
- `Book.Cancel(SeqID) error` — O(1) removal via an order-ID backref index.
- `Book.Snapshot() BookState` — aggregated (L2) depth as JSON.

`internal/oms` also has a `Sequencer`: one goroutine per symbol is the only
thing that ever touches that symbol's `Book`, so the book itself needs no
lock. Producers call `Sequencer.Submit`/`Cancel` from any number of
goroutines, with `context.Context` cancellation/timeout composing naturally
at every step — see [ADR-002](docs/adr/0002-single-writer-sequencer.md) for
why this is the chosen design even though a plain `sync.Mutex` measures
faster for a single symbol in isolation (`book_mutex.go` is kept in the repo
as a permanent comparison baseline, not a throwaway).

## Write-ahead log

`Sequencer` is also where durability lives. Every mutation is appended to a
CRC-checked log and fsynced **before** the book is allowed to see it, so a
trade is never acknowledged unless it is already recoverable. If the fsync
fails, the book is not touched and the whole batch is rejected.

- `OpenWriter(dir)` / `Append` / `Sync` — `Append` only buffers; `Sync` is the
  fsync. Splitting them is what allows group commit.
- `Recover(dir, book, afterSeq)` — replays every segment into a book and
  returns the highest log position seen.
- `Reader.Records()` returns an `iter.Seq2[Record, error]`, stopping at the
  first bad record.

The sequencer drains every request already queued behind the first, appends
them all, and fsyncs **once**. No timer and no linger interval — queue depth
is the signal, so batches grow exactly when load makes them worth having.
That is worth 43× durable throughput between 1 and 64 producers, and the
benchmarks report the measured `orders/fsync` ratio that explains it.

A truncated tail (bytes that never reached disk, so were never acknowledged)
is recovered from silently and truncated away. A CRC mismatch — bytes that are
present but wrong — halts replay with `ErrCorruptWAL` and refuses to append on
top. See [ADR-003](docs/adr/0003-wal-design.md), including what replay
deliberately does *not* reproduce.

Crash recovery is tested by actually crashing: `TestChaos_*` runs the engine
in a child process, panics it mid-stream after 10,000 mutations, and asserts
the recovered book matches the pre-crash state exactly.

```sh
go test ./internal/oms/... -race                                        # correctness + property + crash-recovery tests, race-checked
go test -bench=BenchmarkSubmit -benchtime=10s -benchmem ./internal/oms/  # single-threaded throughput
go test -bench='BenchmarkSequencer_|BenchmarkMutex_' -benchtime=2s -benchmem ./internal/oms/  # channel vs mutex, 1/4/16/64 producers
OMS_BENCH_WAL_DIR=$HOME/.cache go test -bench=SequencerWAL -benchtime=2s -benchmem ./internal/oms/  # durable throughput on REAL storage
go build -o bench ./cmd/bench && ./bench                                # multi-hour unattended stress simulation
```

`OMS_BENCH_WAL_DIR` is not optional for a number you intend to quote: without
it the WAL benchmarks write to `$TMPDIR`, which on most Linux desktops is a
tmpfs, where fsync is nearly free and the result measures encoding overhead
rather than durability.

## Running it

```sh
createdb dhukuti_oms
make serve                    # or: go run ./cmd/server -h  for the flags

# a node you can actually trade against
go run ./cmd/server \
  -addr 127.0.0.1:9090 \
  -wal ./data/wal \
  -db postgres:///dhukuti_oms \
  -seed-accounts 8
```

`-seed-accounts` funds demo accounts (`acct-0`…) so a fresh node can be traded
against; it is development-only, and the server says so in its logs. Starting
without `-wal` or `-db` works and warns loudly about what is not durable.

gRPC reflection is on, so `grpcurl` needs no `.proto`:

```sh
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext -d '{"symbol":"NABIL","account_id":"acct-0","side":"SIDE_SELL","type":"ORDER_TYPE_LIMIT","price":500,"quantity":100}' \
  localhost:9090 dhukuti.oms.v1.OrderService/PlaceOrder
grpcurl -plaintext -d '{"symbol":"NABIL"}' localhost:9090 dhukuti.oms.v1.OrderService/StreamTrades
```

`make proto` regenerates the stubs. The plugin versions come from the `tool`
directives in `go.mod` rather than from `$PATH`, so it reproduces anywhere and
does not disturb a globally installed `protoc-gen-go`.

## gRPC API

[`internal/api`](internal/api) validates what arrives off the network, routes to
the sequencer that owns the symbol, and settles.

- `PlaceOrder` — synchronous: returns once the order is durable and matched,
  carrying every trade it produced. **Order IDs are assigned by the venue**, not
  the client: they are sequential, so a client-chosen ID could name an order
  another account holds.
- `CancelOrder` — checks that the requesting account actually placed the order.
  Guessing a sequential ID is trivial, so without that check any client could
  cancel anyone's orders. The account claim is *not authenticated* in v1 (auth is
  out of scope) — the check is built so that adding auth later makes it real
  without changing this logic.
- `StreamTrades` — live feed from the moment of subscription, published from
  inside the sequencer goroutine so trades cannot arrive out of match order. A
  subscriber that falls behind loses trades rather than stalling the matcher.
- `GetBookSnapshot` — aggregated depth plus the log position it was taken at,
  served from inside the sequencer goroutine so it is a consistent point-in-time
  view. Mainly for comparing two nodes after a failover.

Symbols are a trust boundary: a symbol becomes a directory name under the WAL
root, so it is checked against an allowlist (`A-Z0-9`, ≤12 chars) and the number
of lazily created symbols is capped. See
[ADR-005](docs/adr/0005-multi-symbol-partitioning.md).

## Multi-symbol

[`oms.Registry`](internal/oms/registry.go) maps a symbol to the one sequencer
that owns its book, creating it — and replaying its log — on first use. Each
symbol gets its own goroutine, book, log directory, and log-position / trade-ID
sequences, so unrelated instruments never contend.

## Settlement

[`internal/ledger`](internal/ledger) posts double-entry rows to Postgres: one
trade writes four legs (buyer debits shares and credits cash, seller the
reverse), and for any `(symbol, trade_id, asset)` the debits equal the credits.

Settlement is **idempotent by construction** — a unique index over a trade's legs
plus `ON CONFLICT DO NOTHING` — rather than by checking first, because
check-then-insert is a race. It has to be idempotent: replay re-derives trades
from the log, so the same trade can be presented twice.

See [ADR-004](docs/adr/0004-synchronous-settlement.md), including the self-trade
bug a load test found in the original idempotency key, and the two known gaps
(the pre-trade check is not atomic with the order; a MARKET buy's cost cannot be
bounded without circuit limits).

```sh
OMS_TEST_DATABASE_URL=postgres:///dhukuti_oms_test go test ./internal/ledger/
```

Ledger tests skip without that variable, since CI needs to build without a
database — CI supplies one via a service container so the skip does not become
permanent.

## Replication and failover

One primary ships its write-ahead log to one passive follower, which replays it
into its own books **and its own log**:

```sh
go run ./cmd/follower -primary 127.0.0.1:9090 -wal ./data/follower-wal
grpcurl -plaintext localhost:9090 dhukuti.oms.v1.ReplicationService/ReplicationStatus
```

The follower reads the primary's log *files* rather than being fed from the
sequencer, and that is the load-bearing choice: **a follower cannot exert
backpressure on matching**, because nothing on the matching path waits for it. It
can be slow, stalled, or absent and the primary's throughput is unchanged.

It stores records under the **primary's** log positions, never renumbered. So its
log *is* a primary's log, which buys two things: restart needs no checkpoint file
(its last position is the checkpoint), and promotion is just pointing a normal
server at the directory — no conversion step to get wrong while the venue is down.

Demonstrated end to end in [`docs/bench/week5-failover-demo.txt`](docs/bench/week5-failover-demo.txt):
after 3,714 orders, `kill -9` on the primary and promotion of the follower gives a
book byte-identical to the lost primary's at the same log position, accepting new
orders immediately.

Async replication means a primary lost while the follower is N records behind
loses those N acknowledged orders — `ReplicationStatus` is how you see how wide
that window currently is. The procedure, and the reconciliation step promotion
does *not* do for you, are in [`docs/failover.md`](docs/failover.md).

## Load testing

```sh
go run ./cmd/loadgen -addr 127.0.0.1:9090 -workers 64 -duration 30s -accounts 8
```

[`cmd/loadgen`](cmd/loadgen) is a real gRPC client rather than `hey`/`vegeta`
(HTTP-only) or `ghz` (another dependency): it drives the generated stubs over a
real connection and reports throughput plus p50/p95/p99/p99.9.

## Design decisions

Each non-obvious architectural choice is written up as an ADR before or
alongside the code that implements it:

- [ADR-001: L3 order book as a price-indexed map of FIFO queues](docs/adr/0001-order-book-data-structure.md)
- [ADR-002: Single-writer goroutine per symbol, channel-fed](docs/adr/0002-single-writer-sequencer.md)
- [ADR-003: WAL with group-commit fsync and deterministic replay](docs/adr/0003-wal-design.md)
- [ADR-004: Synchronous in-process settlement, idempotent on trade identity](docs/adr/0004-synchronous-settlement.md)
- [ADR-005: Multi-symbol partitioning via a per-symbol sequencer registry](docs/adr/0005-multi-symbol-partitioning.md)
- [ADR-006: Async WAL shipping to a passive follower, manual failover](docs/adr/0006-wal-shipping-replication.md)

More land as the corresponding subsystem is built (WAL, settlement,
multi-symbol partitioning, replication).

## What this deliberately does not build

Kafka, multi-tenant auth, a standalone risk engine, rate limiting, market
data fan-out, T+2 settlement, smart order routing/FIX, stop/iceberg/hidden
orders, and Raft/consensus-based failover are all out of scope by design —
each has a narrower, honest substitute already in the architecture above
(the WAL is the event log; a single manual-failover follower stands in for
consensus). This keeps the scope one engineer can actually finish and
explain end to end, rather than a shallow layer over tools that do the
interesting work invisibly.
