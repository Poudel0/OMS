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
- [ ] Multi-symbol registry
- [ ] gRPC API (PlaceOrder / CancelOrder / StreamTrades)
- [ ] Postgres double-entry settlement, idempotent on trade ID
- [ ] WAL shipping to a passive follower
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

## Design decisions

Each non-obvious architectural choice is written up as an ADR before or
alongside the code that implements it:

- [ADR-001: L3 order book as a price-indexed map of FIFO queues](docs/adr/0001-order-book-data-structure.md)
- [ADR-002: Single-writer goroutine per symbol, channel-fed](docs/adr/0002-single-writer-sequencer.md)
- [ADR-003: WAL with group-commit fsync and deterministic replay](docs/adr/0003-wal-design.md)

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
