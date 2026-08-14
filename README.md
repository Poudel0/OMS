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
- [ ] Write-ahead log + crash recovery
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

```sh
go test ./internal/oms/... -race                                        # correctness + property tests, race-checked
go test -bench=BenchmarkSubmit -benchtime=10s -benchmem ./internal/oms/  # single-threaded throughput
go test -bench='BenchmarkSequencer_|BenchmarkMutex_' -benchtime=2s -benchmem ./internal/oms/  # channel vs mutex, 1/4/16/64 producers
go build -o bench ./cmd/bench && ./bench                                # multi-hour unattended stress simulation
```

## Design decisions

Each non-obvious architectural choice is written up as an ADR before or
alongside the code that implements it:

- [ADR-001: L3 order book as a price-indexed map of FIFO queues](docs/adr/0001-order-book-data-structure.md)
- [ADR-002: Single-writer goroutine per symbol, channel-fed](docs/adr/0002-single-writer-sequencer.md)

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
