# Documentation

A NEPSE-style stock exchange matching engine in Go: L3 order book, single-writer
concurrency, WAL-backed crash recovery, Postgres double-entry settlement, gRPC
API, and async replication to a passive follower.

**Measured headline:** 1,383 orders/sec sustained over 60s, p50 38.7ms, p99 130ms
— durable, settled, and replicated, on one laptop. 141 tests, race-clean.

---

## Start here, depending on why you're here

| If you want to… | Read |
|---|---|
| Understand what this is and why it's shaped this way | [`architecture.md`](architecture.md) — the narrative writeup |
| Learn the trading domain (order books, settlement, T+2, wash trades) | [`domain.md`](domain.md) |
| Find your way around the code | [`technical.md`](technical.md) |
| Know why a specific decision was made | [`decisions.md`](decisions.md) → the relevant [ADR](adr/) |
| Know what's broken or missing | [`gaps.md`](gaps.md) |
| See the numbers, and what they cost to get right | [`BENCHMARKS.md`](BENCHMARKS.md) |
| Run it | [`../README.md`](../README.md) |
| Fail over to the follower | [`failover.md`](failover.md) |

**New to the project?** `architecture.md` → `domain.md` → `technical.md`.
**Reviewing it?** `decisions.md` and `gaps.md` are where the honesty lives.

---

## The documents

### [`architecture.md`](architecture.md) — the writeup
What this is, how it works, and the four times a measurement contradicted the
design reasoning that produced it. The piece to read first, and the one to link
from a CV.

### [`domain.md`](domain.md) — the business
Every domain term the code uses, defined for a reader who knows software but not
markets: order books, price-time priority, maker/taker, double-entry, T+0 vs T+2,
circuit limits, wash trading, replication lag. Ends with a table of every place
the model departs from a real venue.

### [`technical.md`](technical.md) — the reference
Package map, request flow, every invariant and what enforces it, the concurrency
ownership table, on-disk WAL format, database schema, gRPC surface and status-code
mapping, every tunable constant, testing strategy, and where to add things.

### [`decisions.md`](decisions.md) — decisions and problems
Fifteen decisions with the pro/con each way, and eight problems with what they cost
to find. Ends with a table of which problems production would *never* have
surfaced on its own.

### [`gaps.md`](gaps.md) — limitations
Everything not done or done incompletely, by severity (data loss / correctness /
operational / scope), each with impact and the known fix. Includes the deliberate
scope exclusions and a priority order if work continued.

### [`BENCHMARKS.md`](BENCHMARKS.md) — measurements
Every number, with the conditions it was taken under. Nothing here is a target.
Includes the runs that disappointed and the near-misses where a wrong number was
almost published.

### [`failover.md`](failover.md) — runbook
Manual promotion, step by step, including the reconciliation step promotion does
*not* do for you.

### [`adr/`](adr/) — decision records
| ADR | Subject |
|---|---|
| [001](adr/0001-order-book-data-structure.md) | L3 book: price-indexed map of FIFO queues |
| [002](adr/0002-single-writer-sequencer.md) | Single-writer goroutine per symbol, channel-fed |
| [003](adr/0003-wal-design.md) | WAL, group-commit fsync, deterministic replay |
| [004](adr/0004-synchronous-settlement.md) | Synchronous settlement, idempotent by construction |
| [005](adr/0005-multi-symbol-partitioning.md) | Per-symbol sequencer registry |
| [006](adr/0006-wal-shipping-replication.md) | WAL shipping, passive follower, manual failover |

### [`bench/`](bench/) — raw benchmark output
Unedited tool output behind every figure in `BENCHMARKS.md`, so any number can be
traced to the run that produced it.

---

## The five things worth knowing

If you read nothing else:

1. **fsync is the entire engine.** ~99.5% of a durable order's latency is one
   device flush (~3.4ms on this hardware). Every clever thing about the matching
   path is three orders of magnitude from mattering.
   → [ADR-003](adr/0003-wal-design.md)

2. **Group commit is the only lever, and it makes contention a feature.** One
   fsync per *batch*, not per order. More concurrent load ⇒ bigger batches ⇒
   better per-order throughput. Worth 43×.

3. **Per-symbol partitioning makes durable throughput *worse*** — 2.2× worse
   across 1 → 8 symbols, because it fragments the batching durability depends on.
   The operational conclusion inverts: fewer symbols per node, on one device.
   → [ADR-005](adr/0005-multi-symbol-partitioning.md)

4. **The load test found bugs the unit tests could not.** Self-trades silently
   destroyed half of every affected journal entry — 12,266 rows, net cash
   −40,013,771 — behind a passing suite. Unit tests chose tidy inputs; a random
   workload didn't. → [`decisions.md` P3](decisions.md)

5. **The largest remaining gap loses settled money.** Promotion rebuilds the book,
   not the journal, so executed-but-unsettled trades never reach Postgres. The fix
   is fully specified and not built. → [`gaps.md` #1](gaps.md)

---

## Conventions used throughout

- **No invented numbers.** Every figure comes from a run whose raw output is in
  [`bench/`](bench/). Where a measurement contradicted the expectation, the
  contradiction is documented rather than the expectation quietly dropped.
- **`ponytail:` comments** mark deliberate simplifications in the code. Each names
  its ceiling and the upgrade path, so a shortcut reads as intent rather than
  ignorance.
- **Comments explain *why*, never *what*.** If a line's purpose is not obvious from
  the code, the comment says what would go wrong otherwise.
- **Gaps are documented, not hidden.** A limitation known only to the author is,
  to everyone else, a limitation that does not exist until it causes an incident.
