# Benchmarks

Numbers are what they measure — no target numbers invented ahead of time.
Hardware for every entry below unless noted otherwise: AMD Ryzen 5 4600H
with Radeon Graphics, 12 logical cores, 14 GiB RAM, Linux, on-battery/desktop
usage (not an isolated bench box — expect some run-to-run variance from
background load).

| Week | What | Conditions | Result | Date |
|---|---|---|---|---|
| 1 | `Submit()` throughput, single goroutine, mixed limit(~95%)+market(~5%) | Single symbol, 50/50 buy/sell, depth-controlled workload (see `BenchmarkSubmit` in `internal/oms/bench_book_test.go`), no persistence | 4328 ns/op, 328 B/op, 4 allocs/op → ~231,000 orders/sec | 2026-08-14 |
| 2 | Channel-sequencer throughput, 1/4/16/64 concurrent producers | Same workload shape as week 1, split across N goroutines (`BenchmarkSequencer_*`), before the `sync.Pool` optimization | 1: 17753 ns/op (5 allocs/op) · 4: 13285 ns/op · 16: 10479 ns/op · 64: 10741 ns/op | 2026-08-14 |
| 2 | Mutex-baseline throughput, same conditions | `BenchmarkMutex_*`, `sync.Mutex`-guarded `Book`, same hardware and workload | 1: 3350 ns/op (3 allocs/op) · 4: 4489 ns/op · 16: 4791 ns/op · 64: 5135 ns/op | 2026-08-14 |
| 2 | Channel-sequencer throughput, after `sync.Pool` reply-channel optimization | Same as above, evidence from CPU+alloc profiles in `docs/bench/week2-*-profile-top.txt` | 1: 17373 ns/op (3 allocs/op) · 4: 13283 ns/op · 16: 10549 ns/op · 64: 10212 ns/op — throughput essentially unchanged despite allocs dropping 5→3 | 2026-08-14 |
| 3 | **Durable** throughput, WAL append + group-commit fsync before matching, 1/4/16/64 producers | `BenchmarkSequencerWAL_*` on **real storage**: btrfs on LUKS-encrypted Samsung NVMe SSD, `compress=zstd:3`. Raw: `docs/bench/week3-wal-realdisk.txt` | 1: 3,406,059 ns/op @ 1.00 orders/fsync → ~294/sec · 4: 1,762,632 @ 2.03 → ~567/sec · 16: 364,905 @ 10.52 → ~2,741/sec · 64: 79,405 @ 61.30 → ~12,594/sec | 2026-08-14 |
| 3 | Same, **tmpfs control** (fsync ≈ free) | Identical code, WAL on `/tmp` (tmpfs = RAM). Isolates the WAL's encode/syscall cost from the device flush. Raw: `docs/bench/week3-wal-tmpfs-control.txt` | 1: 22,307 ns/op → ~44,829/sec · 4: 17,048 · 16: 10,602 · 64: 9,765 → ~102,406/sec (9 allocs/op) | 2026-08-14 |
| 3 | No-WAL sequencer, re-measured in the same run as the two rows above | `BenchmarkSequencer_*`, unchanged code path, for a same-session comparison | 1: 16,866 ns/op → ~59,290/sec · 4: 11,231 · 16: 7,422 · 64: 7,353 → ~136,000/sec | 2026-08-14 |

| 4 | **Full stack** end-to-end: gRPC → registry → sequencer → WAL fsync → matcher → Postgres settlement | `cmd/loadgen`, 64 concurrent gRPC clients across 4 symbols, 30s. WAL on btrfs/LUKS/zstd:3 NVMe; Postgres 17 over a local socket on the same machine. Raw: `docs/bench/week4-grpc-loadtest.txt` | **1,973 orders/sec** · p50 28.9ms · p95 55.7ms · p99 71.3ms · p99.9 132.9ms · 47,293 trades, 0 rejected | 2026-08-14 |
| 4 | Same, **without the ledger** (isolates settlement cost) | Identical load, server started with no `-db` | **3,910 orders/sec** · p50 14.8ms · p95 27.8ms · p99 32.9ms · p99.9 105.5ms | 2026-08-14 |

## Notes

- The first two attempts at this number (798 ns/op and 2561–4001 ns/op) were
  discarded — they measured a workload with a fixed price anchor that let
  the book grow one-sidedly without bound as the benchmark's iteration count
  scaled up, which pollutes throughput with GC/growth pathology instead of
  steady-state matching cost. See the reasoning in the `BenchmarkSubmit` doc
  comment and the identical fix in `cmd/bench/main.go`.
- 231k orders/sec single-threaded, mixed limit+market, lands inside this
  project's own predicted honest range (100k–500k on a laptop) — a sign the
  fixed workload is measuring real matching work, not a degenerate case.
- `328 B/op, 4 allocs/op` is the baseline for the profiling pass in Week 2 —
  the goal there is to measure where those allocations come from and reduce
  them with evidence, not guesswork.
- Week 2's honest finding: the mutex baseline is faster than the channel
  sequencer at every producer count, and the `sync.Pool` optimization
  (evidence: `docs/bench/week2-alloc-profile-top.txt` shows
  `(*Sequencer).Submit` at 40.8% of allocated objects) cut allocations
  meaningfully without moving throughput — the CPU profile
  (`docs/bench/week2-cpu-profile-top.txt`) shows the cost is goroutine
  scheduling/channel hand-off itself, not allocation. See
  [ADR-002](adr/0002-single-writer-sequencer.md) for why the channel model
  is still the right choice despite losing the raw-speed comparison.

### Week 3: storage is the benchmark now

- **Always say which storage a durable number came from.** `/tmp` on this
  machine is a tmpfs, so `b.TempDir()` puts the WAL in RAM where fsync is
  nearly free. The default benchmark run therefore does *not* measure
  durability. Point `OMS_BENCH_WAL_DIR` at real storage for that:

  ```sh
  OMS_BENCH_WAL_DIR=$HOME/.cache go test -bench=SequencerWAL -benchtime=2s ./internal/oms/
  ```

  A tmpfs fsync number presented as a durability number is an invented number.
- **The `orders/fsync` column is measured, not inferred** —
  `Sequencer.CommitStats` counts group commits and the requests they covered,
  and the benchmark reports the ratio via `b.ReportMetric`.
- **The WAL's own code costs ~32%; fsync costs 200×.** tmpfs vs no-WAL is
  +32% at 1 producer and +33% at 64 — that's marshalling, the buffered write,
  and extra syscalls. On real storage the same code is 200× slower than no WAL
  at 1 producer, so ~99.5% of a durable order's latency is one device flush.
- **Group commit is verifiable from the table, not just claimed:** multiply
  ns/op by the measured batch size and you get 3.41, 3.57, 3.84, 4.87 ms — one
  roughly constant device flush across a 43× throughput spread. Throughput
  rises because each fsync serves 1 → 61 orders, not because anything got
  faster.
- ~3.4 ms is *this stack's* fsync, not a property of NVMe: btrfs fsync is
  copy-on-write plus tree-log, and dm-crypt adds more on top.
- **The 136k/sec figure is a no-durability number** and must never be quoted
  as venue throughput. The durable numbers are ~294/sec (1 producer) and
  ~12.6k/sec (64). See [ADR-003](adr/0003-wal-design.md).

### Week 4: the end-to-end number, and which one to quote

- **Quote 1,973 orders/sec.** That is the whole system doing its actual job:
  gRPC in, validated, balance-checked, durably logged, matched, and journalled
  to Postgres in a double-entry transaction. Every other number on this page is
  a component measured in isolation.
- **Settlement roughly halves throughput and doubles median latency**
  (3,910 → 1,973 orders/sec, p50 14.8ms → 28.9ms). Two thirds of the way down
  this stack, the interesting concurrency work is being paid for by two fsyncs:
  one in the WAL, one in Postgres's own commit. The obvious next lever is
  batching settlement the way the WAL already batches appends — one transaction
  per group commit rather than per order.
- **Per-symbol partitioning makes durable throughput WORSE, and that was not the
  expectation.** Same total load across a rising symbol count
  (`docs/bench/week4-symbol-scaling.txt`, 64 clients, 20s):

  | Symbols | With WAL | Without WAL |
  |---|---|---|
  | 1 | **7,012/sec** | 44,954/sec |
  | 2 | 4,626 | 41,401 |
  | 4 | 3,690 | 41,541 |
  | 8 | **3,134** | 43,091 |

  Durable throughput falls 2.2× while the no-WAL control stays flat within noise.
  So matching parallelism is real and the loss is entirely in durability: it is
  **group-commit fragmentation**. One symbol queues all 64 clients into one
  sequencer, so each fsync amortises across a big batch; eight symbols give each
  sequencer ~8 clients and eight independent fsync streams contending for a device
  that serialises them anyway. More partitions → smaller batches → less
  throughput.

  Operational conclusion, the reverse of the intuitive one: **on a single device,
  fewer symbols per node is better for durable throughput.** Scaling out means
  more devices or more nodes, not more goroutines. See
  [ADR-005](adr/0005-multi-symbol-partitioning.md).
- **The load test found two bugs the unit tests could not**, both recorded in
  [ADR-004](adr/0004-synchronous-settlement.md): a settlement context that
  inherited client cancellation, and an idempotency key that silently dropped
  half of every self-trade's journal legs (12,266 rows missing, net cash
  -40,013,771 where it should have been 0). Unit tests chose tidy inputs; a
  random workload self-traded within seconds. **After the fix: 47,322 trades,
  all with exactly 4 legs, 0 imbalanced groups, net cash and net shares both 0.**
  Run `Ledger.Imbalance` against a real journal after any load test.
