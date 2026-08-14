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
