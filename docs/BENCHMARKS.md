# Benchmarks

Numbers are what they measure — no target numbers invented ahead of time.
Hardware for every entry below unless noted otherwise: AMD Ryzen 5 4600H
with Radeon Graphics, 12 logical cores, 14 GiB RAM, Linux, on-battery/desktop
usage (not an isolated bench box — expect some run-to-run variance from
background load).

| Week | What | Conditions | Result | Date |
|---|---|---|---|---|
| 1 | `Submit()` throughput, single goroutine, mixed limit(~95%)+market(~5%) | Single symbol, 50/50 buy/sell, depth-controlled workload (see `BenchmarkSubmit` in `internal/oms/bench_book_test.go`), no persistence | 4328 ns/op, 328 B/op, 4 allocs/op → ~231,000 orders/sec | 2026-08-14 |

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
