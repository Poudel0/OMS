# ADR-002: Single-writer goroutine per symbol, channel-fed

- **Status:** Accepted
- **Week:** 2
- **Linked code:** [`internal/oms/sequencer.go`](../../internal/oms/sequencer.go), [`internal/oms/book_mutex.go`](../../internal/oms/book_mutex.go), [`internal/oms/concurrency_bench_test.go`](../../internal/oms/concurrency_bench_test.go)

## Context

`Book` (ADR-001) has no internal synchronization — every `Submit`/`Cancel`
call must be serialized somehow once more than one goroutine can produce
orders concurrently (which is the normal case: a gRPC server handles
concurrent client connections).

## Decision

Each symbol gets one dedicated goroutine (`Sequencer.run`) that is the only
thing that ever touches that symbol's `Book`. Producers call
`Sequencer.Submit`/`Cancel` from any number of goroutines; each call sends a
request over an unbuffered channel and blocks on a per-call reply channel
for the result, composing with `context.Context` for cancellation/timeout at
every step.

## Honest benchmark data — and why it complicates the obvious story

The instinct going in was "channels are the idiomatic Go answer, mutexes are
old news." The data says otherwise. `book_mutex.go` is a straight
`sync.Mutex`-guarded `Book`, benchmarked against the channel sequencer at
1/4/16/64 concurrent producers (`concurrency_bench_test.go`,
`-benchtime=2s`, same machine as ADR-001):

| Producers | Sequencer (channel) | Mutex | Mutex is faster by |
|---|---|---|---|
| 1  | 17373 ns/op | 3341 ns/op | ~5.2x |
| 4  | 13283 ns/op | 4465 ns/op | ~3.0x |
| 16 | 10549 ns/op | 4840 ns/op | ~2.2x |
| 64 | 10212 ns/op | 5216 ns/op | ~2.0x |

The mutex baseline wins at every producer count on this hardware, for a
single symbol. That gap narrows as contention rises (channel overhead is
mostly fixed per-call, while the mutex pays more as contention grows), but
it never closes.

**Profiling before assuming why.** A CPU profile of `BenchmarkSequencer_4`
showed `time.Now()` plus runtime scheduling (`selectgo`, `futex`, `usleep`)
dominating — not the matching logic itself. An allocation profile showed
`(*Sequencer).Submit` alone responsible for 40.8% of all allocated objects:
the fresh `chan submitResult` (and request struct) allocated on every call.

Applying the evidence-backed fix — `sync.Pool` for reply channels, only
returning a channel to the pool in the two states where it's provably empty
(never handed to the goroutine, or already drained) — cut allocations from
5 to 3 per op and bytes/op from ~550 to ~340, matching the mutex baseline's
own allocation profile. **Throughput barely moved** (17373 → 17373 ns/op at
1 producer, within noise). The allocation fix was real and worth keeping,
but it wasn't the bottleneck — the cost is the channel hand-off and goroutine
scheduling itself, not what gets allocated along the way. Measure-then-
optimize discipline means reporting that a fix didn't move the metric it was
aimed at, not moving on silently.

## Why single-writer-per-symbol anyway, given the mutex wins on raw speed

For **one** symbol in isolation, a mutex is simpler and measurably faster.
The channel model earns its keep for reasons a single-symbol microbenchmark
doesn't capture:

- **Cancellation composes for free.** `sync.Mutex.Lock()` cannot be
  cancelled or timed out — once called, a goroutine waits until it acquires
  the lock, full stop. Every call into the sequencer already composes with
  `ctx.Done()` via `select`, both while handing off the request and while
  waiting for the reply (`TestSequencer_SubmitTimesOutIndependentlyOfSequencer`,
  `TestSequencer_ContextCancellationDoesNotHang`). Reproducing that with a
  mutex means replacing it with a size-1 buffered channel used as a
  semaphore — at which point it's not really "just a mutex" anymore.
- **Multi-symbol isolation without extra sharding logic.** A single global
  mutex across every symbol would serialize unrelated instruments against
  each other. The alternative that actually matches the mutex's raw speed —
  one `sync.Mutex` *per symbol* — would offer the same per-symbol
  parallelism ADR-005's registry needs. That's the honest nearest
  competitor, not a single global lock. The sequencer is chosen over it
  because of the point below, not because per-symbol mutexes would be
  slower — they wouldn't be.
- **One place to hang cross-cutting concerns.** The sequencer goroutine is
  the natural, single insertion point for WAL append-before-match (ADR-003),
  request/response latency instrumentation (already in `OrderResponse`), and
  later backpressure or tracing — without touching every call site. A
  per-symbol mutex's critical section *could* host the same WAL-append step,
  but the channel model gets clean shutdown (context cancellation drains
  naturally, see `run`'s `defer close(s.done)`) without a separate
  stop-accepting-new-work flag that a mutex-based design would need to
  build by hand.

In short: this is chosen for composability with cancellation, per-symbol
isolation, and a clean integration point for the WAL — not because it's
faster than the alternative for a single lock. "Honest > impressive."

## Consequences

- `book_mutex.go` stays in the repo as a permanent comparison baseline, not
  a throwaway — future benchmark deltas (Week 6's final numbers) should be
  read against both.
- The `sync.Pool` optimization is kept because it's a real, uncontested
  allocation win even though it didn't move throughput — cheaper GC pressure
  under sustained load is still worth having.
- Multi-symbol partitioning (ADR-005) inherits the per-symbol-goroutine
  design directly: `N` symbols run on `N` independent sequencer goroutines
  with zero cross-symbol contention, which is the actual payoff this
  architecture is for.
