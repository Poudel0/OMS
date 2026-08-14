# Building a NEPSE trading engine: what the measurements changed my mind about

I built a stock exchange matching engine in Go over six weeks — order book,
durability, settlement, replication — with one rule: no invented numbers. Every
figure had to come from a measurement I actually ran.

That rule turned out to be the most valuable part. Four times, the measurement
contradicted the design reasoning that produced it. This is a writeup of the
architecture, but mostly it is a writeup of those four times.

**The finished thing:** 1,383 orders/sec sustained over 60 seconds, p50 38.7ms,
p99 130ms — durably logged, settled to a double-entry Postgres journal, and
replicated to a live follower, on one laptop. 135 tests, race-clean.

---

## The shape

A trading venue has one hard constraint: for a given instrument, orders must
match in a defined order, and the result must survive the machine dying.

The design that falls out is a **single writer per symbol**. One goroutine owns
one order book. Every mutation for that symbol funnels through one channel to
that goroutine, so the book itself needs no lock. Other symbols are entirely
independent — different goroutine, different book, different log.

Inside that goroutine, five steps in a fixed order:

1. **Reserve** — atomically check the order against `owned − already committed`
2. **Append** to the write-ahead log
3. **fsync** — once per batch, not per order
4. **Match** against the book
5. **Settle** and reconcile the reservation

The order is the design. Everything interesting is a consequence of it.

---

## Surprise 1: the concurrency work I was proud of stopped mattering

Week 2 was the hard week. I built the channel-fed sequencer, benchmarked it
against a plain mutex baseline, profiled it, and found `Submit` responsible for
40.8% of allocated objects — fresh reply channels on every call. I pooled them
with `sync.Pool`, cut allocations from 5 to 3 per op, and throughput moved
essentially not at all. The CPU profile said the cost was goroutine scheduling
and channel handoff, not allocation.

I wrote that up honestly, including that the mutex baseline beat the channel
version at every concurrency level (17.4µs vs 3.3µs at one producer). The channel
design was justified on composability instead: `sync.Mutex.Lock()` cannot be
cancelled or timed out, while every sequencer call composes with `ctx.Done()` for
free.

Then Week 3 added the write-ahead log, and the numbers looked like this:

| Producers | No WAL | WAL (tmpfs, fsync ≈ free) | WAL (real disk) |
|---|---|---|---|
| 1 | 16,866 ns/op | 22,307 | **3,406,059** |
| 64 | 7,353 ns/op | 9,765 | 79,405 |

**The WAL's own code costs 32%. Real fsync costs 200×.** At one producer, ~99.5%
of a durable order's latency is a single device flush. Nothing about the matching
engine — not the book, not the channel handoff I spent a week profiling — is
within three orders of magnitude of mattering.

ADR-002's conclusion was correct and had become irrelevant. Both halves of that
are worth saying: the profiling wasn't wrong, and the thing it optimised stopped
being the bottleneck the moment durability arrived.

The tmpfs column is there because of a near-miss. My first WAL benchmark used
`b.TempDir()`, which lands in `$TMPDIR` — and `/tmp` on my machine is a **tmpfs**.
I had measured RAM and was about to publish it as the cost of durability. The
benchmarks now take an explicit `OMS_BENCH_WAL_DIR`, and both configurations are
reported, because the pair is genuinely informative: it separates the WAL's
encoding cost from the device's flush cost.

## Group commit, and why it makes contention a feature

If one fsync costs 3.4ms, the only lever is serving more orders per fsync. So the
sequencer drains every request already queued behind the first, appends them all,
and fsyncs once.

No timer, no linger interval. **Queue depth is the signal** — batches grow exactly
when load makes them worth having, and a lone request commits immediately rather
than waiting for company that may never arrive.

| Producers | orders/fsync | ns/op | ns/op × batch |
|---|---|---|---|
| 1 | 1.00 | 3,406,059 | 3.41 ms |
| 4 | 2.03 | 1,762,632 | 3.57 ms |
| 16 | 10.52 | 364,905 | 3.84 ms |
| 64 | 61.30 | 79,405 | 4.87 ms |

That last column is the whole model in one place: multiply throughput by measured
batch size and you get one roughly constant device flush. Throughput rises 43×
because each fsync went from serving 1 order to 61 — not because anything got
faster. The `orders/fsync` column is measured, not inferred, which is what makes
it an explanation rather than a story.

The nice part: in ADR-002 more concurrency meant more scheduling overhead to
apologise for. With an fsync in the path, more concurrency means bigger batches
means *better* per-order throughput. Contention became a feature.

## Surprise 2: partitioning made durable throughput worse

Week 4 added multi-symbol support: a registry mapping each symbol to its own
sequencer, book, and log. The expectation was obvious — N symbols, N independent
goroutines, more throughput.

Same total load, spread across a rising number of symbols:

| Symbols | With WAL | Without WAL |
|---|---|---|
| 1 | **7,012/sec** | 44,954/sec |
| 8 | **3,134/sec** | 43,091/sec |

Durable throughput **falls 2.2×**. The no-WAL control is flat within noise, which
localises it precisely: matching parallelism is real and works exactly as
designed; the entire loss is in durability.

The mechanism is **group-commit fragmentation**, and it is the previous finding
pointing the other way. One symbol queues all 64 clients into one sequencer, so
each fsync amortises across a big batch. Eight symbols give each sequencer ~8
clients — eight independent fsync streams contending for a device that serialises
them anyway. More partitions, smaller batches, same device capacity, less
throughput.

So the operational conclusion inverts: **on a single device, fewer symbols per
node is better for durable throughput.** Scaling out means more devices or more
nodes, not more goroutines.

I had claimed the partitioning speed-up in an ADR before measuring it. The sweep
that would have proved it was the one that disproved it.

## Surprise 3: the load test found two bugs the unit tests could not

Week 4's ledger writes four double-entry rows per trade — buyer debits shares and
credits cash, seller the reverse — in one transaction, idempotent by construction:
a unique index over a trade's legs plus `ON CONFLICT DO NOTHING`. Idempotence is
not optional, because replay re-derives trades from the log, so the same trade can
be presented twice.

Unit tests passed. Then a 30-second load test with 8 accounts, and the journal
came back with **12,266 rows missing and net cash of −40,013,771** where it should
have been zero. 6,133 of 47,935 trades had only two of their four legs.

The cause was **self-trades**. With a handful of accounts trading randomly, an
account crosses itself within seconds. The key was
`(symbol, trade_id, account_id, asset)` — but when buyer and seller are the *same*
account, the four legs collapse to two keys, and the two counter-legs hit
`ON CONFLICT DO NOTHING` and vanished. Silently. The journal recorded accounts
receiving shares and paying nothing.

Two lessons I'd rather write down than quietly fix:

**`ON CONFLICT DO NOTHING` is a loaded gun.** It cannot distinguish "this is the
retry I designed for" from "my key is wrong". The clause asserts that every
colliding row is genuinely the same row, and that assertion needs a test per *way*
two rows can collide.

**The unit tests missed it because they chose tidy inputs** — accounts literally
named `buyer` and `seller`. Self-trading was not on my list of things to test. It
was on the list of things a random workload does immediately.

Adding `direction` to the key fixed it: 47,322 trades, all four legs, zero
imbalance, net cash zero.

The same load test found a second one. Every in-flight order at shutdown logged
"trades are durable but unsettled" — settlement had inherited the client's request
context. A trade cannot be un-executed, so a client hanging up must not be able to
abandon its settlement; that leaves the journal permanently disagreeing with the
book. `context.WithoutCancel` plus its own timeout.

(It also found a bug in my load generator, which discarded every worker's results
on the expired context and cheerfully reported that a 14,000-order run had
completed nothing.)

## Surprise 4: a balance check that reads the balance is wrong twice

The pre-trade check started as "does this account have the cash". That is wrong in
two ways, and the second is the one that matters:

- Two concurrent orders from one account can both pass a check only one could
  afford.
- A **resting** order has spent nothing. An account with 5,000 can rest ten buys
  worth 5,000 each and pass every check, because no cash moves until they fill.

The second is the interesting one because serialising the checks does not help at
all. The exposure isn't concurrent, it's *outstanding*.

So the check became check-and-**reserve**: atomically verify against
`owned − already committed` and hold that amount against the order. The hold
survives while the order rests, shrinks as it fills, and releases on cancel,
rejection, or a dropped market remainder. A taker that fills below its limit
releases the difference — it was never owed.

Two placements matter. The check moved from the gRPC handler **into the
sequencer**, the only place it can be atomic with matching. And it runs **before**
the log write: replay does not re-run balance checks, so a rejected order sitting
in the log would be applied on recovery, and the recovered book would differ from
the one that was lost.

Reservations also have to be restored after replay, or a recovered node overstates
available balance by exactly what its own resting orders have committed.

---

## Durability: what "deterministic replay" actually means

Trades are **not** logged. They are re-derived by re-running the matching, which
keeps the log small. Two consequences, both worth stating rather than glossing:

**Trade IDs live on the Book, not the Sequencer.** Only the book is rebuilt by
replay, so it is the only place a counter survives one identically. Settlement is
idempotent on trade identity, so if replay renumbered trades, a recovered node
would post the same executions under new IDs and double-count them.

**The log records attempts, not successes.** The append happens before the book is
touched, so a cancel for an order that was already gone is in the log even though
it failed. Replaying it fails identically and leaves the book unchanged — which
means replay must *ignore* mutation errors rather than treat them as corruption.
That looks like sloppiness and is the opposite.

Corruption gets two different treatments, on purpose:

- **Short read** → truncated tail, the ordinary crash outcome. Those bytes were
  never acknowledged to anyone, so recovery stops there silently and the writer
  truncates them away before appending.
- **CRC mismatch** → the bytes are *present* but wrong. Storage lied. Halt, and
  refuse to append on top, because building on a device that has demonstrably
  lost data buries the evidence.

Tested by actually crashing: a child process submits 10,000 mutations, snapshots
its state, and panics mid-stream. A same-process "crash" cannot escape Go's
deferred cleanup, so it would flush the log on the way out — testing the graceful
path while claiming to test the crash path.

Cold-start replay: **100k records rebuild a book in 1.13 seconds**, near-linear.
Nothing prunes the log, so that grows for the life of the node. Snapshots are the
fix and `Recover`'s `afterSeq` parameter is already the hook; there is nothing to
snapshot yet, so it isn't built.

## Replication: reading files is the feature

One primary ships its log to one passive follower. Async, no consensus, manual
failover.

The follower reads the primary's log **files**, not its sequencer. That is the
load-bearing decision: **nothing on the matching path waits for a follower**, so a
follower that is slow, stalled, or hostile cannot change the primary's throughput.
Compare the trade feed, which *is* fed from the sequencer goroutine and therefore
has to drop records for slow subscribers to avoid stalling the venue. Replication
needs no such compromise, because there is nothing to drop.

Measured: a follower costs **6.3%** (1,476 → 1,383 orders/sec) — and the control
run shows that is disk contention from the follower's own fsyncs on the same
device, not backpressure. On its own disk it should cost nothing. A naive
comparison against an earlier 30-second run would have blamed the follower for
30%; only a same-duration control separates it from deeper books.

The follower stores records under the **primary's** log positions, never
renumbered. Two things fall out, and they are the reason:

- **No checkpoint file.** Its own log's last position *is* the checkpoint.
- **Promotion is nearly free.** Its log is byte-format-identical to a primary's,
  so promoting it is `omsd -wal <follower-dir>` — no conversion step to get wrong
  while the venue is down.

Demonstrated: 3,714 orders, `kill -9` the primary, promote the follower. The
promoted node's depth is byte-identical at the same log position, and it accepts
new orders immediately.

## What I chose not to build, and the gap I'd fix first

Stripped deliberately: Kafka (the WAL is the event log), multi-tenant auth, a
standalone risk engine, rate limiting, market-data fan-out, T+2 settlement, FIX,
stop/iceberg orders, and Raft. Each has a narrower honest substitute already in
the design. Saying "manual failover, eventual consistency, here is the data-loss
window" is a stronger position than a half-correct consensus implementation.

The largest real gap, stated plainly: **promotion rebuilds the book, not the
journal.** Trades the old primary executed but had not yet settled exist in the
log and never reach Postgres. The fix is known — put the order's log position on
each journal row so a promoted node can re-settle from the highest settled
position — and settlement is already idempotent and trade IDs already reproduce
exactly, so re-settling is safe. The missing piece is only knowing where to start.
It is documented as a manual reconciliation step in the failover runbook rather
than hidden.

Also outstanding: self-trade *prevention* exists but wash-trading policy is
thinner than a real venue's; split brain is prevented procedurally rather than by
fencing; and batching settlement the way the WAL batches appends is the obvious
next throughput lever.

---

## The thing I'd tell myself at the start

Write down the number you expect before you measure. Four times the measurement
disagreed, and each disagreement was worth more than the design reasoning it
overturned — but only because it was cheap to notice. A benchmark that confirms
what you assumed teaches nothing; the value is entirely in being contradicted, and
you only get contradicted if you committed to a prediction.

The corollary, learned the harder way: **run the real thing under real load.**
Unit tests choose tidy inputs. A random workload self-traded within seconds of
starting and found a bug that had been sitting behind a passing test suite, in the
one place — money — where silent wrongness is least acceptable.

---

**Code:** [github.com/Poudel0/OMS](https://github.com/Poudel0/OMS) ·
**Decisions:** [`docs/adr/`](adr/) ·
**Numbers:** [`docs/BENCHMARKS.md`](BENCHMARKS.md) ·
**Runbook:** [`docs/failover.md`](failover.md)
