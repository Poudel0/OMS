# ADR-003: Write-ahead log with group-commit fsync and deterministic replay

- **Status:** Accepted
- **Week:** 3
- **Linked code:** [`internal/oms/wal.go`](../../internal/oms/wal.go), [`internal/oms/sequencer.go`](../../internal/oms/sequencer.go), [`internal/oms/wal_test.go`](../../internal/oms/wal_test.go), [`internal/oms/sequencer_wal_test.go`](../../internal/oms/sequencer_wal_test.go), [`internal/oms/chaos_test.go`](../../internal/oms/chaos_test.go)

## Context

The book (ADR-001) is in memory only. A process restart loses every resting
order, which for a venue means losing client orders it already acknowledged.
ADR-002 chose a single-writer goroutine per symbol partly because it is the
one place where durability can be inserted without touching any call site;
this is that insertion.

## Decision

Every mutation is appended to a durable log **before** the book is allowed to
see it, and the log is sufficient to rebuild the book from empty.

**Record format.** All integers little-endian.

```
segment := header record*
header  := magic[4]="DWAL" version:uint32
record  := len:uint32 seq:int64 tsUnixNano:int64 payload[len] crc32:uint32
```

The length prefix comes first because a reader cannot check anything until it
knows how far the payload runs; the CRC comes last because it cannot be
computed until the payload exists. The CRC covers the fixed header *and* the
payload, so a torn write anywhere in a record is caught rather than just one
in the payload.

**Payload is JSON.** A hand-rolled binary encoding would be maybe a third of
the size, but an append is dominated by the fsync that follows it (by three
orders of magnitude — see below), and JSON keeps a segment readable with
nothing but `xxd` when a replay goes wrong. The format carries a version
field precisely so this can be swapped later without guesswork.

**Log position is not an order ID.** `Record.Seq` is a log position: assigned
by the sequencer, +1 per record, and it covers cancels too — which introduce
no new order ID of their own. `Order.SeqID` stays the caller-assigned order
identifier it always was. Keeping them separate is what lets a single
sequence order submits and cancels against each other, which replay depends
on. Postgres makes the same split (LSN vs transaction ID).

**One request channel, not two.** The sequencer previously selected over
separate `submits` and `cancels` channels. That had to go: a Go `select`
picks among ready cases *at random*, so a cancel sent later could be applied
before a submit sent earlier. Without a WAL that was merely unfair; with one
it is a correctness bug, because the log would record an order the book did
not apply. One channel gets the required FIFO from the channel itself.

**Group commit.** The sequencer drains every request already queued behind
the first, appends them all, fsyncs **once**, and only then applies any of
them to the book. Under load the channel always has work waiting, so batches
grow and the per-order fsync cost falls; when idle, a lone request commits
immediately rather than waiting for company that may never arrive. No timer,
no configured linger interval — the queue depth *is* the signal.

**Fsync failure rejects the batch and is sticky.** If the append or the fsync
fails, the book is not touched at all and every caller in the batch gets the
error. A trade is never acknowledged unless it is already recoverable. The
failure then latches: a log that failed a write is in an unknown state, and
continuing to match orders against a book that can no longer be recovered is
the one genuinely unrecoverable mistake available here.

## Honest benchmark data — fsync is the whole engine now

Same machine as ADR-001/002 (Ryzen 5 4600H, 12 threads), `-benchtime=2s`,
1/4/16/64 concurrent producers. The `orders/fsync` column is measured, not
inferred — `Sequencer.CommitStats` counts commits and the requests they
covered, and the benchmark reports the ratio.

Storage matters enough here that reporting it is not optional. `/tmp` on this
machine is a **tmpfs**, where fsync is very nearly free — so the default
`b.TempDir()` does *not* measure durability. Both configurations are below,
because the pair separates the WAL's own encoding cost from the device's
flush cost. Real storage is btrfs on a LUKS-encrypted Samsung NVMe SSD with
`compress=zstd:3`; btrfs fsync is expensive (copy-on-write plus tree-log),
and dm-crypt adds more, so treat ~3.4 ms as *this* stack's number and not a
property of NVMe.

**Real storage (btrfs/LUKS/zstd on NVMe) — `docs/bench/week3-wal-realdisk.txt`:**

| Producers | ns/op | orders/fsync | orders/sec | implied fsync (ns/op × batch) |
|---|---|---|---|---|
| 1  | 3,406,059 | 1.00  | 294    | 3.41 ms |
| 4  | 1,762,632 | 2.03  | 567    | 3.57 ms |
| 16 | 364,905   | 10.52 | 2,741  | 3.84 ms |
| 64 | 79,405    | 61.30 | 12,594 | 4.87 ms |

**tmpfs control (fsync ≈ free) — `docs/bench/week3-wal-tmpfs-control.txt`:**

| Producers | ns/op | orders/fsync | orders/sec |
|---|---|---|---|
| 1  | 22,307 | 1.00  | 44,829  |
| 4  | 17,048 | 1.62  | 58,658  |
| 16 | 10,602 | 10.46 | 94,322  |
| 64 | 9,765  | 53.59 | 102,406 |

**No WAL at all (ADR-002's configuration, re-measured in the same run):**

| Producers | ns/op | orders/sec |
|---|---|---|
| 1  | 16,866 | 59,290  |
| 4  | 11,231 | 89,039  |
| 16 | 7,422  | 134,735 |
| 64 | 7,353  | 136,000 |

Three things fall out of this, and only one of them was the guess going in.

**The WAL's own cost is small and flat; fsync is everything.** tmpfs versus
no-WAL isolates the marshalling, the buffered write, and the extra syscalls:
+32% at 1 producer, +33% at 64. That is the price of the code. Against real
storage the same code is **200× slower at 1 producer** than no WAL at all.
So roughly 99.5% of a durable order's latency at low concurrency is one
device flush, and nothing about the matching engine — not the book, not the
channel hand-off ADR-002 spent a week profiling — is within three orders of
magnitude of mattering. ADR-002 concluded the bottleneck was goroutine
scheduling. That was true, and it is now irrelevant.

**Group commit works, and the mechanism is verifiable rather than asserted.**
Multiply ns/op by the measured batch size and you get 3.41, 3.57, 3.84,
4.87 ms — one roughly constant device flush across a 43× spread in
throughput. That is the whole model in one column: throughput rises 43×
from 1 to 64 producers *because* each fsync went from serving 1 order to
serving 61, not because anything got faster. Note also that batch size at 64
producers (61.3) is close to the producer count: while one fsync is in
flight, essentially every other producer piles into the next batch. The
`maxBatch` cap of 256 was never reached and did nothing.

**Contention became a feature.** In ADR-002 more producers meant more
scheduling overhead to apologise for. With an fsync in the path, more
producers mean bigger batches mean *better* per-order throughput. The
sequencer design was chosen there for composability rather than speed; this
is where that choice starts paying, and the honest note is that the payoff
arrived for a reason not predicted in ADR-002.

## Replay determinism, and where it stops

Replay reaches the same book state, verified by comparing
`Book.Snapshot()` after a live run against a replay into a fresh book —
including under 8 concurrent producers, where the interleaving is not
predictable from outside and the log is the only record of what order
mutations were actually applied in.

Two things are deliberately **not** reproduced, and calling replay
"deterministic" without saying so would be overselling it:

- **`Trade.TimeStamp`** is `time.Now()` at match time, so replayed trades
  carry different timestamps than the originals. Only book *state* is
  compared, never trade timestamps.
- **Trades themselves are not logged.** They are derived by re-running the
  matching, which is what makes the log small. The cost is that the exact
  historical trade stream is not recoverable byte-for-byte, only the state
  it produced. Settlement (ADR-004) will need trades to be durable in their
  own right — that belongs in the ledger, not here.

**The log records attempts, not successes.** Because the append happens
before the book is touched, a cancel for an order that was already gone is
in the log even though it failed. Replaying it fails identically and leaves
the book unchanged, so state still matches — but this means replay must
*ignore* mutation errors rather than treat them as a corrupt log. That looks
like sloppiness in `Record.apply` and is the opposite; it is tested directly
(`TestWAL_FailedCancelIsStillLoggedAndReplaysIdentically`).

## Corruption policy: truncated tail recovers, bad bytes halt

The two failure modes are treated differently on purpose.

- **Short read** (fewer bytes on disk than a record needs) → stop the scan,
  no error. This is the ordinary crash outcome: bytes that never reached
  disk were never acknowledged to any client, so nothing was promised about
  them. `OpenWriter` truncates that partial tail away before appending,
  because otherwise the next record would land behind unparseable bytes and
  be unreachable forever.
- **CRC mismatch, or a length field above the 1 MiB cap** → `ErrCorruptWAL`,
  halt. These bytes are all *present*; storage returned something other than
  what was written. Replay stops, and `OpenWriter` refuses to append, because
  building on top of a device that has demonstrably lost data would bury the
  evidence. An operator has to look.

The length cap earns its place independently: without it a torn write that
left garbage in the length prefix would have the reader try to allocate
gigabytes before ever reaching the CRC that would have rejected the record.

The honest edge: a torn write that damages the length prefix at the very end
of a segment is indistinguishable from a truncated tail without more
information, and will be reported as corruption rather than tolerated. That
is the safe direction to be wrong in, and it is the reason the policy is
stated in terms of *bytes present* rather than *where in the file*.

## Crash testing

`TestChaos_AcknowledgedMutationsSurviveACrash` runs the engine in a child copy
of the test binary, submits 10,000 mutations, records the book state, and
panics mid-stream with 5,000 still to go. The parent then recovers from
nothing but the log the dead process left behind and asserts an exact match on
log position, resting count, and full book state.

It has to be a subprocess. A same-process "crash" cannot escape Go's deferred
cleanup, so it would flush and close the log on the way out — testing the
graceful path while claiming to test the crash path. The child therefore
registers no cleanup at all, deliberately.

The child uses a **single** producer, which makes the exact-match assertion
the right one: at the moment it snapshots there is no request in flight, so
"everything acknowledged is recoverable, and nothing beyond it was invented"
is a precise claim rather than a bound. Concurrent-workload replay fidelity is
covered separately by `TestSequencer_ConcurrentWorkloadReplaysToTheSameBook`.

## Consequences

- **Durable single-symbol throughput on this hardware is ~294 orders/sec at 1
  producer and ~12.6k at 64.** Any headline number for this project must say
  which, and must say the storage stack. The 136k/sec from ADR-002 is a
  no-durability number and cannot be quoted as a venue throughput.
- **Latency is now a storage property, not a code property.** Optimising the
  matching path further is pointless until fsync policy changes. The real
  levers, in order: group-commit batching (done, 43×), then faster storage or
  a less fsync-hostile filesystem, then — only if the business accepts the
  risk — relaxing durability to fsync-every-N-milliseconds, which trades a
  bounded window of acknowledged-but-lost orders for throughput. That trade
  is not made here and should not be made without someone signing for it.
- **`maxBatch = 256` is a cap, not a tuning parameter.** Nothing measured
  picked it and nothing reached it. It bounds worst-case latency inheritance
  and batch memory. Leave it alone until a profile mentions it.
- **Rotation is size-based only** (`DefaultSegmentBytes`, 64 MiB). Nothing
  deletes or archives old segments yet, so a long-running node's log grows
  without bound and recovery time grows with it. Snapshots are the fix and
  `Recover`'s `afterSeq` parameter is already the hook for them — a snapshot
  taken at position N means only records above N need replaying. Not built:
  there is no snapshot to take yet.
- **WAL shipping (ADR-006) inherits this format directly.** A follower
  streaming records and applying them via `Record.apply` is the same code
  path replay already uses, which is why `StreamFrom` was not built
  speculatively here — the reader iterator is the whole primitive it needs.
- `Sequencer.CommitStats` is intentionally not a live metric. Its counters
  are unsynchronized because the sequencer goroutine is their only writer;
  they are safe to read only after that goroutine exits. When Prometheus
  metrics land (Week 6) they will need atomics or a report-from-inside
  channel, and that is a deliberate later cost rather than an oversight now.
