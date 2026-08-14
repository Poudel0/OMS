# Decision log: what went wrong, what we did, and what it cost

A scannable index of every significant decision and every problem hit, with the
trade-off each way. The [ADRs](adr/) carry the full reasoning; this exists so you
can find the right one in ten seconds, and so the *problems* are recorded in one
place rather than scattered across six documents.

Read [`architecture.md`](architecture.md) for the narrative version.

---

## Part 1: The decisions

### D1 — Order book as a price-indexed map of FIFO queues

**Alternative considered:** a single sorted structure (tree/skip-list) of orders.

| Pro | Con |
|---|---|
| O(1) best-price lookup (index 0 of a sorted ladder) | Two structures to keep consistent (ladder + map) |
| O(1) cancel via an order-ID backref index | Ladder insert is O(n) memmove on a new price level |
| Price-time priority falls out naturally: price → queue | |

The O(n) insert is on *new price levels only*, and a real book has few distinct
prices relative to orders. → [ADR-001](adr/0001-order-book-data-structure.md)

### D2 — One goroutine per symbol, not a mutex

**Alternative considered, and benchmarked:** `sync.Mutex`-guarded book
(`book_mutex.go`, kept in the repo permanently as the comparison baseline).

| Pro | Con |
|---|---|
| `context` cancellation composes for free — `Mutex.Lock()` cannot be cancelled or timed out | **The mutex was 2–5× faster** at every producer count |
| Per-symbol isolation with no sharding logic | Channel hand-off and goroutine scheduling cost real time |
| One place to hang WAL, metrics, tracing without touching call sites | |

The honest position: chosen for composability, **not** speed. The book had no
synchronisation at all, which is only safe because exactly one goroutine touches
it. → [ADR-002](adr/0002-single-writer-sequencer.md)

### D3 — Group commit: drain the queue, one fsync per batch

| Pro | Con |
|---|---|
| **43× durable throughput** from 1 → 64 producers | The last request in a batch inherits latency from the first |
| No timer, no linger interval — queue depth *is* the signal | Makes tracing a batch awkward (see D12) |
| Contention becomes a *feature*: more load ⇒ bigger batches | |

→ [ADR-003](adr/0003-wal-design.md)

### D4 — WAL before book, always

| Pro | Con |
|---|---|
| Nothing is acknowledged that is not already recoverable | An fsync sits in every order's latency |
| An fsync failure rejects the batch without touching the book | The log records *attempts*, so replay must ignore mutation errors |
| The failure latches — a log that cannot be trusted stops the venue | |

### D5 — Trades are not logged; they are re-derived by replay

| Pro | Con |
|---|---|
| The log stays small — only mutations, not their consequences | **Trade IDs must live on the `Book`**, the only thing replay rebuilds |
| Replay is provably faithful (book-state equality tests) | `Trade.TimeStamp` is *not* reproduced — only book state is comparable |
| | The exact historical trade stream is not recoverable byte-for-byte |

Getting this wrong would double-post to the ledger after a restart, since
settlement is idempotent on trade ID. → [ADR-003](adr/0003-wal-design.md)

### D6 — Corruption: tolerate a short tail, halt on bad bytes

| Pro | Con |
|---|---|
| A truncated tail is the *ordinary* crash outcome; those bytes were never acknowledged | A torn write damaging the length prefix at the very end is reported as corruption |
| Bytes present but wrong means storage lied — halting preserves the evidence | |

The ambiguous case is resolved conservatively. That is the safe direction to be
wrong in.

### D7 — Log position ≠ order ID

| Pro | Con |
|---|---|
| Cancels get positions too (they carry no new order ID) | Two ID spaces to explain |
| One sequence orders submits and cancels against each other | |
| Same split Postgres makes (LSN vs transaction ID) | |

### D8 — Symbol partitioning via a lazy registry

| Pro | Con |
|---|---|
| N symbols, N independent goroutines/books/logs — no cross-symbol contention | **Durable throughput *falls* 2.2× from 1 → 8 symbols** (see P2) |
| Symbol is the natural shard key: matching never crosses instruments | Symbol becomes a directory name ⇒ a trust boundary |
| Lazy creation needs no instrument list | Nothing evicts an idle symbol |

→ [ADR-005](adr/0005-multi-symbol-partitioning.md)

### D9 — Venue-assigned order IDs, plus a cancel-ownership check

| Pro | Con |
|---|---|
| A client cannot claim an identifier another account's order holds | The account claim is **not authenticated**, so the check is only as strong as it |
| Guessing a sequential ID is trivial, so the check is necessary | |
| Adding auth later makes the check real without changing this logic | |

### D10 — Settlement: synchronous, in-process, idempotent by construction

**Alternative considered:** an outbox table plus a worker, or a queue.

| Pro | Con |
|---|---|
| Idempotent **by construction** (unique index) — no check-then-insert race | **Roughly halves throughput** (3,910 → 1,973 orders/sec) |
| No eventual-consistency window between matching and settling | Settlement is on the request path |
| One process, no network hop to talk to itself | Not batched per group commit (the obvious next lever) |

→ [ADR-004](adr/0004-synchronous-settlement.md)

### D11 — Replication reads log *files*, not the sequencer

| Pro | Con |
|---|---|
| **A follower cannot backpressure matching** — nothing on the matching path waits for it | Polling adds up to `TailPollInterval` (5ms) of lag |
| Only reads data that already survived an fsync | Reopens a segment per poll cycle |
| Follower stores the *primary's* positions ⇒ its log **is** a primary log ⇒ promotion is `omsd -wal <dir>` | Costs 6.3% from disk contention on shared storage |

Compare the trade feed, which *is* fed from the sequencer goroutine and therefore
*must* drop for slow subscribers. Replication needs no such compromise.
→ [ADR-006](adr/0006-wal-shipping-replication.md)

### D12 — The group commit is deliberately not a span

| Pro | Con |
|---|---|
| One commit serves many traces; a span has one parent | The trace shows a request waiting inside `Submit` without saying why |
| Attaching it to one arbitrary trace would misattribute the whole cost | |
| Measured as a metric instead, where a fan-in fits naturally | |

Tracing and batching are in genuine tension. This is the seam.

### D13 — Metrics gauges computed at scrape time

| Pro | Con |
|---|---|
| No background goroutine | A scrape does work proportional to symbol count |
| A gauge cannot drift out of sync with what it describes | |
| Zero cost between scrapes | |

### D14 — Reservations, not a balance read

| Pro | Con |
|---|---|
| Closes both holes: concurrent orders *and* outstanding resting exposure | A hold lifecycle to get right on every path |
| Atomic against orders in *other* symbols (cash is global, books are not) | Holds must be restored after replay |
| A taker filling below its limit gets the difference back | |

→ [ADR-004](adr/0004-synchronous-settlement.md)

### D15 — Self-trade prevention cancels the *resting* side

**Alternatives:** reject the incoming order; cancel both.

| Pro | Con |
|---|---|
| Keeps the book uncrossed — rejecting the taker and resting it anyway would leave bid ≥ ask | An order vanishes from the book as a side effect of someone else's action |
| No wash trade prints | |
| The cancelled IDs are *reported*, so it is not silent | |
| Precedent: CME calls this cancel-resting | |

---

## Part 2: The problems

Six weeks, eight problems worth recording. Four were measurements contradicting
the reasoning that produced them; four were outright bugs.

### P1 — fsync made a week of concurrency work irrelevant

**Found:** Week 3, on adding the WAL.

Week 2 profiled the sequencer, found `Submit` responsible for 40.8% of allocated
objects, pooled the reply channels, cut allocations 5 → 3/op — and throughput did
not move. Then the WAL landed: **200× slower** than no-WAL at one producer. ~99.5%
of a durable order's latency is one device flush.

**What we did:** kept the pooling (a real allocation win), and wrote in ADR-003
that ADR-002's conclusion was correct *and had become irrelevant*.

**Lesson:** an optimisation can be right and stop mattering. Both halves belong in
the writeup.

### P2 — Partitioning made durable throughput *worse*

**Found:** Week 4 follow-ups, running the sweep ADR-005 had claimed without.

Expected throughput to rise with symbol count. It **fell 2.2×** (7,012 → 3,134
orders/sec across 1 → 8 symbols). A no-WAL control stayed flat, localising the
loss entirely to durability: **group-commit fragmentation**. More partitions ⇒
smaller batches ⇒ the fixed fsync cost amortised less.

**What we did:** rewrote ADR-005's measured section, and inverted the operational
advice — on one device, *fewer* symbols per node is better.

**Lesson:** I had asserted a speed-up in an ADR before measuring it. The
measurement that would have proved it disproved it.

### P3 — The idempotency key silently destroyed half of every self-trade

**Found:** Week 4, first 30-second load test. **Severity: data corruption.**

Key was `(symbol, trade_id, account_id, asset)`. When one account is on **both**
sides of a trade — which 8 accounts trading randomly do within seconds — the four
journal legs collapse to two keys, and the counter-legs hit
`ON CONFLICT DO NOTHING` and vanished. The journal recorded accounts receiving
shares and paying nothing.

**Measured:** 6,133 of 47,935 trades corrupted. 12,266 rows missing. Net cash
−40,013,771 where it should have been 0.

**What we did:** added `direction` to the unique key. After: 47,322 trades, all
four legs, zero imbalance, net cash 0. Added
`TestLedger_SelfTradeStillWritesAllFourLegs`.

**Two lessons:**
- `ON CONFLICT DO NOTHING` cannot distinguish "the retry I designed for" from "my
  key is wrong". It asserts every colliding row is the same row, and that needs a
  test per *way* two rows can collide.
- The unit tests missed it because they chose **tidy inputs** — accounts literally
  named `buyer` and `seller`. A random workload self-traded immediately.

### P4 — Settlement inherited the client's cancellation

**Found:** same load test. **Severity: data loss.**

Every in-flight order at shutdown logged "trades are durable but unsettled".
Settlement used the request context, so a client hanging up abandoned its own
settlement — leaving the journal permanently disagreeing with the book.

**What we did:** `context.WithoutCancel` plus its own 30s timeout. A trade cannot
be un-executed, so its settlement must not be cancellable by the party that
caused it.

### P5 — A balance check that reads the balance is wrong twice

**Found:** Week 4 review. **Severity: correctness (overdraw).**

Two holes: concurrent orders from one account could both pass a check only one
could afford; and — worse — a **resting** order has spent nothing, so 5,000 could
rest ten buys worth 5,000 each. Serialising the checks fixes neither, because the
exposure is *outstanding*, not concurrent.

**What we did:** check-and-**reserve** against `owned − committed`, moved from the
handler *into the sequencer* (the only place it can be atomic with matching), and
run *before* the log write — because replay does not re-run balance checks, so a
rejected order in the log would be applied on recovery and diverge.

### P6 — `/tmp` is a tmpfs, so the first durability benchmark measured RAM

**Found:** Week 3, before publishing. **Severity: would have been an invented
number.**

`b.TempDir()` lands in `$TMPDIR`, which on most Linux desktops is a tmpfs where
fsync is nearly free. The first WAL benchmark measured memory and was about to be
labelled the cost of durability.

**What we did:** benchmarks take `OMS_BENCH_WAL_DIR`, and **both** configurations
are reported — the pair separates the WAL's encoding cost (+32%) from the device's
flush cost (200×), which is more informative than either alone.

### P7 — A follower looked 5× more expensive than it is

**Found:** Week 6, running a control instead of trusting the obvious comparison.

Against Week 4's 30-second run at 1,973 orders/sec, attaching a follower looked
like a 30% cost. A **same-duration** control without a follower gave 1,476/sec, so
the real cost is **6.3%** — and that is disk contention from the follower fsyncing
its own log to the same device, not backpressure (which the design makes
impossible). Most of the apparent gap was deeper books over a 60s run vs 30s.

**Lesson:** comparing across runs that differ in more than one variable produces a
confident wrong number.

### P8 — Two tests that passed while proving nothing

**Found:** Week 3 and Week 6. **Severity: false confidence.**

1. `TestSequencer_ContextCancellationDoesNotHang` failed ~30% of runs.
   `cancel()` only *requests* shutdown, and Go's `select` picks randomly among
   ready cases, so the goroutine served one more order about half the time. Fixed
   by waiting on `Done()`.
2. `TestPlaceOrder_SettlementSurvivesClientCancellation` waited on `wg.Wait()` —
   but that waits for the **client** call, and cancelling makes gRPC return
   client-side immediately while the server is still inside `Settle`. The
   assertion raced the thing it asserted. `-race` with the full suite exposed it.
   Fixed by waiting on a channel `Settle` closes itself.

**Lesson, learned twice:** wait on the thing you are asserting, never on a proxy
for it. And a test that has never failed has not been shown to work.

---

## Part 3: What each problem cost, and what it bought

| # | Cost to find | Cost to fix | Would production have found it? |
|---|---|---|---|
| P1 | One benchmark | Nothing (kept both findings) | Yes, as "why is it slow" |
| P2 | One sweep | Doc rewrite | Yes, as "why doesn't scaling help" |
| P3 | One load test | One column + one test | **Only via an audit.** Silent corruption |
| P4 | Same load test | Three lines | Yes, as unexplained ledger gaps |
| P5 | Code review | ~150 lines | Yes, as an overdrawn account |
| P6 | Noticing `df /tmp` | An env var | **No.** Would have shipped a wrong number |
| P7 | One control run | Nothing | No. Would have shipped a wrong number |
| P8 | `-race` + repetition | Two lines each | No. Would have stayed green and hollow |

Three of eight would never have surfaced on their own — they would have shipped as
confident, wrong claims. That is the argument for the "no invented numbers" rule
and for running the real thing under real load, in one table.
