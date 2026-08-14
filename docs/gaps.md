# Known gaps and limitations

Everything this system does not do, or does incompletely, with severity, what
actually breaks, and what the fix would be.

This exists because the alternative is worse. A system whose limits live only in
the author's head reads, to everyone else, as a system with no limits — and the
first person to find one finds it in production. Nothing here is a surprise to
the code; all of it is deliberate scope or a named consequence.

**Severity means:**

- **Data loss** — someone's acknowledged order or settled trade can be lost.
- **Correctness** — the system can produce a wrong answer.
- **Operational** — it works, but running it is harder than it should be.
- **Scope** — deliberately not built; a real venue needs it.

---

## Data loss

### 1. Promotion rebuilds the book, not the journal

**This is the largest gap in the system.**

Replay reconstructs the order book. It settles nothing — `Recover` applies records
to a book and never calls the ledger. So trades the old primary **executed but
had not yet written to Postgres** exist in the log and never reach the journal. A
promoted node will not re-derive them, because it does not settle on replay
either.

*Impact:* after a failover, the journal is missing some executions. Cash and
positions in the ledger disagree with what the books say happened. Reconciliation
is manual.

*Why it is survivable:* the trades are not *lost* — they are in the log, with
their identities intact. Settlement is already idempotent on `(symbol, trade_id)`
and replay already reproduces trade IDs **exactly**
(`TestWAL_ReplayReproducesTradeIDsExactly`), so re-settling them is safe.

*Fix, known and not built:* add the order's `log_position` to each
`journal_entries` row. A promoted node then queries the highest settled position
per symbol and re-settles everything above it. The migration runner exists to
land the schema change. The missing piece is only *knowing where to start*.

*Today:* [`failover.md`](failover.md) step 7 documents it as a manual
reconciliation with the query to run. Do not skip that step.

### 2. Async replication window

The primary acknowledges an order once **its own** fsync returns, not once the
follower has the record.

*Impact:* a primary lost while the follower is N records behind loses those N
**acknowledged** orders. Clients were told those orders were accepted.

*Measured:* lag peaked ~550 records during a 60s run at ~1,383 orders/sec across
4 symbols — well under a second of exposure — and ended at 0.

*Why it is the right trade:* synchronous replication would put a network round
trip inside every order's latency, making client-visible performance depend on a
second machine's health. For this venue, that is worse.

*Mitigation:* `oms_replication_records_behind` and `ReplicationStatus` exist so an
operator can see how wide the window is *right now*, and
[`failover.md`](failover.md) makes deciding on it an explicit step.

### 3. In-memory account state has no durable origin

`oms.Accounts` is rebuilt from the journal at startup, which recovers balances
*derived from trades*. But there is no durable record of an account being
**opened** or **funded** — deposits live only in memory and in whatever seeded
them (`-seed-accounts`, in development).

*Impact:* restart a node that was seeded by flag, without re-seeding, and accounts
exist with journal-derived balances only. Real deposits would be lost.

*Fix:* journal deposits and account openings as their own entry types. The
double-entry shape already accommodates it (a deposit is a debit to the account
and a credit to a cash-source account).

---

## Correctness

### 4. MARKET buy cost cannot be bounded

A limit order's worst case is `price × quantity`. A market order has no price, so
there is nothing to reserve against.

*Impact:* a MARKET buy is checked only for a positive available balance and **can
overdraw**. Every other order type is exact.

*Fix:* model NEPSE's daily circuit limits. The worst case then becomes
`upper_band × quantity` and the check is exact. See
[`domain.md` §6](domain.md).

### 5. No self-trade prevention beyond the direct case

The engine cancels a resting order when the incoming order is from the same
`account_id`. That catches the direct wash trade.

*Impact:* nothing catches the same beneficial owner trading through two different
accounts, layering, spoofing, or marking the close.

*Fix:* a surveillance system, not a matching-engine check. Out of scope by design.

### 6. Account claims are not authenticated

`account_id` arrives in the request and is trusted. The cancel-ownership check
compares against it, so the check is only as strong as the claim.

*Impact:* any client can act as any account. This is a **development-only
posture**.

*Why the check exists anyway:* order IDs are sequential integers, so without it
guessing one would be enough to cancel a stranger's order. Adding authentication
later makes the check real without changing the logic around it.

*Fix:* a gRPC interceptor establishing identity, then validate `account_id`
against it. Multi-tenant auth was explicitly stripped from scope.

### 7. Positions may go negative

The journal's arithmetic permits it, and `Accounts.Settle` allows a balance to go
negative rather than refusing to record a movement for a trade that already
happened.

*Impact:* no short-sale controls, no borrow, no locate. An account can end up
owing shares.

*Rationale:* by the time a trade exists, the execution has happened and is
durable. Refusing to record its value movement would make the in-memory view
disagree with the book — a data-integrity problem, where a negative balance is
merely a business one.

---

## Operational

### 8. Nothing prunes any log

Neither the primary's nor the follower's segments are ever archived or deleted.

*Impact:* disk grows for the life of a node, and so does recovery time. **Now
quantified:** 100k records replay in 1.13s (~89k records/sec, near-linear). A node
with 100M records would take ~19 minutes to start.

*Fix:* snapshots. `Recover`'s `afterSeq` parameter is already the hook — a
snapshot at position N means only records above N need replaying. Nothing is
built because there is nothing to snapshot yet.

### 9. Split brain is prevented procedurally, not mechanically

Nothing stops an operator starting a second primary on the same log directory. The
follower cannot become one by accident (`cmd/follower` implements no
`OrderService`), but a human can.

*Impact:* two writers on one log has **no clean recovery**. The same position means
different things on the two nodes and there is no merge.

*Fix:* fencing — a lock file, a lease, or an external arbiter. Not built; this is
the honest limit of a manual-failover design.

*Today:* the runbook's first rule, in bold.

### 10. Replication shares the order API's listener

`ReplicationService` and `OrderService` are registered on the same gRPC server.

*Impact:* you cannot expose the order API without also exposing the write-ahead
log, which is strictly more sensitive.

*Fix:* a second listener with its own credentials. The services are already
separate, so this is a wiring change in `cmd/server`.

### 11. Slow trade-feed subscribers are counted but never disconnected

A `StreamTrades` subscriber whose 256-deep buffer fills loses trades. Every drop
increments `oms_trade_feed_dropped_total`, so it is no longer silent — but the
subscriber keeps its slot and keeps losing trades.

*Impact:* a permanently-behind client silently receives an incomplete feed
forever. Its own drop rate is visible to the operator, not to it.

*Fix:* disconnect after sustained drops and make it resubscribe. The counter is
what makes that decision possible.

### 12. Registry creation holds the write lock across recovery

A symbol's first-ever order replays its log while holding the registry's write
lock.

*Impact:* a first-ever order in one symbol briefly blocks a first-ever order in
another. It does **not** block trading in any symbol already created.

*Fix:* a per-symbol creation lock. Only worth it if cold-start latency across many
symbols becomes a problem.

### 13. Idle symbols are never evicted

A symbol created once holds its goroutine and book for the process lifetime.

*Impact:* memory is a function of symbols *touched*, not symbols *active*.

*Rationale:* with a bounded instrument list this is arguably correct — a listed
scrip should be ready to trade. `MaxSymbols` (512) bounds it.

### 14. `oms.Scrip` is dead code

A struct declared in `types.go` and referenced nowhere. Left over from the first
commit, before the design settled on symbols being plain strings.

*Impact:* none, beyond a reader wondering what it is for.

*Fix:* delete it. Flagged rather than removed here because this pass was
documentation, not code changes.

---

## Performance, understood but not addressed

### 15. Settlement is per-order, not per-batch

The WAL batches appends into one fsync. Settlement does not batch: each order's
trades get their own Postgres transaction.

*Impact:* **settlement roughly halves throughput** (3,910 → 1,973 orders/sec
measured). Two fsyncs per order's worth of work — one in the WAL, one in
Postgres's commit.

*Fix:* one transaction per group commit rather than per order. This is the obvious
next throughput lever and it has a real design question attached: what happens to
the client still waiting when its trade is batched with others?

### 16. Per-symbol partitioning reduces durable throughput

Measured, and it contradicted the design expectation: durable throughput **falls
2.2×** across 1 → 8 symbols (7,012 → 3,134 orders/sec), while a no-WAL control
stays flat.

*Cause:* group-commit fragmentation. One symbol queues all clients into one
sequencer so each fsync amortises across a big batch; eight symbols give each
sequencer ⅛ the clients and eight independent fsync streams contending for a
device that serialises them anyway.

*Consequence, the reverse of the intuitive one:* on a single device, **fewer
symbols per node is better** for durable throughput. Scaling out means more
devices or more nodes, not more goroutines.

*Fix:* per-symbol WAL devices, or symbols split across hosts. See
[ADR-005](adr/0005-multi-symbol-partitioning.md).

### 17. Schema applied by a minimal migration runner

The runner is correct (each migration commits with its `schema_migrations` row,
under an advisory lock) but minimal: no down-migrations, no dry-run, no checksum
verification of already-applied files.

*Impact:* editing an applied migration file silently does nothing, and there is no
rollback path.

*Note:* an earlier version just re-ran one `IF NOT EXISTS` file every boot. That
is precisely why the `direction` fix could be a **rewrite of `0001`** rather than a
migration — nothing had been deployed. That escape hatch is now closed.

---

## Scope: deliberately not built

Each of these has a narrower honest substitute already in the design. The scope
was chosen so one engineer could finish it and explain every line, rather than a
shallow layer over tools doing the interesting work invisibly.

| Not built | Substitute | Why |
|---|---|---|
| Kafka / event bus | The WAL **is** the event log | One less system to operate, same ordering guarantee |
| Raft / consensus | Single primary + follower, manual failover | A half-correct consensus implementation is worse than an honest manual one |
| Multi-tenant auth | Account is a trusted string | Already on the author's CV from production work |
| Risk engine | One cash/shares check | "One inline balance check is enough" |
| Rate limiting | None | A system-design talking point, not a code artifact |
| Market-data WebSocket fan-out | `StreamTrades` over gRPC | Already on the author's CV |
| T+2 settlement | T+0, in-process | T+0 shows the double-entry pattern |
| Smart order routing / FIX | Single venue, gRPC only | No routing decisions to make |
| Stop / iceberg / FOK / IOC | LIMIT + MARKET + CANCEL | Smallest set exercising every mechanism |
| Opening/closing auctions, halts | Continuous trading | Not needed to demonstrate matching |
| Corporate actions | None | Not a matching-engine concern |
| Fee schedules, maker rebates | None | No revenue model |

---

## Priority, if the work continued

1. **Gap 1** (re-settle after promotion) — the only gap that loses settled money,
   and the fix is fully specified.
2. **Gap 8** (snapshots) — bounds both recovery time and disk, and unblocks log
   pruning.
3. **Gap 15** (batch settlement) — the largest single throughput win available.
4. **Gap 9** (fencing) — turns the sharpest operational footgun into an
   impossibility.
5. **Gap 3 / 6** (durable deposits, real auth) — required before anything
   resembling production.

Gaps 4, 5, 7, and the whole scope table are properly *product* decisions, not
engineering debt.
