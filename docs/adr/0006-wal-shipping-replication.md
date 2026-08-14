# ADR-006: Async WAL shipping to a single passive follower, manual failover

- **Status:** Accepted
- **Week:** 5
- **Linked code:** [`internal/oms/tail.go`](../../internal/oms/tail.go), [`internal/api/replication.go`](../../internal/api/replication.go), [`internal/replica/replica.go`](../../internal/replica/replica.go), [`cmd/follower`](../../cmd/follower)
- **Runbook:** [`docs/failover.md`](../failover.md)

## Context

A node's write-ahead log (ADR-003) makes it recoverable from its own disk. It
does nothing about the disk, the machine, or the datacentre. A second node
holding the same state is the cheapest answer, and the log is already exactly
the stream of changes such a node would need.

## Decision

One primary ships its write-ahead log to one passive follower over a gRPC
server-stream. The follower replays each record into its own books **and its own
log**, and can be promoted by hand if the primary is lost.

Async, single follower, manual failover. No consensus, no automatic promotion,
no quorum. Raft would give automatic failover and cost a leader-election
protocol, a membership story, and a much larger surface to get wrong; a venue
that can tolerate a human deciding to fail over does not need it, and one that
cannot needs more than this whole project. Saying "manual failover, eventual
consistency, here is the data-loss window" is a stronger engineering position
than a half-correct consensus implementation.

### The follower reads the log files, not the sequencer

`oms.Tailer` reads segment files from disk. It is not fed from inside the
sequencer goroutine, and that is the load-bearing decision here.

**A follower cannot exert backpressure on matching.** Nothing on the matching
path waits for it, because nothing on the matching path knows it exists. A
follower can be slow, stalled, hostile, or absent, and the primary's throughput
is unchanged. Compare the trade feed (`api.tradeFeed`), which *is* fed from the
sequencer goroutine and therefore has to drop records for slow subscribers to
avoid stalling the venue. Replication needs no such compromise: there is nothing
to drop because there is nothing waiting.

It also means replication reads only data that is already durable. A record a
follower can see is a record that survived an fsync, so the follower can never
be ahead of the primary's own recovery point — which would otherwise be possible
and deeply confusing after a crash.

The cost is polling: `TailPollInterval` (5ms) bounds the latency replication
adds. That is comfortably below the ~3.4ms-and-up a single fsync costs on this
stack, so polling is not what makes a follower lag. A wakeup channel would shave
some of it off and would reintroduce coupling to the sequencer to do it. Not
worth it.

### The follower stores the primary's positions, unchanged

Records go into the follower's log under the **primary's** `Record.Seq`, never
renumbered. Two properties fall out, and they are the reason for the choice:

- **No checkpoint file.** The follower's own log tells it where it got to,
  because its last position *is* the primary's position for that record. Restart
  is just `oms.Recover` on its own directory — the same code a primary uses.
- **Promotion is nearly free.** The follower's log is byte-format-identical to a
  primary's, so promoting it is `omsd -wal <follower-dir>`. There is no
  conversion step to get wrong at the worst possible moment.

The demo (`docs/bench/week5-failover-demo.txt`) shows the payoff: after 3,714
orders, `kill -9` on the primary, and promotion of the follower, the promoted
node's depth JSON is byte-identical to the lost primary's at the same log
position (1873), and it accepts new business at 1874.

### One stream per symbol

Logs are per symbol (ADR-005), so replication is too: N independent streams, no
global ordering between symbols, and one lagging symbol cannot stall the others.
The follower discovers symbols via `ListSymbols` rather than being configured
with them, so a newly listed scrip starts replicating on its own.

### The follower writes before it applies

The follower keeps the same write-ahead discipline as the primary, for the same
reason: a follower that applied a record it had not yet stored would forget it on
restart and resume from a position it had already passed, silently skipping a
mutation. It also group-commits, one fsync per received batch.

Applying is idempotent — records at or below the current position are skipped.
Reconnects ask for everything after the last applied position, but tolerating a
resend is cheaper than a protocol that forbids one.

### The follower serves no client traffic

`cmd/follower` implements no `OrderService`. A follower that answered
`PlaceOrder` would be a second primary, and two primaries writing one symbol's
log is the one failure mode this design has no answer for. Keeping the follower
incapable of it is better than documenting that it must not.

### Replication is a separate service

`ReplicationService` is distinct from `OrderService`. A follower is not a client;
the log is strictly more sensitive than the order API; and a deployment should be
able to expose one without the other. Today both are served from one listener,
which is noted in `cmd/server` as the thing to split when there are credentials
to split them with.

A follower is **not** more trusted than a client: `StreamWAL` routes its symbol
through the same registry validation an order goes through, because the symbol
becomes a path either way.

### Damage stops replication rather than skipping it

If the primary's log fails CRC verification, `StreamWAL` returns `DataLoss` and
the follower stops that symbol instead of retrying. A follower that skipped a
damaged record and applied the ones after it would hold a book that never
existed on any node — worse than a follower that is visibly stuck. Retrying could
not help, so the follower does not pretend it might.

## Lag

The follower reports `(symbol, applied_position, applied_record_timestamp)` on a
timer; `ReplicationStatus` turns that into records-behind and millis-behind.

Records-behind is computed from the primary's own log, so it is accurate even
when no follower has ever connected — and `follower_seen` distinguishes "caught
up" from "nobody is listening", which are the same number and very different
situations. Millis-behind comes from the timestamp of the record the follower
last applied, because "1,400 records behind" means nothing without knowing
whether that is a second or an hour.

Progress reporting is advisory. The primary streams whether or not anyone
reports, and a follower that cannot report is still a working follower.

## What this loses, stated plainly

**Acknowledged-but-unreplicated orders.** Replication is async: the primary
acknowledges an order once *its own* fsync returns, not once the follower has it.
A primary lost while the follower is N records behind loses those N orders. That
is the price of not making client latency depend on a second machine, and it is
the correct trade for this venue — but it is a real window, and
`ReplicationStatus` exists so an operator can see how wide it currently is.

**Executed-but-unsettled trades are not recovered by promotion.** This is the
sharpest gap and it is worth being precise. Replay rebuilds the *book*, not the
*journal*: `Recover` applies records to a book and settles nothing. So trades the
old primary executed but had not yet written to Postgres exist in the log and
never reach the ledger. The promoted node will not re-derive them into the
journal, because it does not settle on replay either.

The fix is known and not built: put the order's log position on each
`journal_entries` row, so a promoted node can find the highest settled position
per symbol and re-settle the gap. Settlement is already idempotent on trade
identity (ADR-004) and trade IDs are already reproduced exactly by replay
(`TestWAL_ReplayReproducesTradeIDsExactly`), so re-settling is safe — the missing
piece is only knowing where to start. That is a schema change plus a promotion
step, and it now has a migration runner to land in.

**Which node settles: the primary, always.** The follower must never settle. A
follower replaying the log re-derives the same trades with the same IDs, so if it
settled too, the same execution would arrive in the journal from two nodes. The
unique index makes that survivable rather than corrupting, but relying on a
constraint to paper over two writers is not a design. So: the primary settles;
the follower matches and logs only; a promoted node settles from the moment it is
promoted.

**Split brain is prevented procedurally, not mechanically.** Nothing stops an
operator starting a second primary on the same log directory. The follower cannot
become one by accident, but a human can. Fencing (a lock file, a
lease, an external arbiter) is what would make that impossible, and it is not
built.

## Consequences

- Failover is a documented human procedure ([`docs/failover.md`](../failover.md)),
  and its correctness rests on the follower's log being a valid primary log — a
  property the promotion test asserts rather than assumes
  (`TestReplica_PromotedFollowerLogServesAsAPrimary`).
- A primary restart is an ordinary event for a follower, not an error: it
  reconnects and resumes from its own position. Tested both ways round —
  follower restart and primary restart.
- `symbolRefreshInterval` is 500ms because it is also the worst-case delay before
  a brand-new symbol starts replicating *at all*. It was 5s until the tests made
  that visible: every replication test sat idle waiting for a second discovery
  pass, because at connect time the symbol had not traded yet and so did not
  exist. A venue listing a new scrip should not wait seconds for its first order
  to be protected.
- Nothing prunes a follower's log any more than a primary's, so both grow
  without bound and recovery time with them. Same answer as ADR-003: snapshots,
  and `Recover`'s `afterSeq` is already the hook.
- Week 6's metrics endpoint should export `ReplicationStatus` directly; it is
  already the right shape, and lag is the first thing anyone will want alerting
  on.
