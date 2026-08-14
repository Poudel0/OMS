# Failover runbook

Manual promotion of a follower to primary. Design rationale is in
[ADR-006](adr/0006-wal-shipping-replication.md); this is the procedure.

Failover is **deliberately manual**. There is no automatic promotion, no quorum,
and no fencing — which means the single most important rule is procedural rather
than enforced by code:

> **Never run two primaries on the same log directory.** Nothing stops you. A
> follower cannot become a primary by accident, but a human can create a second
> one, and two writers on one log is the failure mode this design has no answer
> for.

## Before you need it

Watch lag. It is the width of your data-loss window, and it is the one number
worth alerting on:

```sh
grpcurl -plaintext <primary>:9090 dhukuti.oms.v1.ReplicationService/ReplicationStatus
```

```json
{"symbols":[{"symbol":"NABIL","primaryPosition":"1873","followerPosition":"1873",
             "recordsBehind":"0","followerSeen":true}]}
```

`followerSeen: false` means **no follower has ever reported**, which looks
identical to "caught up" if you only read `recordsBehind`. Check the flag.

## Deciding to fail over

Replication is asynchronous. The primary acknowledges an order once its own fsync
returns, not once the follower has the record. So:

- **`recordsBehind: 0`** — a promotion loses nothing that was acknowledged.
- **`recordsBehind: N`** — promotion loses those N acknowledged orders. They exist
  only on the lost primary's disk. If that disk is intact, recovering it and
  restarting the original primary loses nothing and is the better option.

Do not promote to fix a slow primary. Promote when the primary is gone or its
storage is gone.

## Procedure

**1. Confirm the old primary is really down, and keep it down.**

```sh
# on the primary host
systemctl stop oms-server   # or: kill <pid>
ps aux | grep omsd          # must show nothing
```

If it might come back on its own, disable it first. A primary that restarts after
you promote gives you two primaries.

**2. Record the follower's position, for the incident write-up.**

```sh
# the follower logs this on -status-interval, or read it from its log directory
grpcurl -plaintext <old-primary>:9090 dhukuti.oms.v1.ReplicationService/ReplicationStatus
```

If the primary is unreachable, the follower's own log is the record: its highest
position per symbol is what it holds.

**3. Stop the follower process.**

```sh
systemctl stop oms-follower
```

It must not be writing to the log directory while a server opens it.

**4. Promote: start a normal server on the follower's log directory.**

```sh
omsd -addr :9090 \
     -wal /var/lib/oms/follower-wal \
     -db "$DATABASE_URL"
```

That is the whole promotion. The follower stored records under the primary's
positions in the primary's own format, so its log *is* a primary log — there is
no conversion step. The node recovers each symbol's book from it, resumes log
positions, order IDs, and trade IDs above what it recovered, and rebuilds account
balances from the journal.

**5. Verify before you send traffic.**

```sh
grpcurl -plaintext localhost:9090 -d '{"symbol":"NABIL"}' \
  dhukuti.oms.v1.OrderService/GetBookSnapshot
```

Compare `logPosition` against what you recorded in step 2. Compare `depthJson`
against the old primary's if you can still reach it.

**6. Repoint clients**, by DNS, load balancer, or config — whatever your
deployment uses. There is no redirect: clients connect to an address.

**7. Reconcile unsettled trades. Do not skip this.**

Promotion rebuilds the **book**, not the **journal**. Trades the old primary
executed but had not yet written to Postgres are in the log and were never
settled, and the promoted node does not settle on replay.

Find the gap by comparing the highest trade ID the journal holds per symbol
against what the promoted node's book implies:

```sql
SELECT symbol, max(trade_id) AS last_settled
FROM journal_entries GROUP BY symbol ORDER BY symbol;
```

Any trade above `last_settled` that the log produced is unsettled. Settlement is
idempotent on `(symbol, trade_id)` and replay reproduces trade IDs exactly, so
re-settling is safe — the missing piece is tooling to do it. Until that exists
this is a manual reconciliation, and it is the largest known gap in this design
(ADR-006).

**8. Give the promoted node a follower**, or you are running unprotected. Point a
new follower at it:

```sh
follower -primary <new-primary>:9090 -wal /var/lib/oms/new-follower-wal
```

Start it on an **empty** directory unless you are deliberately reusing a log you
know belongs to the same lineage.

## Failing back

There is no automatic fail-back. The old primary's log has diverged the moment
the promoted node accepted its first order: both logs now have different records
at the same positions. Treat the old primary as **destroyed**:

1. Archive its log directory for the incident review — it holds the acknowledged
   orders that were lost, which is what you need to tell clients about.
2. Wipe it.
3. Start it as a follower of the new primary, on an empty directory.

Do not restart it as a primary and do not merge the two logs. There is no merge:
the same position means different things on the two nodes.

## What each restart path does, for reference

| Scenario | What happens |
|---|---|
| Follower restarts | Recovers from its own log; resumes from its last position. No primary state needed. |
| Primary restarts | Follower reconnects and resumes. Primary truncates any torn tail and continues its own positions. |
| Primary killed `-9` | Same as above; anything acknowledged was already fsynced. |
| Primary's log damaged | `StreamWAL` returns `DataLoss` and the follower stops that symbol rather than skipping the hole. Investigate; do not restart blindly. |
| Both nodes lost | Whatever survives on disk. Neither node's log is a backup of the other's in any archival sense — nothing prunes them, but nothing ships them off-host either. |
