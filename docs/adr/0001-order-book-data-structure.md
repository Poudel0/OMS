# ADR-001: L3 order book as a price-indexed map of FIFO queues

- **Status:** Accepted
- **Week:** 1
- **Linked code:** [`internal/oms/book.go`](../../internal/oms/book.go)

## Context

The matching engine needs an in-memory book that supports, per symbol:

- insert a resting order at a price
- find the best bid / best ask in O(1)
- match an incoming order against the opposite side in strict price-time
  priority (best price first, then FIFO within a price)
- cancel an arbitrary resting order by ID

The book is rebuilt from scratch on every process start (no persistence yet
— that's ADR-003), so the data structure only needs to be efficient and
correct in memory, not serializable.

## Decision

The book is `bids []Price` and `asks []Price` — the *distinct* prices with
resting interest, kept sorted (bids descending, asks ascending) so the best
price is always index 0. Each price maps, via `levels map[Price]*priceLevel`,
to a `priceLevel` wrapping a `container/list.List`: a FIFO queue of the
orders resting at that exact price.

Cancelling in O(1) needs a third structure: `index map[SeqID]OrderRef`, where
`OrderRef` holds both the `*priceLevel` an order lives in and its
`*list.Element` — `list.Remove` requires the list that owns the element, so
the backref has to carry both, not just the element pointer.

## Alternatives considered

- **Single global sorted structure over all orders** (e.g. a skip list or
  balanced tree keyed by `(price, arrival)`), no per-price grouping. Rejected:
  loses the natural FIFO-per-price grouping for free that `container/list`
  gives, and NEPSE's discrete tick sizes mean the number of distinct prices
  (P) is small relative to the number of orders — grouping by price first is
  the cheaper axis to search.
- **A heap keyed by price** for O(1) best-price peek. Rejected: heaps are
  poor at anything other than peek/pop-min — cancelling an arbitrary order or
  walking depth for a snapshot both need better-than-O(n) access into the
  middle of the structure, which a heap doesn't offer.
- **A balanced tree/skip-list keyed directly by price** instead of a sorted
  slice + map. Considered, but for the price range and level counts realistic
  for a single instrument, the O(P) slice-shift insert is cheap in practice
  and much simpler to implement and test than tree rebalancing; only worth
  revisiting if profiling shows the ladder-shift cost matters (see ADR-002's
  benchmarking).

## Consequences

- Insert: O(log P) to find the insertion point (`sort.Search`) + O(P) to
  shift the ladder slice, where P is the number of distinct price levels —
  not the number of orders. O(1) once the level already exists.
- Match: O(1) per fill — `popFront`/`pushFront` on `container/list` are O(1).
- Cancel: O(1) via the `index` backref.
- This is an **L3** book — every individual order and its queue position
  within a price is visible, not just aggregated size per price. That's a
  side effect of using a real per-order queue rather than a simple
  `map[Price]int64` quantity counter, and it's what `Snapshot()`'s L2
  aggregation is built on top of, not the other way around.
- The three structures (`bids`/`asks`, `levels`, `index`) must be kept in
  sync on every mutation. This bit us once already: `matchAgainstBids`
  initially didn't update `index` on partial fills, leaving stale backrefs
  that `Cancel` could act on incorrectly. Covered now by
  `TestCancel_AfterRequeueOnPartialFill_IndexStaysCorrect` and the
  `TestSubmit_Properties` invariant suite.
