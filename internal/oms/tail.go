package oms

import (
	"context"
	"fmt"
	"time"
)

// TailPollInterval is how often a Tailer looks for newly appended records once
// it has caught up.
//
// It bounds the latency replication adds, and 5ms is comfortably below the
// ~3.4ms-and-up an fsync costs on real storage (ADR-003), so polling is not the
// thing that makes a follower lag.
const TailPollInterval = 5 * time.Millisecond

// Tailer reads a symbol's log forward from a position, following it across
// segment rotations and waiting for records that have not been written yet.
//
// It reads the log **files**, not the sequencer, and that is a deliberate safety
// property rather than an implementation convenience: a follower — however slow,
// however far behind, however badly it is behaving — cannot exert any
// backpressure on the matching path, because nothing on the matching path is
// waiting for it. Contrast the trade feed (api.tradeFeed), which is fed from
// inside the sequencer goroutine and therefore has to drop for slow subscribers.
// Here there is nothing to drop, and no need to.
//
// A Tailer is single-use and not safe for concurrent use.
//
// ponytail: it polls rather than being woken when a commit lands. A wakeup
// channel would shave up to TailPollInterval off replication lag, and would
// couple replication to the sequencer to do it. Not worth it while one fsync
// already costs more than the whole poll interval — revisit if lag ever has to
// be tighter than a few milliseconds.
type Tailer struct {
	dir string

	// afterSeq is the highest position already returned; the next record
	// yielded is the first one above it.
	afterSeq int64

	// Position within the segment currently being read.
	segment string
	offset  int64
}

// NewTailer returns a Tailer that yields records with a position strictly above
// afterSeq. Pass 0 to start from the beginning of the log, which is also the
// initial-sync case: there is no separate full-copy step, because the log is the
// snapshot.
func NewTailer(dir string, afterSeq int64) *Tailer {
	return &Tailer{dir: dir, afterSeq: afterSeq}
}

// AfterSeq reports the highest position the Tailer has yielded.
func (t *Tailer) AfterSeq() int64 { return t.afterSeq }

// Next returns the next batch of available records, blocking until at least one
// exists or ctx is done. The batch is bounded by max.
//
// It returns ctx.Err() when the context ends, and ErrCorruptWAL if the log is
// damaged — a follower must stop rather than replicate past a hole.
func (t *Tailer) Next(ctx context.Context, max int) ([]Record, error) {
	for {
		batch, err := t.poll(max)
		if err != nil {
			return nil, err
		}
		if len(batch) > 0 {
			return batch, nil
		}
		// Caught up. Wait for more, or for the caller to give up.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(TailPollInterval):
		}
	}
}

// poll reads whatever is currently available without blocking.
func (t *Tailer) poll(max int) ([]Record, error) {
	var out []Record
	for len(out) < max {
		if t.segment == "" {
			if err := t.locateSegment(); err != nil {
				return out, err
			}
			if t.segment == "" {
				return out, nil // no segments yet; the symbol has never traded
			}
		}

		records, err := t.readSegment(max - len(out))
		if err != nil {
			return out, err
		}
		out = append(out, records...)
		if len(records) > 0 {
			continue // there may be more in this segment
		}

		// Nothing more in this segment. Move on only if a later one exists —
		// otherwise this is the live segment and we are simply caught up.
		next, err := t.nextSegment()
		if err != nil {
			return out, err
		}
		if next == "" {
			return out, nil
		}
		t.segment, t.offset = next, walHeaderSize
	}
	return out, nil
}

// locateSegment picks the segment that contains the next record to read: the
// last one whose first position is at or below the next one wanted. Segments are
// named for their first position, so this is a scan over sorted names.
func (t *Tailer) locateSegment() error {
	segs, err := segmentPaths(t.dir)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return nil
	}

	want := t.afterSeq + 1
	chosen := segs[0]
	for _, path := range segs {
		first, err := seqFromName(path)
		if err != nil {
			return fmt.Errorf("oms: unparseable wal segment name %s: %w", path, err)
		}
		if first > want {
			break
		}
		chosen = path
	}
	t.segment, t.offset = chosen, walHeaderSize
	return nil
}

// readSegment reads up to max records from the current segment, skipping any at
// or below afterSeq (which happens when resuming mid-segment).
//
// It reopens the segment each call and seeks to the last verified offset. That
// is what makes a partial record harmless: the bytes are simply re-read once the
// rest of them land.
//
// ponytail: reopening per poll costs a syscall pair every TailPollInterval per
// symbol. Keeping the handle open would save it, at the cost of having to reason
// about a bufio.Reader that has already seen EOF. Revisit if a profile ever
// mentions it.
func (t *Tailer) readSegment(max int) ([]Record, error) {
	r, err := OpenReaderAt(t.segment, t.offset)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var out []Record
	for rec, err := range r.Records() {
		if err != nil {
			return out, err
		}
		t.offset = r.Offset()
		if rec.Seq <= t.afterSeq {
			continue // already applied; resuming mid-segment
		}
		t.afterSeq = rec.Seq
		out = append(out, rec)
		if len(out) >= max {
			break
		}
	}
	return out, nil
}

// nextSegment returns the segment after the current one, or "" if the current
// one is the newest.
func (t *Tailer) nextSegment() (string, error) {
	segs, err := segmentPaths(t.dir)
	if err != nil {
		return "", err
	}
	for i, path := range segs {
		if path == t.segment && i+1 < len(segs) {
			return segs[i+1], nil
		}
	}
	return "", nil
}
