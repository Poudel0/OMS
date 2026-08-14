package oms

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func submitRecord(seq int64, id SeqID, price Price) Record {
	return Record{Seq: seq, Kind: RecordSubmit, TS: walTS, Order: Order{
		SeqID: id, Symbol: "NABIL", Placer: "acc", Type: Limit,
		Price: price, Quantity: 10, Side: Buy,
	}}
}

func TestTailer_ReadsExistingRecordsFromTheStart(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := w.Append(submitRecord(int64(i), SeqID(i), Price(500+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	tail := NewTailer(dir, 0)
	got, err := tail.Next(t.Context(), 100)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("Next() returned %d records, want 5", len(got))
	}
	for i, rec := range got {
		if rec.Seq != int64(i+1) {
			t.Errorf("record %d position = %d, want %d", i, rec.Seq, i+1)
		}
	}
	if tail.AfterSeq() != 5 {
		t.Errorf("AfterSeq() = %d, want 5", tail.AfterSeq())
	}
}

func TestTailer_ResumesStrictlyAboveAPosition(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := w.Append(submitRecord(int64(i), SeqID(i), Price(500+i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// This is the follower-restart path: it knows it applied through 3.
	tail := NewTailer(dir, 3)
	got, err := tail.Next(t.Context(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("Next() returned %d records, want 2", len(got))
	}
	if got[0].Seq != 4 || got[1].Seq != 5 {
		t.Errorf("positions = %d,%d, want 4,5", got[0].Seq, got[1].Seq)
	}
}

func TestTailer_FollowsRecordsAppendedWhileWaiting(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(submitRecord(1, 1, 500)); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	tail := NewTailer(dir, 0)
	if got, err := tail.Next(t.Context(), 100); err != nil || len(got) != 1 {
		t.Fatalf("first Next() = %d records, %v; want 1, nil", len(got), err)
	}

	// Now the tailer is caught up and waiting. A record appended after that must
	// still reach it — this is the live-streaming case, not a one-shot read.
	done := make(chan []Record, 1)
	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		got, err := tail.Next(ctx, 100)
		if err != nil {
			errc <- err
			return
		}
		done <- got
	}()

	time.Sleep(30 * time.Millisecond) // let the tailer reach its wait
	if err := w.Append(submitRecord(2, 2, 501)); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-done:
		if len(got) != 1 || got[0].Seq != 2 {
			t.Errorf("Next() = %+v, want one record at position 2", got)
		}
	case err := <-errc:
		t.Fatalf("Next() error = %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Next() never saw the appended record")
	}
}

func TestTailer_CrossesSegmentRotations(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.MaxSegmentBytes = 200 // roll roughly every record

	const n = 12
	for i := 1; i <= n; i++ {
		if err := w.Append(submitRecord(int64(i), SeqID(i), Price(500+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := segmentPaths(dir)
	if len(segs) < 2 {
		t.Fatalf("segments = %d, want rotation to have produced several", len(segs))
	}

	// Read in small batches so the tailer is forced to walk segment boundaries
	// repeatedly rather than slurping everything in one call.
	tail := NewTailer(dir, 0)
	var all []Record
	for len(all) < n {
		got, err := tail.Next(t.Context(), 2)
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		all = append(all, got...)
	}
	for i, rec := range all {
		if rec.Seq != int64(i+1) {
			t.Fatalf("record %d position = %d, want %d — records lost or reordered across a rotation",
				i, rec.Seq, i+1)
		}
	}
}

func TestTailer_ResumesMidSegmentAfterARotation(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.MaxSegmentBytes = 400
	for i := 1; i <= 20; i++ {
		if err := w.Append(submitRecord(int64(i), SeqID(i), Price(500+i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Starting from a position in the middle of a rotated log must land in the
	// right segment and skip exactly the already-applied records.
	tail := NewTailer(dir, 14)
	var all []Record
	for len(all) < 6 {
		got, err := tail.Next(t.Context(), 100)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, got...)
	}
	if all[0].Seq != 15 {
		t.Errorf("first record = %d, want 15", all[0].Seq)
	}
	if all[len(all)-1].Seq != 20 {
		t.Errorf("last record = %d, want 20", all[len(all)-1].Seq)
	}
}

func TestTailer_ToleratesAPartialRecordThenReadsIt(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(submitRecord(1, 1, 500)); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(submitRecord(2, 2, 501)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	segs, _ := segmentPaths(dir)
	full, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	// Truncate mid-second-record, as a tailer racing an in-progress write sees.
	if err := os.WriteFile(segs[0], full[:len(full)-6], 0o644); err != nil {
		t.Fatal(err)
	}

	tail := NewTailer(dir, 0)
	got, err := tail.Next(t.Context(), 100)
	if err != nil {
		t.Fatalf("Next() over a partial record error = %v, want nil", err)
	}
	if len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("Next() = %+v, want only the complete record", got)
	}

	// The rest of the bytes land. The tailer must pick the record up rather than
	// having consumed and lost it.
	if err := os.WriteFile(segs[0], full, 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	got, err = tail.Next(ctx, 100)
	if err != nil {
		t.Fatalf("Next() after completion error = %v", err)
	}
	if len(got) != 1 || got[0].Seq != 2 {
		t.Errorf("Next() = %+v, want the now-complete record at position 2", got)
	}
}

func TestTailer_StopsOnCorruption(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, 3)

	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, walHeaderSize+recHeaderSize+2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// A follower must refuse to replicate past a hole: applying records after a
	// damaged one would silently produce a book that never existed.
	tail := NewTailer(dir, 0)
	if _, err := tail.Next(t.Context(), 100); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Next() over corruption = %v, want ErrCorruptWAL", err)
	}
}

func TestTailer_EmptyDirWaitsRatherThanFailing(t *testing.T) {
	// A symbol that has never traded has no segments at all. That is not an
	// error; the follower simply has nothing to apply yet.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	tail := NewTailer(t.TempDir(), 0)
	_, err := tail.Next(ctx, 100)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Next() on an empty dir = %v, want it to wait until the deadline", err)
	}
}

func TestTailer_RespectsTheBatchCap(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if err := w.Append(submitRecord(int64(i), SeqID(i), Price(500+i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	tail := NewTailer(dir, 0)
	got, err := tail.Next(t.Context(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("Next(max=3) returned %d records, want 3", len(got))
	}
}
