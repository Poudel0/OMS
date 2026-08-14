package oms

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// walTS is a fixed wall-clock instant. Tests use it rather than time.Now()
// because a time.Time from time.Now() carries a monotonic clock reading that
// JSON cannot represent, so a round-tripped copy would never compare equal
// field-for-field even though it is perfectly correct.
var walTS = time.Unix(1_700_000_000, 123_456_789).UTC()

// collect replays every segment in dir and returns the records in log order,
// failing the test on any error.
func collect(t *testing.T, dir string) []Record {
	t.Helper()
	segs, err := segmentPaths(dir)
	if err != nil {
		t.Fatalf("segmentPaths() error = %v", err)
	}
	var out []Record
	for _, path := range segs {
		r, err := OpenReader(path)
		if err != nil {
			t.Fatalf("OpenReader(%s) error = %v", filepath.Base(path), err)
		}
		for rec, err := range r.Records() {
			if err != nil {
				t.Fatalf("Records() error = %v", err)
			}
			out = append(out, rec)
		}
		r.Close()
	}
	return out
}

func TestWAL_AppendReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}

	want := []Record{
		{Seq: 1, Kind: RecordSubmit, TS: walTS, Order: Order{
			SeqID: 7, Symbol: "NABIL", Placer: "acc-1", Type: Limit,
			Price: 512, Quantity: 300, Side: Buy, TimeStamp: walTS,
		}},
		{Seq: 2, Kind: RecordSubmit, TS: walTS, Order: Order{
			SeqID: 8, Symbol: "NABIL", Placer: "acc-2", Type: Market,
			Quantity: 150, Side: Sell, TimeStamp: walTS,
		}},
		{Seq: 3, Kind: RecordCancel, TS: walTS, CancelID: 7},
	}
	for _, rec := range want {
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append(%d) error = %v", rec.Seq, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got := collect(t, dir)
	if len(got) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Seq != want[i].Seq || got[i].Kind != want[i].Kind {
			t.Errorf("record %d = seq %d kind %d, want seq %d kind %d",
				i, got[i].Seq, got[i].Kind, want[i].Seq, want[i].Kind)
		}
		if !got[i].TS.Equal(want[i].TS) {
			t.Errorf("record %d TS = %v, want %v", i, got[i].TS, want[i].TS)
		}
		if got[i].CancelID != want[i].CancelID {
			t.Errorf("record %d CancelID = %d, want %d", i, got[i].CancelID, want[i].CancelID)
		}
		if got[i].Order.SeqID != want[i].Order.SeqID ||
			got[i].Order.Symbol != want[i].Order.Symbol ||
			got[i].Order.Placer != want[i].Order.Placer ||
			got[i].Order.Type != want[i].Order.Type ||
			got[i].Order.Price != want[i].Order.Price ||
			got[i].Order.Quantity != want[i].Order.Quantity ||
			got[i].Order.Side != want[i].Order.Side {
			t.Errorf("record %d Order = %+v, want %+v", i, got[i].Order, want[i].Order)
		}
		if !got[i].Order.TimeStamp.Equal(want[i].Order.TimeStamp) {
			t.Errorf("record %d Order.TimeStamp = %v, want %v",
				i, got[i].Order.TimeStamp, want[i].Order.TimeStamp)
		}
	}
}

func TestWAL_SegmentHeaderIsWritten(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	segs, _ := segmentPaths(dir)
	if len(segs) != 1 {
		t.Fatalf("segments = %d, want 1", len(segs))
	}
	raw, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != walHeaderSize {
		t.Fatalf("empty segment is %d bytes, want just the %d-byte header", len(raw), walHeaderSize)
	}
	if string(raw[0:4]) != walMagic {
		t.Errorf("magic = %q, want %q", raw[0:4], walMagic)
	}
	if v := binary.LittleEndian.Uint32(raw[4:8]); v != walVersion {
		t.Errorf("version = %d, want %d", v, walVersion)
	}
}

func TestWAL_BadMagicIsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0000000000000000001.wal")
	if err := os.WriteFile(path, []byte("XXXX\x01\x00\x00\x00"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(path); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("OpenReader() on bad magic = %v, want ErrCorruptWAL", err)
	}
}

func TestWAL_FutureVersionIsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0000000000000000001.wal")
	raw := make([]byte, walHeaderSize)
	copy(raw[0:4], walMagic)
	binary.LittleEndian.PutUint32(raw[4:8], walVersion+1)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	// A newer format must not be silently misread as the current one.
	if _, err := OpenReader(path); err == nil {
		t.Fatal("OpenReader() on a future version = nil error, want a version complaint")
	}
}

// writeSegment lays down n submit records and returns the segment's path.
func writeSegment(t *testing.T, dir string, n int) string {
	t.Helper()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for i := 1; i <= n; i++ {
		rec := Record{Seq: int64(i), Kind: RecordSubmit, TS: walTS, Order: Order{
			SeqID: SeqID(i), Symbol: "NABIL", Placer: "acc", Type: Limit,
			Price: Price(500 + i), Quantity: 10, Side: Buy,
		}}
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	segs, _ := segmentPaths(dir)
	if len(segs) != 1 {
		t.Fatalf("segments = %d, want 1", len(segs))
	}
	return segs[0]
}

func TestWAL_TruncatedTailIsTolerated(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, 3)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Lop 5 bytes off, which lands inside the third record's trailing CRC.
	// Those bytes were never acknowledged to a client, so replay must accept
	// the log and simply stop at the last intact record.
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	got := collect(t, dir)
	if len(got) != 2 {
		t.Fatalf("replayed %d records past a torn tail, want 2", len(got))
	}
	if got[1].Seq != 2 {
		t.Errorf("last surviving record seq = %d, want 2", got[1].Seq)
	}
}

func TestWAL_CorruptRecordHalts(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, 3)

	// Flip a bit inside the first record's payload. Unlike a short tail,
	// these bytes are all present — the storage returned data that does not
	// match what was written, and replay must refuse to continue.
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x00}, walHeaderSize+recHeaderSize+2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	book := NewBook()
	if _, err := Recover(dir, book, 0); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Recover() over a corrupt record = %v, want ErrCorruptWAL", err)
	}
}

func TestWAL_OversizedLengthHalts(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, 1)

	// A torn write can leave garbage in the length prefix. The reader must
	// reject it against the cap rather than trying to allocate it.
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var huge [4]byte
	binary.LittleEndian.PutUint32(huge[:], 1<<30)
	if _, err := f.WriteAt(huge[:], walHeaderSize); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := Recover(dir, NewBook(), 0); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Recover() over an oversized length = %v, want ErrCorruptWAL", err)
	}
}

func TestWAL_OpenWriterTruncatesTornTailAndKeepsAppending(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, 3)

	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() over a torn tail error = %v", err)
	}
	// Records 1 and 2 survived; record 3 did not, so numbering resumes at 3.
	if got := w.LastSeq(); got != 2 {
		t.Errorf("LastSeq() = %d, want 2 (record 3 was torn away)", got)
	}
	if err := w.Append(Record{Seq: 3, Kind: RecordCancel, TS: walTS, CancelID: 1}); err != nil {
		t.Fatalf("Append() after truncation error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// The decisive assertion: the new record is readable, which it would not
	// be if it had been appended behind the leftover partial record.
	got := collect(t, dir)
	if len(got) != 3 {
		t.Fatalf("replayed %d records, want 3", len(got))
	}
	if got[2].Kind != RecordCancel || got[2].CancelID != 1 {
		t.Errorf("last record = %+v, want a cancel of order 1", got[2])
	}
}

func TestWAL_CorruptTailBlocksReopen(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, 2)

	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, walHeaderSize+recHeaderSize+2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Appending on top of a segment that has demonstrably lost data would
	// bury the evidence; the operator has to see this.
	if _, err := OpenWriter(dir); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("OpenWriter() over corruption = %v, want ErrCorruptWAL", err)
	}
}

func TestWAL_RotationSplitsSegmentsAndReplaysInOrder(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	w.MaxSegmentBytes = 200 // forces a roll roughly every record

	const n = 10
	for i := 1; i <= n; i++ {
		rec := Record{Seq: int64(i), Kind: RecordSubmit, TS: walTS, Order: Order{
			SeqID: SeqID(i), Symbol: "NABIL", Placer: "acc", Type: Limit,
			Price: Price(500 + i), Quantity: 10, Side: Buy,
		}}
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	segs, _ := segmentPaths(dir)
	if len(segs) < 2 {
		t.Fatalf("segments = %d, want rotation to have produced more than 1", len(segs))
	}

	got := collect(t, dir)
	if len(got) != n {
		t.Fatalf("replayed %d records across %d segments, want %d", len(got), len(segs), n)
	}
	// Lexical filename order must equal log order, which is the only reason
	// segmentPaths can get away with a plain string sort.
	for i, rec := range got {
		if rec.Seq != int64(i+1) {
			t.Fatalf("record %d has seq %d, want %d — segments replayed out of order", i, rec.Seq, i+1)
		}
	}

	// Numbering must survive a reopen across a rotated log.
	w2, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() after rotation error = %v", err)
	}
	if got := w2.LastSeq(); got != n {
		t.Errorf("LastSeq() after rotation = %d, want %d", got, n)
	}
	w2.Close()
}

func TestWAL_RecoverRebuildsIdenticalBookState(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	w.MaxSegmentBytes = 4096 // exercise recovery across a rotation too

	// Drive a book and its log with the same records, in the same order, so
	// that whatever the log replays into a fresh book must land in the same
	// state as the live one.
	live := NewBook()
	rng := rand.New(rand.NewSource(42))
	var seq int64
	var resting []SeqID

	for i := 1; i <= 2000; i++ {
		seq++
		if len(resting) > 0 && rng.Intn(5) == 0 {
			victim := resting[rng.Intn(len(resting))]
			rec := Record{Seq: seq, Kind: RecordCancel, TS: walTS, CancelID: victim}
			if err := w.Append(rec); err != nil {
				t.Fatalf("Append() error = %v", err)
			}
			rec.Apply(live)
			continue
		}
		side := Buy
		if rng.Intn(2) == 0 {
			side = Sell
		}
		orderType := Limit
		if rng.Intn(20) == 0 {
			orderType = Market
		}
		o := Order{
			SeqID: SeqID(i), Symbol: "NABIL", Placer: "acc", Type: orderType,
			Price: Price(495 + rng.Intn(11)), Quantity: int64(1 + rng.Intn(100)), Side: side,
		}
		rec := Record{Seq: seq, Kind: RecordSubmit, TS: walTS, Order: o}
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		rec.Apply(live)
		if orderType == Limit {
			resting = append(resting, o.SeqID)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed := NewBook()
	lastSeq, err := Recover(dir, replayed, 0)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if lastSeq != seq {
		t.Errorf("Recover() lastSeq = %d, want %d", lastSeq, seq)
	}
	if live.RestingCount() == 0 {
		t.Fatal("live book ended empty — the workload never rested anything, test proves nothing")
	}
	if got, want := replayed.Snapshot(), live.Snapshot(); got != want {
		t.Errorf("replayed book state diverged from live\n got: %s\nwant: %s", got, want)
	}
	if replayed.RestingCount() != live.RestingCount() {
		t.Errorf("replayed RestingCount() = %d, want %d", replayed.RestingCount(), live.RestingCount())
	}
}

func TestWAL_RecoverSkipsRecordsAtOrBeforeAfterSeq(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for i := 1; i <= 4; i++ {
		rec := Record{Seq: int64(i), Kind: RecordSubmit, TS: walTS, Order: Order{
			SeqID: SeqID(i), Type: Limit, Price: Price(500 + i), Quantity: 10, Side: Buy,
		}}
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	book := NewBook()
	lastSeq, err := Recover(dir, book, 2)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	// Skipped records are still counted: the watermark is a log position, so
	// it has to reflect the whole log, not just the part that was applied.
	if lastSeq != 4 {
		t.Errorf("lastSeq = %d, want 4", lastSeq)
	}
	if got := book.RestingCount(); got != 2 {
		t.Fatalf("RestingCount() = %d, want 2 (records 3 and 4 only)", got)
	}
	if bid, _ := book.BestBid(); bid != 504 {
		t.Errorf("BestBid() = %d, want 504", bid)
	}
}

func TestWAL_RecoverOnEmptyDirIsNotAnError(t *testing.T) {
	lastSeq, err := Recover(t.TempDir(), NewBook(), 0)
	if err != nil {
		t.Fatalf("Recover() on an empty dir error = %v, want nil (first ever boot)", err)
	}
	if lastSeq != 0 {
		t.Errorf("lastSeq = %d, want 0", lastSeq)
	}
}

func TestWAL_FailedCancelIsStillLoggedAndReplaysIdentically(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The log records attempts, not successes: this cancel targets an order
	// that never existed. Replay must accept it and reach the same state,
	// rather than treating the mutation's error as a corrupt log.
	recs := []Record{
		{Seq: 1, Kind: RecordSubmit, TS: walTS, Order: Order{SeqID: 1, Type: Limit, Price: 500, Quantity: 10, Side: Buy}},
		{Seq: 2, Kind: RecordCancel, TS: walTS, CancelID: 999},
	}
	live := NewBook()
	for _, rec := range recs {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
		rec.Apply(live)
	}
	w.Close()

	replayed := NewBook()
	if _, err := Recover(dir, replayed, 0); err != nil {
		t.Fatalf("Recover() error = %v, want nil", err)
	}
	if got, want := replayed.Snapshot(), live.Snapshot(); got != want {
		t.Errorf("state after replaying a failed cancel = %s, want %s", got, want)
	}
	if replayed.RestingCount() != 1 {
		t.Errorf("RestingCount() = %d, want 1", replayed.RestingCount())
	}
}
