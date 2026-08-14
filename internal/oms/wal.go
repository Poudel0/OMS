package oms

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrCorruptWAL reports a record that was fully present on disk but did not
// verify — a CRC mismatch, or a length field larger than any record this
// writer would ever emit. It is deliberately distinct from a *truncated*
// tail: bytes that never made it to disk are an expected crash outcome and
// stop a scan silently, whereas bytes that made it but are wrong mean the
// storage lied and replay must not continue past them. See ADR-003.
var ErrCorruptWAL = errors.New("oms: corrupt wal record")

// On-disk layout, all integers little-endian.
//
//	segment := header record*
//	header  := magic[4]="DWAL" version:uint32
//	record  := len:uint32 seq:int64 tsUnixNano:int64 payload[len] crc32:uint32
//
// crc32 (IEEE) covers the record's fixed header and its payload, so a torn
// write anywhere in the record is caught. The length prefix comes first
// because a reader must know how far the payload runs before it can check
// anything at all; the CRC comes last because it cannot be computed until
// the payload exists.
const (
	walMagic      = "DWAL"
	walVersion    = 1
	walHeaderSize = 8  // magic(4) + version(4)
	recHeaderSize = 20 // len(4) + seq(8) + ts(8)
	walCRCSize    = 4

	// maxRecordPayload bounds what a reader will allocate for one record.
	// Without it, a torn write that leaves garbage in the length prefix
	// would have the reader try to allocate multiple gigabytes before it
	// ever reaches the CRC that would have rejected the record.
	maxRecordPayload = 1 << 20

	// DefaultSegmentBytes is the size at which the writer rolls to a new
	// segment file. Rotation exists so that old, fully-settled log can be
	// archived or deleted a whole file at a time.
	DefaultSegmentBytes = 64 << 20

	segmentSuffix = ".wal"
	// Segments are named for the first seq they contain, zero-padded to the
	// full width of an int64 so that lexical filename order is also seq
	// order — which is what lets segmentPaths just sort strings.
	segmentNameFmt = "%019d" + segmentSuffix
)

// RecordKind distinguishes the two mutations the log has to replay. The zero
// value is invalid, same convention as OrderType/OrderSide.
type RecordKind uint8

const (
	RecordUnknown RecordKind = iota
	RecordSubmit
	RecordCancel
)

// Record is one entry in the write-ahead log: the intent to apply a single
// mutation to the book, durable before the book ever sees it.
//
// Seq is a log position, not an order identifier — it is assigned by the
// sequencer, increases by one per record, and covers cancels too (which
// introduce no new order ID of their own). Order.SeqID remains the
// caller-assigned order identifier. Keeping the two separate is what lets
// the log order cancels and submits in a single sequence; see ADR-003.
type Record struct {
	Seq      int64
	Kind     RecordKind
	TS       time.Time
	Order    Order // set when Kind == RecordSubmit
	CancelID SeqID // set when Kind == RecordCancel
}

// walPayload is the variable part of a record. It is JSON rather than a
// hand-rolled binary encoding because a WAL append is dominated by the
// fsync that follows it, not by marshalling, and JSON keeps a segment
// readable with nothing but `xxd` when a replay goes wrong.
//
// ponytail: JSON payload, ~3x the bytes of a packed binary encoding and it
// re-parses field names per record. Swap for a hand-rolled encoder if
// segment size or replay time ever shows up in a profile — the format is
// versioned (walVersion) precisely so that swap can happen.
type walPayload struct {
	Kind     RecordKind `json:"k"`
	Order    *Order     `json:"o,omitempty"`
	CancelID SeqID      `json:"c,omitempty"`
}

// apply replays this record against book, discarding the mutation's error.
//
// Discarding it is correct, not lazy: the log is written *before* the book is
// touched, so it records attempts rather than successes. A Cancel for an ID
// that was already gone failed during the original run and left the book
// unchanged; replaying it fails identically and leaves the book unchanged
// again. Treating that as a replay failure would reject logs that are in
// fact perfectly faithful.
func (rec Record) apply(book *Book) {
	switch rec.Kind {
	case RecordSubmit:
		_, _ = book.Submit(rec.Order)
	case RecordCancel:
		_ = book.Cancel(rec.CancelID)
	}
}

// Writer appends records to the newest segment in a directory.
//
// Append only buffers; durability happens in Sync, and separating the two is
// the entire point. A caller that has several records in hand — the
// sequencer, which drains its request channel before committing — appends
// them all and pays for one fsync instead of one per record. Nothing may be
// acknowledged to a client until Sync has returned nil.
//
// A Writer is not safe for concurrent use: it is owned by the single
// goroutine that owns the book it fronts.
type Writer struct {
	dir string
	f   *os.File
	bw  *bufio.Writer

	// MaxSegmentBytes is the rotation threshold. Set it immediately after
	// OpenWriter and before the first Append; it is read on every append.
	MaxSegmentBytes int64

	segBytes int64
	lastSeq  int64
}

// OpenWriter opens dir for appending, creating it if needed, and prepares the
// newest segment to receive records.
//
// If the process previously died mid-append, the newest segment ends in a
// partial record. That tail is truncated away here — it was never fsynced,
// so it was never acknowledged to any client, and leaving it in place would
// put the next record behind unparseable bytes. A tail that is *corrupt*
// rather than merely short is returned as an error instead: appending on top
// of storage that has demonstrably lost data would bury the evidence.
func OpenWriter(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("oms: create wal dir: %w", err)
	}
	segs, err := segmentPaths(dir)
	if err != nil {
		return nil, err
	}
	w := &Writer{dir: dir, MaxSegmentBytes: DefaultSegmentBytes}
	if len(segs) == 0 {
		return w, w.openSegment(1)
	}

	last := segs[len(segs)-1]
	lastSeq, goodOffset, err := scanSegment(last)
	if err != nil {
		return nil, err
	}
	// A segment created but crashed on before its first record carries its
	// starting seq only in its name; recover the watermark from there so a
	// restart can never reissue a seq an earlier segment already used.
	if nameSeq, err := seqFromName(last); err == nil && nameSeq-1 > lastSeq {
		lastSeq = nameSeq - 1
	}

	f, err := os.OpenFile(last, os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("oms: reopen wal segment: %w", err)
	}
	if err := f.Truncate(goodOffset); err != nil {
		f.Close()
		return nil, fmt.Errorf("oms: truncate torn wal tail: %w", err)
	}
	if _, err := f.Seek(goodOffset, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("oms: seek wal segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("oms: sync truncated wal segment: %w", err)
	}
	w.f, w.bw, w.segBytes, w.lastSeq = f, bufio.NewWriter(f), goodOffset, lastSeq
	return w, nil
}

// LastSeq reports the highest log position durably present in the
// directory. The sequencer resumes numbering from here so that a restart
// never reuses a position.
func (w *Writer) LastSeq() int64 { return w.lastSeq }

// Append buffers rec, rotating to a new segment first if this record would
// push the current one past MaxSegmentBytes. It does not make rec durable —
// call Sync for that.
func (w *Writer) Append(rec Record) error {
	payload, err := json.Marshal(walPayload{Kind: rec.Kind, Order: orderPtr(rec), CancelID: rec.CancelID})
	if err != nil {
		return fmt.Errorf("oms: marshal wal payload: %w", err)
	}
	if len(payload) > maxRecordPayload {
		return fmt.Errorf("oms: wal payload %d bytes exceeds cap %d", len(payload), maxRecordPayload)
	}

	total := int64(recHeaderSize + len(payload) + walCRCSize)
	// The second clause keeps a record larger than the whole threshold from
	// rotating forever into fresh empty segments.
	if w.segBytes+total > w.MaxSegmentBytes && w.segBytes > walHeaderSize {
		if err := w.rotate(rec.Seq); err != nil {
			return err
		}
	}

	var hdr [recHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint64(hdr[4:12], uint64(rec.Seq))
	binary.LittleEndian.PutUint64(hdr[12:20], uint64(rec.TS.UnixNano()))

	crc := crc32.Update(0, crc32.IEEETable, hdr[:])
	crc = crc32.Update(crc, crc32.IEEETable, payload)
	var tail [walCRCSize]byte
	binary.LittleEndian.PutUint32(tail[:], crc)

	if _, err := w.bw.Write(hdr[:]); err != nil {
		return fmt.Errorf("oms: write wal header: %w", err)
	}
	if _, err := w.bw.Write(payload); err != nil {
		return fmt.Errorf("oms: write wal payload: %w", err)
	}
	if _, err := w.bw.Write(tail[:]); err != nil {
		return fmt.Errorf("oms: write wal crc: %w", err)
	}
	w.segBytes += total
	if rec.Seq > w.lastSeq {
		w.lastSeq = rec.Seq
	}
	return nil
}

// Sync flushes buffered records and fsyncs the segment. Everything appended
// before it is durable once it returns nil, and nothing is durable if it
// returns an error.
func (w *Writer) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("oms: flush wal: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("oms: fsync wal: %w", err)
	}
	return nil
}

// Close syncs and releases the segment.
func (w *Writer) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f, w.bw = nil, nil
	return err
}

func (w *Writer) rotate(firstSeq int64) error {
	if err := w.Close(); err != nil {
		return err
	}
	return w.openSegment(firstSeq)
}

func (w *Writer) openSegment(firstSeq int64) error {
	path := filepath.Join(w.dir, fmt.Sprintf(segmentNameFmt, firstSeq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("oms: create wal segment: %w", err)
	}
	var hdr [walHeaderSize]byte
	copy(hdr[0:4], walMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], walVersion)
	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		return fmt.Errorf("oms: write wal segment header: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("oms: sync wal segment header: %w", err)
	}
	// fsync the directory too, or the new segment's *name* can be missing
	// after a crash even though its contents are on disk.
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.f, w.bw, w.segBytes = f, bufio.NewWriter(f), walHeaderSize
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("oms: open wal dir for sync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("oms: sync wal dir: %w", err)
	}
	return nil
}

func orderPtr(rec Record) *Order {
	if rec.Kind != RecordSubmit {
		return nil
	}
	o := rec.Order
	return &o
}

// Reader scans one segment's records in order.
type Reader struct {
	f      *os.File
	br     *bufio.Reader
	offset int64 // bytes consumed by fully-verified records, plus the header
}

// OpenReader opens a segment and validates its header.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("oms: open wal segment: %w", err)
	}
	br := bufio.NewReader(f)
	var hdr [walHeaderSize]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("%w: %s has no readable segment header: %v", ErrCorruptWAL, path, err)
	}
	if string(hdr[0:4]) != walMagic {
		f.Close()
		return nil, fmt.Errorf("%w: %s bad magic %q", ErrCorruptWAL, path, hdr[0:4])
	}
	if v := binary.LittleEndian.Uint32(hdr[4:8]); v != walVersion {
		f.Close()
		return nil, fmt.Errorf("oms: wal segment %s is version %d, this build reads %d", path, v, walVersion)
	}
	return &Reader{f: f, br: br, offset: walHeaderSize}, nil
}

// Close releases the segment.
func (r *Reader) Close() error { return r.f.Close() }

// Offset reports the byte offset just past the last fully-verified record —
// i.e. the point a writer should truncate to and resume appending from.
func (r *Reader) Offset() int64 { return r.offset }

// Records iterates the segment in log order, stopping at the first bad
// record. A truncated tail ends the iteration with no error; a record that
// is present but fails verification yields ErrCorruptWAL and then stops.
func (r *Reader) Records() iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for {
			rec, err := r.next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return // clean end, or a tail that never reached disk
				}
				yield(Record{}, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
		}
	}
}

// next reads one record. It reports io.EOF both for a clean end of segment
// and for a short read partway through a record; the two are the same thing
// as far as replay is concerned, since r.offset only advances past records
// that verified.
func (r *Reader) next() (Record, error) {
	var hdr [recHeaderSize]byte
	if _, err := io.ReadFull(r.br, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, io.EOF
		}
		return Record{}, err
	}

	n := binary.LittleEndian.Uint32(hdr[0:4])
	if n > maxRecordPayload {
		return Record{}, fmt.Errorf("%w: record at offset %d claims %d payload bytes (cap %d)",
			ErrCorruptWAL, r.offset, n, maxRecordPayload)
	}

	payload := make([]byte, n)
	if _, err := io.ReadFull(r.br, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, io.EOF
		}
		return Record{}, err
	}
	var tail [walCRCSize]byte
	if _, err := io.ReadFull(r.br, tail[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, io.EOF
		}
		return Record{}, err
	}

	want := binary.LittleEndian.Uint32(tail[:])
	got := crc32.Update(crc32.Update(0, crc32.IEEETable, hdr[:]), crc32.IEEETable, payload)
	if got != want {
		return Record{}, fmt.Errorf("%w: crc mismatch at offset %d (computed %08x, stored %08x)",
			ErrCorruptWAL, r.offset, got, want)
	}

	var p walPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		// The CRC already passed, so these bytes are exactly what was
		// written — a decode failure here means a format change, not
		// storage damage.
		return Record{}, fmt.Errorf("oms: decode wal payload at offset %d: %w", r.offset, err)
	}

	rec := Record{
		Seq:      int64(binary.LittleEndian.Uint64(hdr[4:12])),
		Kind:     p.Kind,
		TS:       time.Unix(0, int64(binary.LittleEndian.Uint64(hdr[12:20]))),
		CancelID: p.CancelID,
	}
	if p.Order != nil {
		rec.Order = *p.Order
	}
	r.offset += int64(recHeaderSize) + int64(n) + walCRCSize
	return rec, nil
}

// Recover replays every segment in dir into book and returns the highest log
// position seen. Records at or below afterSeq are skipped but still counted,
// so a caller holding a snapshot taken at some position can replay only what
// followed it.
//
// Replay goes straight at the Book rather than through a Sequencer: the log
// is the source of truth being read, and routing it back through the writer
// would append every recovered record a second time.
func Recover(dir string, book *Book, afterSeq int64) (lastSeq int64, err error) {
	segs, err := segmentPaths(dir)
	if err != nil {
		return 0, err
	}
	for _, path := range segs {
		r, err := OpenReader(path)
		if err != nil {
			return lastSeq, err
		}
		for rec, err := range r.Records() {
			if err != nil {
				r.Close()
				return lastSeq, fmt.Errorf("oms: replay %s: %w", filepath.Base(path), err)
			}
			if rec.Seq > lastSeq {
				lastSeq = rec.Seq
			}
			if rec.Seq > afterSeq {
				rec.apply(book)
			}
		}
		if err := r.Close(); err != nil {
			return lastSeq, err
		}
	}
	return lastSeq, nil
}

// scanSegment reads path to its end, reporting the highest seq it holds and
// the offset just past its last intact record.
func scanSegment(path string) (lastSeq, goodOffset int64, err error) {
	r, err := OpenReader(path)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()
	for rec, err := range r.Records() {
		if err != nil {
			return lastSeq, r.Offset(), err
		}
		if rec.Seq > lastSeq {
			lastSeq = rec.Seq
		}
	}
	return lastSeq, r.Offset(), nil
}

// segmentPaths returns dir's segments in log order. Fixed-width zero-padded
// names make that the same as lexical order.
func segmentPaths(dir string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if err != nil {
		return nil, fmt.Errorf("oms: list wal segments: %w", err)
	}
	sort.Strings(paths)
	return paths, nil
}

func seqFromName(path string) (int64, error) {
	return strconv.ParseInt(strings.TrimSuffix(filepath.Base(path), segmentSuffix), 10, 64)
}
