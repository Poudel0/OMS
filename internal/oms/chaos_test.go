package oms

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The crash test runs the matching engine in a child copy of this test binary
// and kills it with a panic partway through a stream of orders. A subprocess
// is the only honest way to do this: a same-process "crash" cannot avoid Go's
// deferred cleanup, so it would always end up flushing and closing the log on
// the way out — testing the graceful path while claiming to test the crash
// path. Only a process that genuinely dies leaves the log in the state a real
// crash leaves it in.
const (
	chaosDirEnv  = "OMS_CHAOS_WAL_DIR"
	chaosSnapEnv = "OMS_CHAOS_SNAPSHOT"

	// crashAfter is where the child dies; totalOrders is what it intended to
	// submit, so the panic genuinely lands mid-stream rather than at a
	// convenient boundary.
	chaosCrashAfter  = 10_000
	chaosTotalOrders = 15_000

	// chaosMarker proves the child reached the panic on purpose. Without it, a
	// child that died during setup would also exit non-zero and the test
	// would pass for the wrong reason.
	chaosMarker = "CHAOS-CHILD-CRASHING-NOW"
)

// chaosSnapshot is what the child records about its own state at the instant
// before it dies, for the parent to hold recovery against.
type chaosSnapshot struct {
	State        BookState `json:"state"`
	RestingCount int       `json:"resting_count"`
	LastSeq      int64     `json:"last_seq"`
	Orders       int       `json:"orders"`
	Cancels      int       `json:"cancels"`
}

func TestChaos_AcknowledgedMutationsSurviveACrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	snapPath := filepath.Join(t.TempDir(), "snapshot.json")

	child := exec.Command(os.Args[0], "-test.run=^TestChaosCrashChild$", "-test.v")
	child.Env = append(os.Environ(), chaosDirEnv+"="+dir, chaosSnapEnv+"="+snapPath)
	out, err := child.CombinedOutput()

	if err == nil {
		t.Fatalf("child exited cleanly, want a crash\n%s", out)
	}
	if !strings.Contains(string(out), chaosMarker) {
		t.Fatalf("child died before reaching its planned panic — it failed for some other reason\n%s", out)
	}

	raw, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("child left no pre-crash snapshot: %v\n%s", err, out)
	}
	var want chaosSnapshot
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("decode pre-crash snapshot: %v", err)
	}
	if want.RestingCount == 0 || want.LastSeq == 0 {
		t.Fatalf("pre-crash snapshot is empty (%+v) — the child crashed too early to prove anything", want)
	}

	// Restart: nothing but the log the dead process left behind.
	recovered := NewBook()
	lastSeq, err := Recover(dir, recovered, 0)
	if err != nil {
		t.Fatalf("Recover() after crash error = %v", err)
	}

	// The guarantee under test: every mutation the child acknowledged is
	// recoverable, and nothing beyond it was invented. The child had a single
	// producer, so at the moment it snapshotted there was no request in
	// flight — which is what makes an exact comparison the right assertion
	// here rather than a "state is at least as new as" one.
	if lastSeq != want.LastSeq {
		t.Errorf("recovered log position = %d, want %d (%d acknowledged mutations lost or invented)",
			lastSeq, want.LastSeq, want.LastSeq-lastSeq)
	}
	if got := recovered.RestingCount(); got != want.RestingCount {
		t.Errorf("recovered RestingCount() = %d, want %d", got, want.RestingCount)
	}
	if got := recovered.Snapshot(); got != want.State {
		t.Errorf("recovered book state diverged from pre-crash state\n got: %s\nwant: %s", got, want.State)
	}
	t.Logf("crash survived: %d submits + %d cancels, %d log positions, %d orders still resting",
		want.Orders, want.Cancels, want.LastSeq, want.RestingCount)
}

// TestChaosCrashChild is the helper process for
// TestChaos_AcknowledgedMutationsSurviveACrash. It is a no-op unless the
// parent invoked it with the environment set.
//
// It deliberately registers no cleanup: a deferred Sequencer.Close would run
// during the panic and flush the log cleanly, which is exactly the behaviour
// this test must not have.
func TestChaosCrashChild(t *testing.T) {
	dir := os.Getenv(chaosDirEnv)
	snapPath := os.Getenv(chaosSnapEnv)
	if dir == "" || snapPath == "" {
		t.Skip("helper process for TestChaos_AcknowledgedMutationsSurviveACrash")
	}

	w, err := OpenWriter(dir)
	if err != nil {
		t.Fatalf("child OpenWriter() error = %v", err)
	}
	book := NewBook()
	ctx := context.Background()
	seq := NewSequencerWithWAL(ctx, book, w)

	rng := rand.New(rand.NewSource(7))
	var lastSeq int64
	var submits, cancels int
	var resting []SeqID

	for i := 1; i <= chaosTotalOrders; i++ {
		if len(resting) > 0 && rng.Intn(6) == 0 {
			victim := resting[rng.Intn(len(resting))]
			if err := seq.Cancel(ctx, victim); err == nil {
				cancels++
			}
			// A cancel is logged whether or not it found its target, so its
			// log position counts either way.
			lastSeq++
		} else {
			side := Buy
			if rng.Intn(2) == 0 {
				side = Sell
			}
			orderType := Limit
			if rng.Intn(25) == 0 {
				orderType = Market
			}
			o := Order{
				SeqID: SeqID(i), Symbol: "NABIL", Placer: "acc", Type: orderType,
				Price: Price(495 + rng.Intn(11)), Quantity: int64(1 + rng.Intn(100)), Side: side,
			}
			resp, err := seq.Submit(ctx, o)
			if err != nil {
				t.Fatalf("child Submit(%d) error = %v", i, err)
			}
			lastSeq = resp.Seq
			submits++
			if orderType == Limit {
				resting = append(resting, o.SeqID)
			}
		}

		if i == chaosCrashAfter {
			snap := chaosSnapshot{
				State:        book.Snapshot(),
				RestingCount: book.RestingCount(),
				LastSeq:      lastSeq,
				Orders:       submits,
				Cancels:      cancels,
			}
			raw, err := json.Marshal(snap)
			if err != nil {
				t.Fatalf("child marshal snapshot: %v", err)
			}
			if err := os.WriteFile(snapPath, raw, 0o644); err != nil {
				t.Fatalf("child write snapshot: %v", err)
			}
			// Everything above was acknowledged, so everything above is
			// already fsynced. Die without flushing anything else.
			os.Stderr.WriteString(chaosMarker + "\n")
			panic("chaos: simulated crash mid-stream")
		}
	}
	t.Fatal("child ran to completion without crashing")
}
