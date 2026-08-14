package replica

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Poudel0/OMS/internal/api"
	"github.com/Poudel0/OMS/internal/oms"
	"github.com/Poudel0/OMS/internal/pb"
)

// primaryNode is a running primary a test can trade against and replicate from.
type primaryNode struct {
	client   pb.OrderServiceClient
	repl     pb.ReplicationServiceClient
	accounts *oms.Accounts
	walDir   string

	reg  *oms.Registry
	gs   *grpc.Server
	lis  *bufconn.Listener
	conn *grpc.ClientConn
}

// startPrimary brings up a primary over an in-process listener, on an existing
// WAL directory if one is supplied. Passing a directory is how the restart and
// promotion tests reuse a node's log.
func startPrimary(t *testing.T, walDir string) *primaryNode {
	t.Helper()
	if walDir == "" {
		walDir = filepath.Join(t.TempDir(), "primary")
	}

	accounts := oms.NewAccounts()
	// Two accounts so that orders have someone to trade against without
	// self-trade prevention cancelling everything.
	accounts.Deposit("alice", 1_000_000_000)
	accounts.Deposit("bob", 1_000_000_000)
	accounts.SetPosition("alice", "NABIL", 10_000_000)
	accounts.SetPosition("bob", "NABIL", 10_000_000)
	accounts.SetPosition("alice", "ADBL", 10_000_000)
	accounts.SetPosition("bob", "ADBL", 10_000_000)

	log := slog.New(slog.DiscardHandler)
	reg := oms.NewRegistry(context.Background(), walDir, accounts)
	srv := api.NewServer(reg, accounts, nil, log)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterOrderServiceServer(gs, srv)
	pb.RegisterReplicationServiceServer(gs, api.NewReplicationServer(walDir, reg, log))
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///primary",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	return &primaryNode{
		client:   pb.NewOrderServiceClient(conn),
		repl:     pb.NewReplicationServiceClient(conn),
		accounts: accounts,
		walDir:   walDir,
		reg:      reg,
		gs:       gs,
		lis:      lis,
		conn:     conn,
	}
}

// stop shuts the primary down cleanly, flushing its logs — the graceful case.
func (p *primaryNode) stop() {
	p.conn.Close()
	p.gs.Stop()
	p.lis.Close()
	_ = p.reg.Close()
}

func (p *primaryNode) place(t *testing.T, symbol, account string, side pb.Side, price, qty int64) *pb.PlaceOrderResponse {
	t.Helper()
	resp, err := p.client.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		Symbol: symbol, AccountId: account, Side: side,
		Type: pb.OrderType_ORDER_TYPE_LIMIT, Price: price, Quantity: qty,
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	return resp
}

// startFollower runs a follower against a primary, in the given directory.
func startFollower(t *testing.T, p *primaryNode, dir string) (*Replica, context.CancelFunc) {
	t.Helper()
	if dir == "" {
		dir = filepath.Join(t.TempDir(), "follower")
	}
	ctx, cancel := context.WithCancel(context.Background())
	rep := New(dir, slog.New(slog.DiscardHandler))
	go func() { _ = rep.Run(ctx, p.repl) }()
	t.Cleanup(func() {
		cancel()
		_ = rep.Close()
	})
	return rep, cancel
}

// waitForPosition waits until the follower has applied through want, or fails.
func waitForPosition(t *testing.T, rep *Replica, symbol string, want int64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if rep.Position(symbol) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follower stuck at position %d for %s, want >= %d", rep.Position(symbol), symbol, want)
}

func primarySnapshot(t *testing.T, p *primaryNode, symbol string) oms.BookState {
	t.Helper()
	seq, err := p.reg.Get(symbol)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := seq.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestReplica_ReachesEventualEqualityWithThePrimary(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()
	rep, _ := startFollower(t, p, "")

	// 1,000 orders, half crossing, so the follower has to reproduce fills,
	// partial fills, and resting orders — not just appends.
	const orders = 1000
	var lastPos int64
	for i := range orders {
		side := pb.Side_SIDE_BUY
		account := "alice"
		if i%2 == 0 {
			side, account = pb.Side_SIDE_SELL, "bob"
		}
		resp := p.place(t, "NABIL", account, side, int64(495+i%10), 10)
		lastPos = resp.GetLogPosition()
	}

	waitForPosition(t, rep, "NABIL", lastPos)

	got, ok := rep.Snapshot("NABIL")
	if !ok {
		t.Fatal("follower has no NABIL book")
	}
	if want := primarySnapshot(t, p, "NABIL"); got != want {
		t.Errorf("follower book diverged from primary\n got: %s\nwant: %s", got, want)
	}
}

func TestReplica_ReplicatesCancelsAndOwnershipRefusals(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()
	rep, _ := startFollower(t, p, "")

	resting := p.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 500, 10)
	p.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 499, 10)

	// A cancel that the primary refused, because bob does not own the order. The
	// follower re-evaluates the constraint, so if cancel_requested_by did not
	// travel over the wire this would succeed on the follower and the books would
	// diverge.
	_, err := p.client.CancelOrder(context.Background(), &pb.CancelOrderRequest{
		Symbol: "NABIL", OrderId: resting.GetOrderId(), AccountId: "bob",
	})
	if err == nil {
		t.Fatal("CancelOrder() by the wrong account = nil error, want a refusal")
	}

	// And one the primary allowed.
	if _, err := p.client.CancelOrder(context.Background(), &pb.CancelOrderRequest{
		Symbol: "NABIL", OrderId: resting.GetOrderId(), AccountId: "alice",
	}); err != nil {
		t.Fatalf("CancelOrder() by the owner error = %v", err)
	}

	// Positions: 2 submits + 1 refused cancel + 1 allowed cancel = 4 records.
	waitForPosition(t, rep, "NABIL", 4)

	got, _ := rep.Snapshot("NABIL")
	if want := primarySnapshot(t, p, "NABIL"); got != want {
		t.Errorf("follower diverged\n got: %s\nwant: %s", got, want)
	}
	if rep.RestingCount("NABIL") != 1 {
		t.Errorf("follower RestingCount() = %d, want 1", rep.RestingCount("NABIL"))
	}
}

func TestReplica_ReplicatesEverySymbolIndependently(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()
	rep, _ := startFollower(t, p, "")

	for _, symbol := range []string{"NABIL", "ADBL"} {
		p.place(t, symbol, "bob", pb.Side_SIDE_SELL, 500, 10)
		p.place(t, symbol, "alice", pb.Side_SIDE_BUY, 500, 4)
	}

	for _, symbol := range []string{"NABIL", "ADBL"} {
		waitForPosition(t, rep, symbol, 2)
		got, ok := rep.Snapshot(symbol)
		if !ok {
			t.Fatalf("follower has no %s book", symbol)
		}
		if want := primarySnapshot(t, p, symbol); got != want {
			t.Errorf("%s diverged\n got: %s\nwant: %s", symbol, got, want)
		}
	}
}

func TestReplica_ResumesAfterItsOwnRestart(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()

	followerDir := filepath.Join(t.TempDir(), "follower")

	// First life: replicate some orders, then stop.
	ctx1, cancel1 := context.WithCancel(context.Background())
	rep1 := New(followerDir, slog.New(slog.DiscardHandler))
	go func() { _ = rep1.Run(ctx1, p.repl) }()

	for range 20 {
		p.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 500, 1)
	}
	waitForPosition(t, rep1, "NABIL", 20)
	cancel1()
	if err := rep1.Close(); err != nil {
		t.Fatalf("follower Close() error = %v", err)
	}

	// More orders arrive while the follower is down.
	var lastPos int64
	for range 20 {
		lastPos = p.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 499, 1).GetLogPosition()
	}

	// Second life over the same directory. It resumes from its own log — there is
	// no checkpoint file, because the log's last position IS the checkpoint.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	rep2 := New(followerDir, slog.New(slog.DiscardHandler))
	defer rep2.Close()
	go func() { _ = rep2.Run(ctx2, p.repl) }()

	waitForPosition(t, rep2, "NABIL", lastPos)
	got, _ := rep2.Snapshot("NABIL")
	if want := primarySnapshot(t, p, "NABIL"); got != want {
		t.Errorf("follower diverged after its own restart\n got: %s\nwant: %s", got, want)
	}
}

func TestReplica_ReconnectsAfterThePrimaryRestarts(t *testing.T) {
	p1 := startPrimary(t, "")
	walDir := p1.walDir
	rep, _ := startFollower(t, p1, "")

	for range 10 {
		p1.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 500, 1)
	}
	waitForPosition(t, rep, "NABIL", 10)

	// The primary goes away. The follower must keep trying rather than exiting:
	// a primary restart is an expected event, not a fatal one.
	p1.stop()
	time.Sleep(50 * time.Millisecond)

	// A new primary over the same log, and the follower's existing stream is dead.
	// Rather than reconnect the old client (bufconn cannot), point the follower at
	// the new node the way a real deployment's DNS or load balancer would.
	p2 := startPrimary(t, walDir)
	defer p2.stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep2 := New(filepath.Join(t.TempDir(), "follower2"), slog.New(slog.DiscardHandler))
	defer rep2.Close()
	go func() { _ = rep2.Run(ctx, p2.repl) }()

	var lastPos int64
	for range 10 {
		lastPos = p2.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 499, 1).GetLogPosition()
	}
	// The restarted primary continued its own positions, so the follower must see
	// records from both lives.
	if lastPos <= 10 {
		t.Fatalf("restarted primary reused positions: last = %d, want > 10", lastPos)
	}
	waitForPosition(t, rep2, "NABIL", lastPos)

	got, _ := rep2.Snapshot("NABIL")
	if want := primarySnapshot(t, p2, "NABIL"); got != want {
		t.Errorf("follower diverged across a primary restart\n got: %s\nwant: %s", got, want)
	}
}

func TestReplica_PromotedFollowerLogServesAsAPrimary(t *testing.T) {
	p := startPrimary(t, "")
	followerDir := filepath.Join(t.TempDir(), "follower")
	rep, cancel := startFollower(t, p, followerDir)

	var lastPos int64
	for i := range 40 {
		side := pb.Side_SIDE_BUY
		account := "alice"
		if i%2 == 0 {
			side, account = pb.Side_SIDE_SELL, "bob"
		}
		lastPos = p.place(t, "NABIL", account, side, int64(498+i%5), 10).GetLogPosition()
	}
	waitForPosition(t, rep, "NABIL", lastPos)
	wantState := primarySnapshot(t, p, "NABIL")

	// Failover: the primary is lost, and the follower stops replicating.
	p.stop()
	cancel()
	if err := rep.Close(); err != nil {
		t.Fatalf("follower Close() error = %v", err)
	}

	// Promotion is just starting a normal node over the follower's log. That works
	// only because the follower stored records under the PRIMARY's positions in
	// the primary's own format — there is no conversion step to get wrong while
	// the venue is down.
	promoted := startPrimary(t, followerDir)
	defer promoted.stop()

	if got := primarySnapshot(t, promoted, "NABIL"); got != wantState {
		t.Errorf("promoted node's book differs from the lost primary's\n got: %s\nwant: %s", got, wantState)
	}

	// And it can trade, continuing the log rather than restarting it.
	resp := promoted.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 400, 1)
	if resp.GetLogPosition() != lastPos+1 {
		t.Errorf("promoted node's next position = %d, want %d", resp.GetLogPosition(), lastPos+1)
	}
}

func TestReplica_ReportsLagToThePrimary(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()
	rep, _ := startFollower(t, p, "")

	var lastPos int64
	for range 30 {
		lastPos = p.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 500, 1).GetLogPosition()
	}
	waitForPosition(t, rep, "NABIL", lastPos)

	// Progress reports are on a timer, so wait for one to land rather than
	// assuming it already has.
	deadline := time.Now().Add(20 * time.Second)
	var seen bool
	for time.Now().Before(deadline) {
		status, err := p.repl.ReplicationStatus(context.Background(), &pb.ReplicationStatusRequest{})
		if err != nil {
			t.Fatalf("ReplicationStatus() error = %v", err)
		}
		for _, lag := range status.GetSymbols() {
			if lag.GetSymbol() != "NABIL" || !lag.GetFollowerSeen() {
				continue
			}
			seen = true
			if lag.GetPrimaryPosition() != lastPos {
				t.Errorf("primary position = %d, want %d", lag.GetPrimaryPosition(), lastPos)
			}
			if lag.GetFollowerPosition() != lastPos {
				t.Errorf("follower position = %d, want %d", lag.GetFollowerPosition(), lastPos)
			}
			if lag.GetRecordsBehind() != 0 {
				t.Errorf("records behind = %d, want 0 (the follower is caught up)", lag.GetRecordsBehind())
			}
		}
		if seen {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !seen {
		t.Fatal("primary never saw a progress report from the follower")
	}
}

func TestReplica_LagIsVisibleWhileTheFollowerIsBehind(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()

	// No follower at all. The primary must still report its own position and say
	// plainly that nobody has reported — silence is not the same as caught up.
	for range 5 {
		p.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 500, 1)
	}
	status, err := p.repl.ReplicationStatus(context.Background(), &pb.ReplicationStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.GetSymbols()) != 1 {
		t.Fatalf("status covers %d symbols, want 1", len(status.GetSymbols()))
	}
	lag := status.GetSymbols()[0]
	if lag.GetFollowerSeen() {
		t.Error("FollowerSeen = true with no follower running")
	}
	if lag.GetPrimaryPosition() != 5 {
		t.Errorf("primary position = %d, want 5", lag.GetPrimaryPosition())
	}
}

func TestReplica_RefusesToStreamPastADamagedLog(t *testing.T) {
	p := startPrimary(t, "")
	walDir := p.walDir
	for range 5 {
		p.place(t, "NABIL", "alice", pb.Side_SIDE_BUY, 500, 1)
	}
	p.stop() // flush so the bytes are on disk to damage

	// Corrupt a record in the middle of the primary's log.
	segs, err := filepath.Glob(filepath.Join(walDir, "NABIL", "*.wal"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments found: %v", err)
	}
	f, err := os.OpenFile(segs[0], os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Offset chosen inside the first record's payload: past the 8-byte segment
	// header and the 20-byte record header.
	if _, err := f.WriteAt([]byte{0xFF}, 8+20+2); err != nil {
		t.Fatal(err)
	}
	f.Close()

	p2 := startPrimary(t, walDir)
	defer p2.stop()

	// The primary must refuse rather than skip the hole: a follower that applied
	// records from beyond it would hold a book that never existed anywhere.
	stream, err := p2.repl.StreamWAL(context.Background(), &pb.StreamWALRequest{Symbol: "NABIL"})
	if err == nil {
		_, err = stream.Recv()
	}
	if err == nil {
		t.Fatal("StreamWAL() over a damaged log = nil error, want a refusal")
	}
}

func TestReplica_HandlesAHighVolumeStreamWithRotations(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()

	rep, _ := startFollower(t, p, "")

	const orders = 500
	var lastPos int64
	for i := range orders {
		side := pb.Side_SIDE_BUY
		account := "alice"
		if i%2 == 0 {
			side, account = pb.Side_SIDE_SELL, "bob"
		}
		lastPos = p.place(t, "NABIL", account, side, int64(495+i%11), int64(1+i%20)).GetLogPosition()
	}
	waitForPosition(t, rep, "NABIL", lastPos)

	got, _ := rep.Snapshot("NABIL")
	if want := primarySnapshot(t, p, "NABIL"); got != want {
		t.Errorf("follower diverged under load\n got: %s\nwant: %s", got, want)
	}
	if rep.Position("NABIL") != lastPos {
		t.Errorf("follower position = %d, want %d", rep.Position("NABIL"), lastPos)
	}
}

func TestReplica_ClosingWithoutRunningIsSafe(t *testing.T) {
	rep := New(filepath.Join(t.TempDir(), "f"), slog.New(slog.DiscardHandler))
	if err := rep.Close(); err != nil {
		t.Errorf("Close() on an unused follower = %v, want nil", err)
	}
}

func TestReplica_RejectsAMalformedSymbol(t *testing.T) {
	p := startPrimary(t, "")
	defer p.stop()

	// A follower is not more trusted than a client: the symbol becomes a path, so
	// it goes through the same validation.
	for _, symbol := range []string{"../../etc", "nabil", "NA BIL", ""} {
		stream, err := p.repl.StreamWAL(context.Background(), &pb.StreamWALRequest{Symbol: symbol})
		if err == nil {
			_, err = stream.Recv()
		}
		if err == nil {
			t.Errorf("StreamWAL(%q) = nil error, want a rejection", symbol)
		}
	}
}
