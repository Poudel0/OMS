package api

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Poudel0/OMS/internal/oms"
	"github.com/Poudel0/OMS/internal/pb"
)

// newTestVenue starts a real gRPC server over an in-process listener and
// returns a connected client. bufconn rather than a TCP port: the request still
// goes through marshalling, the gRPC stack, and streaming, so this exercises
// the actual API surface, but nothing binds a port and tests can run in
// parallel.
//
// The registry is given a real WAL directory, so these tests cover the whole
// path the sprint's "gRPC client → handler → sequencer → matcher → trade" test
// was meant to cover, durability included.
func newTestVenue(t *testing.T, ledger Ledger) (pb.OrderServiceClient, *oms.Accounts) {
	t.Helper()

	reg := oms.NewRegistry(t.Context(), t.TempDir())
	accounts := oms.NewAccounts()
	srv := NewServer(reg, accounts, ledger, slog.New(slog.DiscardHandler))

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterOrderServiceServer(gs, srv)
	go func() {
		if err := gs.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Serve() error = %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
		gs.Stop()
		lis.Close()
		reg.Close()
	})
	return pb.NewOrderServiceClient(conn), accounts
}

func limitOrder(symbol, account string, side pb.Side, price, qty int64) *pb.PlaceOrderRequest {
	return &pb.PlaceOrderRequest{
		Symbol: symbol, AccountId: account, Side: side,
		Type: pb.OrderType_ORDER_TYPE_LIMIT, Price: price, Quantity: qty,
	}
}

// wantCode asserts the gRPC status code, since the code is the part clients
// actually branch on.
func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("status = %v (%v), want %v", status.Code(err), err, want)
	}
}

func TestPlaceOrder_EndToEndProducesATrade(t *testing.T) {
	ctx := t.Context()
	client, accounts := newTestVenue(t, nil)

	accounts.Deposit("buyer", 1_000_000)
	accounts.SetPosition("seller", "NABIL", 1_000)

	sell, err := client.PlaceOrder(ctx, limitOrder("NABIL", "seller", pb.Side_SIDE_SELL, 500, 100))
	if err != nil {
		t.Fatalf("PlaceOrder(sell) error = %v", err)
	}
	if sell.GetOrderId() == 0 {
		t.Error("OrderId = 0, want a venue-assigned id")
	}
	if len(sell.GetTrades()) != 0 {
		t.Errorf("Trades = %v, want none (nothing to cross)", sell.GetTrades())
	}
	if sell.GetRestingQuantity() != 100 {
		t.Errorf("RestingQuantity = %d, want 100", sell.GetRestingQuantity())
	}

	buy, err := client.PlaceOrder(ctx, limitOrder("NABIL", "buyer", pb.Side_SIDE_BUY, 500, 60))
	if err != nil {
		t.Fatalf("PlaceOrder(buy) error = %v", err)
	}
	trades := buy.GetTrades()
	if len(trades) != 1 {
		t.Fatalf("Trades = %v, want exactly one", trades)
	}
	tr := trades[0]
	if tr.GetTradeId() == 0 {
		t.Error("TradeId = 0, want a real id — settlement is idempotent on it")
	}
	if tr.GetPrice() != 500 || tr.GetQuantity() != 60 {
		t.Errorf("trade = price %d qty %d, want 500/60", tr.GetPrice(), tr.GetQuantity())
	}
	if tr.GetTakerSide() != pb.Side_SIDE_BUY {
		t.Errorf("TakerSide = %v, want SIDE_BUY", tr.GetTakerSide())
	}
	if tr.GetMakerAccountId() != "seller" || tr.GetTakerAccountId() != "buyer" {
		t.Errorf("accounts = maker %q taker %q, want seller/buyer", tr.GetMakerAccountId(), tr.GetTakerAccountId())
	}
	if buy.GetRestingQuantity() != 0 {
		t.Errorf("RestingQuantity = %d, want 0 (fully filled)", buy.GetRestingQuantity())
	}
	if buy.GetLogPosition() != 2 {
		t.Errorf("LogPosition = %d, want 2", buy.GetLogPosition())
	}

	// Balances moved: 60 shares at 500 = 30,000.
	if got := accounts.Cash("buyer"); got != 1_000_000-30_000 {
		t.Errorf("buyer cash = %d, want %d", got, 1_000_000-30_000)
	}
	if got := accounts.Cash("seller"); got != 30_000 {
		t.Errorf("seller cash = %d, want 30000", got)
	}
	if got := accounts.Position("buyer", "NABIL"); got != 60 {
		t.Errorf("buyer position = %d, want 60", got)
	}
	if got := accounts.Position("seller", "NABIL"); got != 940 {
		t.Errorf("seller position = %d, want 940", got)
	}
}

func TestPlaceOrder_RejectsInvalidRequests(t *testing.T) {
	ctx := t.Context()
	client, accounts := newTestVenue(t, nil)
	accounts.Deposit("acc", 1_000_000)
	accounts.SetPosition("acc", "NABIL", 1_000)

	tests := []struct {
		name string
		req  *pb.PlaceOrderRequest
		want codes.Code
	}{
		{"empty symbol", &pb.PlaceOrderRequest{AccountId: "acc", Side: pb.Side_SIDE_BUY, Type: pb.OrderType_ORDER_TYPE_LIMIT, Price: 1, Quantity: 1}, codes.InvalidArgument},
		{"path traversal symbol", limitOrder("../../etc", "acc", pb.Side_SIDE_BUY, 1, 1), codes.InvalidArgument},
		{"lowercase symbol", limitOrder("nabil", "acc", pb.Side_SIDE_BUY, 1, 1), codes.InvalidArgument},
		{"empty account", limitOrder("NABIL", "", pb.Side_SIDE_BUY, 1, 1), codes.InvalidArgument},
		{"unprintable account", limitOrder("NABIL", "a\x00b", pb.Side_SIDE_BUY, 1, 1), codes.InvalidArgument},
		{"unspecified side", &pb.PlaceOrderRequest{Symbol: "NABIL", AccountId: "acc", Type: pb.OrderType_ORDER_TYPE_LIMIT, Price: 1, Quantity: 1}, codes.InvalidArgument},
		{"unspecified type", &pb.PlaceOrderRequest{Symbol: "NABIL", AccountId: "acc", Side: pb.Side_SIDE_BUY, Price: 1, Quantity: 1}, codes.InvalidArgument},
		{"zero quantity", limitOrder("NABIL", "acc", pb.Side_SIDE_BUY, 500, 0), codes.InvalidArgument},
		{"negative quantity", limitOrder("NABIL", "acc", pb.Side_SIDE_BUY, 500, -5), codes.InvalidArgument},
		{"absurd quantity", limitOrder("NABIL", "acc", pb.Side_SIDE_BUY, 500, maxQuantity+1), codes.InvalidArgument},
		{"zero price on limit", limitOrder("NABIL", "acc", pb.Side_SIDE_BUY, 0, 10), codes.InvalidArgument},
		{"negative price", limitOrder("NABIL", "acc", pb.Side_SIDE_BUY, -500, 10), codes.InvalidArgument},
		{"absurd price", limitOrder("NABIL", "acc", pb.Side_SIDE_BUY, maxPrice+1, 10), codes.InvalidArgument},
		{"price set on market order", &pb.PlaceOrderRequest{
			Symbol: "NABIL", AccountId: "acc", Side: pb.Side_SIDE_BUY,
			Type: pb.OrderType_ORDER_TYPE_MARKET, Price: 500, Quantity: 10,
		}, codes.InvalidArgument},
		{"unknown account", limitOrder("NABIL", "ghost", pb.Side_SIDE_BUY, 500, 10), codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.PlaceOrder(ctx, tc.req)
			if err == nil {
				t.Fatalf("PlaceOrder(%s) = nil error, want %v", tc.name, tc.want)
			}
			wantCode(t, err, tc.want)
		})
	}
}

func TestPlaceOrder_EnforcesTheBalanceCheck(t *testing.T) {
	ctx := t.Context()
	client, accounts := newTestVenue(t, nil)

	accounts.Deposit("poor", 100)
	accounts.SetPosition("empty", "NABIL", 5)

	// 10 shares at 500 costs 5000; the account has 100.
	_, err := client.PlaceOrder(ctx, limitOrder("NABIL", "poor", pb.Side_SIDE_BUY, 500, 10))
	wantCode(t, err, codes.FailedPrecondition)

	// Selling more than is held.
	_, err = client.PlaceOrder(ctx, limitOrder("NABIL", "empty", pb.Side_SIDE_SELL, 500, 10))
	wantCode(t, err, codes.FailedPrecondition)

	// A rejected order must not have reached the book at all.
	accounts.Deposit("rich", 1_000_000)
	resp, err := client.PlaceOrder(ctx, limitOrder("NABIL", "rich", pb.Side_SIDE_BUY, 500, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTrades()) != 0 {
		t.Errorf("Trades = %v, want none — a rejected sell rested anyway", resp.GetTrades())
	}
}

func TestCancelOrder_OwnershipIsEnforced(t *testing.T) {
	ctx := t.Context()
	client, accounts := newTestVenue(t, nil)
	accounts.Deposit("alice", 1_000_000)
	accounts.Deposit("bob", 1_000_000)

	placed, err := client.PlaceOrder(ctx, limitOrder("NABIL", "alice", pb.Side_SIDE_BUY, 500, 10))
	if err != nil {
		t.Fatal(err)
	}

	// Order IDs are sequential, so guessing one is trivial. Without the
	// ownership check this would succeed.
	_, err = client.CancelOrder(ctx, &pb.CancelOrderRequest{
		Symbol: "NABIL", OrderId: placed.GetOrderId(), AccountId: "bob",
	})
	wantCode(t, err, codes.PermissionDenied)

	if _, err := client.CancelOrder(ctx, &pb.CancelOrderRequest{
		Symbol: "NABIL", OrderId: placed.GetOrderId(), AccountId: "alice",
	}); err != nil {
		t.Fatalf("CancelOrder() by the owner = %v, want nil", err)
	}

	// Cancelling twice fails: the book cannot distinguish an already-cancelled
	// order from one that never existed.
	_, err = client.CancelOrder(ctx, &pb.CancelOrderRequest{
		Symbol: "NABIL", OrderId: placed.GetOrderId(), AccountId: "alice",
	})
	wantCode(t, err, codes.NotFound)
}

func TestCancelOrder_RejectsInvalidRequests(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestVenue(t, nil)

	for _, tc := range []struct {
		name string
		req  *pb.CancelOrderRequest
		want codes.Code
	}{
		{"no symbol", &pb.CancelOrderRequest{OrderId: 1, AccountId: "a"}, codes.InvalidArgument},
		{"no account", &pb.CancelOrderRequest{Symbol: "NABIL", OrderId: 1}, codes.InvalidArgument},
		{"zero order id", &pb.CancelOrderRequest{Symbol: "NABIL", OrderId: 0, AccountId: "a"}, codes.InvalidArgument},
		{"negative order id", &pb.CancelOrderRequest{Symbol: "NABIL", OrderId: -1, AccountId: "a"}, codes.InvalidArgument},
		{"bad symbol", &pb.CancelOrderRequest{Symbol: "../x", OrderId: 1, AccountId: "a"}, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.CancelOrder(ctx, tc.req)
			if err == nil {
				t.Fatalf("CancelOrder(%s) = nil error, want %v", tc.name, tc.want)
			}
			wantCode(t, err, tc.want)
		})
	}
}

func TestStreamTrades_DeliversTradesInMatchOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client, accounts := newTestVenue(t, nil)

	accounts.Deposit("buyer", 10_000_000)
	accounts.SetPosition("seller", "NABIL", 10_000)

	stream, err := client.StreamTrades(ctx, &pb.StreamTradesRequest{Symbol: "NABIL"})
	if err != nil {
		t.Fatalf("StreamTrades() error = %v", err)
	}

	const n = 20
	received := make(chan *pb.Trade, n)
	var recvErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range n {
			tr, err := stream.Recv()
			if err != nil {
				recvErr = err
				return
			}
			received <- tr
		}
	}()

	// Rest the sell side first, then cross it one order at a time so the trade
	// sequence is fully determined.
	for i := range n {
		if _, err := client.PlaceOrder(ctx, limitOrder("NABIL", "seller", pb.Side_SIDE_SELL, int64(500+i), 10)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		if _, err := client.PlaceOrder(ctx, limitOrder("NABIL", "buyer", pb.Side_SIDE_BUY, int64(500+i), 10)); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for streamed trades")
	}
	if recvErr != nil {
		t.Fatalf("Recv() error = %v", recvErr)
	}

	// Trades are published from the sequencer goroutine, so their ids must
	// arrive strictly increasing. Publishing from the handler instead would let
	// concurrent calls interleave and break this.
	close(received)
	var last int64
	count := 0
	for tr := range received {
		if tr.GetTradeId() <= last {
			t.Errorf("trade id %d arrived after %d — the feed reordered trades", tr.GetTradeId(), last)
		}
		last = tr.GetTradeId()
		count++
	}
	if count != n {
		t.Errorf("received %d trades, want %d", count, n)
	}
}

func TestStreamTrades_EndsWhenTheClientGoesAway(t *testing.T) {
	client, _ := newTestVenue(t, nil)

	ctx, cancel := context.WithCancel(t.Context())
	stream, err := client.StreamTrades(ctx, &pb.StreamTradesRequest{Symbol: "NABIL"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	// The server must return rather than leaking a goroutine parked on a feed
	// nobody will ever read.
	if _, err := stream.Recv(); err == nil {
		t.Fatal("Recv() after cancel = nil error, want a cancellation error")
	}
}

func TestStreamTrades_RejectsBadSymbol(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestVenue(t, nil)

	stream, err := client.StreamTrades(ctx, &pb.StreamTradesRequest{Symbol: "../../etc"})
	if err == nil {
		_, err = stream.Recv() // the status arrives on first Recv
	}
	wantCode(t, err, codes.InvalidArgument)
}

func TestPlaceOrder_MarketOrderCrossesMultipleLevels(t *testing.T) {
	ctx := t.Context()
	client, accounts := newTestVenue(t, nil)
	accounts.Deposit("buyer", 10_000_000)
	accounts.SetPosition("seller", "NABIL", 1_000)

	for _, price := range []int64{500, 501, 502} {
		if _, err := client.PlaceOrder(ctx, limitOrder("NABIL", "seller", pb.Side_SIDE_SELL, price, 10)); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := client.PlaceOrder(ctx, &pb.PlaceOrderRequest{
		Symbol: "NABIL", AccountId: "buyer", Side: pb.Side_SIDE_BUY,
		Type: pb.OrderType_ORDER_TYPE_MARKET, Quantity: 25,
	})
	if err != nil {
		t.Fatalf("PlaceOrder(market) error = %v", err)
	}
	trades := resp.GetTrades()
	if len(trades) != 3 {
		t.Fatalf("trades = %d, want 3 (walked three price levels)", len(trades))
	}
	// Cheapest first, and each trade at the maker's price.
	for i, want := range []int64{500, 501, 502} {
		if trades[i].GetPrice() != want {
			t.Errorf("trade %d price = %d, want %d", i, trades[i].GetPrice(), want)
		}
	}
	if got := trades[2].GetQuantity(); got != 5 {
		t.Errorf("last fill = %d, want 5 (25 - 10 - 10)", got)
	}
	// A market order's unfilled remainder is dropped, never rested.
	if resp.GetRestingQuantity() != 0 {
		t.Errorf("RestingQuantity = %d, want 0", resp.GetRestingQuantity())
	}
}

func TestPlaceOrder_SymbolsAreIsolated(t *testing.T) {
	ctx := t.Context()
	client, accounts := newTestVenue(t, nil)
	accounts.Deposit("buyer", 10_000_000)
	accounts.SetPosition("seller", "ADBL", 1_000)

	if _, err := client.PlaceOrder(ctx, limitOrder("ADBL", "seller", pb.Side_SIDE_SELL, 500, 10)); err != nil {
		t.Fatal(err)
	}
	resp, err := client.PlaceOrder(ctx, limitOrder("NABIL", "buyer", pb.Side_SIDE_BUY, 500, 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTrades()) != 0 {
		t.Errorf("a NABIL buy matched an ADBL sell: %v", resp.GetTrades())
	}
	// Each symbol keeps its own log, so both are at position 1.
	if resp.GetLogPosition() != 1 {
		t.Errorf("NABIL LogPosition = %d, want 1 (positions are per-symbol)", resp.GetLogPosition())
	}
}

// recordingLedger captures what settlement was asked to persist.
type recordingLedger struct {
	mu     sync.Mutex
	trades []oms.Trade
	err    error
}

func (l *recordingLedger) Settle(_ context.Context, trades []oms.Trade) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	l.trades = append(l.trades, trades...)
	return nil
}

func (l *recordingLedger) settled() []oms.Trade {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]oms.Trade(nil), l.trades...)
}

func TestPlaceOrder_SettlesEveryTrade(t *testing.T) {
	ctx := t.Context()
	ledger := &recordingLedger{}
	client, accounts := newTestVenue(t, ledger)
	accounts.Deposit("buyer", 10_000_000)
	accounts.SetPosition("seller", "NABIL", 1_000)

	if _, err := client.PlaceOrder(ctx, limitOrder("NABIL", "seller", pb.Side_SIDE_SELL, 500, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PlaceOrder(ctx, limitOrder("NABIL", "buyer", pb.Side_SIDE_BUY, 500, 10)); err != nil {
		t.Fatal(err)
	}

	settled := ledger.settled()
	if len(settled) != 1 {
		t.Fatalf("settled %d trades, want 1", len(settled))
	}
	if settled[0].Buyer() != "buyer" || settled[0].Seller() != "seller" {
		t.Errorf("settled trade buyer/seller = %q/%q, want buyer/seller", settled[0].Buyer(), settled[0].Seller())
	}
	if settled[0].SeqID == 0 {
		t.Error("settled trade has SeqID 0 — idempotency key is missing")
	}
}

func TestPlaceOrder_SettlementFailureIsReportedNotSwallowed(t *testing.T) {
	ctx := t.Context()
	ledger := &recordingLedger{err: errors.New("database is down")}
	client, accounts := newTestVenue(t, ledger)
	accounts.Deposit("buyer", 10_000_000)
	accounts.SetPosition("seller", "NABIL", 1_000)

	if _, err := client.PlaceOrder(ctx, limitOrder("NABIL", "seller", pb.Side_SIDE_SELL, 500, 10)); err != nil {
		t.Fatal(err)
	}
	// The trade is already durable in the write-ahead log, so this is not a
	// lost execution — it is an unsettled one, and the client has to be told.
	_, err := client.PlaceOrder(ctx, limitOrder("NABIL", "buyer", pb.Side_SIDE_BUY, 500, 10))
	wantCode(t, err, codes.Internal)
}

// gateLedger parks inside Settle until the test releases it, so the test can
// cancel the client's request at a precisely known moment: after settlement has
// begun. It then records what the context looked like on the other side of that
// cancellation.
type gateLedger struct {
	entered chan struct{}
	release chan struct{}

	mu           sync.Mutex
	errOnRelease error
	settled      int
}

func newGateLedger() *gateLedger {
	return &gateLedger{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (l *gateLedger) Settle(ctx context.Context, trades []oms.Trade) error {
	l.entered <- struct{}{}
	<-l.release // the test cancels the client's context while we sit here

	l.mu.Lock()
	defer l.mu.Unlock()
	l.errOnRelease = ctx.Err()
	l.settled += len(trades)
	return nil
}

func TestPlaceOrder_SettlementSurvivesClientCancellation(t *testing.T) {
	ledger := newGateLedger()
	client, accounts := newTestVenue(t, ledger)
	accounts.Deposit("buyer", 10_000_000)
	accounts.SetPosition("seller", "NABIL", 1_000)

	if _, err := client.PlaceOrder(t.Context(), limitOrder("NABIL", "seller", pb.Side_SIDE_SELL, 500, 10)); err != nil {
		t.Fatal(err)
	}

	// A trade cannot be undone, so a client that hangs up must not be able to
	// abandon its settlement. Before this was fixed, settlement inherited the
	// request context: a load test left every in-flight order at shutdown
	// "durable but unsettled", and the journal would have disagreed with the
	// book permanently.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		// The client's own result is irrelevant — it may never receive one.
		// What must hold is that the trade it caused still gets settled.
		_, _ = client.PlaceOrder(ctx, limitOrder("NABIL", "buyer", pb.Side_SIDE_BUY, 500, 10))
	})

	// Settlement has started, so the trade has definitely executed. Only now
	// does the client disappear.
	select {
	case <-ledger.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("settlement never started")
	}
	cancel()
	close(ledger.release)
	wg.Wait()

	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.errOnRelease != nil {
		t.Errorf("settlement context was cancelled mid-write (%v) — a client hangup can abandon settlement",
			ledger.errOnRelease)
	}
	if ledger.settled != 1 {
		t.Errorf("settled %d trades, want 1", ledger.settled)
	}
}

func TestPlaceOrder_ConcurrentClientsAcrossSymbols(t *testing.T) {
	ctx := t.Context()
	client, accounts := newTestVenue(t, nil)

	symbols := []string{"NABIL", "ADBL", "HBL", "NRIC"}
	const perSymbol = 100
	for _, s := range symbols {
		accounts.SetPosition("seller", s, 1_000_000)
	}
	accounts.Deposit("seller", 1)
	accounts.Deposit("buyer", 1_000_000_000)

	var wg sync.WaitGroup
	for _, symbol := range symbols {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			for i := range perSymbol {
				side := pb.Side_SIDE_BUY
				account := "buyer"
				if i%2 == 0 {
					side, account = pb.Side_SIDE_SELL, "seller"
				}
				if _, err := client.PlaceOrder(ctx, limitOrder(symbol, account, side, int64(495+i%10), 10)); err != nil {
					t.Errorf("PlaceOrder(%s) error = %v", symbol, err)
					return
				}
			}
		}(symbol)
	}
	wg.Wait()

	// Each symbol ran on its own sequencer and its own log, so each should have
	// exactly perSymbol positions — no cross-symbol sharing of either.
	for _, symbol := range symbols {
		resp, err := client.PlaceOrder(ctx, limitOrder(symbol, "buyer", pb.Side_SIDE_BUY, 400, 1))
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetLogPosition() != perSymbol+1 {
			t.Errorf("%s LogPosition = %d, want %d", symbol, resp.GetLogPosition(), perSymbol+1)
		}
	}
}
