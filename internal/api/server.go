// Package api serves the venue's gRPC surface: it validates what arrives off
// the network, routes each request to the sequencer that owns the symbol, and
// hands executed trades to settlement and to any live market-data subscribers.
package api

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Poudel0/OMS/internal/oms"
	"github.com/Poudel0/OMS/internal/pb"
)

// Limits on what the network may ask for. These are not tuning parameters:
// price and quantity are multiplied together to check affordability, and an
// account ID becomes a map key and a database value, so all three need a
// ceiling that is enforced before any of them is used.
const (
	maxAccountIDLen = 64
	maxPrice        = 1 << 40 // ~1.1e12 ticks, far above any NEPSE instrument
	maxQuantity     = 1 << 32 // ~4.3e9 shares, far above any NEPSE issue size
)

// settleTimeout bounds how long a ledger write may take once the trade has
// already executed. It is deliberately generous: the alternative to waiting is
// an unsettled trade, which costs an operator far more than a slow response
// costs a client.
const settleTimeout = 30 * time.Second

// Ledger records executed trades durably and idempotently. The gRPC server
// holds one, or nil.
//
// It is an interface for a reason that is about to be real rather than
// speculative: the Postgres implementation needs a database, and CI has none,
// so the server has to be constructible and testable without one. A nil Ledger
// means trades are matched and logged but never settled to a durable journal —
// correct for tests and for the ADR-002 benchmark configuration, and wrong
// anywhere money is at stake.
type Ledger interface {
	// Settle posts each trade's double-entry rows. It must be idempotent on
	// trade identity, because the write-ahead log can replay a trade that was
	// already settled.
	Settle(ctx context.Context, trades []oms.Trade) error
}

// Server implements the OrderService gRPC API over a symbol registry.
type Server struct {
	pb.UnimplementedOrderServiceServer

	reg      *oms.Registry
	accounts *oms.Accounts
	ledger   Ledger
	log      *slog.Logger

	mu    sync.Mutex
	feeds map[string]*tradeFeed
}

// NewServer wires a registry, an account store, and an optional ledger into a
// gRPC service. accounts must be non-nil; ledger may be nil (see Ledger).
//
// It installs itself as the registry's trade callback, so the registry must not
// have been used yet.
func NewServer(reg *oms.Registry, accounts *oms.Accounts, ledger Ledger, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		reg:      reg,
		accounts: accounts,
		ledger:   ledger,
		log:      log,
		feeds:    make(map[string]*tradeFeed),
	}
	reg.OnTrades = s.onTrades
	return s
}

// onTrades runs on the sequencer goroutine for the symbol that produced the
// trades, in matching order. Everything it does must be non-blocking, which is
// why the durable ledger write does not happen here: a database round trip on
// this path would put an unbounded stall between the matcher and its next order.
//
// In-memory balances are *not* updated here. The sequencer does that itself, in
// the same atomic step that reconciles the order's reservation — splitting the
// two would leave a window where `owned - reserved` was wrong, which is the
// whole class of bug reservations exist to close.
func (s *Server) onTrades(symbol string, trades []oms.Trade) {
	s.feed(symbol).publish(symbol, trades)
}

func (s *Server) feed(symbol string) *tradeFeed {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.feeds[symbol]
	if !ok {
		f = newTradeFeed()
		s.feeds[symbol] = f
	}
	return f
}

// PlaceOrder validates, checks affordability, routes to the symbol's sequencer,
// and settles.
func (s *Server) PlaceOrder(ctx context.Context, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
	order, err := orderFromRequest(req)
	if err != nil {
		return nil, err
	}

	seq, err := s.reg.Get(req.GetSymbol())
	if err != nil {
		return nil, registryError(err)
	}

	// The inline pre-trade check happens inside the sequencer, not here. It has
	// to: checking here and matching there leaves a gap in which a second order
	// from the same account can pass the same check, and a *resting* order has
	// spent nothing yet, so no amount of serialising the checks would help. The
	// sequencer does an atomic check-and-reserve against
	// `owned - already committed`, before the order is logged — see
	// oms.Accounts. Its rejection arrives here as a Submit error.
	resp, err := seq.Submit(ctx, order)
	if err != nil {
		return nil, submitError(err)
	}

	// Durable settlement happens off the matcher's critical path, unlike the
	// in-memory balance update in onTrades. The trade is already durable in the
	// write-ahead log by now, so if this fails the execution is not lost — it is
	// unsettled, and replay will re-present it. That is exactly why Settle has
	// to be idempotent.
	//
	// It runs on a context stripped of the caller's cancellation. The trade has
	// already executed and cannot be undone, so a client that hangs up must not
	// be able to abandon its settlement — doing so would leave the journal
	// permanently disagreeing with the book. A load test exposed exactly this:
	// every in-flight order at shutdown logged "trades are durable but
	// unsettled" because settlement inherited the cancelled request context.
	// The timeout is its own, so a wedged database still cannot pin the handler
	// open forever.
	if s.ledger != nil && len(resp.Trades) > 0 {
		settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
		defer cancel()
		if err := s.ledger.Settle(settleCtx, resp.Trades); err != nil {
			s.log.ErrorContext(ctx, "settlement failed; trades are durable but unsettled",
				"symbol", req.GetSymbol(), "order_id", resp.OrderID, "trades", len(resp.Trades), "err", err)
			return nil, status.Errorf(codes.Internal, "settlement failed for order %d", resp.OrderID)
		}
	}

	return &pb.PlaceOrderResponse{
		OrderId:               int64(resp.OrderID),
		LogPosition:           resp.Seq,
		Trades:                tradesToPB(resp.Trades),
		RestingQuantity:       resp.RestingQuantity,
		SelfPreventedOrderIds: seqIDsToInt64(resp.SelfPrevented),
	}, nil
}

// CancelOrder removes a resting order, refusing if it belongs to another
// account.
func (s *Server) CancelOrder(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	if err := validateSymbol(req.GetSymbol()); err != nil {
		return nil, err
	}
	account, err := validateAccountID(req.GetAccountId())
	if err != nil {
		return nil, err
	}
	if req.GetOrderId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "order_id must be positive")
	}

	seq, err := s.reg.Get(req.GetSymbol())
	if err != nil {
		return nil, registryError(err)
	}

	pos, err := seq.CancelFor(ctx, oms.SeqID(req.GetOrderId()), account)
	if err != nil {
		return nil, cancelError(err)
	}
	return &pb.CancelOrderResponse{LogPosition: pos}, nil
}

// StreamTrades pushes a symbol's trades to the client until the client goes
// away or the server shuts down. It starts from the moment of subscription:
// this is a live feed, not a replayable history.
func (s *Server) StreamTrades(req *pb.StreamTradesRequest, stream pb.OrderService_StreamTradesServer) error {
	if err := validateSymbol(req.GetSymbol()); err != nil {
		return err
	}
	// Subscribe before creating the sequencer, so that trades from the very
	// first order in a symbol cannot slip through between the two.
	id, ch := s.feed(req.GetSymbol()).subscribe()
	defer s.feed(req.GetSymbol()).unsubscribe(id)

	if _, err := s.reg.Get(req.GetSymbol()); err != nil {
		return registryError(err)
	}

	ctx := stream.Context()
	for {
		select {
		case batch := <-ch:
			for _, t := range batch.trades {
				if err := stream.Send(tradeToPB(t)); err != nil {
					return err // client hung up or the stream broke
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// orderFromRequest validates every field that arrived over the network and
// converts it. Nothing downstream re-checks these, so this is the only place
// that stands between a client and the matcher.
func orderFromRequest(req *pb.PlaceOrderRequest) (oms.Order, error) {
	if err := validateSymbol(req.GetSymbol()); err != nil {
		return oms.Order{}, err
	}
	account, err := validateAccountID(req.GetAccountId())
	if err != nil {
		return oms.Order{}, err
	}

	var side oms.OrderSide
	switch req.GetSide() {
	case pb.Side_SIDE_BUY:
		side = oms.Buy
	case pb.Side_SIDE_SELL:
		side = oms.Sell
	default:
		return oms.Order{}, status.Error(codes.InvalidArgument, "side must be SIDE_BUY or SIDE_SELL")
	}

	var orderType oms.OrderType
	switch req.GetType() {
	case pb.OrderType_ORDER_TYPE_LIMIT:
		orderType = oms.Limit
	case pb.OrderType_ORDER_TYPE_MARKET:
		orderType = oms.Market
	default:
		return oms.Order{}, status.Error(codes.InvalidArgument, "type must be ORDER_TYPE_LIMIT or ORDER_TYPE_MARKET")
	}

	qty := req.GetQuantity()
	if qty <= 0 || qty > maxQuantity {
		return oms.Order{}, status.Errorf(codes.InvalidArgument, "quantity must be in 1..%d", maxQuantity)
	}

	price := req.GetPrice()
	switch orderType {
	case oms.Limit:
		if price <= 0 || price > maxPrice {
			return oms.Order{}, status.Errorf(codes.InvalidArgument, "price must be in 1..%d for a limit order", maxPrice)
		}
	case oms.Market:
		// The book ignores a market order's price. Refusing a non-zero one
		// rather than silently dropping it means a client that thought it was
		// setting a limit finds out here instead of after it has been filled at
		// any price available.
		if price != 0 {
			return oms.Order{}, status.Error(codes.InvalidArgument, "price must be 0 for a market order")
		}
	}

	return oms.Order{
		// SeqID is deliberately left zero: the sequencer assigns order IDs.
		// A client-chosen ID could name an order another account holds.
		Symbol:   req.GetSymbol(),
		Placer:   account,
		Type:     orderType,
		Price:    oms.Price(price),
		Quantity: qty,
		Side:     side,
	}, nil
}

func validateSymbol(symbol string) error {
	if symbol == "" {
		return status.Error(codes.InvalidArgument, "symbol is required")
	}
	// The registry is the authority on symbol format, since it is the thing
	// that turns a symbol into a directory name. Checking emptiness here just
	// produces a clearer message for the common mistake.
	return nil
}

func validateAccountID(account string) (string, error) {
	if account == "" {
		return "", status.Error(codes.InvalidArgument, "account_id is required")
	}
	if len(account) > maxAccountIDLen {
		return "", status.Errorf(codes.InvalidArgument, "account_id must be at most %d bytes", maxAccountIDLen)
	}
	for i := 0; i < len(account); i++ {
		// Printable ASCII only. An account ID reaches log lines, map keys, and
		// database rows; control characters and invalid UTF-8 have no business
		// in any of those.
		if c := account[i]; c < 0x20 || c > 0x7e {
			return "", status.Error(codes.InvalidArgument, "account_id must be printable ASCII")
		}
	}
	return account, nil
}

// seqIDsToInt64 converts order IDs for the wire.
func seqIDsToInt64(ids []oms.SeqID) []int64 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		out = append(out, int64(id))
	}
	return out
}

func registryError(err error) error {
	switch {
	case errors.Is(err, oms.ErrInvalidSymbol):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, oms.ErrTooManySymbols):
		return status.Errorf(codes.ResourceExhausted, "%v", err)
	default:
		return status.Errorf(codes.Internal, "symbol unavailable: %v", err)
	}
}

func submitError(err error) error {
	switch {
	// The pre-trade check runs inside the sequencer, so its rejections arrive
	// as Submit errors and have to be mapped to client-meaningful codes here
	// rather than being flattened into Internal.
	case errors.Is(err, oms.ErrUnknownAccount):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, oms.ErrInsufficientFunds), errors.Is(err, oms.ErrInsufficientShares):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	case errors.Is(err, oms.ErrSequencerClosed):
		return status.Error(codes.Unavailable, "venue is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		// A write-ahead log failure lands here. It is not the client's fault
		// and retrying will not help until an operator intervenes.
		return status.Errorf(codes.Internal, "order not accepted: %v", err)
	}
}

func cancelError(err error) error {
	switch {
	case errors.Is(err, oms.ErrNotOrderOwner):
		return status.Error(codes.PermissionDenied, "order belongs to another account")
	case errors.Is(err, oms.ErrSequencerClosed):
		return status.Error(codes.Unavailable, "venue is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		// The book cannot tell an unknown ID from one that already filled or
		// was already cancelled, and neither can this message.
		return status.Error(codes.NotFound, "no such resting order")
	}
}

func tradesToPB(trades []oms.Trade) []*pb.Trade {
	if len(trades) == 0 {
		return nil
	}
	out := make([]*pb.Trade, 0, len(trades))
	for _, t := range trades {
		out = append(out, tradeToPB(t))
	}
	return out
}

func tradeToPB(t oms.Trade) *pb.Trade {
	side := pb.Side_SIDE_UNSPECIFIED
	switch t.TakerSide {
	case oms.Buy:
		side = pb.Side_SIDE_BUY
	case oms.Sell:
		side = pb.Side_SIDE_SELL
	}
	return &pb.Trade{
		TradeId:            int64(t.SeqID),
		Symbol:             t.Symbol,
		Price:              int64(t.Price),
		Quantity:           t.Quantity,
		MakerOrderId:       int64(t.MakerSeqID),
		TakerOrderId:       int64(t.TakerSeqID),
		MakerAccountId:     t.MakerAccID,
		TakerAccountId:     t.TakerAccID,
		ExecutedAtUnixNano: t.TimeStamp.UnixNano(),
		TakerSide:          side,
	}
}
