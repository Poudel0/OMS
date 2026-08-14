package api

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Poudel0/OMS/internal/oms"
	"github.com/Poudel0/OMS/internal/pb"
)

// walBatchCap bounds how many records one StreamWAL message carries. It keeps a
// far-behind follower's initial catch-up from building one enormous message,
// without making a caught-up stream chatty — a caught-up stream sends whatever
// arrived, which is usually a handful.
const walBatchCap = 512

// ReplicationServer serves the primary's write-ahead log to followers.
//
// It reads the log **files** rather than hooking the sequencer, which is the
// central design choice and the reason replication is safe to expose: a
// follower, no matter how slow or how far behind, cannot apply backpressure to
// the matching path, because nothing on the matching path waits for it. See
// oms.Tailer and ADR-006.
type ReplicationServer struct {
	pb.UnimplementedReplicationServiceServer

	walDir string
	reg    *oms.Registry
	log    *slog.Logger

	mu       sync.Mutex
	progress map[string]followerProgress
}

// followerProgress is the last thing a follower told us about a symbol. It is
// advisory: the primary streams whether or not anyone reports.
type followerProgress struct {
	position   int64
	recordedAt time.Time
	reportedAt time.Time
}

// NewReplicationServer serves the logs under walDir. reg is consulted for which
// symbols exist in memory; the directory is the authority for which have logs.
func NewReplicationServer(walDir string, reg *oms.Registry, log *slog.Logger) *ReplicationServer {
	if log == nil {
		log = slog.Default()
	}
	return &ReplicationServer{
		walDir:   walDir,
		reg:      reg,
		log:      log,
		progress: make(map[string]followerProgress),
	}
}

// ListSymbols reports which symbols have logs, so a follower needs no configured
// symbol list.
//
// The filesystem is the authority rather than the registry: a symbol whose log
// exists but which this process has not yet lazily loaded still has records a
// follower must replicate.
func (s *ReplicationServer) ListSymbols(ctx context.Context, _ *pb.ListSymbolsRequest) (*pb.ListSymbolsResponse, error) {
	if s.walDir == "" {
		return nil, status.Error(codes.FailedPrecondition, "this node has no write-ahead log, so it cannot be replicated")
	}
	entries, err := filepath.Glob(filepath.Join(s.walDir, "*"))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list symbols: %v", err)
	}
	symbols := make([]string, 0, len(entries))
	for _, path := range entries {
		symbol := filepath.Base(path)
		// Anything that is not a legitimate symbol directory is not ours.
		if _, err := s.reg.Get(symbol); err != nil {
			continue
		}
		symbols = append(symbols, symbol)
	}
	return &pb.ListSymbolsResponse{Symbols: symbols}, nil
}

// StreamWAL streams one symbol's records from after_position onward and keeps
// streaming as new records are appended, until the follower goes away.
func (s *ReplicationServer) StreamWAL(req *pb.StreamWALRequest, stream pb.ReplicationService_StreamWALServer) error {
	if s.walDir == "" {
		return status.Error(codes.FailedPrecondition, "this node has no write-ahead log, so it cannot be replicated")
	}
	symbol := req.GetSymbol()
	// Route through the registry so the symbol is validated exactly as an order
	// would be: it becomes a path below, and a follower is not more trusted than
	// a client.
	if _, err := s.reg.Get(symbol); err != nil {
		return registryError(err)
	}
	if req.GetAfterPosition() < 0 {
		return status.Error(codes.InvalidArgument, "after_position must not be negative")
	}

	ctx := stream.Context()
	tail := oms.NewTailer(filepath.Join(s.walDir, symbol), req.GetAfterPosition())

	for {
		records, err := tail.Next(ctx, walBatchCap)
		if err != nil {
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				return nil // the follower disconnected; that is not a failure
			case errors.Is(err, oms.ErrCorruptWAL):
				// Refuse to stream past a hole. A follower that applied records
				// after a damaged one would hold a book that never existed.
				s.log.ErrorContext(ctx, "refusing to replicate past a damaged log",
					"symbol", symbol, "err", err)
				return status.Errorf(codes.DataLoss, "log for %s is damaged: %v", symbol, err)
			default:
				return status.Errorf(codes.Internal, "read log for %s: %v", symbol, err)
			}
		}

		batch := &pb.WALBatch{
			Records:             make([]*pb.WALRecord, 0, len(records)),
			PrimaryLastPosition: tail.AfterSeq(),
		}
		for _, rec := range records {
			batch.Records = append(batch.Records, recordToPB(rec))
		}
		if err := stream.Send(batch); err != nil {
			return err // follower hung up mid-send
		}
	}
}

// ReportProgress records how far a follower has applied.
func (s *ReplicationServer) ReportProgress(ctx context.Context, req *pb.ReportProgressRequest) (*pb.ReportProgressResponse, error) {
	symbol := req.GetSymbol()
	if _, err := s.reg.Get(symbol); err != nil {
		return nil, registryError(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.progress[symbol] = followerProgress{
		position:   req.GetAppliedPosition(),
		recordedAt: time.Unix(0, req.GetAppliedRecordLoggedAtUnixNano()),
		reportedAt: time.Now(),
	}
	return &pb.ReportProgressResponse{}, nil
}

// ReplicationStatus reports per-symbol lag: how many records the follower is
// behind, and how far behind in time.
//
// Records-behind comes from the primary's own log, so it is accurate even if no
// follower has ever connected. Millis-behind comes from the timestamp of the
// record the follower last applied, which is what turns a count into something an
// operator can reason about.
func (s *ReplicationServer) ReplicationStatus(ctx context.Context, _ *pb.ReplicationStatusRequest) (*pb.ReplicationStatusResponse, error) {
	symbolsResp, err := s.ListSymbols(ctx, &pb.ListSymbolsRequest{})
	if err != nil {
		return nil, err
	}

	now := time.Now()
	out := &pb.ReplicationStatusResponse{Symbols: make([]*pb.SymbolLag, 0, len(symbolsResp.GetSymbols()))}
	for _, symbol := range symbolsResp.GetSymbols() {
		primaryPos, err := s.primaryPosition(symbol)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read position for %s: %v", symbol, err)
		}

		s.mu.Lock()
		prog, seen := s.progress[symbol]
		s.mu.Unlock()

		lag := &pb.SymbolLag{
			Symbol:          symbol,
			PrimaryPosition: primaryPos,
			FollowerSeen:    seen,
		}
		if seen {
			lag.FollowerPosition = prog.position
			lag.RecordsBehind = primaryPos - prog.position
			if lag.RecordsBehind > 0 && !prog.recordedAt.IsZero() {
				lag.MillisBehind = now.Sub(prog.recordedAt).Milliseconds()
			}
		}
		out.Symbols = append(out.Symbols, lag)
	}
	return out, nil
}

// primaryPosition reports the highest position durably in a symbol's log. It
// reads the log rather than asking the sequencer, so it reflects what a follower
// could actually receive.
func (s *ReplicationServer) primaryPosition(symbol string) (int64, error) {
	w, err := oms.LastPosition(filepath.Join(s.walDir, symbol))
	if err != nil {
		return 0, err
	}
	return w, nil
}

func recordToPB(rec oms.Record) *pb.WALRecord {
	out := &pb.WALRecord{
		Position:         rec.Seq,
		LoggedAtUnixNano: rec.TS.UnixNano(),
	}
	switch rec.Kind {
	case oms.RecordSubmit:
		out.Kind = pb.WALRecordKind_WAL_RECORD_KIND_SUBMIT
		out.Order = &pb.Order{
			OrderId:          int64(rec.Order.SeqID),
			Symbol:           rec.Order.Symbol,
			AccountId:        rec.Order.Placer,
			Type:             orderTypeToPB(rec.Order.Type),
			Price:            int64(rec.Order.Price),
			Quantity:         rec.Order.Quantity,
			Side:             sideToPB(rec.Order.Side),
			PlacedAtUnixNano: rec.Order.TimeStamp.UnixNano(),
		}
	case oms.RecordCancel:
		out.Kind = pb.WALRecordKind_WAL_RECORD_KIND_CANCEL
		out.CancelOrderId = int64(rec.CancelID)
		out.CancelRequestedBy = rec.CancelBy
	}
	return out
}

// RecordFromPB converts a streamed record back. It is exported because the
// follower (internal/replica) needs it and it must stay the exact inverse of
// recordToPB — the two are a pair, and separating them across packages would
// invite them to drift.
func RecordFromPB(in *pb.WALRecord) (oms.Record, error) {
	rec := oms.Record{
		Seq: in.GetPosition(),
		TS:  time.Unix(0, in.GetLoggedAtUnixNano()),
	}
	switch in.GetKind() {
	case pb.WALRecordKind_WAL_RECORD_KIND_SUBMIT:
		o := in.GetOrder()
		if o == nil {
			return oms.Record{}, errors.New("api: submit record has no order")
		}
		side, err := sideFromPB(o.GetSide())
		if err != nil {
			return oms.Record{}, err
		}
		orderType, err := orderTypeFromPB(o.GetType())
		if err != nil {
			return oms.Record{}, err
		}
		rec.Kind = oms.RecordSubmit
		rec.Order = oms.Order{
			SeqID:     oms.SeqID(o.GetOrderId()),
			Symbol:    o.GetSymbol(),
			Placer:    o.GetAccountId(),
			Type:      orderType,
			Price:     oms.Price(o.GetPrice()),
			Quantity:  o.GetQuantity(),
			Side:      side,
			TimeStamp: time.Unix(0, o.GetPlacedAtUnixNano()),
		}
	case pb.WALRecordKind_WAL_RECORD_KIND_CANCEL:
		rec.Kind = oms.RecordCancel
		rec.CancelID = oms.SeqID(in.GetCancelOrderId())
		rec.CancelBy = in.GetCancelRequestedBy()
	default:
		return oms.Record{}, errors.New("api: wal record has no kind")
	}
	return rec, nil
}

func sideToPB(side oms.OrderSide) pb.Side {
	switch side {
	case oms.Buy:
		return pb.Side_SIDE_BUY
	case oms.Sell:
		return pb.Side_SIDE_SELL
	}
	return pb.Side_SIDE_UNSPECIFIED
}

func sideFromPB(side pb.Side) (oms.OrderSide, error) {
	switch side {
	case pb.Side_SIDE_BUY:
		return oms.Buy, nil
	case pb.Side_SIDE_SELL:
		return oms.Sell, nil
	}
	return oms.UnknownSide, errors.New("api: order has no side")
}

func orderTypeToPB(t oms.OrderType) pb.OrderType {
	switch t {
	case oms.Limit:
		return pb.OrderType_ORDER_TYPE_LIMIT
	case oms.Market:
		return pb.OrderType_ORDER_TYPE_MARKET
	}
	return pb.OrderType_ORDER_TYPE_UNSPECIFIED
}

func orderTypeFromPB(t pb.OrderType) (oms.OrderType, error) {
	switch t {
	case pb.OrderType_ORDER_TYPE_LIMIT:
		return oms.Limit, nil
	case pb.OrderType_ORDER_TYPE_MARKET:
		return oms.Market, nil
	}
	return oms.UnknownType, errors.New("api: order has no type")
}
