// Command server runs one Dhukuti trading node: a gRPC front end over a
// per-symbol registry of single-writer sequencers, each with its own
// write-ahead log, settling into a Postgres double-entry journal.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Poudel0/OMS/internal/api"
	"github.com/Poudel0/OMS/internal/ledger"
	"github.com/Poudel0/OMS/internal/oms"
	"github.com/Poudel0/OMS/internal/pb"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server exited with an error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr        = flag.String("addr", ":9090", "gRPC listen address")
		walDir      = flag.String("wal", "./data/wal", "write-ahead log root; one subdirectory per symbol. Empty disables durability")
		dsn         = flag.String("db", os.Getenv("DATABASE_URL"), "Postgres DSN for the settlement ledger; empty runs without durable settlement")
		seed        = flag.Int("seed-accounts", 0, "development only: create N funded demo accounts (acct-0..acct-N-1)")
		seedSymbols = flag.String("seed-symbols", "NABIL,ADBL,HBL,NRIC", "symbols to give seeded accounts a position in")
		shutdownIn  = flag.Duration("shutdown-timeout", 15*time.Second, "how long to let in-flight RPCs finish before forcing the server down")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	// The signal context is cancelled on the first SIGINT/SIGTERM, and every
	// sequencer inherits it. A second signal aborts, because a shutdown that
	// itself hangs must still be interruptible.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *walDir == "" {
		log.Warn("running without a write-ahead log: acknowledged orders will not survive a restart")
	}

	var settle api.Ledger
	if *dsn != "" {
		l, err := ledger.Open(ctx, *dsn)
		if err != nil {
			return fmt.Errorf("open ledger: %w", err)
		}
		defer l.Close()
		if err := l.Migrate(ctx); err != nil {
			return fmt.Errorf("migrate ledger: %w", err)
		}
		settle = l
		log.Info("settlement ledger ready")
	} else {
		log.Warn("running without a settlement ledger: trades will match but nothing will be journalled")
	}

	accounts := oms.NewAccounts()
	if *seed > 0 {
		seedAccounts(accounts, *seed, *seedSymbols, log)
	}

	reg := oms.NewRegistry(ctx, *walDir)
	srv := api.NewServer(reg, accounts, settle, log)

	gs := grpc.NewServer()
	pb.RegisterOrderServiceServer(gs, srv)
	// Reflection lets grpcurl explore the API without a copy of the .proto.
	// Fine for a single-tenant venue you operate yourself; a public deployment
	// would gate this behind an admin listener.
	reflection.Register(gs)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}
	log.Info("serving", "addr", lis.Addr().String(), "wal", *walDir, "ledger", settle != nil)

	serveErr := make(chan error, 1)
	go func() {
		if err := gs.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		log.Info("shutdown requested")
	}

	// Order matters. Drain in-flight RPCs first, so an order that has already
	// been accepted still gets its reply; only then close the logs. Closing
	// them first would fail requests the venue had effectively taken on.
	drained := make(chan struct{})
	go func() { gs.GracefulStop(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(*shutdownIn):
		log.Warn("in-flight RPCs did not finish in time; forcing shutdown", "timeout", *shutdownIn)
		gs.Stop()
	}

	// Close flushes and fsyncs every symbol's log, and reports if that failed —
	// the last chance to notice that a shutdown lost acknowledged orders.
	if err := reg.Close(); err != nil {
		return fmt.Errorf("closing symbol registry: %w", err)
	}
	log.Info("shutdown complete")
	return <-serveErr
}

// seedAccounts funds demo accounts so a fresh node can actually be traded
// against. Development only: real accounts are opened and funded through the
// ledger, not a command-line flag.
func seedAccounts(accounts *oms.Accounts, n int, symbolList string, log *slog.Logger) {
	const (
		seedCash     = 1_000_000_000
		seedPosition = 1_000_000
	)
	symbols := strings.FieldsFunc(symbolList, func(r rune) bool { return r == ',' })
	for i := range n {
		account := fmt.Sprintf("acct-%d", i)
		accounts.Deposit(account, seedCash)
		for _, symbol := range symbols {
			accounts.SetPosition(account, symbol, seedPosition)
		}
	}
	log.Warn("seeded development accounts", "count", n, "symbols", symbols,
		"cash", seedCash, "position_per_symbol", seedPosition)
}
