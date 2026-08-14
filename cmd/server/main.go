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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Poudel0/OMS/internal/api"
	"github.com/Poudel0/OMS/internal/ledger"
	"github.com/Poudel0/OMS/internal/metrics"
	"github.com/Poudel0/OMS/internal/oms"
	"github.com/Poudel0/OMS/internal/pb"
	"github.com/Poudel0/OMS/internal/tracing"
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
		metricsAddr = flag.String("metrics-addr", ":9091", "address for the Prometheus /metrics endpoint; empty disables it")
		otlpAddr    = flag.String("otlp", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "OTLP collector host:port for traces; empty disables tracing")
		otlpEnv     = flag.String("environment", "dev", "deployment.environment attribute on exported traces")
		traceRatio  = flag.Float64("trace-ratio", 0.05, "head-sampling fraction for traces; 1.0 traces everything")
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

	// Tracing is opt-in: with no endpoint, OTel's default provider is a no-op and
	// every span in the codebase costs essentially nothing. A node should not
	// refuse to start because a collector is down.
	shutdownTracing, err := tracing.Setup(ctx, *otlpAddr, *otlpEnv, *traceRatio)
	if err != nil {
		return fmt.Errorf("set up tracing: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutCtx); err != nil {
			log.Warn("flushing traces on shutdown", "err", err)
		}
	}()
	if *otlpAddr != "" {
		log.Info("tracing enabled", "otlp", *otlpAddr, "sample_ratio", *traceRatio)
	}

	accounts := oms.NewAccounts()

	// Rebuild balances from the journal before anything can trade. The in-memory
	// store is lost on every restart while the journal is not, so this is the
	// only thing that stops a restarted node from checking orders against
	// balances of zero. It must happen before the registry replays any log,
	// because replay restores holds against these balances.
	if l, ok := settle.(*ledger.Ledger); ok {
		cash, positions, err := l.Balances(ctx)
		if err != nil {
			return fmt.Errorf("rebuild balances from journal: %w", err)
		}
		if err := accounts.LoadBalances(cash, positions); err != nil {
			return fmt.Errorf("load rebuilt balances: %w", err)
		}
		log.Info("rebuilt balances from the journal", "accounts", len(cash))
	}

	if *seed > 0 {
		seedAccounts(accounts, *seed, *seedSymbols, log)
	}

	reg := oms.NewRegistry(ctx, *walDir, accounts)

	// Replication is served from the same gRPC listener for now. A real
	// deployment would put it on its own port with its own credentials: a
	// follower is not a client, and the log is strictly more sensitive than the
	// order API.
	var repl *api.ReplicationServer
	if *walDir != "" {
		repl = api.NewReplicationServer(*walDir, reg, log)
	} else {
		log.Warn("replication disabled: a node with no write-ahead log has nothing to ship")
	}

	// The metrics collector reads replication lag from the same code path that
	// serves ReplicationStatus, so the gRPC view and the Prometheus view cannot
	// disagree about what lag means.
	var lagSource metrics.LagSource
	if repl != nil {
		lagSource = repl
	}
	met := metrics.New(reg, lagSource)
	srv := api.NewServerWithObserver(reg, accounts, settle, log, met)

	// The interceptor starts a span per RPC and continues one the client sent,
	// which is what makes the handler->sequencer->settlement chain a single trace
	// rather than three unrelated ones.
	gs := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	pb.RegisterOrderServiceServer(gs, srv)
	if repl != nil {
		pb.RegisterReplicationServiceServer(gs, repl)
	}

	var metricsSrv *http.Server
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", met.Handler())
		// A liveness probe that does not touch the book: if the process answers,
		// the process is up. Readiness would need to mean "logs open and
		// recovered", which is a different question and not one a load balancer
		// should be guessing at.
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
		})
		metricsSrv = &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics endpoint failed", "err", err)
			}
		}()
		log.Info("metrics listening", "addr", *metricsAddr, "path", "/metrics")
	}
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

	if metricsSrv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := metricsSrv.Shutdown(shutCtx); err != nil {
			log.Warn("metrics endpoint did not shut down cleanly", "err", err)
		}
		cancel()
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
