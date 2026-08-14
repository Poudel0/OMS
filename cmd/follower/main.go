// Command follower runs a passive replica of a Dhukuti node: it streams the
// primary's write-ahead log, replays it into its own books, and keeps its own log
// so that it can be promoted by hand.
//
// It serves no client traffic. A follower that answered PlaceOrder would be a
// second primary, and two primaries writing the same symbol is the one failure
// mode this design has no answer for — see ADR-006 on manual failover.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Poudel0/OMS/internal/pb"
	"github.com/Poudel0/OMS/internal/replica"
)

func main() {
	if err := run(); err != nil {
		slog.Error("follower exited with an error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		primary = flag.String("primary", "127.0.0.1:9090", "address of the primary's ReplicationService")
		dir     = flag.String("wal", "./data/follower-wal", "where to store this follower's own log")
		status  = flag.Duration("status-interval", 10*time.Second, "how often to log replication position; 0 disables")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if *dir == "" {
		return fmt.Errorf("-wal is required: a follower with no log of its own cannot resume or be promoted")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*primary, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial primary %s: %w", *primary, err)
	}
	defer conn.Close()

	rep := replica.New(*dir, log)
	defer func() {
		if err := rep.Close(); err != nil {
			log.Error("closing follower logs", "err", err)
		}
	}()

	log.Info("following", "primary", *primary, "wal", *dir)
	if *status > 0 {
		go logStatus(ctx, rep, log, *status)
	}

	// Run only returns when the context ends; a primary that goes away is
	// reconnected to, not treated as fatal.
	if err := rep.Run(ctx, pb.NewReplicationServiceClient(conn)); err != nil && ctx.Err() == nil {
		return err
	}
	log.Info("follower stopped")
	return nil
}

func logStatus(ctx context.Context, rep *replica.Replica, log *slog.Logger, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, symbol := range rep.Symbols() {
			log.Info("replication position",
				"symbol", symbol,
				"position", rep.Position(symbol),
				"resting", rep.RestingCount(symbol))
		}
	}
}
