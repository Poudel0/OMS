// Command loadgen drives a running Dhukuti node over gRPC and reports
// throughput and latency percentiles.
//
// It exists because hey and vegeta speak HTTP, not gRPC, and ghz would be
// another dependency to install for something a real client does better: this
// measures the actual generated stubs over an actual connection, which is what
// a client will experience.
//
// The latency reported is end-to-end per PlaceOrder call, so on a node with a
// write-ahead log it includes that batch's fsync. Compare against the
// no-durability numbers only with that in mind — see docs/BENCHMARKS.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Poudel0/OMS/internal/pb"
)

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:9090", "gRPC address of the node under test")
		workers   = flag.Int("workers", 32, "concurrent client goroutines")
		duration  = flag.Duration("duration", 30*time.Second, "how long to sustain load")
		symbolCSV = flag.String("symbols", "NABIL,ADBL,HBL,NRIC", "symbols to spread load across")
		accounts  = flag.Int("accounts", 8, "how many seeded accounts to trade as (must match the server's -seed-accounts)")
		basePrice = flag.Int64("price", 500, "centre of the price band orders are drawn from")
		spread    = flag.Int64("spread", 5, "half-width of the price band, in ticks")
	)
	flag.Parse()

	symbols := strings.FieldsFunc(*symbolCSV, func(r rune) bool { return r == ',' })
	if len(symbols) == 0 {
		fmt.Fprintln(os.Stderr, "need at least one symbol")
		os.Exit(1)
	}
	if *accounts < 2 {
		fmt.Fprintln(os.Stderr, "need at least 2 accounts so orders have someone to trade against")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewOrderServiceClient(conn)

	type workerResult struct {
		latencies []time.Duration
		trades    int
		rejected  int
	}
	results := make([]workerResult, *workers)

	start := time.Now()
	var wg sync.WaitGroup
	for w := range *workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Per-worker RNG seeded by index: reproducible, and no shared
			// state to contend on while measuring someone else's contention.
			rng := rand.New(rand.NewSource(int64(w) + 1))
			// Preallocate generously so an append never lands inside a timed
			// region and shows up as latency.
			res := workerResult{latencies: make([]time.Duration, 0, 1<<16)}
			// Publish results on every exit path, not just the loop's natural
			// end. The run always finishes with a call in flight failing on the
			// expired context, so an early return that skipped this threw away
			// everything the worker had measured.
			defer func() { results[w] = res }()

			for ctx.Err() == nil {
				side := pb.Side_SIDE_BUY
				if rng.Intn(2) == 0 {
					side = pb.Side_SIDE_SELL
				}
				req := &pb.PlaceOrderRequest{
					Symbol:    symbols[rng.Intn(len(symbols))],
					AccountId: fmt.Sprintf("acct-%d", rng.Intn(*accounts)),
					Side:      side,
					Type:      pb.OrderType_ORDER_TYPE_LIMIT,
					Price:     *basePrice + int64(rng.Intn(int(2*(*spread)+1))) - *spread,
					Quantity:  int64(1 + rng.Intn(50)),
				}

				t0 := time.Now()
				resp, err := client.PlaceOrder(ctx, req)
				elapsed := time.Since(t0)

				if err != nil {
					if ctx.Err() != nil {
						return // the run ended mid-flight; not a failure
					}
					// A rejection is a real answer from a working venue
					// (insufficient funds, say) and belongs in its own bucket
					// rather than inflating either the success or error count.
					res.rejected++
					continue
				}
				res.latencies = append(res.latencies, elapsed)
				res.trades += len(resp.GetTrades())
			}
		}(w)
	}
	wg.Wait()
	wall := time.Since(start)

	var all []time.Duration
	var trades, rejected int
	for _, r := range results {
		all = append(all, r.latencies...)
		trades += r.trades
		rejected += r.rejected
	}
	if len(all) == 0 {
		fmt.Fprintf(os.Stderr, "no orders completed (%d rejected) — is the server seeded with -seed-accounts %d?\n", rejected, *accounts)
		os.Exit(1)
	}
	slices.Sort(all)

	fmt.Printf("target        %s\n", *addr)
	fmt.Printf("workers       %d\n", *workers)
	fmt.Printf("symbols       %v\n", symbols)
	fmt.Printf("wall clock    %s\n", wall.Round(time.Millisecond))
	fmt.Printf("accepted      %d orders\n", len(all))
	fmt.Printf("rejected      %d\n", rejected)
	fmt.Printf("trades        %d\n", trades)
	fmt.Printf("throughput    %.0f orders/sec\n", float64(len(all))/wall.Seconds())
	fmt.Println("latency")
	for _, p := range []float64{50, 95, 99, 99.9} {
		fmt.Printf("  p%-5s      %s\n", trimZero(p), percentile(all, p).Round(time.Microsecond))
	}
	fmt.Printf("  max         %s\n", all[len(all)-1].Round(time.Microsecond))
}

// percentile returns the p-th percentile of a sorted slice using nearest-rank,
// which needs no interpolation and cannot invent a latency nobody observed.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p / 100 * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func trimZero(p float64) string {
	return strings.TrimSuffix(strings.TrimRight(fmt.Sprintf("%.1f", p), "0"), ".")
}
