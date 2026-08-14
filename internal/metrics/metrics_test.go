package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Poudel0/OMS/internal/oms"
)

// scrape renders the metrics endpoint, which is the only view that matters — a
// counter that is incremented but not exported is not a metric.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func mustContain(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, want := range lines {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}

func TestMetrics_ExportsRequestPathCounters(t *testing.T) {
	reg := oms.NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()
	m := New(reg, nil)

	m.ObserveOrder("NABIL", "SIDE_BUY", "ORDER_TYPE_LIMIT", "accepted", 2, 5*time.Millisecond)
	m.ObserveOrder("NABIL", "SIDE_BUY", "ORDER_TYPE_LIMIT", "insufficient_funds", 0, time.Millisecond)
	m.ObserveCancel("NABIL", "cancelled")
	m.ObserveSettlement("ok", 3*time.Millisecond)
	m.ObserveFeedDrop("NABIL")

	body := scrape(t, m)
	mustContain(t, body,
		`oms_orders_total{outcome="accepted",side="SIDE_BUY",symbol="NABIL",type="ORDER_TYPE_LIMIT"} 1`,
		`oms_orders_total{outcome="insufficient_funds",side="SIDE_BUY",symbol="NABIL",type="ORDER_TYPE_LIMIT"} 1`,
		`oms_cancels_total{outcome="cancelled",symbol="NABIL"} 1`,
		`oms_trades_total{symbol="NABIL"} 2`,
		`oms_settlements_total{outcome="ok"} 1`,
		`oms_trade_feed_dropped_total{symbol="NABIL"} 1`,
		"oms_order_duration_seconds_bucket",
	)
}

func TestMetrics_ExportsGoRuntimeMetrics(t *testing.T) {
	reg := oms.NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()

	// The sprint asked for GC pause time specifically, and this design spends a
	// goroutine per symbol, so goroutine count is worth having too. Both come
	// free from the runtime collector rather than being hand-rolled.
	body := scrape(t, New(reg, nil))
	mustContain(t, body, "go_gc_duration_seconds", "go_goroutines")
}

func TestMetrics_DerivesPerSymbolGaugesFromLiveState(t *testing.T) {
	ctx := t.Context()
	accounts := oms.NewAccounts()
	accounts.Deposit("alice", 10_000_000)
	reg := oms.NewRegistry(ctx, t.TempDir(), accounts)
	defer reg.Close()
	m := New(reg, nil)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := seq.Submit(ctx, oms.Order{Placer: "alice", Type: oms.Limit, Price: 500, Quantity: 1, Side: oms.Buy}); err != nil {
			t.Fatal(err)
		}
	}

	// These are computed at scrape time rather than maintained, so they cannot
	// drift out of sync with the book they describe.
	body := scrape(t, m)
	mustContain(t, body,
		`oms_resting_orders{symbol="NABIL"} 3`,
		`oms_log_position{symbol="NABIL"} 3`,
		`oms_group_commit_requests_total{symbol="NABIL"} 3`,
		`oms_group_commits_total{symbol="NABIL"}`,
	)
}

// staticLag is a LagSource with fixed values.
type staticLag map[string]FollowerLag

func (s staticLag) SymbolLag() map[string]FollowerLag { return s }

func TestMetrics_ExportsReplicationLagAndWhetherAnyoneIsWatching(t *testing.T) {
	reg := oms.NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()

	m := New(reg, staticLag{
		"NABIL": {PrimaryPosition: 1000, FollowerPosition: 940, FollowerSeen: true, MillisBehind: 250},
		// No follower has ever reported for ADBL. Its records_behind is the full
		// position, and follower_seen says why — the two together are what stop
		// "0 behind" and "nobody is watching" looking identical.
		"ADBL": {PrimaryPosition: 500, FollowerPosition: 0, FollowerSeen: false},
	})

	body := scrape(t, m)
	mustContain(t, body,
		`oms_replication_records_behind{symbol="NABIL"} 60`,
		`oms_replication_millis_behind{symbol="NABIL"} 250`,
		`oms_replication_follower_seen{symbol="NABIL"} 1`,
		`oms_replication_records_behind{symbol="ADBL"} 500`,
		`oms_replication_follower_seen{symbol="ADBL"} 0`,
	)
}

func TestMetrics_NilLagSourceExportsNoReplicationSeries(t *testing.T) {
	reg := oms.NewRegistry(t.Context(), t.TempDir(), nil)
	defer reg.Close()

	// A node with no replication should not export replication series at all,
	// rather than exporting zeroes that look like a healthy caught-up follower.
	body := scrape(t, New(reg, nil))
	if strings.Contains(body, "oms_replication_records_behind{") {
		t.Error("/metrics exports replication lag with no replication configured")
	}
}

func TestMetrics_ScrapeIsSafeWhileTheVenueIsBusy(t *testing.T) {
	ctx := context.Background()
	accounts := oms.NewAccounts()
	accounts.Deposit("alice", 1_000_000_000)
	reg := oms.NewRegistry(ctx, t.TempDir(), accounts)
	defer reg.Close()
	m := New(reg, nil)

	seq, err := reg.Get("NABIL")
	if err != nil {
		t.Fatal(err)
	}

	// The gauges read the book's state, so a scrape racing live mutations is
	// exactly the case that must not be a data race. Run under -race.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 300 {
			_, _ = seq.Submit(ctx, oms.Order{Placer: "alice", Type: oms.Limit, Price: 500, Quantity: 1, Side: oms.Buy})
		}
	}()
	for range 30 {
		scrape(t, m)
	}
	<-done
	scrape(t, m)
}
