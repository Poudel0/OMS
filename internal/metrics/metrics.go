// Package metrics exposes the venue's Prometheus metrics.
//
// The split here is deliberate: counters and histograms are incremented on the
// request path, while everything derivable from live state (book depth, log
// position, replication lag, mean group-commit size) is computed **on scrape**
// by a custom collector.
//
// Computing gauges at scrape time rather than maintaining them means no
// background goroutine, no chance of a gauge drifting out of sync with the thing
// it describes, and no cost at all between scrapes. It is the standard advice
// for values you can already read cheaply, and here they are all either an
// atomic load or a map iteration.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Poudel0/OMS/internal/oms"
)

// Metrics holds the venue's collectors. One per process.
type Metrics struct {
	reg *prometheus.Registry

	ordersTotal   *prometheus.CounterVec
	cancelsTotal  *prometheus.CounterVec
	tradesTotal   *prometheus.CounterVec
	orderLatency  *prometheus.HistogramVec
	settleTotal   *prometheus.CounterVec
	settleLatency prometheus.Histogram
	feedDropped   *prometheus.CounterVec
}

// LagSource reports per-symbol replication lag. The replication server
// implements it; a node without replication supplies nil.
type LagSource interface {
	// SymbolLag returns records-behind per symbol, and whether a follower has
	// ever reported for it.
	SymbolLag() map[string]FollowerLag
}

// FollowerLag is one symbol's replication position.
type FollowerLag struct {
	PrimaryPosition  int64
	FollowerPosition int64
	FollowerSeen     bool
	MillisBehind     int64
}

// New builds the collectors and registers them, along with Go runtime and
// process metrics.
//
// reg is required: the per-symbol gauges are read from it at scrape time. lag
// may be nil.
func New(registry *oms.Registry, lag LagSource) *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),

		// Outcome is a label rather than separate metrics because the interesting
		// question is almost always a ratio: what share of orders are being
		// rejected, and for which reason.
		ordersTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oms_orders_total",
			Help: "Orders submitted, by symbol, side, type and outcome.",
		}, []string{"symbol", "side", "type", "outcome"}),

		cancelsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oms_cancels_total",
			Help: "Cancel requests, by symbol and outcome.",
		}, []string{"symbol", "outcome"}),

		tradesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oms_trades_total",
			Help: "Trades executed, by symbol.",
		}, []string{"symbol"}),

		// Buckets span 100µs to ~26s. That range is not arbitrary: a no-durability
		// order is tens of microseconds, one fsync on this stack is ~3.4ms
		// (ADR-003), and the p99.9 under load is ~130ms — so the interesting
		// decade is 1ms..1s and the buckets are densest there.
		orderLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "oms_order_duration_seconds",
			Help:    "End-to-end PlaceOrder duration, including the group commit's fsync.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 19),
		}, []string{"symbol"}),

		settleTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oms_settlements_total",
			Help: "Ledger settlement attempts, by outcome. A failure means trades are durable but unsettled.",
		}, []string{"outcome"}),

		settleLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "oms_settlement_duration_seconds",
			Help:    "Time spent writing a batch of trades to the settlement journal.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 19),
		}),

		feedDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oms_trade_feed_dropped_total",
			Help: "Trade batches dropped because a StreamTrades subscriber was too far behind.",
		}, []string{"symbol"}),
	}

	m.reg.MustRegister(
		m.ordersTotal, m.cancelsTotal, m.tradesTotal,
		m.orderLatency, m.settleTotal, m.settleLatency, m.feedDropped,
		// Runtime metrics come free and cover the GC pause time the sprint asked
		// for (go_gc_duration_seconds) plus goroutine counts, which matter here
		// because this design spends goroutines per symbol.
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		&stateCollector{reg: registry, lag: lag},
	)
	return m
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ObserveOrder records one PlaceOrder call.
func (m *Metrics) ObserveOrder(symbol, side, orderType, outcome string, trades int, d time.Duration) {
	m.ordersTotal.WithLabelValues(symbol, side, orderType, outcome).Inc()
	m.orderLatency.WithLabelValues(symbol).Observe(d.Seconds())
	if trades > 0 {
		m.tradesTotal.WithLabelValues(symbol).Add(float64(trades))
	}
}

// ObserveCancel records one CancelOrder call.
func (m *Metrics) ObserveCancel(symbol, outcome string) {
	m.cancelsTotal.WithLabelValues(symbol, outcome).Inc()
}

// ObserveSettlement records one ledger write.
func (m *Metrics) ObserveSettlement(outcome string, d time.Duration) {
	m.settleTotal.WithLabelValues(outcome).Inc()
	m.settleLatency.Observe(d.Seconds())
}

// ObserveFeedDrop records a trade batch a subscriber was too slow to receive.
//
// This is the counter ADR-005 asked for by name: dropping for a slow subscriber
// is the right behaviour, but doing it *silently* is not, and a feed with no
// drop counter looks identical to one that never drops.
func (m *Metrics) ObserveFeedDrop(symbol string) {
	m.feedDropped.WithLabelValues(symbol).Inc()
}

// Descriptors for the scrape-time gauges.
var (
	descResting = prometheus.NewDesc(
		"oms_resting_orders", "Orders currently resting in a symbol's book.", []string{"symbol"}, nil)
	descLogPosition = prometheus.NewDesc(
		"oms_log_position", "Highest write-ahead log position assigned for a symbol.", []string{"symbol"}, nil)
	descCommits = prometheus.NewDesc(
		"oms_group_commits_total", "Group commits performed for a symbol; each is one fsync.", []string{"symbol"}, nil)
	descBatched = prometheus.NewDesc(
		"oms_group_commit_requests_total", "Requests covered by those group commits.", []string{"symbol"}, nil)
	descReplPrimary = prometheus.NewDesc(
		"oms_replication_primary_position", "Primary's log position for a symbol.", []string{"symbol"}, nil)
	descReplFollower = prometheus.NewDesc(
		"oms_replication_follower_position", "Follower's last reported position for a symbol.", []string{"symbol"}, nil)
	descReplBehind = prometheus.NewDesc(
		"oms_replication_records_behind", "Records the follower is behind on a symbol.", []string{"symbol"}, nil)
	descReplMillis = prometheus.NewDesc(
		"oms_replication_millis_behind", "Milliseconds between now and the follower's last applied record.", []string{"symbol"}, nil)
	descReplSeen = prometheus.NewDesc(
		"oms_replication_follower_seen", "1 if a follower has ever reported progress for a symbol, else 0.", []string{"symbol"}, nil)
)

// stateCollector reads live state at scrape time.
//
// oms_group_commits_total and oms_group_commit_requests_total are exported as a
// pair rather than pre-divided into a ratio, because Prometheus can only compute
// a rate over the raw counters — `rate(requests) / rate(commits)` gives the mean
// batch size *over the scrape window*, which is far more useful than a lifetime
// average that flattens out under any change in load. That ratio is the number
// ADR-003's entire group-commit argument rests on, so it needs to be observable
// live rather than only in a benchmark.
type stateCollector struct {
	reg *oms.Registry
	lag LagSource
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descResting
	ch <- descLogPosition
	ch <- descCommits
	ch <- descBatched
	ch <- descReplPrimary
	ch <- descReplFollower
	ch <- descReplBehind
	ch <- descReplMillis
	ch <- descReplSeen
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	for _, symbol := range c.reg.Symbols() {
		seq, err := c.reg.Get(symbol)
		if err != nil {
			continue // vanished between listing and lookup; nothing to report
		}
		commits, requests := seq.CommitStats()
		ch <- prometheus.MustNewConstMetric(descResting, prometheus.GaugeValue, float64(seq.RestingOrders()), symbol)
		ch <- prometheus.MustNewConstMetric(descLogPosition, prometheus.GaugeValue, float64(seq.LastPosition()), symbol)
		ch <- prometheus.MustNewConstMetric(descCommits, prometheus.CounterValue, float64(commits), symbol)
		ch <- prometheus.MustNewConstMetric(descBatched, prometheus.CounterValue, float64(requests), symbol)
	}

	if c.lag == nil {
		return
	}
	for symbol, l := range c.lag.SymbolLag() {
		ch <- prometheus.MustNewConstMetric(descReplPrimary, prometheus.GaugeValue, float64(l.PrimaryPosition), symbol)
		ch <- prometheus.MustNewConstMetric(descReplFollower, prometheus.GaugeValue, float64(l.FollowerPosition), symbol)
		ch <- prometheus.MustNewConstMetric(descReplBehind, prometheus.GaugeValue, float64(l.PrimaryPosition-l.FollowerPosition), symbol)
		ch <- prometheus.MustNewConstMetric(descReplMillis, prometheus.GaugeValue, float64(l.MillisBehind), symbol)
		// Exported as its own gauge because "0 behind" and "nobody is watching"
		// are the same number and very different situations.
		seen := 0.0
		if l.FollowerSeen {
			seen = 1
		}
		ch <- prometheus.MustNewConstMetric(descReplSeen, prometheus.GaugeValue, seen, symbol)
	}
}
