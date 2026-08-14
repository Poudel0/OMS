package api

import (
	"sync"

	"github.com/Poudel0/OMS/internal/oms"
)

// feedBuffer is how many trades a subscriber may fall behind by before it
// starts losing them. Deep enough to absorb a scheduling hiccup, shallow
// enough that a genuinely stalled subscriber does not hold megabytes.
const feedBuffer = 256

// tradeFeed fans one symbol's trades out to its live subscribers.
//
// Publishing happens on the sequencer goroutine (see oms.Sequencer.OnTrades),
// which is what makes the feed's order match the matching order — but also
// means publish must never block, because blocking here stops the symbol from
// matching at all.
//
// A subscriber whose buffer is full loses trades rather than applying
// backpressure. That is the correct trade for a live market-data feed — one slow
// client must not be able to halt the venue — and it is no longer silent: every
// drop increments oms_trade_feed_dropped_total. StreamTrades makes no
// completeness guarantee, and its proto comment says so.
//
// ponytail: still not *disconnected*. A subscriber that is permanently behind
// keeps its slot and keeps losing trades, where a real venue would cut it off and
// make it resubscribe. The counter is what makes that decision possible; making
// it automatically is the next step.
type tradeFeed struct {
	// onDrop, if set, is called once per subscriber that could not keep up. It is
	// what turns "drops silently" into "drops, and says so" — see
	// metrics.ObserveFeedDrop.
	onDrop func()

	mu     sync.Mutex
	subs   map[int64]chan *tradeBatch
	nextID int64
}

// tradeBatch is one Submit's worth of trades, kept together so a subscriber
// sees the same grouping the matcher produced.
type tradeBatch struct {
	symbol string
	trades []oms.Trade
}

func newTradeFeed() *tradeFeed {
	return &tradeFeed{subs: make(map[int64]chan *tradeBatch)}
}

// subscribe registers a new subscriber and returns its id and channel. The
// caller must unsubscribe when done, or the feed will keep writing to a
// channel nobody reads.
func (f *tradeFeed) subscribe() (int64, <-chan *tradeBatch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	ch := make(chan *tradeBatch, feedBuffer)
	f.subs[f.nextID] = ch
	return f.nextID, ch
}

func (f *tradeFeed) unsubscribe(id int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.subs, id)
}

// publish delivers a batch to every subscriber that has room for it. It never
// blocks: see the type comment for why that is a requirement rather than a
// shortcut.
func (f *tradeFeed) publish(symbol string, trades []oms.Trade) {
	batch := &tradeBatch{symbol: symbol, trades: trades}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.subs {
		select {
		case ch <- batch:
		default:
			// Subscriber is behind; drop rather than stall the matcher.
			if f.onDrop != nil {
				f.onDrop()
			}
		}
	}
}

func (f *tradeFeed) subscriberCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs)
}
