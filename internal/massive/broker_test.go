package massive

import (
	"testing"

	"qqq-edge-universal/internal/marketdata"
)

func TestSubscriptionParams(t *testing.T) {
	got := subscriptionParams("spy", marketdata.StreamKinds{Trades: true, Quotes: true, Minutes: true})
	want := "T.SPY,Q.SPY,AM.SPY"
	if got != want {
		t.Fatalf("subscriptionParams(...) = %q, want %q", got, want)
	}
}

func TestBrokerAggregatesKindsAcrossWatchers(t *testing.T) {
	b, err := NewBroker("test-key")
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}

	tradesOnly := b.Subscribe("SPY", marketdata.StreamKinds{Trades: true})
	quotesOnly := b.Subscribe("SPY", marketdata.StreamKinds{Quotes: true})
	if tradesOnly == nil || quotesOnly == nil {
		t.Fatal("expected subscriptions")
	}

	kinds := b.liveKinds["SPY"]
	if !kinds.Trades || !kinds.Quotes || kinds.Minutes {
		t.Fatalf("liveKinds[SPY] = %#v, want trades+quotes", kinds)
	}

	b.Unsubscribe(tradesOnly)
	kinds = b.liveKinds["SPY"]
	if kinds.Trades || !kinds.Quotes || kinds.Minutes {
		t.Fatalf("liveKinds[SPY] after unsubscribe = %#v, want quotes only", kinds)
	}
}
