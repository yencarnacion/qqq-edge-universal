package marketdata

import "testing"

func TestNewSubscriptionBuildsRequestedChannels(t *testing.T) {
	sub := NewSubscription(" qqq ", StreamKinds{Trades: true, Quotes: true})
	if sub == nil {
		t.Fatal("NewSubscription returned nil")
	}
	if sub.Symbol != "QQQ" {
		t.Fatalf("Symbol = %q, want QQQ", sub.Symbol)
	}
	if sub.Trades == nil || sub.Quotes == nil {
		t.Fatal("expected trade and quote channels")
	}
	if sub.Minutes != nil {
		t.Fatal("did not expect minutes channel")
	}
	if !sub.Kinds().Trades || !sub.Kinds().Quotes || sub.Kinds().Minutes {
		t.Fatalf("Kinds = %#v, want trades+quotes only", sub.Kinds())
	}
}

func TestNewSubscriptionRejectsEmptySymbolAndKinds(t *testing.T) {
	if sub := NewSubscription("", StreamKinds{Trades: true}); sub != nil {
		t.Fatalf("empty symbol returned %#v, want nil", sub)
	}
	if sub := NewSubscription("QQQ", StreamKinds{}); sub != nil {
		t.Fatalf("empty kinds returned %#v, want nil", sub)
	}
}
