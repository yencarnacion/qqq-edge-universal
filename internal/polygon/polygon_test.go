package polygon

import (
	"reflect"
	"sort"
	"testing"
)

func TestBatchWSMsgsBatchesAndPreservesOrder(t *testing.T) {
	params := []string{
		"T.AAPL", "AM.AAPL", "Q.AAPL",
		"T.MSFT", "AM.MSFT", "Q.MSFT",
		"T.NVDA", "AM.NVDA", "Q.NVDA",
	}

	msgs := batchWSMsgs("subscribe", params)
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].Action != "subscribe" {
		t.Fatalf("action = %q, want subscribe", msgs[0].Action)
	}
	if got := msgs[0].Params; got != "T.AAPL,AM.AAPL,Q.AAPL,T.MSFT,AM.MSFT,Q.MSFT,T.NVDA,AM.NVDA,Q.NVDA" {
		t.Fatalf("params = %q", got)
	}
}

func TestBatchWSMsgsSplitsWhenParamsGrowTooLarge(t *testing.T) {
	params := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		params = append(params, "T.SYMBOL123456789")
	}

	msgs := batchWSMsgs("subscribe", params)
	if len(msgs) < 2 {
		t.Fatalf("len(msgs) = %d, want at least 2", len(msgs))
	}
	for _, msg := range msgs {
		if len(msg.Params) > maxSubscriptionParamLen {
			t.Fatalf("message len = %d, want <= %d", len(msg.Params), maxSubscriptionParamLen)
		}
	}
}

func TestCurrentSubscriptionMessagesMergesSymbolsAndKinds(t *testing.T) {
	b := NewBroker("test")
	b.subscribed["AAPL"] = StreamKinds{Trades: true, Minutes: true}
	b.subscribed["MSFT"] = StreamKinds{Trades: true, Quotes: true}

	msgs := b.currentSubscriptionMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}

	got := compactStrings(splitParams(msgs[0].Params))
	want := []string{"T.AAPL", "AM.AAPL", "T.MSFT", "Q.MSFT"}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("params = %v, want %v", got, want)
	}
}

func splitParams(raw string) []string {
	if raw == "" {
		return nil
	}
	out := make([]string, 0, 8)
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == ',' {
			out = append(out, raw[start:i])
			start = i + 1
		}
	}
	return out
}
