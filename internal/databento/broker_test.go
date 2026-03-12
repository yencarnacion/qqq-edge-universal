package databento

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	dbn "github.com/NimbleMarkets/dbn-go"

	"qqq-edge-universal/internal/marketdata"
)

func TestBatchSymbolsSplitsAndPreservesOrder(t *testing.T) {
	symbols := []string{"AAPL", "MSFT", "NVDA"}
	chunks := batchSymbols(symbols, 256)
	if len(chunks) != 1 {
		t.Fatalf("len(chunks) = %d, want 1", len(chunks))
	}
	if got := chunks[0]; !reflect.DeepEqual(got, symbols) {
		t.Fatalf("chunk = %v, want %v", got, symbols)
	}
}

func TestBatchSymbolsRespectsByteLimit(t *testing.T) {
	symbols := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		symbols = append(symbols, strings.Repeat("S", 12)+strconv.Itoa(i))
	}
	chunks := batchSymbols(symbols, 64)
	if len(chunks) < 2 {
		t.Fatalf("len(chunks) = %d, want at least 2", len(chunks))
	}
	for _, chunk := range chunks {
		raw := stringsJoin(chunk, ",")
		if len(raw) > 64 {
			t.Fatalf("chunk size = %d, want <= 64", len(raw))
		}
	}
}

func TestCurrentSubscriptionsSplitSchemasByRequestedKinds(t *testing.T) {
	b, err := NewBroker(strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	b.liveKinds["AAPL"] = marketdata.StreamKinds{Trades: true, Quotes: true}
	b.liveKinds["MSFT"] = marketdata.StreamKinds{Minutes: true}
	b.liveKinds["NVDA"] = marketdata.StreamKinds{Trades: true, Minutes: true}

	got := make(map[string][]string)
	for _, req := range b.currentSubscriptions() {
		got[req.schema] = append(got[req.schema], req.symbols...)
	}
	for schema := range got {
		sort.Strings(got[schema])
	}

	want := map[string][]string{
		liveSchemaMbp1:    {"AAPL", "NVDA"},
		liveSchemaOhlcv1m: {"MSFT", "NVDA"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("currentSubscriptions() = %#v, want %#v", got, want)
	}
}

func TestSubscribeEnqueuesNativeMinuteSchemaForMinuteRequests(t *testing.T) {
	b, err := NewBroker(strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}

	sub := b.Subscribe("QQQ", marketdata.StreamKinds{Minutes: true})
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}
	defer sub.Close()

	select {
	case req := <-b.outbound:
		if req.schema != liveSchemaOhlcv1m || !reflect.DeepEqual(req.symbols, []string{"QQQ"}) {
			t.Fatalf("outbound req = %#v, want ohlcv-1m for QQQ", req)
		}
	default:
		t.Fatal("expected outbound subscription request")
	}
}

func TestHandleCmbp1DispatchesQuoteAndTrade(t *testing.T) {
	b, err := NewBroker(strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	b.symbolMap[123] = "QQQ"

	sub := b.Subscribe("QQQ", marketdata.StreamKinds{Trades: true, Quotes: true})
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}
	defer sub.Close()

	msg := &dbn.Cmbp1Msg{
		Header: dbn.RHeader{
			InstrumentID: 123,
			TsEvent:      uint64(1710000000000000000),
		},
		Price:  607_330_000_000,
		Size:   25,
		Action: byte(dbn.Action_Trade),
		Level: dbn.ConsolidatedBidAskPair{
			BidPx: 607_320_000_000,
			AskPx: 607_340_000_000,
			BidSz: 11,
			AskSz: 17,
		},
	}

	b.handleCmbp1(msg)

	select {
	case q := <-sub.Quotes:
		if q.Sym != "QQQ" || q.Bp != 607.32 || q.Ap != 607.34 {
			t.Fatalf("quote = %#v, want QQQ 607.32 x 607.34", q)
		}
	default:
		t.Fatal("expected quote dispatch")
	}

	select {
	case tr := <-sub.Trades:
		if tr.Sym != "QQQ" || tr.P != 607.33 || tr.S != 25 {
			t.Fatalf("trade = %#v, want QQQ 607.33 x 25", tr)
		}
	default:
		t.Fatal("expected trade dispatch")
	}
}

func TestHandleOhlcv1mDispatchesAggregateMinute(t *testing.T) {
	b, err := NewBroker(strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	b.symbolMap[123] = "QQQ"

	sub := b.Subscribe("QQQ", marketdata.StreamKinds{Minutes: true})
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}
	defer sub.Close()

	msg := &dbn.OhlcvMsg{
		Header: dbn.RHeader{
			InstrumentID: 123,
			TsEvent:      uint64(1710000000000000000),
		},
		Open:   607_300_000_000,
		High:   607_500_000_000,
		Low:    607_200_000_000,
		Close:  607_400_000_000,
		Volume: 12345,
	}

	b.handleOhlcv1m(msg)

	select {
	case bar := <-sub.Minutes:
		if bar.Sym != "QQQ" || bar.O != 607.3 || bar.H != 607.5 || bar.L != 607.2 || bar.C != 607.4 || bar.V != 12345 {
			t.Fatalf("bar = %#v, want native QQQ ohlcv-1m dispatch", bar)
		}
	default:
		t.Fatal("expected aggregate-minute dispatch")
	}
}

func stringsJoin(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += sep + p
	}
	return out
}
