package databento

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	dbn "github.com/NimbleMarkets/dbn-go"
)

func TestAdjustedRangeFormForAvailableEndClampsEnd(t *testing.T) {
	form := url.Values{}
	form.Set("start", "2026-03-11T08:06:00.000000000Z")
	form.Set("end", "2026-03-11T19:53:31.000000000Z")
	form.Set("symbols", "AAPL")

	body := []byte(`{"detail":{"case":"data_end_after_available_end","message":"The dataset EQUS.MINI has data available up to '2026-03-11 19:40:00+00:00'.","status_code":422,"payload":{"dataset":"EQUS.MINI","start":"2026-03-11T08:06:00.000000000Z","end":"2026-03-11T19:53:31.000000000Z","available_start":"2023-03-28T00:00:00.000000000Z","available_end":"2026-03-11T19:40:00.000000000Z"}}}`)

	got, ok, noData := adjustedRangeFormForAvailableEnd(form, body)
	if noData {
		t.Fatal("expected retry form, got noData")
	}
	if !ok {
		t.Fatal("expected retry form")
	}
	if got.Get("end") != "2026-03-11T19:40:00Z" {
		t.Fatalf("end = %q, want %q", got.Get("end"), "2026-03-11T19:40:00Z")
	}
	if got.Get("symbols") != "AAPL" {
		t.Fatalf("symbols = %q, want AAPL", got.Get("symbols"))
	}
	if form.Get("end") != "2026-03-11T19:53:31.000000000Z" {
		t.Fatalf("original form mutated: end = %q", form.Get("end"))
	}
}

func TestAdjustedRangeFormForAvailableEndRejectsInvalidWindow(t *testing.T) {
	form := url.Values{}
	form.Set("start", "2026-03-11T19:45:00.000000000Z")
	form.Set("end", "2026-03-11T19:53:31.000000000Z")

	body := []byte(`{"detail":{"case":"data_end_after_available_end","payload":{"available_end":"2026-03-11T19:40:00.000000000Z"}}}`)

	if got, ok, noData := adjustedRangeFormForAvailableEnd(form, body); ok || got != nil || noData {
		t.Fatalf("expected no retry form, got %#v", got)
	}
}

func TestAdjustedRangeFormForAvailableEndReturnsNoDataWhenStartAfterAvailableEnd(t *testing.T) {
	form := url.Values{}
	form.Set("start", "2026-03-12T14:27:00.000000000Z")
	form.Set("end", "2026-03-12T14:28:00.000000000Z")

	body := []byte(`{"detail":{"case":"data_start_after_available_end","payload":{"available_end":"2026-03-12T14:20:00.000000000Z"}}}`)

	got, ok, noData := adjustedRangeFormForAvailableEnd(form, body)
	if ok || got != nil {
		t.Fatalf("expected no retry form, got %#v", got)
	}
	if !noData {
		t.Fatal("expected noData")
	}
}

func TestPostRangeRetriesTimeoutAndSucceeds(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			time.Sleep(40 * time.Millisecond)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := &HistoricalClient{
		cfg: Config{
			APIKey:            "test-key",
			Dataset:           defaultDataset,
			StypeIn:           dbn.SType_RawSymbol,
			HistoricalTimeout: 15 * time.Millisecond,
			HistoricalRetries: 1,
			HistoricalBackoff: 5 * time.Millisecond,
		},
		httpClient: server.Client(),
		rangeURL:   server.URL,
	}

	form := url.Values{}
	form.Set("symbols", "AAPL")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	body, err := client.postRange(ctx, form)
	if err != nil {
		t.Fatalf("postRange() error = %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body = %q, want %q", string(body), "ok")
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRangeOhlcv1mReturnsEmptyWhenRequestedWindowIsAfterAvailableEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"detail":{"case":"data_start_after_available_end","payload":{"available_end":"2026-03-12T14:20:00.000000000Z"}}}`))
	}))
	defer server.Close()

	client := &HistoricalClient{
		cfg: Config{
			APIKey:            "test-key",
			Dataset:           defaultDataset,
			StypeIn:           dbn.SType_RawSymbol,
			HistoricalTimeout: time.Second,
			HistoricalRetries: 1,
			HistoricalBackoff: 5 * time.Millisecond,
		},
		httpClient: server.Client(),
		rangeURL:   server.URL,
	}

	start := time.Date(2026, time.March, 12, 14, 27, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	bars, err := client.RangeOhlcv1m(context.Background(), "AAPL", start, end)
	if err != nil {
		t.Fatalf("RangeOhlcv1m() error = %v", err)
	}
	if len(bars) != 0 {
		t.Fatalf("bars len = %d, want 0", len(bars))
	}
}
