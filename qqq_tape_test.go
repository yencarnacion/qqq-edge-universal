package main

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	md "qqq-edge-universal/internal/marketdata"
)

func TestLoadQQQHoldingsFiltersAndKeepsRawETFWeights(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "qqq.csv")
	csv := `No.,Symbol,Name,% Weight,Shares
1,AAPL,Apple Inc.,10.00%,"1"
2,,Cash,0.50%,"1"
3,BME:FER,Ferrovial SE,0.25%,"1"
4,MSFT,Microsoft Corporation,5.00%,"1"
`
	if err := os.WriteFile(path, []byte(csv), 0o644); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}

	got, err := loadQQQHoldings(path, 10)
	if err != nil {
		t.Fatalf("loadQQQHoldings: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("leaders len = %d, want 2", len(got))
	}
	if got[0].Symbol != "AAPL" || got[1].Symbol != "MSFT" {
		t.Fatalf("symbols = %#v, want [AAPL MSFT]", got)
	}
	if math.Abs(got[0].Weight-0.10) > 1e-9 || math.Abs(got[1].Weight-0.05) > 1e-9 {
		t.Fatalf("weights = %#v, want raw ETF weights [0.10 0.05]", got)
	}
	sum := got[0].Weight + got[1].Weight
	if math.Abs(sum-0.15) > 1e-9 {
		t.Fatalf("weight sum = %.12f, want 0.15", sum)
	}
}

func TestQQQFairValueTurnsPositiveWhenLeadersLeadHigher(t *testing.T) {
	et := mustET("America/New_York")
	eng := newQQQTapeEngine(newHub(10), et, []qqqHolding{{Symbol: "AAPL", Weight: 0.60}, {Symbol: "MSFT", Weight: 0.40}})

	base := time.Date(2026, time.March, 2, 10, 0, 0, 0, et)

	eng.OnQuote(md.Quote{Sym: "QQQ", Bp: 500.00, Bs: 12, Ap: 500.02, As: 6, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.00, Bs: 20, Ap: 220.02, As: 12, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.00, Bs: 18, Ap: 410.02, As: 10, T: base.UnixMilli()})

	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.18, Bs: 26, Ap: 220.20, As: 8, T: base.Add(900 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "AAPL", P: 220.20, S: 400, T: base.Add(910 * time.Millisecond).UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.28, Bs: 22, Ap: 410.30, As: 8, T: base.Add(920 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "MSFT", P: 410.30, S: 200, T: base.Add(930 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "QQQ", P: 500.02, S: 100, T: base.Add(940 * time.Millisecond).UnixMilli()})

	snap := eng.Snapshot()
	if snap.EdgeBps <= 0 {
		t.Fatalf("edge_bps = %.3f, want positive", snap.EdgeBps)
	}
	if snap.FairValue <= snap.Mid {
		t.Fatalf("fair_value = %.4f, mid = %.4f, want fair value > mid", snap.FairValue, snap.Mid)
	}
	if snap.LeadLagGapBps <= 0 {
		t.Fatalf("lead_lag_gap_bps = %.3f, want positive", snap.LeadLagGapBps)
	}
	if snap.LeaderFlow1000 <= 0 {
		t.Fatalf("leader_flow_1000 = %.3f, want positive", snap.LeaderFlow1000)
	}
	if snap.Top[0].Contribution <= 0 {
		t.Fatalf("top contribution = %#v, want positive", snap.Top)
	}
}

func TestQQQFairValueRespectsTrackedCoverageWithoutRenormalizing(t *testing.T) {
	et := mustET("America/New_York")
	eng := newQQQTapeEngine(newHub(10), et, []qqqHolding{
		{Symbol: "AAPL", Weight: 0.30},
		{Symbol: "MSFT", Weight: 0.20},
	})

	base := time.Date(2026, time.March, 2, 10, 0, 0, 0, et)
	eng.OnQuote(md.Quote{Sym: "QQQ", Bp: 500.00, Bs: 12, Ap: 500.02, As: 6, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.00, Bs: 20, Ap: 220.02, As: 12, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.00, Bs: 18, Ap: 410.02, As: 10, T: base.UnixMilli()})

	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.22, Bs: 22, Ap: 220.24, As: 8, T: base.Add(900 * time.Millisecond).UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.41, Bs: 19, Ap: 410.43, As: 8, T: base.Add(920 * time.Millisecond).UnixMilli()})

	snap := eng.Snapshot()
	if math.Abs(snap.BasketCoverage-0.50) > 1e-9 {
		t.Fatalf("basket_coverage = %.4f, want 0.50", snap.BasketCoverage)
	}
	if snap.FairGapBps <= 0 {
		t.Fatalf("fair_gap_bps = %.3f, want positive", snap.FairGapBps)
	}
	if snap.FairGapBps >= 4.0 {
		t.Fatalf("fair_gap_bps = %.3f, want tracked coverage preserved (not renormalized)", snap.FairGapBps)
	}
}

func TestQQQFairValueWeightsLeaderNotionalNotJustDirection(t *testing.T) {
	et := mustET("America/New_York")
	eng := newQQQTapeEngine(newHub(10), et, []qqqHolding{{Symbol: "AAPL", Weight: 0.50}, {Symbol: "MSFT", Weight: 0.50}})

	base := time.Date(2026, time.March, 2, 10, 0, 0, 0, et)
	eng.OnQuote(md.Quote{Sym: "QQQ", Bp: 500.00, Bs: 12, Ap: 500.02, As: 6, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.00, Bs: 20, Ap: 220.02, As: 8, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.00, Bs: 18, Ap: 410.02, As: 9, T: base.UnixMilli()})

	eng.OnTrade(md.Trade{Sym: "AAPL", P: 220.02, S: 500, T: base.Add(100 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "MSFT", P: 410.00, S: 10, T: base.Add(110 * time.Millisecond).UnixMilli()})

	snap := eng.Snapshot()
	if snap.LeaderFlow250 <= 0 {
		t.Fatalf("leader_flow_250 = %.3f, want positive because buy notional dominates", snap.LeaderFlow250)
	}
}

func TestQQQTapeTradableTransitionDoesNotEmitLiveAlerts(t *testing.T) {
	et := mustET("America/New_York")
	h := newHub(10)
	eng := newQQQTapeEngine(h, et, []qqqHolding{
		{Symbol: "AAPL", Weight: 0.55},
		{Symbol: "MSFT", Weight: 0.30},
		{Symbol: "NVDA", Weight: 0.15},
	})

	base := time.Date(2026, time.March, 2, 10, 0, 0, 0, et)

	eng.OnQuote(md.Quote{Sym: "QQQ", Bp: 500.00, Bs: 15, Ap: 500.02, As: 11, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.00, Bs: 18, Ap: 220.02, As: 10, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.00, Bs: 16, Ap: 410.02, As: 11, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "NVDA", Bp: 900.00, Bs: 14, Ap: 900.04, As: 12, T: base.UnixMilli()})

	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.09, Bs: 22, Ap: 220.11, As: 9, T: base.Add(900 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "AAPL", P: 220.11, S: 160, T: base.Add(950 * time.Millisecond).UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.10, Bs: 18, Ap: 410.12, As: 10, T: base.Add(1000 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "MSFT", P: 410.12, S: 90, T: base.Add(1050 * time.Millisecond).UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "NVDA", Bp: 900.52, Bs: 15, Ap: 900.56, As: 11, T: base.Add(1100 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "NVDA", P: 900.56, S: 40, T: base.Add(1150 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "QQQ", P: 500.02, S: 200, T: base.Add(1200 * time.Millisecond).UnixMilli()})

	alerts := h.getHistory()
	if len(alerts) != 0 {
		t.Fatalf("alerts len = %d, want 0", len(alerts))
	}

	eng.OnTrade(md.Trade{Sym: "QQQ", P: 500.02, S: 100, T: base.Add(1300 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "AAPL", P: 220.11, S: 40, T: base.Add(1350 * time.Millisecond).UnixMilli()})

	alerts = h.getHistory()
	if len(alerts) != 0 {
		t.Fatalf("alerts len after repeated tradable updates = %d, want 0", len(alerts))
	}
}

func TestQQQTapeSnapshotRegressionAligned(t *testing.T) {
	got := runQQQTapeRegressionScenario(t, false)
	want := qqqTapeMsg{
		Type:           "qqq_tape",
		Time:           "10:00:01 ET",
		Score:          100,
		Bias:           "Strong Buy",
		QQQPrice:       500.02,
		Mid:            500.01,
		FairValue:      500.1432,
		FairGapBps:     2.665,
		FairGapCents:   0.1332,
		EdgeBps:        5.092,
		EdgeCents:      0.2546,
		ExecEdgeBps:    5.092,
		ExecEdgeCents:  0.2546,
		SpreadBps:      0.4,
		LeaderBreadth:  2.665,
		BasketCoverage: 1,
		ResidualWeight: 0,
		TradeImpulse:   1.5,
		QuoteImbalance: 0.154,
		MicroEdge:      0.154,
		LeaderRetBps:   2.665,
		QQQRetBps:      0,
		LeadLagGapBps:  2.665,
		LeaderFlow250:  1,
		LeaderFlow1000: 1,
		LeaderFlow3000: 1,
		QQQFlow250:     1,
		QQQFlow1000:    1,
		FreshnessMs:    290,
		Tradable:       true,
		Top: []qqqLeaderContribution{
			{Sym: "AAPL", Weight: 0.55, Score: 4.075, Contribution: 2.241},
			{Sym: "MSFT", Weight: 0.3, Score: 2.756, Contribution: 0.827},
			{Sym: "NVDA", Weight: 0.15, Score: 4.905, Contribution: 0.736},
		},
		TSUnix: 1772463601300,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("aligned snapshot changed\n got: %+v\nwant: %+v", got, want)
	}
}

func TestQQQTapeSnapshotRegressionConflicted(t *testing.T) {
	got := runQQQTapeRegressionScenario(t, true)
	want := qqqTapeMsg{
		Type:           "qqq_tape",
		Time:           "10:00:01 ET",
		Score:          70.4,
		Bias:           "Strong Buy",
		QQQPrice:       500,
		Mid:            500.01,
		FairValue:      500.1432,
		FairGapBps:     2.665,
		FairGapCents:   0.1332,
		EdgeBps:        4.344,
		EdgeCents:      0.2172,
		ExecEdgeBps:    4.344,
		ExecEdgeCents:  0.2172,
		SpreadBps:      0.4,
		LeaderBreadth:  2.665,
		BasketCoverage: 1,
		ResidualWeight: 0,
		TradeImpulse:   1.5,
		QuoteImbalance: -0.538,
		MicroEdge:      -0.538,
		LeaderRetBps:   2.665,
		QQQRetBps:      0,
		LeadLagGapBps:  2.665,
		LeaderFlow250:  1,
		LeaderFlow1000: 1,
		LeaderFlow3000: 1,
		QQQFlow250:     -1,
		QQQFlow1000:    -1,
		FreshnessMs:    290,
		Tradable:       false,
		Top: []qqqLeaderContribution{
			{Sym: "AAPL", Weight: 0.55, Score: 4.075, Contribution: 2.241},
			{Sym: "MSFT", Weight: 0.3, Score: 2.756, Contribution: 0.827},
			{Sym: "NVDA", Weight: 0.15, Score: 4.905, Contribution: 0.736},
		},
		TSUnix: 1772463601300,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("conflicted snapshot changed\n got: %+v\nwant: %+v", got, want)
	}
}

func runQQQTapeRegressionScenario(t *testing.T, conflictQQQ bool) qqqTapeMsg {
	t.Helper()

	et := mustET("America/New_York")
	eng := newQQQTapeEngine(nil, et, []qqqHolding{
		{Symbol: "AAPL", Weight: 0.55},
		{Symbol: "MSFT", Weight: 0.30},
		{Symbol: "NVDA", Weight: 0.15},
	})
	base := time.Date(2026, time.March, 2, 10, 0, 0, 0, et)

	qqqBidSize, qqqAskSize := 15.0, 11.0
	qqqTradePrice := 500.02
	if conflictQQQ {
		qqqBidSize, qqqAskSize = 6.0, 20.0
		qqqTradePrice = 500.00
	}

	eng.OnQuote(md.Quote{Sym: "QQQ", Bp: 500.00, Bs: int64(qqqBidSize), Ap: 500.02, As: int64(qqqAskSize), T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.00, Bs: 18, Ap: 220.02, As: 10, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.00, Bs: 16, Ap: 410.02, As: 11, T: base.UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "NVDA", Bp: 900.00, Bs: 14, Ap: 900.04, As: 12, T: base.UnixMilli()})

	eng.OnQuote(md.Quote{Sym: "AAPL", Bp: 220.09, Bs: 22, Ap: 220.11, As: 9, T: base.Add(900 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "AAPL", P: 220.11, S: 160, T: base.Add(950 * time.Millisecond).UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "MSFT", Bp: 410.10, Bs: 18, Ap: 410.12, As: 10, T: base.Add(1000 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "MSFT", P: 410.12, S: 90, T: base.Add(1050 * time.Millisecond).UnixMilli()})
	eng.OnQuote(md.Quote{Sym: "NVDA", Bp: 900.52, Bs: 15, Ap: 900.56, As: 11, T: base.Add(1100 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "NVDA", P: 900.56, S: 40, T: base.Add(1150 * time.Millisecond).UnixMilli()})
	eng.OnTrade(md.Trade{Sym: "QQQ", P: qqqTradePrice, S: 200, T: base.Add(1200 * time.Millisecond).UnixMilli()})

	nowRecv := base.Add(1300 * time.Millisecond)

	eng.mu.Lock()
	defer eng.mu.Unlock()

	// Freeze receive times so the snapshot remains deterministic across test runs.
	eng.qqqQuote.ReceiveTime = base.Add(1250 * time.Millisecond)
	eng.qqqLastRecvAt = base.Add(1200 * time.Millisecond)
	eng.leaders["AAPL"].Quote.ReceiveTime = base.Add(900 * time.Millisecond)
	eng.leaders["AAPL"].LastReceiveAt = base.Add(950 * time.Millisecond)
	eng.leaders["MSFT"].Quote.ReceiveTime = base.Add(1000 * time.Millisecond)
	eng.leaders["MSFT"].LastReceiveAt = base.Add(1050 * time.Millisecond)
	eng.leaders["NVDA"].Quote.ReceiveTime = base.Add(1100 * time.Millisecond)
	eng.leaders["NVDA"].LastReceiveAt = base.Add(1150 * time.Millisecond)

	return eng.snapshotLocked(nowRecv)
}
