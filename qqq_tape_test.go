package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	poly "qqq-edge/internal/polygon"
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

	eng.OnQuote(poly.Quote{Sym: "QQQ", Bp: 500.00, Bs: 12, Ap: 500.02, As: 6, T: base.UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "AAPL", Bp: 220.00, Bs: 20, Ap: 220.02, As: 12, T: base.UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "MSFT", Bp: 410.00, Bs: 18, Ap: 410.02, As: 10, T: base.UnixMilli()})

	eng.OnQuote(poly.Quote{Sym: "AAPL", Bp: 220.18, Bs: 26, Ap: 220.20, As: 8, T: base.Add(900 * time.Millisecond).UnixMilli()})
	eng.OnTrade(poly.Trade{Sym: "AAPL", P: 220.20, S: 400, T: base.Add(910 * time.Millisecond).UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "MSFT", Bp: 410.28, Bs: 22, Ap: 410.30, As: 8, T: base.Add(920 * time.Millisecond).UnixMilli()})
	eng.OnTrade(poly.Trade{Sym: "MSFT", P: 410.30, S: 200, T: base.Add(930 * time.Millisecond).UnixMilli()})
	eng.OnTrade(poly.Trade{Sym: "QQQ", P: 500.02, S: 100, T: base.Add(940 * time.Millisecond).UnixMilli()})

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
	eng.OnQuote(poly.Quote{Sym: "QQQ", Bp: 500.00, Bs: 12, Ap: 500.02, As: 6, T: base.UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "AAPL", Bp: 220.00, Bs: 20, Ap: 220.02, As: 12, T: base.UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "MSFT", Bp: 410.00, Bs: 18, Ap: 410.02, As: 10, T: base.UnixMilli()})

	eng.OnQuote(poly.Quote{Sym: "AAPL", Bp: 220.22, Bs: 22, Ap: 220.24, As: 8, T: base.Add(900 * time.Millisecond).UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "MSFT", Bp: 410.41, Bs: 19, Ap: 410.43, As: 8, T: base.Add(920 * time.Millisecond).UnixMilli()})

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
	eng.OnQuote(poly.Quote{Sym: "QQQ", Bp: 500.00, Bs: 12, Ap: 500.02, As: 6, T: base.UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "AAPL", Bp: 220.00, Bs: 20, Ap: 220.02, As: 8, T: base.UnixMilli()})
	eng.OnQuote(poly.Quote{Sym: "MSFT", Bp: 410.00, Bs: 18, Ap: 410.02, As: 9, T: base.UnixMilli()})

	eng.OnTrade(poly.Trade{Sym: "AAPL", P: 220.02, S: 500, T: base.Add(100 * time.Millisecond).UnixMilli()})
	eng.OnTrade(poly.Trade{Sym: "MSFT", P: 410.00, S: 10, T: base.Add(110 * time.Millisecond).UnixMilli()})

	snap := eng.Snapshot()
	if snap.LeaderFlow250 <= 0 {
		t.Fatalf("leader_flow_250 = %.3f, want positive because buy notional dominates", snap.LeaderFlow250)
	}
}
