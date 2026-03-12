package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	md "qqq-edge-universal/internal/marketdata"
)

const (
	qqqTapeLeaderLimit      = 25
	qqqTapeHistoryRetention = 14 * time.Second
	qqqTapeBroadcastMinGap  = 200 * time.Millisecond
	// Shift the tape model from sub-second twitching to a human-usable
	// 1-10 second execution horizon.
	qqqTapeFastWindow        = 1 * time.Second
	qqqTapeMediumWindow      = 3 * time.Second
	qqqTapeSlowWindow        = 8 * time.Second
	qqqTapeTradableFreshness = 2500 * time.Millisecond
)

type qqqHolding struct {
	Symbol string
	Name   string
	Weight float64
}

func loadQQQHoldings(path string, leaderLimit int) ([]qqqHolding, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("qqq holdings csv is empty")
	}

	header := make(map[string]int, len(rows[0]))
	for i, h := range rows[0] {
		header[strings.TrimSpace(h)] = i
	}
	symIdx, ok := header["Symbol"]
	if !ok {
		return nil, fmt.Errorf("qqq holdings csv missing Symbol column")
	}
	weightIdx, ok := header["% Weight"]
	if !ok {
		return nil, fmt.Errorf("qqq holdings csv missing %% Weight column")
	}
	nameIdx := -1
	if v, ok := header["Name"]; ok {
		nameIdx = v
	}

	out := make([]qqqHolding, 0, len(rows)-1)
	seen := make(map[string]struct{}, len(rows)-1)
	for _, row := range rows[1:] {
		if symIdx >= len(row) || weightIdx >= len(row) {
			continue
		}
		sym := strings.ToUpper(strings.TrimSpace(row[symIdx]))
		if !isValidQQQLeaderSymbol(sym) {
			continue
		}
		if _, dup := seen[sym]; dup {
			continue
		}
		wt := parseQQQWeight(row[weightIdx])
		if wt <= 0 {
			continue
		}
		name := ""
		if nameIdx >= 0 && nameIdx < len(row) {
			name = strings.TrimSpace(row[nameIdx])
		}
		seen[sym] = struct{}{}
		out = append(out, qqqHolding{Symbol: sym, Name: name, Weight: wt})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("qqq holdings csv had no valid stock symbols")
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Weight == out[j].Weight {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].Weight > out[j].Weight
	})

	if leaderLimit > 0 && len(out) > leaderLimit {
		out = out[:leaderLimit]
	}

	var sum float64
	for _, h := range out {
		sum += h.Weight
	}
	if sum <= 0 {
		return nil, fmt.Errorf("qqq holdings weights sum to zero")
	}
	return out, nil
}

func isValidQQQLeaderSymbol(sym string) bool {
	if sym == "" {
		return false
	}
	for i, r := range sym {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		case r == '.' || r == '-':
		default:
			return false
		}
	}
	return true
}

func parseQQQWeight(raw string) float64 {
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw), "%"))
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v / 100.0
}

type tapeQuoteState struct {
	Bid         float64
	Ask         float64
	BidSize     float64
	AskSize     float64
	MarketTime  time.Time
	ReceiveTime time.Time
}

type tapeFlowEvent struct {
	At             time.Time
	Notional       float64
	SignedNotional float64
}

type tapeMidEvent struct {
	At  time.Time
	Mid float64
}

type leaderTapeState struct {
	Quote         tapeQuoteState
	LastTradePx   float64
	LastTradeAt   time.Time
	LastReceiveAt time.Time
	Trades        []tapeFlowEvent
	MidHistory    []tapeMidEvent
}

type qqqLeaderContribution struct {
	Sym          string  `json:"sym"`
	Weight       float64 `json:"weight"`
	Score        float64 `json:"score"`
	Contribution float64 `json:"contribution"`
}

type qqqTapeMsg struct {
	Type           string                  `json:"type"`
	Time           string                  `json:"time"`
	Score          float64                 `json:"score"`
	Bias           string                  `json:"bias"`
	QQQPrice       float64                 `json:"qqq_price"`
	Mid            float64                 `json:"mid"`
	FairValue      float64                 `json:"fair_value"`
	FairGapBps     float64                 `json:"fair_gap_bps"`
	FairGapCents   float64                 `json:"fair_gap_cents"`
	EdgeBps        float64                 `json:"edge_bps"`
	EdgeCents      float64                 `json:"edge_cents"`
	ExecEdgeBps    float64                 `json:"exec_edge_bps"`
	ExecEdgeCents  float64                 `json:"exec_edge_cents"`
	SpreadBps      float64                 `json:"spread_bps"`
	LeaderBreadth  float64                 `json:"leader_breadth"`
	BasketCoverage float64                 `json:"basket_coverage"`
	ResidualWeight float64                 `json:"residual_weight"`
	TradeImpulse   float64                 `json:"trade_impulse"`
	QuoteImbalance float64                 `json:"quote_imbalance"`
	MicroEdge      float64                 `json:"micro_edge"`
	LeaderRetBps   float64                 `json:"leader_ret_bps"`
	QQQRetBps      float64                 `json:"qqq_ret_bps"`
	LeadLagGapBps  float64                 `json:"lead_lag_gap_bps"`
	LeaderFlow250  float64                 `json:"leader_flow_250"`
	LeaderFlow1000 float64                 `json:"leader_flow_1000"`
	LeaderFlow3000 float64                 `json:"leader_flow_3000"`
	QQQFlow250     float64                 `json:"qqq_flow_250"`
	QQQFlow1000    float64                 `json:"qqq_flow_1000"`
	FreshnessMs    int64                   `json:"freshness_ms"`
	Tradable       bool                    `json:"tradable"`
	Top            []qqqLeaderContribution `json:"top,omitempty"`
	TSUnix         int64                   `json:"ts_unix"`
}

type qqqTapeEngine struct {
	mu sync.RWMutex

	h  *hub
	et *time.Location

	leaderSymbols    []string
	leaderWeights    map[string]float64
	trackedWeightSum float64
	residualWeight   float64
	leaders          map[string]*leaderTapeState

	qqqQuote       tapeQuoteState
	qqqLastTrade   float64
	qqqLastTradeAt time.Time
	qqqLastRecvAt  time.Time
	qqqTrades      []tapeFlowEvent
	qqqMidHistory  []tapeMidEvent

	lastMsg       qqqTapeMsg
	lastBroadcast time.Time
	lastTradable  bool
	lastTradeDir  int
}

func newQQQTapeEngine(h *hub, et *time.Location, leaders []qqqHolding) *qqqTapeEngine {
	if et == nil {
		et = time.UTC
	}
	syms := make([]string, 0, len(leaders))
	weights := make(map[string]float64, len(leaders))
	leaderStates := make(map[string]*leaderTapeState, len(leaders))
	var sum float64
	for _, ld := range leaders {
		sym := strings.ToUpper(strings.TrimSpace(ld.Symbol))
		if sym == "" || ld.Weight <= 0 {
			continue
		}
		syms = append(syms, sym)
		weights[sym] = ld.Weight
		leaderStates[sym] = &leaderTapeState{
			Trades:     make([]tapeFlowEvent, 0, 64),
			MidHistory: make([]tapeMidEvent, 0, 64),
		}
		sum += ld.Weight
	}
	if sum > 1.0+1e-9 {
		for sym, w := range weights {
			weights[sym] = w / sum
		}
		sum = 1.0
	}
	trackedWeightSum := clampTape(sum, 0, 1)

	e := &qqqTapeEngine{
		h:                h,
		et:               et,
		leaderSymbols:    syms,
		leaderWeights:    weights,
		trackedWeightSum: trackedWeightSum,
		residualWeight:   clampTape(1.0-trackedWeightSum, 0, 1),
		leaders:          leaderStates,
		qqqTrades:        make([]tapeFlowEvent, 0, 128),
		qqqMidHistory:    make([]tapeMidEvent, 0, 128),
	}
	e.Reset()
	return e
}

func (e *qqqTapeEngine) Symbols() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]string, 0, 1+len(e.leaderSymbols))
	out = append(out, "QQQ")
	out = append(out, e.leaderSymbols...)
	return out
}

func (e *qqqTapeEngine) LeaderCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.leaderSymbols)
}

func (e *qqqTapeEngine) Snapshot() qqqTapeMsg {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastMsg
}

func (e *qqqTapeEngine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.qqqQuote = tapeQuoteState{}
	e.qqqLastTrade = 0
	e.qqqLastTradeAt = time.Time{}
	e.qqqLastRecvAt = time.Time{}
	e.qqqTrades = e.qqqTrades[:0]
	e.qqqMidHistory = e.qqqMidHistory[:0]
	for sym := range e.leaders {
		e.leaders[sym] = &leaderTapeState{
			Trades:     make([]tapeFlowEvent, 0, 64),
			MidHistory: make([]tapeMidEvent, 0, 64),
		}
	}
	e.lastBroadcast = time.Time{}
	e.lastMsg = neutralQQQTapeMsg(time.Now().In(e.et))
	e.lastTradable = false
	e.lastTradeDir = 0
}

func neutralQQQTapeMsg(at time.Time) qqqTapeMsg {
	if at.IsZero() {
		at = time.Now()
	}
	return qqqTapeMsg{
		Type:   "qqq_tape",
		Time:   etClock(at),
		Bias:   "Balanced",
		TSUnix: at.UnixMilli(),
	}
}

func (e *qqqTapeEngine) OnQuote(q md.Quote) {
	sym := strings.ToUpper(strings.TrimSpace(q.Sym))
	if sym == "" {
		return
	}
	recvTS := time.Now().In(e.et)
	eventTS := recvTS
	if q.T > 0 {
		eventTS = time.UnixMilli(q.T).In(e.et)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	state := tapeQuoteState{
		Bid:         q.Bp,
		Ask:         q.Ap,
		BidSize:     float64(q.Bs),
		AskSize:     float64(q.As),
		MarketTime:  eventTS,
		ReceiveTime: recvTS,
	}
	mid, ok := validMid(state)
	if sym == "QQQ" {
		e.qqqQuote = state
		if ok {
			e.qqqMidHistory = appendMidEvent(e.qqqMidHistory, tapeMidEvent{At: eventTS, Mid: mid})
			e.qqqMidHistory = pruneMidEvents(e.qqqMidHistory, eventTS)
		}
		e.maybeBroadcastLocked(recvTS)
		return
	}
	if st, exists := e.leaders[sym]; exists {
		st.Quote = state
		if ok {
			st.MidHistory = appendMidEvent(st.MidHistory, tapeMidEvent{At: eventTS, Mid: mid})
			st.MidHistory = pruneMidEvents(st.MidHistory, eventTS)
		}
		e.maybeBroadcastLocked(recvTS)
	}
}

func (e *qqqTapeEngine) OnTrade(t md.Trade) {
	sym := strings.ToUpper(strings.TrimSpace(t.Sym))
	if sym == "" {
		return
	}
	recvTS := time.Now().In(e.et)
	eventTS := recvTS
	if t.T > 0 {
		eventTS = time.UnixMilli(t.T).In(e.et)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if sym == "QQQ" {
		sign := classifyTapeTrade(t.P, e.qqqQuote, e.qqqLastTrade)
		e.qqqLastTrade = t.P
		e.qqqLastTradeAt = eventTS
		e.qqqLastRecvAt = recvTS
		if sign != 0 && t.S > 0 {
			notional := math.Abs(t.P * float64(t.S))
			e.qqqTrades = appendFlowEvent(e.qqqTrades, tapeFlowEvent{At: eventTS, Notional: notional, SignedNotional: sign * notional})
			e.qqqTrades = pruneFlowEvents(e.qqqTrades, eventTS)
		}
		if mid, ok := validMid(e.qqqQuote); ok {
			e.qqqMidHistory = appendMidEvent(e.qqqMidHistory, tapeMidEvent{At: eventTS, Mid: mid})
		} else if t.P > 0 {
			e.qqqMidHistory = appendMidEvent(e.qqqMidHistory, tapeMidEvent{At: eventTS, Mid: t.P})
		}
		e.qqqMidHistory = pruneMidEvents(e.qqqMidHistory, eventTS)
		e.maybeBroadcastLocked(recvTS)
		return
	}

	st, ok := e.leaders[sym]
	if !ok {
		return
	}

	sign := classifyTapeTrade(t.P, st.Quote, st.LastTradePx)
	st.LastTradePx = t.P
	st.LastTradeAt = eventTS
	st.LastReceiveAt = recvTS
	if sign != 0 && t.S > 0 {
		notional := math.Abs(t.P * float64(t.S))
		st.Trades = appendFlowEvent(st.Trades, tapeFlowEvent{At: eventTS, Notional: notional, SignedNotional: sign * notional})
		st.Trades = pruneFlowEvents(st.Trades, eventTS)
	}
	if mid, ok := validMid(st.Quote); ok {
		st.MidHistory = appendMidEvent(st.MidHistory, tapeMidEvent{At: eventTS, Mid: mid})
	} else if t.P > 0 {
		st.MidHistory = appendMidEvent(st.MidHistory, tapeMidEvent{At: eventTS, Mid: t.P})
	}
	st.MidHistory = pruneMidEvents(st.MidHistory, eventTS)
	e.maybeBroadcastLocked(recvTS)
}

func classifyTapeTrade(price float64, q tapeQuoteState, prevPx float64) float64 {
	if q.Bid > 0 && q.Ask > 0 && q.Ask >= q.Bid {
		if price >= q.Ask-1e-9 {
			return 1
		}
		if price <= q.Bid+1e-9 {
			return -1
		}
	}
	if prevPx > 0 {
		switch {
		case price > prevPx+1e-9:
			return 1
		case price < prevPx-1e-9:
			return -1
		}
	}
	return 0
}

func (e *qqqTapeEngine) maybeBroadcastLocked(nowRecv time.Time) {
	if nowRecv.IsZero() {
		nowRecv = time.Now().In(e.et)
	}
	prev := e.lastMsg
	msg := e.snapshotLocked(nowRecv)
	e.lastMsg = msg
	e.maybeEmitTradableAlertLocked(msg)

	if !e.lastBroadcast.IsZero() &&
		nowRecv.Sub(e.lastBroadcast) < qqqTapeBroadcastMinGap &&
		math.Abs(msg.Score-prev.Score) < 0.85 &&
		msg.Bias == prev.Bias &&
		math.Abs(msg.EdgeBps-prev.EdgeBps) < 0.12 {
		return
	}
	e.lastBroadcast = nowRecv
	if e.h != nil {
		e.h.broadcast(msg)
	}
}

func (e *qqqTapeEngine) maybeEmitTradableAlertLocked(msg qqqTapeMsg) {
	e.lastTradable = msg.Tradable
	if !msg.Tradable {
		e.lastTradeDir = 0
	}
}

func (e *qqqTapeEngine) latestSignalTimeLocked() time.Time {
	best := e.qqqQuote.MarketTime
	if e.qqqLastTradeAt.After(best) {
		best = e.qqqLastTradeAt
	}
	for _, sym := range e.leaderSymbols {
		st := e.leaders[sym]
		if st == nil {
			continue
		}
		if st.Quote.MarketTime.After(best) {
			best = st.Quote.MarketTime
		}
		if st.LastTradeAt.After(best) {
			best = st.LastTradeAt
		}
	}
	if best.IsZero() {
		best = time.Now().In(e.et)
	}
	return best
}

func (e *qqqTapeEngine) snapshotLocked(nowRecv time.Time) qqqTapeMsg {
	if nowRecv.IsZero() {
		nowRecv = time.Now().In(e.et)
	}
	signalNow := e.latestSignalTimeLocked()

	mid, _ := validMid(e.qqqQuote)
	price := e.qqqLastTrade
	if price <= 0 {
		price = mid
	}
	spreadBps := 0.0
	if mid > 0 && e.qqqQuote.Ask >= e.qqqQuote.Bid && e.qqqQuote.Bid > 0 {
		spreadBps = roundTape((e.qqqQuote.Ask-e.qqqQuote.Bid)/mid*10000.0, 3)
	}

	trackedLeaderRet1, trackedLeaderRet3, trackedLeaderRet8, leaderFlow1, leaderFlow3, leaderFlow8, top := e.leaderSignalsLocked(signalNow)
	qqqRet1 := windowReturn(e.qqqMidHistory, signalNow, qqqTapeFastWindow)
	qqqRet3 := windowReturn(e.qqqMidHistory, signalNow, qqqTapeMediumWindow)
	qqqRet8 := windowReturn(e.qqqMidHistory, signalNow, qqqTapeSlowWindow)
	qqqFlow1 := flowImbalance(e.qqqTrades, signalNow, qqqTapeFastWindow)
	qqqFlow3 := flowImbalance(e.qqqTrades, signalNow, qqqTapeMediumWindow)
	qqqFlow8 := flowImbalance(e.qqqTrades, signalNow, qqqTapeSlowWindow)
	quoteImb := tapeQuoteImbalance(e.qqqQuote)
	micro := tapeMicroEdge(e.qqqQuote)

	syntheticRet1 := trackedLeaderRet1 + e.residualWeight*qqqRet1
	syntheticRet3 := trackedLeaderRet3 + e.residualWeight*qqqRet3
	syntheticRet8 := trackedLeaderRet8 + e.residualWeight*qqqRet8

	fairGap1Bps := 10000.0 * (syntheticRet1 - qqqRet1)
	fairGap3Bps := 10000.0 * (syntheticRet3 - qqqRet3)
	fairGap8Bps := 10000.0 * (syntheticRet8 - qqqRet8)
	fairGapBps := 0.55*fairGap1Bps + 0.30*fairGap3Bps + 0.15*fairGap8Bps

	leaderFlowEdgeBps := 0.75*leaderFlow1 + 0.50*leaderFlow3 + 0.25*leaderFlow8
	qqqConfirmBps := 0.30*qqqFlow1 + 0.22*qqqFlow3 + 0.12*qqqFlow8 + 0.08*micro + 0.06*quoteImb

	fairPersistence := signAgreement(fairGap1Bps, fairGap3Bps, fairGap8Bps)
	flowPersistence := signAgreement(leaderFlow1, leaderFlow3, leaderFlow8)

	continuationBoost := 0.18*math.Copysign(math.Min(math.Abs(fairGapBps), 4.0), fairGapBps)*fairPersistence +
		0.10*math.Copysign(math.Min(math.Abs(leaderFlowEdgeBps), 2.0), leaderFlowEdgeBps)*flowPersistence
	conflictPenalty := conflictPenalty(fairGapBps, leaderFlowEdgeBps, qqqConfirmBps)

	execEdgeBps := fairGapBps + leaderFlowEdgeBps + 0.45*qqqConfirmBps + continuationBoost - conflictPenalty
	freshnessMs := e.freshnessLocked(nowRecv)
	if freshnessMs > 1000 {
		penalty := math.Exp(-float64(freshnessMs-1000) / 2600.0)
		execEdgeBps *= penalty
	}
	execEdgeBps = clampTape(execEdgeBps, -14, 14)
	fairValue := 0.0
	fairGapCents := 0.0
	execEdgeCents := 0.0
	if mid > 0 {
		fairValue = mid * (1 + fairGapBps/10000.0)
		fairGapCents = fairValue - mid
		execEdgeCents = mid * execEdgeBps / 10000.0
	}

	tradableThreshold := spreadBps*1.60 + 0.45
	aligned := sameDirection(fairGapBps, leaderFlowEdgeBps)
	qqqNotFighting := qqqConfirmBps == 0 || sameDirection(fairGapBps, qqqConfirmBps) || math.Abs(qqqConfirmBps) < 0.18
	tradable := mid > 0 &&
		spreadBps > 0 &&
		math.Abs(execEdgeBps) >= tradableThreshold &&
		math.Abs(fairGapBps) >= 0.45 &&
		fairPersistence >= 0.55 &&
		aligned &&
		qqqNotFighting &&
		freshnessMs <= int64(qqqTapeTradableFreshness/time.Millisecond)

	scoreBase := 0.0
	if tradableThreshold > 0 {
		scoreBase = (execEdgeBps / tradableThreshold) * 38.0
	} else {
		scoreBase = execEdgeBps * 40.0
	}
	score := clampTape(scoreBase, -100, 100)
	score *= 0.80 + 0.20*fairPersistence
	if !aligned {
		score *= 0.72
	}
	if !qqqNotFighting {
		score *= 0.80
	}
	if !tradable {
		score *= 0.88
	}
	if freshnessMs > int64(qqqTapeTradableFreshness/time.Millisecond) {
		score *= 0.78
	}
	score = roundTape(score, 1)

	trackedLeaderRetComposite := 0.55*trackedLeaderRet1 + 0.30*trackedLeaderRet3 + 0.15*trackedLeaderRet8
	qqqRetComposite := 0.55*qqqRet1 + 0.30*qqqRet3 + 0.15*qqqRet8

	return qqqTapeMsg{
		Type:           "qqq_tape",
		Time:           etClock(nowRecv),
		Score:          score,
		Bias:           tapeBias(score),
		QQQPrice:       roundTape(price, 4),
		Mid:            roundTape(mid, 4),
		FairValue:      roundTape(fairValue, 4),
		FairGapBps:     roundTape(fairGapBps, 3),
		FairGapCents:   roundTape(fairGapCents, 4),
		EdgeBps:        roundTape(execEdgeBps, 3),
		EdgeCents:      roundTape(execEdgeCents, 4),
		ExecEdgeBps:    roundTape(execEdgeBps, 3),
		ExecEdgeCents:  roundTape(execEdgeCents, 4),
		SpreadBps:      spreadBps,
		LeaderBreadth:  roundTape(fairGapBps, 3),
		BasketCoverage: roundTape(e.trackedWeightSum, 4),
		ResidualWeight: roundTape(e.residualWeight, 4),
		TradeImpulse:   roundTape(leaderFlowEdgeBps, 3),
		QuoteImbalance: roundTape(quoteImb, 3),
		MicroEdge:      roundTape(micro, 3),
		LeaderRetBps:   roundTape(10000.0*trackedLeaderRetComposite, 3),
		QQQRetBps:      roundTape(10000.0*qqqRetComposite, 3),
		LeadLagGapBps:  roundTape(fairGapBps, 3),
		LeaderFlow250:  roundTape(leaderFlow1, 3),
		LeaderFlow1000: roundTape(leaderFlow3, 3),
		LeaderFlow3000: roundTape(leaderFlow8, 3),
		QQQFlow250:     roundTape(qqqFlow1, 3),
		QQQFlow1000:    roundTape(qqqFlow3, 3),
		FreshnessMs:    freshnessMs,
		Tradable:       tradable,
		Top:            top,
		TSUnix:         nowRecv.UnixMilli(),
	}
}

func (e *qqqTapeEngine) leaderSignalsLocked(now time.Time) (retFast, retMed, retSlow, flowFast, flowMed, flowSlow float64, top []qqqLeaderContribution) {
	type leaderMetric struct {
		sym                string
		weight             float64
		rFast, rMed, rSlow float64
		fFast, fMed, fSlow float64
		gFast, gMed, gSlow float64
	}
	metrics := make([]leaderMetric, 0, len(e.leaderSymbols))
	var basketSignedFast, basketGrossFast float64
	var basketSignedMed, basketGrossMed float64
	var basketSignedSlow, basketGrossSlow float64
	for _, sym := range e.leaderSymbols {
		st := e.leaders[sym]
		if st == nil {
			continue
		}
		weight := e.leaderWeights[sym]
		rFast := windowReturn(st.MidHistory, now, qqqTapeFastWindow)
		rMed := windowReturn(st.MidHistory, now, qqqTapeMediumWindow)
		rSlow := windowReturn(st.MidHistory, now, qqqTapeSlowWindow)
		sFast, gFast := flowSums(st.Trades, now, qqqTapeFastWindow)
		sMed, gMed := flowSums(st.Trades, now, qqqTapeMediumWindow)
		sSlow, gSlow := flowSums(st.Trades, now, qqqTapeSlowWindow)
		fFast := normalizeFlow(sFast, gFast)
		fMed := normalizeFlow(sMed, gMed)
		fSlow := normalizeFlow(sSlow, gSlow)

		retFast += weight * rFast
		retMed += weight * rMed
		retSlow += weight * rSlow
		basketSignedFast += weight * sFast
		basketGrossFast += weight * gFast
		basketSignedMed += weight * sMed
		basketGrossMed += weight * gMed
		basketSignedSlow += weight * sSlow
		basketGrossSlow += weight * gSlow
		metrics = append(metrics, leaderMetric{sym: sym, weight: weight, rFast: rFast, rMed: rMed, rSlow: rSlow, fFast: fFast, fMed: fMed, fSlow: fSlow, gFast: gFast, gMed: gMed, gSlow: gSlow})
	}
	flowFast = normalizeFlow(basketSignedFast, basketGrossFast)
	flowMed = normalizeFlow(basketSignedMed, basketGrossMed)
	flowSlow = normalizeFlow(basketSignedSlow, basketGrossSlow)
	out := make([]qqqLeaderContribution, 0, len(metrics))
	for _, m := range metrics {
		activityFast := 0.0
		if basketGrossFast > 0 {
			activityFast = math.Sqrt(clampTape((m.weight*m.gFast)/basketGrossFast, 0, 1))
		}
		activityMed := 0.0
		if basketGrossMed > 0 {
			activityMed = math.Sqrt(clampTape((m.weight*m.gMed)/basketGrossMed, 0, 1))
		}
		activitySlow := 0.0
		if basketGrossSlow > 0 {
			activitySlow = math.Sqrt(clampTape((m.weight*m.gSlow)/basketGrossSlow, 0, 1))
		}
		flowBps := 0.75*m.fFast*(0.35+0.65*activityFast) + 0.50*m.fMed*(0.35+0.65*activityMed) + 0.25*m.fSlow*(0.35+0.65*activitySlow)
		symBps := 10000.0*(0.55*m.rFast+0.30*m.rMed+0.15*m.rSlow) + flowBps
		contrib := m.weight * symBps
		out = append(out, qqqLeaderContribution{Sym: m.sym, Weight: roundTape(m.weight, 4), Score: roundTape(symBps, 3), Contribution: roundTape(contrib, 3)})
	}
	sort.Slice(out, func(i, j int) bool {
		ai := math.Abs(out[i].Contribution)
		aj := math.Abs(out[j].Contribution)
		if ai == aj {
			return out[i].Sym < out[j].Sym
		}
		return ai > aj
	})
	if len(out) > 5 {
		out = out[:5]
	}
	return retFast, retMed, retSlow, flowFast, flowMed, flowSlow, out
}

func (e *qqqTapeEngine) freshnessLocked(nowRecv time.Time) int64 {
	worstAge := int64(0)
	qAge := qqqTapeHistoryRetention
	if !e.qqqQuote.ReceiveTime.IsZero() && !e.qqqLastRecvAt.IsZero() {
		if e.qqqQuote.ReceiveTime.After(e.qqqLastRecvAt) {
			qAge = nowRecv.Sub(e.qqqQuote.ReceiveTime)
		} else {
			qAge = nowRecv.Sub(e.qqqLastRecvAt)
		}
	} else if !e.qqqQuote.ReceiveTime.IsZero() {
		qAge = nowRecv.Sub(e.qqqQuote.ReceiveTime)
	} else if !e.qqqLastRecvAt.IsZero() {
		qAge = nowRecv.Sub(e.qqqLastRecvAt)
	}
	if qAge < 0 {
		qAge = 0
	}
	worstAge = int64(qAge / time.Millisecond)

	var weightedLeaderAge float64
	var weightSum float64
	for _, sym := range e.leaderSymbols {
		st := e.leaders[sym]
		if st == nil {
			continue
		}
		age := qqqTapeHistoryRetention
		switch {
		case !st.Quote.ReceiveTime.IsZero() && !st.LastReceiveAt.IsZero():
			if st.Quote.ReceiveTime.After(st.LastReceiveAt) {
				age = nowRecv.Sub(st.Quote.ReceiveTime)
			} else {
				age = nowRecv.Sub(st.LastReceiveAt)
			}
		case !st.Quote.ReceiveTime.IsZero():
			age = nowRecv.Sub(st.Quote.ReceiveTime)
		case !st.LastReceiveAt.IsZero():
			age = nowRecv.Sub(st.LastReceiveAt)
		}
		if age < 0 {
			age = 0
		}
		w := e.leaderWeights[sym]
		weightedLeaderAge += w * float64(age/time.Millisecond)
		weightSum += w
	}
	if weightSum > 0 {
		leaderAge := int64(weightedLeaderAge / weightSum)
		if leaderAge > worstAge {
			worstAge = leaderAge
		}
	}
	if worstAge < 0 {
		return 0
	}
	return worstAge
}

func validMid(q tapeQuoteState) (float64, bool) {
	if q.Bid <= 0 || q.Ask <= 0 || q.Ask < q.Bid {
		return 0, false
	}
	return 0.5 * (q.Bid + q.Ask), true
}

func appendMidEvent(dst []tapeMidEvent, ev tapeMidEvent) []tapeMidEvent {
	if ev.Mid <= 0 || ev.At.IsZero() {
		return dst
	}
	n := len(dst)
	if n == 0 {
		return append(dst, ev)
	}
	// fast path: append in order or replace same timestamp
	last := dst[n-1]
	if ev.At.After(last.At) {
		if last.Mid == ev.Mid {
			return append(dst, ev)
		}
		return append(dst, ev)
	}
	if ev.At.Equal(last.At) {
		dst[n-1] = ev
		return dst
	}
	// out-of-order event: keep the history time-sorted.
	i := sort.Search(len(dst), func(i int) bool { return !dst[i].At.Before(ev.At) })
	if i < len(dst) && dst[i].At.Equal(ev.At) {
		dst[i] = ev
		return dst
	}
	dst = append(dst, tapeMidEvent{})
	copy(dst[i+1:], dst[i:])
	dst[i] = ev
	return dst
}

func appendFlowEvent(dst []tapeFlowEvent, ev tapeFlowEvent) []tapeFlowEvent {
	if ev.At.IsZero() || ev.Notional <= 0 {
		return dst
	}
	n := len(dst)
	if n == 0 || ev.At.After(dst[n-1].At) || ev.At.Equal(dst[n-1].At) {
		return append(dst, ev)
	}
	i := sort.Search(len(dst), func(i int) bool { return !dst[i].At.Before(ev.At) })
	dst = append(dst, tapeFlowEvent{})
	copy(dst[i+1:], dst[i:])
	dst[i] = ev
	return dst
}

func pruneMidEvents(dst []tapeMidEvent, now time.Time) []tapeMidEvent {
	cutoff := now.Add(-qqqTapeHistoryRetention)
	keep := dst[:0]
	for _, ev := range dst {
		if ev.At.After(cutoff) || ev.At.Equal(cutoff) {
			keep = append(keep, ev)
		}
	}
	return keep
}

func pruneFlowEvents(dst []tapeFlowEvent, now time.Time) []tapeFlowEvent {
	cutoff := now.Add(-qqqTapeHistoryRetention)
	keep := dst[:0]
	for _, ev := range dst {
		if ev.At.After(cutoff) || ev.At.Equal(cutoff) {
			keep = append(keep, ev)
		}
	}
	return keep
}

func latestMid(history []tapeMidEvent) (float64, bool) {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Mid > 0 {
			return history[i].Mid, true
		}
	}
	return 0, false
}

func midAtOrBefore(history []tapeMidEvent, at time.Time) (float64, bool) {
	for i := len(history) - 1; i >= 0; i-- {
		if !history[i].At.After(at) && history[i].Mid > 0 {
			return history[i].Mid, true
		}
	}
	return 0, false
}

func earliestMid(history []tapeMidEvent) (float64, bool) {
	for i := 0; i < len(history); i++ {
		if history[i].Mid > 0 {
			return history[i].Mid, true
		}
	}
	return 0, false
}

func windowReturn(history []tapeMidEvent, now time.Time, window time.Duration) float64 {
	if len(history) == 0 || window <= 0 {
		return 0
	}
	curr, ok := latestMid(history)
	if !ok || curr <= 0 {
		return 0
	}
	anchorAt := now.Add(-window)
	anchor, ok := midAtOrBefore(history, anchorAt)
	coverage := 1.0
	if !ok || anchor <= 0 {
		anchor, ok = earliestMid(history)
		if !ok || anchor <= 0 {
			return 0
		}
		firstAt := history[0].At
		if now.After(firstAt) {
			coverage = clampTape(float64(now.Sub(firstAt))/float64(window), 0, 1)
		} else {
			coverage = 0
		}
	}
	return clampTape(((curr-anchor)/anchor)*coverage, -0.02, 0.02)
}

func flowSums(events []tapeFlowEvent, now time.Time, window time.Duration) (signed, gross float64) {
	if len(events) == 0 || window <= 0 {
		return 0, 0
	}
	cutoff := now.Add(-window)
	for _, ev := range events {
		if ev.At.Before(cutoff) {
			continue
		}
		signed += ev.SignedNotional
		gross += ev.Notional
	}
	return signed, gross
}

func normalizeFlow(signed, gross float64) float64 {
	if gross <= 0 {
		return 0
	}
	return clampTape(signed/gross, -1, 1)
}

func flowImbalance(events []tapeFlowEvent, now time.Time, window time.Duration) float64 {
	signed, gross := flowSums(events, now, window)
	return normalizeFlow(signed, gross)
}

func tapeQuoteImbalance(q tapeQuoteState) float64 {
	den := q.BidSize + q.AskSize
	if den <= 0 {
		return 0
	}
	return clampTape((q.BidSize-q.AskSize)/den, -1, 1)
}

func tapeMicroEdge(q tapeQuoteState) float64 {
	if q.Bid <= 0 || q.Ask <= 0 || q.Ask < q.Bid {
		return 0
	}
	den := q.BidSize + q.AskSize
	if den <= 0 {
		return 0
	}
	mid := 0.5 * (q.Bid + q.Ask)
	spread := q.Ask - q.Bid
	if mid <= 0 || spread <= 0 {
		return 0
	}
	micro := (q.Ask*q.BidSize + q.Bid*q.AskSize) / den
	return clampTape((micro-mid)/(spread/2.0), -1, 1)
}

func tapeBias(score float64) string {
	switch {
	case score >= 45:
		return "Strong Buy"
	case score >= 16:
		return "Buy Lean"
	case score <= -45:
		return "Strong Sell"
	case score <= -16:
		return "Sell Lean"
	default:
		return "Balanced"
	}
}

func clampTape(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func roundTape(v float64, digits int) float64 {
	pow := math.Pow(10, float64(digits))
	return math.Round(v*pow) / pow
}

func signAgreement(vals ...float64) float64 {
	pos, neg := 0, 0
	for _, v := range vals {
		if math.Abs(v) < 1e-9 {
			continue
		}
		if v > 0 {
			pos++
		} else if v < 0 {
			neg++
		}
	}
	total := pos + neg
	if total == 0 {
		return 0
	}
	if pos == total || neg == total {
		return 1
	}
	if pos >= 2 || neg >= 2 {
		return 2.0 / 3.0
	}
	return 1.0 / 3.0
}

func sameDirection(a, b float64) bool {
	if math.Abs(a) < 1e-9 || math.Abs(b) < 1e-9 {
		return false
	}
	return (a > 0 && b > 0) || (a < 0 && b < 0)
}

func conflictPenalty(fairGapBps, leaderFlowEdgeBps, qqqConfirmBps float64) float64 {
	penalty := 0.0
	if fairGapBps*leaderFlowEdgeBps < 0 {
		penalty += 0.30 * math.Min(math.Abs(fairGapBps), math.Abs(leaderFlowEdgeBps))
	}
	if fairGapBps*qqqConfirmBps < 0 {
		penalty += 0.18 * math.Min(math.Abs(fairGapBps), math.Abs(qqqConfirmBps))
	}
	return penalty
}
