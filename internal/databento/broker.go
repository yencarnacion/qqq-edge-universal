package databento

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	dbn "github.com/NimbleMarkets/dbn-go"
	dbn_live "github.com/NimbleMarkets/dbn-go/live"

	"qqq-edge-universal/internal/marketdata"
)

type liveSubReq struct {
	schema  string
	symbols []string
}

type Broker struct {
	cfg Config

	mu        sync.RWMutex
	watchers  map[string]map[*marketdata.Subscription]struct{}
	liveKinds map[string]marketdata.StreamKinds
	outbound  chan liveSubReq

	mapMu     sync.RWMutex
	symbolMap map[uint32]string
}

var _ marketdata.LiveProvider = (*Broker)(nil)

func NewBroker(apiKey string) (*Broker, error) {
	cfg, err := LoadConfig(apiKey)
	if err != nil {
		return nil, err
	}
	return &Broker{
		cfg:       cfg,
		watchers:  make(map[string]map[*marketdata.Subscription]struct{}),
		liveKinds: make(map[string]marketdata.StreamKinds),
		outbound:  make(chan liveSubReq, 128),
		symbolMap: make(map[uint32]string),
	}, nil
}

func (b *Broker) Name() string {
	return "databento"
}

func (b *Broker) Subscribe(symbol string, kinds marketdata.StreamKinds) *marketdata.Subscription {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	sub := marketdata.NewSubscription(symbol, kinds)
	if sub == nil {
		return nil
	}

	var reqs []liveSubReq
	b.mu.Lock()
	if _, ok := b.watchers[symbol]; !ok {
		b.watchers[symbol] = make(map[*marketdata.Subscription]struct{})
	}
	prevKinds := b.liveKinds[symbol]
	b.watchers[symbol][sub] = struct{}{}
	nextKinds := b.aggregateKindsLocked(symbol)
	if add := subtractKinds(nextKinds, prevKinds); !add.Empty() {
		reqs = subscriptionsForKinds(symbol, add)
	}
	if nextKinds.Empty() {
		delete(b.liveKinds, symbol)
	} else {
		b.liveKinds[symbol] = nextKinds
	}
	b.mu.Unlock()

	for _, req := range reqs {
		b.enqueue(req)
	}
	return sub
}

func (b *Broker) Unsubscribe(sub *marketdata.Subscription) {
	if sub == nil {
		return
	}
	sub.Close()
	b.mu.Lock()
	if ws := b.watchers[sub.Symbol]; ws != nil {
		prevKinds := b.liveKinds[sub.Symbol]
		delete(ws, sub)
		if len(ws) == 0 {
			delete(b.watchers, sub.Symbol)
		}
		nextKinds := b.aggregateKindsLocked(sub.Symbol)
		if nextKinds.Empty() {
			delete(b.liveKinds, sub.Symbol)
		} else if nextKinds != prevKinds {
			b.liveKinds[sub.Symbol] = nextKinds
		}
	}
	b.mu.Unlock()
}

func (b *Broker) RemoveSymbol(symbol string) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return
	}
	b.mu.Lock()
	if len(b.watchers[symbol]) > 0 {
		b.mu.Unlock()
		return
	}
	delete(b.watchers, symbol)
	delete(b.liveKinds, symbol)
	b.mu.Unlock()
}

func (b *Broker) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := b.runOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[databento] disconnected: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}
}

func (b *Broker) runOnce(ctx context.Context) error {
	client, err := dbn_live.NewLiveClient(dbn_live.LiveConfig{
		ApiKey:               b.cfg.APIKey,
		Dataset:              b.cfg.Dataset,
		Client:               "qqq-edge-universal",
		Encoding:             dbn.Encoding_Dbn,
		SendTsOut:            false,
		VersionUpgradePolicy: dbn.VersionUpgradePolicy_AsIs,
	})
	if err != nil {
		return fmt.Errorf("new live client: %w", err)
	}
	defer client.Stop()

	if _, err := client.Authenticate(b.cfg.APIKey); err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}

	for _, req := range b.currentSubscriptions() {
		if err := b.subscribeRequest(client, req); err != nil {
			return err
		}
	}
	if err := client.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	scanner := client.GetDbnScanner()
	if scanner == nil {
		return fmt.Errorf("expected dbn scanner; got nil")
	}
	if meta, err := scanner.Metadata(); err == nil {
		b.seedSymbolMap(meta, time.Now().UTC())
	}

	errCh := make(chan error, 1)
	go func() {
		for scanner.Next() {
			if err := b.handleRecord(scanner); err != nil {
				errCh <- err
				return
			}
		}
		if err := scanner.Error(); err != nil && err != io.EOF {
			errCh <- err
			return
		}
		errCh <- io.EOF
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-b.outbound:
			if req.schema == "" || len(req.symbols) == 0 {
				continue
			}
			for _, chunk := range batchSymbols(req.symbols, b.cfg.MaxControlMsgBytes) {
				if err := b.subscribeRequest(client, liveSubReq{schema: req.schema, symbols: chunk}); err != nil {
					return err
				}
			}
		case err := <-errCh:
			if err == io.EOF {
				return io.EOF
			}
			return err
		}
	}
}

func (b *Broker) subscribeRequest(client *dbn_live.LiveClient, req liveSubReq) error {
	if req.schema == "" || len(req.symbols) == 0 {
		return nil
	}
	if err := client.Subscribe(dbn_live.SubscriptionRequestMsg{
		Schema:  req.schema,
		StypeIn: b.cfg.StypeIn,
		Symbols: req.symbols,
	}); err != nil {
		return fmt.Errorf("subscribe %s %v: %w", req.schema, req.symbols, err)
	}
	return nil
}

func (b *Broker) currentSubscriptions() []liveSubReq {
	b.mu.RLock()
	bySchema := map[string][]string{
		liveSchemaMbp1:    nil,
		liveSchemaOhlcv1m: nil,
	}
	for sym, kinds := range b.liveKinds {
		for _, req := range subscriptionsForKinds(sym, kinds) {
			bySchema[req.schema] = append(bySchema[req.schema], req.symbols...)
		}
	}
	b.mu.RUnlock()

	out := make([]liveSubReq, 0, 4)
	for schema, symbols := range bySchema {
		for _, chunk := range batchSymbols(symbols, b.cfg.MaxControlMsgBytes) {
			out = append(out, liveSubReq{schema: schema, symbols: chunk})
		}
	}
	return out
}

func (b *Broker) enqueue(req liveSubReq) {
	if req.schema == "" || len(req.symbols) == 0 {
		return
	}
	select {
	case b.outbound <- req:
	default:
	}
}

func (b *Broker) aggregateKindsLocked(symbol string) marketdata.StreamKinds {
	var out marketdata.StreamKinds
	for sub := range b.watchers[symbol] {
		kinds := sub.Kinds()
		out.Trades = out.Trades || kinds.Trades
		out.Quotes = out.Quotes || kinds.Quotes
		out.Minutes = out.Minutes || kinds.Minutes
	}
	return out
}

func subtractKinds(left, right marketdata.StreamKinds) marketdata.StreamKinds {
	return marketdata.StreamKinds{
		Trades:  left.Trades && !right.Trades,
		Quotes:  left.Quotes && !right.Quotes,
		Minutes: left.Minutes && !right.Minutes,
	}
}

func subscriptionsForKinds(symbol string, kinds marketdata.StreamKinds) []liveSubReq {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" || kinds.Empty() {
		return nil
	}

	reqs := make([]liveSubReq, 0, 2)
	if kinds.Trades || kinds.Quotes {
		reqs = append(reqs, liveSubReq{schema: liveSchemaMbp1, symbols: []string{symbol}})
	}
	if kinds.Minutes {
		reqs = append(reqs, liveSubReq{schema: liveSchemaOhlcv1m, symbols: []string{symbol}})
	}
	return reqs
}

func batchSymbols(symbols []string, maxBytes int) [][]string {
	if maxBytes <= 0 {
		maxBytes = defaultMaxControlMsgBytes
	}
	clean := make([]string, 0, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for _, sym := range symbols {
		sym = strings.ToUpper(strings.TrimSpace(sym))
		if sym == "" {
			continue
		}
		if _, ok := seen[sym]; ok {
			continue
		}
		seen[sym] = struct{}{}
		clean = append(clean, sym)
	}
	if len(clean) == 0 {
		return nil
	}

	var out [][]string
	var chunk []string
	chunkBytes := 0
	for _, sym := range clean {
		add := len(sym)
		if len(chunk) > 0 {
			add++
		}
		if len(chunk) > 0 && chunkBytes+add > maxBytes {
			out = append(out, chunk)
			chunk = nil
			chunkBytes = 0
		}
		chunk = append(chunk, sym)
		chunkBytes += len(sym)
		if len(chunk) > 1 {
			chunkBytes++
		}
	}
	if len(chunk) > 0 {
		out = append(out, chunk)
	}
	return out
}

func (b *Broker) handleRecord(scanner *dbn.DbnScanner) error {
	hdr, err := scanner.GetLastHeader()
	if err != nil {
		return fmt.Errorf("read record header: %w", err)
	}
	switch hdr.RType {
	case dbn.RType_SymbolMapping:
		msg, err := scanner.DecodeSymbolMappingMsg()
		if err != nil {
			return fmt.Errorf("decode symbol mapping: %w", err)
		}
		b.mapMu.Lock()
		b.symbolMap[msg.Header.InstrumentID] = strings.ToUpper(strings.TrimSpace(msg.StypeOutSymbol))
		b.mapMu.Unlock()
		return nil
	case dbn.RType_Mbp1:
		msg, err := dbn.DbnScannerDecode[dbn.Mbp1Msg](scanner)
		if err != nil {
			return fmt.Errorf("decode mbp-1: %w", err)
		}
		b.handleMbp1(msg)
	case dbn.RType_Cmbp1:
		msg, err := dbn.DbnScannerDecode[dbn.Cmbp1Msg](scanner)
		if err != nil {
			return fmt.Errorf("decode cmbp-1: %w", err)
		}
		b.handleCmbp1(msg)
	case dbn.RType_Ohlcv1M:
		msg, err := dbn.DbnScannerDecode[dbn.OhlcvMsg](scanner)
		if err != nil {
			return fmt.Errorf("decode ohlcv-1m: %w", err)
		}
		b.handleOhlcv1m(msg)
	}
	return nil
}

func (b *Broker) handleMbp1(msg *dbn.Mbp1Msg) {
	if msg == nil {
		return
	}
	sym := b.lookupSymbol(msg.Header.InstrumentID)
	if sym == "" {
		return
	}
	eventTS := time.Unix(0, int64(msg.Header.TsEvent)).UTC()
	eventMs := eventTS.UnixMilli()

	quote := marketdata.Quote{
		Ev:  "Q",
		Sym: sym,
		Bp:  float64(msg.Level.BidPx) / priceScale,
		Bs:  int64(msg.Level.BidSz),
		Ap:  float64(msg.Level.AskPx) / priceScale,
		As:  int64(msg.Level.AskSz),
		T:   eventMs,
	}
	b.dispatchQuote(quote)

	if msg.Action != byte(dbn.Action_Trade) || msg.Price <= 0 || msg.Size == 0 {
		return
	}
	trade := marketdata.Trade{
		Ev:  "T",
		Sym: sym,
		P:   float64(msg.Price) / priceScale,
		S:   int64(msg.Size),
		T:   eventMs,
	}
	b.dispatchTrade(trade)
}

func (b *Broker) handleCmbp1(msg *dbn.Cmbp1Msg) {
	if msg == nil {
		return
	}
	sym := b.lookupSymbol(msg.Header.InstrumentID)
	if sym == "" {
		return
	}
	eventTS := time.Unix(0, int64(msg.Header.TsEvent)).UTC()
	eventMs := eventTS.UnixMilli()

	quote := marketdata.Quote{
		Ev:  "Q",
		Sym: sym,
		Bp:  float64(msg.Level.BidPx) / priceScale,
		Bs:  int64(msg.Level.BidSz),
		Ap:  float64(msg.Level.AskPx) / priceScale,
		As:  int64(msg.Level.AskSz),
		T:   eventMs,
	}
	b.dispatchQuote(quote)

	if msg.Action != byte(dbn.Action_Trade) || msg.Price <= 0 || msg.Size == 0 {
		return
	}
	trade := marketdata.Trade{
		Ev:  "T",
		Sym: sym,
		P:   float64(msg.Price) / priceScale,
		S:   int64(msg.Size),
		T:   eventMs,
	}
	b.dispatchTrade(trade)
}

func (b *Broker) handleOhlcv1m(msg *dbn.OhlcvMsg) {
	if msg == nil {
		return
	}
	sym := b.lookupSymbol(msg.Header.InstrumentID)
	if sym == "" {
		return
	}
	startTS := time.Unix(0, int64(msg.Header.TsEvent)).UTC()
	b.dispatchAM(marketdata.AggregateMinute{
		Ev:  "AM",
		Sym: sym,
		V:   float64(msg.Volume),
		O:   float64(msg.Open) / priceScale,
		H:   float64(msg.High) / priceScale,
		L:   float64(msg.Low) / priceScale,
		C:   float64(msg.Close) / priceScale,
		S:   startTS.UnixMilli(),
		E:   startTS.Add(time.Minute).UnixMilli(),
	})
}

func (b *Broker) lookupSymbol(instrumentID uint32) string {
	b.mapMu.RLock()
	defer b.mapMu.RUnlock()
	return b.symbolMap[instrumentID]
}

func (b *Broker) seedSymbolMap(meta *dbn.Metadata, ts time.Time) {
	if meta == nil {
		return
	}
	isInverse, err := meta.IsInverseMapping()
	if err != nil {
		return
	}
	ymd := dbn.TimeToYMD(ts)
	next := make(map[uint32]string, len(meta.Mappings))
	for _, mapping := range meta.Mappings {
		for _, interval := range mapping.Intervals {
			if ymd < interval.StartDate || ymd >= interval.EndDate || interval.Symbol == "" {
				continue
			}
			var instrumentID uint64
			var symbol string
			if isInverse {
				instrumentID, err = strconv.ParseUint(mapping.RawSymbol, 10, 32)
				symbol = interval.Symbol
			} else {
				instrumentID, err = strconv.ParseUint(interval.Symbol, 10, 32)
				symbol = mapping.RawSymbol
			}
			if err != nil {
				continue
			}
			next[uint32(instrumentID)] = strings.ToUpper(strings.TrimSpace(symbol))
		}
	}
	if len(next) == 0 {
		return
	}
	b.mapMu.Lock()
	for k, v := range next {
		b.symbolMap[k] = v
	}
	b.mapMu.Unlock()
}

func (b *Broker) dispatchTrade(t marketdata.Trade) {
	b.mu.RLock()
	ws := b.watchers[t.Sym]
	b.mu.RUnlock()
	for sub := range ws {
		if sub.Trades == nil {
			continue
		}
		select {
		case <-sub.Done():
		case sub.Trades <- t:
		default:
		}
	}
}

func (b *Broker) dispatchAM(a marketdata.AggregateMinute) {
	b.mu.RLock()
	ws := b.watchers[a.Sym]
	b.mu.RUnlock()
	for sub := range ws {
		if sub.Minutes == nil {
			continue
		}
		select {
		case <-sub.Done():
		case sub.Minutes <- a:
		default:
		}
	}
}

func (b *Broker) dispatchQuote(q marketdata.Quote) {
	b.mu.RLock()
	ws := b.watchers[q.Sym]
	b.mu.RUnlock()
	for sub := range ws {
		if sub.Quotes == nil {
			continue
		}
		select {
		case <-sub.Done():
		case sub.Quotes <- q:
		default:
		}
	}
}
