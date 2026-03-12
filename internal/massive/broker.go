package massive

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"qqq-edge-universal/internal/marketdata"
)

type wsMsg struct {
	Action string `json:"action"`
	Params string `json:"params,omitempty"`
}

type Broker struct {
	cfg Config

	mu        sync.RWMutex
	watchers  map[string]map[*marketdata.Subscription]struct{}
	liveKinds map[string]marketdata.StreamKinds
	outbound  chan wsMsg
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
		outbound:  make(chan wsMsg, 1024),
	}, nil
}

func (b *Broker) Name() string {
	return "massive"
}

func (b *Broker) Subscribe(symbol string, kinds marketdata.StreamKinds) *marketdata.Subscription {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	sub := marketdata.NewSubscription(symbol, kinds)
	if sub == nil {
		return nil
	}

	var msg wsMsg
	b.mu.Lock()
	if _, ok := b.watchers[symbol]; !ok {
		b.watchers[symbol] = make(map[*marketdata.Subscription]struct{})
	}
	prevKinds := b.liveKinds[symbol]
	b.watchers[symbol][sub] = struct{}{}
	nextKinds := b.aggregateKindsLocked(symbol)
	if add := subtractKinds(nextKinds, prevKinds); !add.Empty() {
		msg = subscribeMsgFor(symbol, add)
	}
	if nextKinds.Empty() {
		delete(b.liveKinds, symbol)
	} else {
		b.liveKinds[symbol] = nextKinds
	}
	b.mu.Unlock()

	b.enqueue(msg)
	return sub
}

func (b *Broker) Unsubscribe(sub *marketdata.Subscription) {
	if sub == nil {
		return
	}
	sub.Close()

	var msg wsMsg
	b.mu.Lock()
	if ws := b.watchers[sub.Symbol]; ws != nil {
		prevKinds := b.liveKinds[sub.Symbol]
		delete(ws, sub)
		if len(ws) == 0 {
			delete(b.watchers, sub.Symbol)
		}
		nextKinds := b.aggregateKindsLocked(sub.Symbol)
		if remove := subtractKinds(prevKinds, nextKinds); !remove.Empty() {
			msg = unsubscribeMsgFor(sub.Symbol, remove)
		}
		if nextKinds.Empty() {
			delete(b.liveKinds, sub.Symbol)
		} else {
			b.liveKinds[sub.Symbol] = nextKinds
		}
	}
	b.mu.Unlock()

	b.enqueue(msg)
}

func (b *Broker) RemoveSymbol(symbol string) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return
	}

	var msg wsMsg
	b.mu.Lock()
	if len(b.watchers[symbol]) == 0 {
		if prevKinds := b.liveKinds[symbol]; !prevKinds.Empty() {
			msg = unsubscribeMsgFor(symbol, prevKinds)
		}
		delete(b.watchers, symbol)
		delete(b.liveKinds, symbol)
	}
	b.mu.Unlock()

	b.enqueue(msg)
}

func (b *Broker) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := b.runOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[massive] disconnected: %v", err)
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
	dialer := websocket.Dialer{
		HandshakeTimeout:  10 * time.Second,
		EnableCompression: true,
	}
	conn, _, err := dialer.DialContext(ctx, b.cfg.WSURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(authMsg(b.cfg.APIKey)); err != nil {
		return fmt.Errorf("auth write: %w", err)
	}

	for _, msg := range b.currentSubscriptions() {
		if err := conn.WriteJSON(msg); err != nil {
			return fmt.Errorf("subscribe write: %w", err)
		}
	}

	ping := time.NewTicker(45 * time.Second)
	defer ping.Stop()

	errCh := make(chan error, 1)
	go func() {
		for {
			var msgs []json.RawMessage
			if err := conn.ReadJSON(&msgs); err != nil {
				errCh <- err
				return
			}
			for _, raw := range msgs {
				if err := b.handleMessage(raw); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ping.C:
			_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
		case msg := <-b.outbound:
			if msg.Action == "" {
				continue
			}
			if err := conn.WriteJSON(msg); err != nil {
				return fmt.Errorf("control write: %w", err)
			}
		case err := <-errCh:
			return err
		}
	}
}

func (b *Broker) handleMessage(raw json.RawMessage) error {
	var ev struct {
		Ev string `json:"ev"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("decode message type: %w", err)
	}
	switch ev.Ev {
	case "T":
		var trade marketdata.Trade
		if err := json.Unmarshal(raw, &trade); err != nil {
			return fmt.Errorf("decode trade: %w", err)
		}
		trade.Sym = strings.ToUpper(strings.TrimSpace(trade.Sym))
		if trade.Sym != "" {
			b.dispatchTrade(trade)
		}
	case "Q":
		var quote marketdata.Quote
		if err := json.Unmarshal(raw, &quote); err != nil {
			return fmt.Errorf("decode quote: %w", err)
		}
		quote.Sym = strings.ToUpper(strings.TrimSpace(quote.Sym))
		if quote.Sym != "" {
			b.dispatchQuote(quote)
		}
	case "AM":
		var agg marketdata.AggregateMinute
		if err := json.Unmarshal(raw, &agg); err != nil {
			return fmt.Errorf("decode minute aggregate: %w", err)
		}
		agg.Sym = strings.ToUpper(strings.TrimSpace(agg.Sym))
		if agg.Sym != "" {
			b.dispatchAM(agg)
		}
	}
	return nil
}

func (b *Broker) currentSubscriptions() []wsMsg {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]wsMsg, 0, len(b.liveKinds))
	for symbol, kinds := range b.liveKinds {
		if kinds.Empty() {
			continue
		}
		out = append(out, subscribeMsgFor(symbol, kinds))
	}
	return out
}

func (b *Broker) aggregateKindsLocked(symbol string) marketdata.StreamKinds {
	var out marketdata.StreamKinds
	for sub := range b.watchers[symbol] {
		kinds := sub.Kinds()
		out.Trades = out.Trades || kinds.Trades
		out.Minutes = out.Minutes || kinds.Minutes
		out.Quotes = out.Quotes || kinds.Quotes
	}
	return out
}

func subtractKinds(left, right marketdata.StreamKinds) marketdata.StreamKinds {
	return marketdata.StreamKinds{
		Trades:  left.Trades && !right.Trades,
		Minutes: left.Minutes && !right.Minutes,
		Quotes:  left.Quotes && !right.Quotes,
	}
}

func authMsg(key string) wsMsg {
	return wsMsg{Action: "auth", Params: key}
}

func subscribeMsgFor(symbol string, kinds marketdata.StreamKinds) wsMsg {
	return wsMsg{Action: "subscribe", Params: subscriptionParams(symbol, kinds)}
}

func unsubscribeMsgFor(symbol string, kinds marketdata.StreamKinds) wsMsg {
	return wsMsg{Action: "unsubscribe", Params: subscriptionParams(symbol, kinds)}
}

func subscriptionParams(symbol string, kinds marketdata.StreamKinds) string {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	parts := make([]string, 0, 3)
	if kinds.Trades {
		parts = append(parts, "T."+symbol)
	}
	if kinds.Quotes {
		parts = append(parts, "Q."+symbol)
	}
	if kinds.Minutes {
		parts = append(parts, "AM."+symbol)
	}
	return strings.Join(parts, ",")
}

func (b *Broker) enqueue(msg wsMsg) {
	if msg.Action == "" || msg.Params == "" {
		return
	}
	select {
	case b.outbound <- msg:
	default:
	}
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
