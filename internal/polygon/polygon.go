// File: internal/polygon/polygon.go
package polygon

// NOTE: This broker is intentionally permissive and local-dev friendly per spec.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultWS = "wss://socket.massive.com/stocks"
)

type Trade struct {
	Ev  string  `json:"ev"`  // "T"
	Sym string  `json:"sym"` // "AAPL"
	P   float64 `json:"p"`   // price
	S   int64   `json:"s"`   // size
	T   int64   `json:"t"`   // SIP ms
}

type Quote struct {
	Ev  string  `json:"ev"`  // "Q"
	Sym string  `json:"sym"` // "AAPL"
	Bx  int     `json:"bx"`
	Bp  float64 `json:"bp"`
	Bs  int64   `json:"bs"`
	Ax  int     `json:"ax"`
	Ap  float64 `json:"ap"`
	As  int64   `json:"as"`
	C   int     `json:"c"`
	I   []int   `json:"i"`
	T   int64   `json:"t"` // SIP ms
	Q   int64   `json:"q"`
	Z   int     `json:"z"`
}

type AggregateMinute struct {
	Ev  string  `json:"ev"` // "AM"
	Sym string  `json:"sym"`
	V   float64 `json:"v"` // volume may be non-integer
	O   float64 `json:"o"`
	H   float64 `json:"h"`
	L   float64 `json:"l"`
	C   float64 `json:"c"`
	S   int64   `json:"s"` // start ms
	E   int64   `json:"e"` // end ms
}

type StreamKinds struct {
	Trades  bool
	Minutes bool
	Quotes  bool
}

func (k StreamKinds) empty() bool {
	return !k.Trades && !k.Minutes && !k.Quotes
}

func kindsAdded(prev, next StreamKinds) StreamKinds {
	return StreamKinds{
		Trades:  !prev.Trades && next.Trades,
		Minutes: !prev.Minutes && next.Minutes,
		Quotes:  !prev.Quotes && next.Quotes,
	}
}

func kindsRemoved(prev, next StreamKinds) StreamKinds {
	return StreamKinds{
		Trades:  prev.Trades && !next.Trades,
		Minutes: prev.Minutes && !next.Minutes,
		Quotes:  prev.Quotes && !next.Quotes,
	}
}

type Subscription struct {
	Symbol    string
	Trades    chan Trade
	Minutes   chan AggregateMinute
	Quotes    chan Quote
	kinds     StreamKinds
	done      chan struct{}
	closeOnce sync.Once
}

// Done returns a channel closed when the subscription is closed.
func (s *Subscription) Done() <-chan struct{} {
	return s.done
}

func (s *Subscription) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})
}

type Broker struct {
	apiKey     string
	wsURL      string
	mu         sync.RWMutex
	conn       *websocket.Conn
	dialing    bool
	subscribed map[string]StreamKinds // symbol -> aggregated requested event kinds
	watchers   map[string]map[*Subscription]struct{}
	outbound   chan any
	closed     chan struct{}
}

func NewBroker(apiKey string, wsOptional ...string) *Broker {
	u := defaultWS
	if len(wsOptional) > 0 && strings.TrimSpace(wsOptional[0]) != "" {
		u = wsOptional[0]
	}
	return &Broker{
		apiKey:     apiKey,
		wsURL:      u,
		subscribed: make(map[string]StreamKinds),
		watchers:   make(map[string]map[*Subscription]struct{}),
		outbound:   make(chan any, 1024),
		closed:     make(chan struct{}),
	}
}

func (b *Broker) Subscribe(symbol string, kinds StreamKinds) *Subscription {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if s == "" || kinds.empty() {
		return nil
	}
	sub := &Subscription{
		Symbol: s,
		kinds:  kinds,
		done:   make(chan struct{}),
	}
	if kinds.Trades {
		sub.Trades = make(chan Trade, 256)
	}
	if kinds.Minutes {
		sub.Minutes = make(chan AggregateMinute, 256)
	}
	if kinds.Quotes {
		sub.Quotes = make(chan Quote, 256)
	}

	b.mu.Lock()
	if _, ok := b.watchers[s]; !ok {
		b.watchers[s] = make(map[*Subscription]struct{})
	}
	b.watchers[s][sub] = struct{}{}
	prev := b.subscribed[s]
	next := b.aggregateKindsLocked(s)
	added := kindsAdded(prev, next)
	b.subscribed[s] = next
	b.mu.Unlock()

	if !added.empty() {
		if msg, ok := subscribeMsgForKinds(s, added); ok {
			select {
			case b.outbound <- msg:
			default:
			}
		}
	}
	return sub
}

func (b *Broker) aggregateKindsLocked(symbol string) StreamKinds {
	var out StreamKinds
	for sub := range b.watchers[symbol] {
		out.Trades = out.Trades || sub.kinds.Trades
		out.Minutes = out.Minutes || sub.kinds.Minutes
		out.Quotes = out.Quotes || sub.kinds.Quotes
	}
	return out
}

func (b *Broker) Unsubscribe(sub *Subscription) {
	if sub == nil {
		return
	}
	sub.Close()
	b.mu.Lock()
	ws := b.watchers[sub.Symbol]
	if ws != nil {
		delete(ws, sub)
	}
	prev := b.subscribed[sub.Symbol]
	next := b.aggregateKindsLocked(sub.Symbol)
	removed := kindsRemoved(prev, next)
	if next.empty() {
		delete(b.watchers, sub.Symbol)
		delete(b.subscribed, sub.Symbol)
	} else {
		b.subscribed[sub.Symbol] = next
	}
	b.mu.Unlock()

	if !removed.empty() {
		if msg, ok := unsubscribeMsgForKinds(sub.Symbol, removed); ok {
			select {
			case b.outbound <- msg:
			default:
			}
		}
	}
}

type wsMsg struct {
	Action string `json:"action"`
	Params string `json:"params,omitempty"`
}

func authMsg(key string) wsMsg { return wsMsg{Action: "auth", Params: key} }

func subscribeMsgForKinds(sym string, kinds StreamKinds) (wsMsg, bool) {
	params := paramsForKinds(sym, kinds)
	if params == "" {
		return wsMsg{}, false
	}
	return wsMsg{Action: "subscribe", Params: params}, true
}

func unsubscribeMsgForKinds(sym string, kinds StreamKinds) (wsMsg, bool) {
	params := paramsForKinds(sym, kinds)
	if params == "" {
		return wsMsg{}, false
	}
	return wsMsg{Action: "unsubscribe", Params: params}, true
}

func paramsForKinds(sym string, kinds StreamKinds) string {
	parts := make([]string, 0, 3)
	if kinds.Trades {
		parts = append(parts, "T."+sym)
	}
	if kinds.Minutes {
		parts = append(parts, "AM."+sym)
	}
	if kinds.Quotes {
		parts = append(parts, "Q."+sym)
	}
	return strings.Join(parts, ",")
}

func (b *Broker) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := b.runOnce(ctx); err != nil {
			log.Printf("[polygon] disconnected: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// exponential up to 30s
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
	conn, _, err := dialer.DialContext(ctx, b.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()

	// auth
	if err := conn.WriteJSON(authMsg(b.apiKey)); err != nil {
		return fmt.Errorf("auth write: %w", err)
	}

	// ask for current subscriptions
	b.mu.RLock()
	for s, kinds := range b.subscribed {
		if msg, ok := subscribeMsgForKinds(s, kinds); ok {
			_ = conn.WriteJSON(msg)
		}
	}
	b.mu.RUnlock()

	// ping
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
				// peek ev
				var ev struct {
					Ev string `json:"ev"`
				}
				_ = json.Unmarshal(raw, &ev)
				switch ev.Ev {
				case "T":
					var t Trade
					if err := json.Unmarshal(raw, &t); err == nil {
						b.dispatchTrade(t)
					}
				case "Q":
					var q Quote
					if err := json.Unmarshal(raw, &q); err == nil {
						b.dispatchQuote(q)
					}
				case "AM":
					var a AggregateMinute
					if err := json.Unmarshal(raw, &a); err == nil {
						b.dispatchAM(a)
					}
				default:
					// ignore "status" and others
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
			_ = conn.WriteJSON(msg)
		case err := <-errCh:
			return err
		}
	}
}

func (b *Broker) dispatchTrade(t Trade) {
	b.mu.RLock()
	ws := b.watchers[t.Sym]
	b.mu.RUnlock()
	for sub := range ws {
		if sub.Trades == nil {
			continue
		}
		select {
		case <-sub.done:
			// skip closed
		case sub.Trades <- t:
		default:
			// drop if slow consumer
		}
	}
}

func (b *Broker) dispatchAM(a AggregateMinute) {
	b.mu.RLock()
	ws := b.watchers[a.Sym]
	b.mu.RUnlock()
	for sub := range ws {
		if sub.Minutes == nil {
			continue
		}
		select {
		case <-sub.done:
		case sub.Minutes <- a:
		default:
		}
	}
}

func (b *Broker) dispatchQuote(q Quote) {
	b.mu.RLock()
	ws := b.watchers[q.Sym]
	b.mu.RUnlock()
	for sub := range ws {
		if sub.Quotes == nil {
			continue
		}
		select {
		case <-sub.done:
		case sub.Quotes <- q:
		default:
		}
	}
}

// RemoveSymbol drops a symbol from the auto-resubscribe set and enqueues an unsubscribe
// to the live connection (if connected). Future reconnects will not resubscribe it.
func (b *Broker) RemoveSymbol(symbol string) {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if s == "" {
		return
	}
	b.mu.Lock()
	if len(b.watchers[s]) > 0 {
		b.mu.Unlock()
		return
	}
	prev := b.subscribed[s]
	delete(b.subscribed, s)
	delete(b.watchers, s)
	b.mu.Unlock()
	if prev.empty() {
		return
	}
	if msg, ok := unsubscribeMsgForKinds(s, prev); ok {
		select {
		case b.outbound <- msg:
		default:
		}
	}
}
