package marketdata

import (
	"strings"
	"sync"
	"time"
)

type Trade struct {
	Ev  string  `json:"ev"`
	Sym string  `json:"sym"`
	P   float64 `json:"p"`
	S   int64   `json:"s"`
	T   int64   `json:"t"`
}

type Quote struct {
	Ev  string  `json:"ev"`
	Sym string  `json:"sym"`
	Bx  int     `json:"bx"`
	Bp  float64 `json:"bp"`
	Bs  int64   `json:"bs"`
	Ax  int     `json:"ax"`
	Ap  float64 `json:"ap"`
	As  int64   `json:"as"`
	C   int     `json:"c"`
	I   []int   `json:"i"`
	T   int64   `json:"t"`
	Q   int64   `json:"q"`
	Z   int     `json:"z"`
}

type AggregateMinute struct {
	Ev  string  `json:"ev"`
	Sym string  `json:"sym"`
	V   float64 `json:"v"`
	O   float64 `json:"o"`
	H   float64 `json:"h"`
	L   float64 `json:"l"`
	C   float64 `json:"c"`
	S   int64   `json:"s"`
	E   int64   `json:"e"`
}

type Ohlcv1mBar struct {
	Symbol string
	Start  time.Time
	End    time.Time
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

type StreamKinds struct {
	Trades  bool
	Minutes bool
	Quotes  bool
}

func (k StreamKinds) Empty() bool {
	return !k.Trades && !k.Minutes && !k.Quotes
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

func NewSubscription(symbol string, kinds StreamKinds) *Subscription {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" || kinds.Empty() {
		return nil
	}
	sub := &Subscription{
		Symbol: symbol,
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
	return sub
}

func (s *Subscription) Kinds() StreamKinds {
	if s == nil {
		return StreamKinds{}
	}
	return s.kinds
}

func (s *Subscription) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *Subscription) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		close(s.done)
	})
}
