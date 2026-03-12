package marketdata

import (
	"context"
	"net/http"
	"time"
)

type LiveProvider interface {
	Name() string
	Subscribe(symbol string, kinds StreamKinds) *Subscription
	Unsubscribe(sub *Subscription)
	Run(ctx context.Context) error
}

type HistoricalProvider interface {
	Name() string
	RangeOhlcv1m(ctx context.Context, symbol string, start, end time.Time) ([]Ohlcv1mBar, error)
}

type Factory interface {
	Name() string
	NewLive() (LiveProvider, error)
	NewHistorical(httpClient *http.Client) (HistoricalProvider, error)
}
