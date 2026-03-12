package massive

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	massiverest "github.com/massive-com/client-go/v2/rest"
	mmodels "github.com/massive-com/client-go/v2/rest/models"

	"qqq-edge-universal/internal/marketdata"
)

type HistoricalClient struct {
	cfg  Config
	rest *massiverest.Client
}

var _ marketdata.HistoricalProvider = (*HistoricalClient)(nil)

func NewHistoricalClient(apiKey string, httpClient *http.Client) (*HistoricalClient, error) {
	cfg, err := LoadConfig(apiKey)
	if err != nil {
		return nil, err
	}
	return &HistoricalClient{
		cfg:  cfg,
		rest: massiverest.NewWithClient(cfg.APIKey, httpClient),
	}, nil
}

func (c *HistoricalClient) Name() string {
	return "massive"
}

func (c *HistoricalClient) RangeOhlcv1m(ctx context.Context, symbol string, start, end time.Time) ([]marketdata.Ohlcv1mBar, error) {
	if c == nil {
		return nil, fmt.Errorf("historical client is nil")
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil, fmt.Errorf("missing symbol")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	params := mmodels.ListAggsParams{
		Ticker:     symbol,
		Multiplier: 1,
		Timespan:   mmodels.Minute,
		From:       mmodels.Millis(start.UTC()),
		To:         mmodels.Millis(end.UTC()),
	}.WithOrder(mmodels.Asc).WithLimit(50_000).WithAdjusted(true)

	iter := c.rest.ListAggs(ctx, params)
	out := make([]marketdata.Ohlcv1mBar, 0, 512)
	for iter.Next() {
		agg := iter.Item()
		barStart := time.Time(agg.Timestamp).UTC()
		if barStart.Before(start.UTC()) || !barStart.Before(end.UTC()) {
			continue
		}
		out = append(out, marketdata.Ohlcv1mBar{
			Symbol: symbol,
			Start:  barStart,
			End:    barStart.Add(time.Minute),
			Open:   agg.Open,
			High:   agg.High,
			Low:    agg.Low,
			Close:  agg.Close,
			Volume: agg.Volume,
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
