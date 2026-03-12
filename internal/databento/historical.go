package databento

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	dbn "github.com/NimbleMarkets/dbn-go"
	dbn_hist "github.com/NimbleMarkets/dbn-go/hist"

	"qqq-edge-universal/internal/marketdata"
)

type HistoricalClient struct {
	cfg        Config
	httpClient *http.Client
	rangeURL   string
}

var _ marketdata.HistoricalProvider = (*HistoricalClient)(nil)

var errHistoricalRangeUnavailable = errors.New("historical range unavailable")

func NewHistoricalClient(apiKey string, httpClient *http.Client) (*HistoricalClient, error) {
	cfg, err := LoadConfig(apiKey)
	if err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &HistoricalClient{cfg: cfg, httpClient: httpClient, rangeURL: historicalRangeURL}, nil
}

func (c *HistoricalClient) Name() string {
	return "databento"
}

func (c *HistoricalClient) Dataset() string {
	if c == nil {
		return ""
	}
	return c.cfg.Dataset
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
	params := dbn_hist.SubmitJobParams{
		Dataset:     c.cfg.Dataset,
		Symbols:     symbol,
		Schema:      dbn.Schema_Ohlcv1M,
		DateRange:   dbn_hist.DateRange{Start: start.UTC(), End: end.UTC()},
		Encoding:    dbn.Encoding_Dbn,
		Compression: dbn.Compress_None,
		StypeIn:     c.cfg.StypeIn,
		StypeOut:    dbn.SType_InstrumentId,
	}
	form := url.Values{}
	if err := params.ApplyToURLValues(&form); err != nil {
		return nil, err
	}
	body, err := c.postRange(ctx, form)
	if err != nil {
		if errors.Is(err, errHistoricalRangeUnavailable) {
			return nil, nil
		}
		return nil, err
	}

	scanner := dbn.NewDbnScanner(bytes.NewReader(body))
	if _, err := scanner.Metadata(); err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}

	out := make([]marketdata.Ohlcv1mBar, 0, 512)
	for scanner.Next() {
		hdr, err := scanner.GetLastHeader()
		if err != nil {
			return nil, fmt.Errorf("read record header: %w", err)
		}
		if hdr.RType != dbn.RType_Ohlcv1M {
			continue
		}
		rec, err := dbn.DbnScannerDecode[dbn.OhlcvMsg](scanner)
		if err != nil {
			return nil, fmt.Errorf("decode ohlcv-1m: %w", err)
		}
		startTS := time.Unix(0, int64(rec.Header.TsEvent)).UTC()
		out = append(out, marketdata.Ohlcv1mBar{
			Symbol: symbol,
			Start:  startTS,
			End:    startTS.Add(time.Minute),
			Open:   float64(rec.Open) / priceScale,
			High:   float64(rec.High) / priceScale,
			Low:    float64(rec.Low) / priceScale,
			Close:  float64(rec.Close) / priceScale,
			Volume: float64(rec.Volume),
		})
	}
	if err := scanner.Error(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("scan ohlcv-1m: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *HistoricalClient) postRange(ctx context.Context, form url.Values) ([]byte, error) {
	currentForm := cloneURLValues(form)
	maxAttempts := c.cfg.HistoricalRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		body, status, err := c.postRangeOnce(ctx, currentForm)
		if err != nil {
			if !isRetryableHistoricalError(ctx, err) || attempt == maxAttempts {
				return nil, err
			}
			if err := sleepHistoricalBackoff(ctx, c.cfg.HistoricalBackoff, attempt); err != nil {
				return nil, err
			}
			continue
		}
		if status == http.StatusOK {
			return body, nil
		}
		if status == http.StatusUnprocessableEntity {
			if retryForm, ok, noData := adjustedRangeFormForAvailableEnd(currentForm, body); ok {
				currentForm = retryForm
				continue
			} else if noData {
				return nil, errHistoricalRangeUnavailable
			}
		}
		if isRetryableHistoricalStatus(status) && attempt < maxAttempts {
			if err := sleepHistoricalBackoff(ctx, c.cfg.HistoricalBackoff, attempt); err != nil {
				return nil, err
			}
			continue
		}
		return nil, fmt.Errorf("historical get_range: HTTP %d %s", status, strings.TrimSpace(string(body)))
	}

	return nil, fmt.Errorf("historical get_range: exhausted retry budget")
}

func (c *HistoricalClient) postRangeOnce(ctx context.Context, form url.Values) ([]byte, int, error) {
	reqCtx := ctx
	cancel := func() {}
	if c.cfg.HistoricalTimeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, c.cfg.HistoricalTimeout)
	}
	defer cancel()

	rangeURL := c.rangeURL
	if strings.TrimSpace(rangeURL) == "" {
		rangeURL = historicalRangeURL
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, rangeURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/octet-stream")
	auth := base64.StdEncoding.EncodeToString([]byte(c.cfg.APIKey + ":"))
	req.Header.Set("Authorization", "Basic "+auth)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return body, resp.StatusCode, nil
}

func cloneURLValues(src url.Values) url.Values {
	cloned := make(url.Values, len(src))
	for k, vals := range src {
		cloned[k] = append([]string(nil), vals...)
	}
	return cloned
}

func isRetryableHistoricalError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isRetryableHistoricalStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func sleepHistoricalBackoff(ctx context.Context, base time.Duration, attempt int) error {
	if base <= 0 {
		return nil
	}
	delay := base * time.Duration(attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func adjustedRangeFormForAvailableEnd(form url.Values, body []byte) (url.Values, bool, bool) {
	var apiErr struct {
		Detail struct {
			Case    string `json:"case"`
			Payload struct {
				AvailableEnd string `json:"available_end"`
			} `json:"payload"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(body, &apiErr); err != nil {
		return nil, false, false
	}
	availableEnd, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(apiErr.Detail.Payload.AvailableEnd))
	if err != nil {
		return nil, false, false
	}
	start, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(form.Get("start")))
	if err != nil {
		return nil, false, false
	}
	currentEnd, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(form.Get("end")))
	if err != nil {
		return nil, false, false
	}

	switch apiErr.Detail.Case {
	case "data_end_after_available_end":
		if !availableEnd.After(start) || !availableEnd.Before(currentEnd) {
			return nil, false, false
		}
	case "data_start_after_available_end":
		if !start.After(availableEnd) {
			return nil, false, false
		}
		return nil, false, true
	default:
		return nil, false, false
	}

	cloned := make(url.Values, len(form))
	for k, vals := range form {
		cloned[k] = append([]string(nil), vals...)
	}
	cloned.Set("end", availableEnd.UTC().Format(time.RFC3339Nano))
	return cloned, true, false
}
