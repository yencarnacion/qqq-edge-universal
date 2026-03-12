package databento

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	dbn "github.com/NimbleMarkets/dbn-go"
)

const (
	defaultDataset            = "EQUS.MINI"
	defaultStypeIn            = "raw_symbol"
	defaultMaxControlMsgBytes = 20_000
	defaultHistoricalTimeout  = 10 * time.Second
	defaultHistoricalRetries  = 2
	defaultHistoricalBackoff  = 1500 * time.Millisecond
	priceScale                = 1_000_000_000.0
	liveSchemaMbp1            = "mbp-1"
	liveSchemaOhlcv1m         = "ohlcv-1m"
	historicalRangeURL        = "https://hist.databento.com/v0/timeseries.get_range"
)

type Config struct {
	APIKey             string
	Dataset            string
	StypeIn            dbn.SType
	MaxControlMsgBytes int
	HistoricalTimeout  time.Duration
	HistoricalRetries  int
	HistoricalBackoff  time.Duration
}

func LoadConfig(apiKey string) (Config, error) {
	if strings.TrimSpace(apiKey) == "" {
		return Config{}, fmt.Errorf("missing databento key")
	}
	dataset := strings.TrimSpace(os.Getenv("DATABENTO_DATASET"))
	if dataset == "" {
		dataset = defaultDataset
	}
	stypeRaw := strings.TrimSpace(os.Getenv("DATABENTO_STYPE_IN"))
	if stypeRaw == "" {
		stypeRaw = defaultStypeIn
	}
	stypeIn, err := dbn.STypeFromString(stypeRaw)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DATABENTO_STYPE_IN %q: %w", stypeRaw, err)
	}
	maxBytes := defaultMaxControlMsgBytes
	if raw := strings.TrimSpace(os.Getenv("DATABENTO_MAX_CONTROL_MSG_BYTES")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			maxBytes = parsed
		}
	}
	historicalTimeout := defaultHistoricalTimeout
	if raw := strings.TrimSpace(os.Getenv("DATABENTO_HISTORICAL_TIMEOUT")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			historicalTimeout = parsed
		}
	}
	historicalRetries := defaultHistoricalRetries
	if raw := strings.TrimSpace(os.Getenv("DATABENTO_HISTORICAL_RETRIES")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			historicalRetries = parsed
		}
	}
	historicalBackoff := defaultHistoricalBackoff
	if raw := strings.TrimSpace(os.Getenv("DATABENTO_HISTORICAL_BACKOFF")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed >= 0 {
			historicalBackoff = parsed
		}
	}
	return Config{
		APIKey:             strings.TrimSpace(apiKey),
		Dataset:            dataset,
		StypeIn:            stypeIn,
		MaxControlMsgBytes: maxBytes,
		HistoricalTimeout:  historicalTimeout,
		HistoricalRetries:  historicalRetries,
		HistoricalBackoff:  historicalBackoff,
	}, nil
}
