package providers

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"qqq-edge-universal/internal/databento"
	"qqq-edge-universal/internal/marketdata"
	"qqq-edge-universal/internal/massive"
)

const DefaultMarketDataProvider = "massive"

func NormalizeMarketDataProvider(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	switch name {
	case "":
		return DefaultMarketDataProvider
	case "massive", "polygon", "masive":
		return "massive"
	case "databento":
		return "databento"
	}
	return name
}

func MarketDataProviderFromEnv() string {
	return NormalizeMarketDataProvider(os.Getenv("MARKET_DATA_PROVIDER"))
}

func ResolveMarketDataProvider(configValue, envValue string) string {
	if strings.TrimSpace(configValue) != "" {
		return NormalizeMarketDataProvider(configValue)
	}
	return NormalizeMarketDataProvider(envValue)
}

func NewLiveProvider(name string) (marketdata.LiveProvider, error) {
	switch NormalizeMarketDataProvider(name) {
	case "databento":
		return databento.NewBroker(strings.TrimSpace(os.Getenv("DATABENTO_API_KEY")))
	case "massive":
		return massive.NewBroker(strings.TrimSpace(os.Getenv("MASSIVE_API_KEY")))
	default:
		return nil, fmt.Errorf("unsupported market data provider %q", name)
	}
}

func NewHistoricalProvider(name string, httpClient *http.Client) (marketdata.HistoricalProvider, error) {
	switch NormalizeMarketDataProvider(name) {
	case "databento":
		return databento.NewHistoricalClient(strings.TrimSpace(os.Getenv("DATABENTO_API_KEY")), httpClient)
	case "massive":
		return massive.NewHistoricalClient(strings.TrimSpace(os.Getenv("MASSIVE_API_KEY")), httpClient)
	default:
		return nil, fmt.Errorf("unsupported market data provider %q", name)
	}
}
