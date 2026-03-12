package massive

import (
	"fmt"
	"os"
	"strings"
)

const defaultStocksWSURL = "wss://socket.massive.com/stocks"

type Config struct {
	APIKey string
	WSURL  string
}

func LoadConfig(apiKey string) (Config, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return Config{}, fmt.Errorf("missing massive key")
	}
	wsURL := strings.TrimSpace(os.Getenv("MASSIVE_WS_URL"))
	if wsURL == "" {
		wsURL = defaultStocksWSURL
	}
	return Config{
		APIKey: apiKey,
		WSURL:  wsURL,
	}, nil
}
