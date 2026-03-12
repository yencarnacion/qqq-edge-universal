package providers

import "testing"

func TestNormalizeMarketDataProviderDefaultsToMassive(t *testing.T) {
	if got := NormalizeMarketDataProvider(""); got != DefaultMarketDataProvider {
		t.Fatalf("NormalizeMarketDataProvider(\"\") = %q, want %q", got, DefaultMarketDataProvider)
	}
	if got := NormalizeMarketDataProvider("  MASSIVE  "); got != DefaultMarketDataProvider {
		t.Fatalf("NormalizeMarketDataProvider(trimmed) = %q, want %q", got, DefaultMarketDataProvider)
	}
}

func TestNormalizeMarketDataProviderMassiveAliases(t *testing.T) {
	tests := []string{"massive", "polygon", "masive", "  MASSIVE "}
	for _, input := range tests {
		if got := NormalizeMarketDataProvider(input); got != "massive" {
			t.Fatalf("NormalizeMarketDataProvider(%q) = %q, want %q", input, got, "massive")
		}
	}
}

func TestResolveMarketDataProviderPrefersConfig(t *testing.T) {
	if got := ResolveMarketDataProvider("massive", "databento"); got != "massive" {
		t.Fatalf("ResolveMarketDataProvider(config, env) = %q, want %q", got, "massive")
	}
	if got := ResolveMarketDataProvider("", "databento"); got != "databento" {
		t.Fatalf("ResolveMarketDataProvider(empty config, env) = %q, want %q", got, "databento")
	}
	if got := ResolveMarketDataProvider("", ""); got != DefaultMarketDataProvider {
		t.Fatalf("ResolveMarketDataProvider(default) = %q, want %q", got, DefaultMarketDataProvider)
	}
}

func TestNewLiveProviderRejectsUnknownProvider(t *testing.T) {
	provider, err := NewLiveProvider("unknown")
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
	if provider != nil {
		t.Fatalf("provider = %#v, want nil", provider)
	}
}
