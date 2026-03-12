package massive

import "testing"

func TestLoadConfigRequiresAPIKey(t *testing.T) {
	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error for missing API key")
	}
}
