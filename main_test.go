package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"qqq-edge-universal/internal/marketdata"
)

type stubHistoricalProvider struct {
	barsBySymbol map[string][]marketdata.Ohlcv1mBar
}

func (s *stubHistoricalProvider) Name() string {
	return "stub"
}

func (s *stubHistoricalProvider) RangeOhlcv1m(_ context.Context, symbol string, start, end time.Time) ([]marketdata.Ohlcv1mBar, error) {
	bars := append([]marketdata.Ohlcv1mBar(nil), s.barsBySymbol[symbol]...)
	out := make([]marketdata.Ohlcv1mBar, 0, len(bars))
	for _, bar := range bars {
		if bar.Start.Before(start) || !bar.Start.Before(end) {
			continue
		}
		out = append(out, bar)
	}
	return out, nil
}

func TestServeStaticSoundEndpointsServeConfiguredFiles(t *testing.T) {
	webDir := t.TempDir()
	mustWriteFile(t, filepath.Join(webDir, "index.html"), []byte("ok"))

	up := []byte("ID3-up-sound")
	down := []byte("ID3-down-sound")
	upPath := filepath.Join(webDir, "up.mp3")
	downPath := filepath.Join(webDir, "down.mp3")
	mustWriteFile(t, upPath, up)
	mustWriteFile(t, downPath, down)

	mux := http.NewServeMux()
	serveStatic(mux, webDir, upPath, downPath)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tests := []struct {
		path string
		want []byte
	}{
		{path: "/alert.mp3", want: up},
		{path: "/alert-up.mp3", want: up},
		{path: "/alert-down.mp3", want: down},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := srv.Client().Get(srv.URL + tc.path)
			if err != nil {
				t.Fatalf("GET %s: %v", tc.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
			}
			if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "audio/mpeg") {
				t.Fatalf("Content-Type = %q, want audio/mpeg", got)
			}
			if got := resp.Header.Get("Cache-Control"); got != "public, max-age=864000" {
				t.Fatalf("Cache-Control = %q, want public, max-age=864000", got)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !bytes.Equal(body, tc.want) {
				t.Fatalf("response body mismatch for %s", tc.path)
			}
		})
	}
}

func TestServeStaticSoundFallbackIsStableWAV(t *testing.T) {
	webDir := t.TempDir()
	mustWriteFile(t, filepath.Join(webDir, "index.html"), []byte("ok"))

	cachedBeepWAV = nil
	want := synthBeepWAV(400, 880.0, 44100)

	mux := http.NewServeMux()
	serveStatic(mux, webDir, "", "")
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tests := []string{"/alert.mp3", "/alert-up.mp3", "/alert-down.mp3"}
	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			resp, err := srv.Client().Get(srv.URL + path)
			if err != nil {
				t.Fatalf("GET %s: %v", path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
			}
			if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "audio/wav") {
				t.Fatalf("Content-Type = %q, want audio/wav", got)
			}
			if got := resp.Header.Get("Cache-Control"); got != "public, max-age=864000" {
				t.Fatalf("Cache-Control = %q, want public, max-age=864000", got)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !bytes.Equal(body, want) {
				t.Fatalf("fallback body mismatch for %s", path)
			}
		})
	}
}

func TestSynthBeepWAVHasExpectedSignalShapeAndAmplitude(t *testing.T) {
	wav := synthBeepWAV(400, 880.0, 44100)
	if len(wav) < 44 {
		t.Fatalf("wav too short: %d", len(wav))
	}
	if string(wav[0:4]) != "RIFF" || string(wav[8:12]) != "WAVE" {
		t.Fatalf("invalid wav header")
	}

	sampleRate := binary.LittleEndian.Uint32(wav[24:28])
	if sampleRate != 44100 {
		t.Fatalf("sampleRate = %d, want 44100", sampleRate)
	}
	bitsPerSample := binary.LittleEndian.Uint16(wav[34:36])
	if bitsPerSample != 16 {
		t.Fatalf("bitsPerSample = %d, want 16", bitsPerSample)
	}
	dataSize := binary.LittleEndian.Uint32(wav[40:44])
	if int(dataSize) != len(wav)-44 {
		t.Fatalf("dataSize = %d, want %d", dataSize, len(wav)-44)
	}
	sampleCount := int(dataSize) / 2
	if sampleCount != 17640 { // 400ms @ 44.1kHz
		t.Fatalf("sampleCount = %d, want 17640", sampleCount)
	}

	maxAbs := 0
	for i := 44; i+1 < len(wav); i += 2 {
		v := int(int16(binary.LittleEndian.Uint16(wav[i : i+2])))
		if v < 0 {
			v = -v
		}
		if v > maxAbs {
			maxAbs = v
		}
	}
	if maxAbs < 2900 || maxAbs > 3000 {
		t.Fatalf("max amplitude = %d, want in [2900,3000]", maxAbs)
	}
}

func TestOdEngineTradeAlertsOnlyOnTrueBreakouts(t *testing.T) {
	et := mustET("America/New_York")
	start := time.Date(2026, time.March, 2, 9, 30, 0, 0, et)
	end := start.Add(6 * time.Hour)

	h := newHub(50)
	eng := newOdEngine(h, et, start, end, start, "hod", "lod", alertSourceTrades, true)
	eng.setAllowed([]string{"ABC"})
	eng.upsertSymbol("ABC", "ABC Co", []string{"watchlist"})

	eng.trade("ABC", 10.00, start.Add(1*time.Second)) // initialize only
	eng.trade("ABC", 11.00, start.Add(2*time.Second)) // HOD alert
	eng.trade("ABC", 11.00, start.Add(3*time.Second)) // no duplicate
	eng.trade("ABC", 10.50, start.Add(4*time.Second)) // no alert
	eng.trade("ABC", 9.00, start.Add(5*time.Second))  // LOD alert
	eng.trade("ABC", 9.00, start.Add(6*time.Second))  // no duplicate
	eng.trade("ABC", 8.80, start.Add(7*time.Second))  // new LOD alert
	eng.trade("ABC", 12.00, start.Add(8*time.Second)) // new HOD alert

	alerts := h.getHistory()
	if len(alerts) != 4 {
		t.Fatalf("alerts len = %d, want 4", len(alerts))
	}

	gotKinds := []string{alerts[0].Kind, alerts[1].Kind, alerts[2].Kind, alerts[3].Kind}
	wantKinds := []string{"hod", "lod", "lod", "hod"}
	if !reflect.DeepEqual(gotKinds, wantKinds) {
		t.Fatalf("kinds = %v, want %v", gotKinds, wantKinds)
	}
}

func TestOdEngineSeedHiLoRequiresTrueBreakout(t *testing.T) {
	et := mustET("America/New_York")
	start := time.Date(2026, time.March, 2, 9, 30, 0, 0, et)
	end := start.Add(6 * time.Hour)

	h := newHub(50)
	eng := newOdEngine(h, et, start, end, start, "hod", "lod", alertSourceTrades, true)
	eng.setAllowed([]string{"ABC"})
	eng.upsertSymbol("ABC", "ABC Co", []string{"watchlist"})
	eng.seedHiLo("ABC", "ABC Co", []string{"watchlist"}, 9.0, 11.0)

	eng.trade("ABC", 11.00, start.Add(1*time.Second)) // at seed high, no alert
	eng.trade("ABC", 9.00, start.Add(2*time.Second))  // at seed low, no alert
	eng.trade("ABC", 11.01, start.Add(3*time.Second)) // breakout high
	eng.trade("ABC", 8.99, start.Add(4*time.Second))  // breakout low

	alerts := h.getHistory()
	if len(alerts) != 2 {
		t.Fatalf("alerts len = %d, want 2", len(alerts))
	}
	if alerts[0].Kind != "hod" || alerts[1].Kind != "lod" {
		t.Fatalf("unexpected alert kinds: %#v", alerts)
	}
}

func TestSeedBreakoutHiLoWarmsProviderAgnosticLocalState(t *testing.T) {
	et := mustET("America/New_York")
	anchor := time.Date(2026, time.March, 2, 9, 30, 0, 0, et)
	nowET := anchor.Add(3 * time.Minute)
	endET := anchor.Add(6 * time.Hour)

	h := newHub(50)
	eng := newOdEngine(h, et, anchor, endET, nowET, "lhigh", "llow", alertSourceTrades, true)
	eng.setAllowed([]string{"ABC"})
	eng.upsertSymbol("ABC", "ABC Co", []string{"watchlist"})

	historical := &stubHistoricalProvider{
		barsBySymbol: map[string][]marketdata.Ohlcv1mBar{
			"ABC": {
				{Symbol: "ABC", Start: anchor.UTC(), End: anchor.Add(time.Minute).UTC(), Open: 10.00, High: 10.40, Low: 9.80, Close: 10.20},
				{Symbol: "ABC", Start: anchor.Add(time.Minute).UTC(), End: anchor.Add(2 * time.Minute).UTC(), Open: 10.20, High: 10.55, Low: 9.90, Close: 10.10},
				{Symbol: "ABC", Start: anchor.Add(2 * time.Minute).UTC(), End: anchor.Add(3 * time.Minute).UTC(), Open: 10.10, High: 10.30, Low: 9.95, Close: 10.00},
			},
		},
	}

	seedBreakoutHiLo(context.Background(), historical, et, []string{"ABC"}, map[string]string{"ABC": "ABC Co"}, map[string][]string{"ABC": []string{"watchlist"}}, anchor, nowET, endET, eng)

	eng.trade("ABC", 10.55, nowET.Add(1*time.Second))
	eng.trade("ABC", 9.80, nowET.Add(2*time.Second))
	eng.trade("ABC", 10.56, nowET.Add(3*time.Second))
	eng.trade("ABC", 9.79, nowET.Add(4*time.Second))

	alerts := h.getHistory()
	if len(alerts) != 2 {
		t.Fatalf("alerts len = %d, want 2", len(alerts))
	}
	if alerts[0].Kind != "lhigh" || alerts[0].Price != 10.56 {
		t.Fatalf("first alert = %#v, want lhigh at 10.56", alerts[0])
	}
	if alerts[1].Kind != "llow" || alerts[1].Price != 9.79 {
		t.Fatalf("second alert = %#v, want llow at 9.79", alerts[1])
	}
}

func TestOdEngineQuoteAlertsUseAskForHighAndBidForLow(t *testing.T) {
	et := mustET("America/New_York")
	start := time.Date(2026, time.March, 2, 9, 30, 0, 0, et)
	end := start.Add(6 * time.Hour)

	h := newHub(50)
	eng := newOdEngine(h, et, start, end, start, "lhigh", "llow", alertSourceNBBO, true)
	eng.setAllowed([]string{"ABC"})
	eng.upsertSymbol("ABC", "ABC Co", []string{"watchlist"})

	eng.quote("ABC", 9.95, 10.05, start.Add(1*time.Second))
	eng.quote("ABC", 9.96, 10.05, start.Add(2*time.Second))
	eng.quote("ABC", 9.96, 10.06, start.Add(3*time.Second))
	eng.quote("ABC", 9.94, 10.06, start.Add(4*time.Second))

	alerts := h.getHistory()
	if len(alerts) != 2 {
		t.Fatalf("alerts len = %d, want 2", len(alerts))
	}
	if alerts[0].Kind != "lhigh" || alerts[0].Price != 10.06 {
		t.Fatalf("first alert = %#v, want lhigh at ask 10.06", alerts[0])
	}
	if alerts[1].Kind != "llow" || alerts[1].Price != 9.94 {
		t.Fatalf("second alert = %#v, want llow at bid 9.94", alerts[1])
	}
}

func TestOdEngineCanSwitchBetweenTradesAndNBBOWithoutReset(t *testing.T) {
	et := mustET("America/New_York")
	start := time.Date(2026, time.March, 2, 9, 30, 0, 0, et)
	end := start.Add(6 * time.Hour)

	h := newHub(50)
	eng := newOdEngine(h, et, start, end, start, "lhigh", "llow", alertSourceTrades, true)
	eng.setAllowed([]string{"ABC"})
	eng.upsertSymbol("ABC", "ABC Co", []string{"watchlist"})

	eng.trade("ABC", 10.00, start.Add(1*time.Second))       // initialize trades
	eng.quote("ABC", 9.95, 10.05, start.Add(2*time.Second)) // initialize nbbo
	eng.trade("ABC", 10.10, start.Add(3*time.Second))       // trades alert
	eng.quote("ABC", 9.96, 10.06, start.Add(4*time.Second)) // no alert while trades active
	eng.setSource(alertSourceNBBO)
	eng.quote("ABC", 9.96, 10.06, start.Add(5*time.Second)) // equal nbbo high, no reset
	eng.quote("ABC", 9.97, 10.07, start.Add(6*time.Second)) // nbbo alert
	eng.trade("ABC", 10.20, start.Add(7*time.Second))       // no alert while nbbo active

	alerts := h.getHistory()
	if len(alerts) != 2 {
		t.Fatalf("alerts len = %d, want 2", len(alerts))
	}
	if alerts[0].Price != 10.10 || alerts[1].Price != 10.07 {
		t.Fatalf("unexpected prices after source switch: %#v", alerts)
	}
}

func TestWatchSubscriptionKindsFollowActiveAlertSource(t *testing.T) {
	tests := []struct {
		name   string
		source alertSource
		want   marketdata.StreamKinds
	}{
		{
			name:   "trades",
			source: alertSourceTrades,
			want:   marketdata.StreamKinds{Trades: true},
		},
		{
			name:   "nbbo",
			source: alertSourceNBBO,
			want:   marketdata.StreamKinds{Quotes: true},
		},
		{
			name:   "default",
			source: alertSource("unexpected"),
			want:   marketdata.StreamKinds{Trades: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := watchSubscriptionKinds(tc.source); got != tc.want {
				t.Fatalf("watchSubscriptionKinds(%q) = %#v, want %#v", tc.source, got, tc.want)
			}
		})
	}
}

func TestNormalizeClockHelpers(t *testing.T) {
	clockTests := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{in: "09:30", want: "09:30", wantOK: true},
		{in: "16:06", want: "16:06", wantOK: true},
		{in: "9:30", want: "09:30", wantOK: true},
		{in: "24:00", wantOK: false},
		{in: "", wantOK: false},
	}
	for _, tc := range clockTests {
		got, ok := normalizeClockHHMM(tc.in)
		if ok != tc.wantOK {
			t.Fatalf("normalizeClockHHMM(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
		}
		if tc.wantOK && got != tc.want {
			t.Fatalf("normalizeClockHHMM(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestClockOnDateETUsesETCalendarDay(t *testing.T) {
	et := mustET("America/New_York")
	// 01:00 UTC on Mar 2 is still Mar 1 in ET.
	dateUTC := time.Date(2026, time.March, 2, 1, 0, 0, 0, time.UTC)

	got, ok := clockOnDateET(dateUTC, et, "09:30")
	if !ok {
		t.Fatalf("clockOnDateET returned ok=false")
	}
	want := time.Date(2026, time.March, 1, 9, 30, 0, 0, et)
	if !got.Equal(want) {
		t.Fatalf("clockOnDateET = %v, want %v", got, want)
	}
}

func TestOdEngineLocalModeAlertsAfterBoundaryWithLocalKinds(t *testing.T) {
	et := mustET("America/New_York")
	start := time.Date(2026, time.March, 2, 10, 0, 0, 0, et)
	end := time.Date(2026, time.March, 2, 20, 0, 0, 0, et)
	alertsAfter := time.Date(2026, time.March, 2, 10, 30, 0, 0, et)

	h := newHub(50)
	eng := newOdEngine(h, et, start, end, alertsAfter, "lhigh", "llow", alertSourceTrades, true)
	eng.setAllowed([]string{"ABC"})
	eng.upsertSymbol("ABC", "ABC Co", []string{"watchlist"})

	// Before alertsAfter: update local hi/low state but never emit.
	eng.trade("ABC", 10.00, start.Add(5*time.Minute))
	eng.trade("ABC", 10.50, start.Add(10*time.Minute))
	eng.trade("ABC", 9.80, start.Add(20*time.Minute))
	eng.trade("ABC", 10.70, start.Add(29*time.Minute))

	// At/after alertsAfter: only true breakouts should emit local kinds.
	eng.trade("ABC", 10.70, start.Add(31*time.Minute)) // equal high, no alert
	eng.trade("ABC", 10.71, start.Add(32*time.Minute)) // local high alert
	eng.trade("ABC", 9.79, start.Add(33*time.Minute))  // local low alert

	alerts := h.getHistory()
	if len(alerts) != 2 {
		t.Fatalf("alerts len = %d, want 2", len(alerts))
	}
	if alerts[0].Kind != "lhigh" || alerts[1].Kind != "llow" {
		t.Fatalf("unexpected local alert kinds: %#v", alerts)
	}
	if alerts[0].Name != "ABC Co" || len(alerts[0].Sources) != 1 || alerts[0].Sources[0] != "watchlist" {
		t.Fatalf("unexpected metadata on local high alert: %#v", alerts[0])
	}
}

func TestOdEngineLocalModeHonorsEnabledToggle(t *testing.T) {
	et := mustET("America/New_York")
	start := time.Date(2026, time.March, 2, 9, 30, 0, 0, et)
	end := start.Add(6 * time.Hour)

	h := newHub(50)
	eng := newOdEngine(h, et, start, end, start, "lhigh", "llow", alertSourceTrades, false)
	eng.setAllowed([]string{"ABC"})
	eng.upsertSymbol("ABC", "ABC Co", []string{"watchlist"})

	eng.trade("ABC", 10.00, start.Add(1*time.Second))
	eng.trade("ABC", 11.00, start.Add(2*time.Second))

	eng.setEnabled(true)
	eng.trade("ABC", 12.00, start.Add(3*time.Second)) // initialize only once enabled
	eng.trade("ABC", 12.10, start.Add(4*time.Second)) // breakout -> alert

	alerts := h.getHistory()
	if len(alerts) != 1 {
		t.Fatalf("alerts len = %d, want 1", len(alerts))
	}
	if alerts[0].Kind != "lhigh" {
		t.Fatalf("alert kind = %q, want lhigh", alerts[0].Kind)
	}
}

func TestWebSoundRoutingAndLoudnessContract(t *testing.T) {
	js, err := os.ReadFile(filepath.Join("web", "app.js"))
	if err != nil {
		t.Fatalf("read web/app.js: %v", err)
	}
	src := string(js)

	requiredJS := []string{
		"let soundEnabled = true;",
		"function addIncomingAlert(a) {",
		`if (kind === "llow") {`,
		"playDownSound();",
		"playUpSound();",
		"function maybePlayTapeSound(msg) {",
	}
	for _, frag := range requiredJS {
		if !strings.Contains(src, frag) {
			t.Fatalf("web/app.js missing sound-routing contract fragment: %q", frag)
		}
	}

	forbiddenJS := []string{
		"createGain(",
		".volume =",
		"playbackRate =",
	}
	for _, frag := range forbiddenJS {
		if strings.Contains(src, frag) {
			t.Fatalf("web/app.js contains sound-altering fragment: %q", frag)
		}
	}

	html, err := os.ReadFile(filepath.Join("web", "index.html"))
	if err != nil {
		t.Fatalf("read web/index.html: %v", err)
	}
	index := string(html)
	requiredHTML := []string{
		`<audio id="alertAudioUp" src="/alert.mp3"`,
		`<audio id="alertAudioDown" src="/alert-down.mp3"`,
	}
	for _, frag := range requiredHTML {
		if !strings.Contains(index, frag) {
			t.Fatalf("web/index.html missing audio fallback fragment: %q", frag)
		}
	}
}

func TestStartStreamHonorsSelectedLocalAnchorContract(t *testing.T) {
	js, err := os.ReadFile(filepath.Join("web", "app.js"))
	if err != nil {
		t.Fatalf("read web/app.js: %v", err)
	}
	src := string(js)
	requiredJS := []string{
		"const localClock = currentLocalTimeInput() || currentETClockHHMM();",
		"local_time: localClock,",
	}
	for _, frag := range requiredJS {
		if !strings.Contains(src, frag) {
			t.Fatalf("web/app.js missing local-anchor start fragment: %q", frag)
		}
	}
	forbiddenJS := []string{
		"local_time: nowClock,",
		"localTimeInput.value = nowClock;",
	}
	for _, frag := range forbiddenJS {
		if strings.Contains(src, frag) {
			t.Fatalf("web/app.js contains stale start-anchor fragment: %q", frag)
		}
	}

	goSrcBytes, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	goSrc := string(goSrcBytes)
	requiredGo := []string{
		`if strings.TrimSpace(req.LocalTime) != "" {`,
		`} else if strings.ToLower(req.Mode) == "start" {`,
	}
	for _, frag := range requiredGo {
		if !strings.Contains(goSrc, frag) {
			t.Fatalf("main.go missing local-anchor fragment: %q", frag)
		}
	}
}

func TestTapePaceTracksBreadthChangingLocalAlertsContract(t *testing.T) {
	js, err := os.ReadFile(filepath.Join("web", "app.js"))
	if err != nil {
		t.Fatalf("read web/app.js: %v", err)
	}
	src := string(js)
	requiredJS := []string{
		"function applyBreakoutTransition(a, dirsBySymbol) {",
		"const paceTransition = applyBreakoutTransition(a, tapePaceDirsBySymbol);",
		`tapePaceEventsMs.push(alertTimeMs(a));`,
		`Pace of tape: ${count} local high/low alerts changed breakout breadth in the last ${windowSeconds} seconds`,
	}
	for _, frag := range requiredJS {
		if !strings.Contains(src, frag) {
			t.Fatalf("web/app.js missing tape-pace fragment: %q", frag)
		}
	}
	forbiddenJS := []string{
		`tapePaceEventsMs.push(Number(a.ts_unix || Date.now()));`,
		`Pace of tape: ${count} local high/low alerts in the last ${windowSeconds} seconds`,
	}
	for _, frag := range forbiddenJS {
		if strings.Contains(src, frag) {
			t.Fatalf("web/app.js contains stale tape-pace fragment: %q", frag)
		}
	}

	html, err := os.ReadFile(filepath.Join("web", "index.html"))
	if err != nil {
		t.Fatalf("read web/index.html: %v", err)
	}
	index := string(html)
	if !strings.Contains(index, `Counts local high/low alerts that change breakout breadth in the rolling pace window`) {
		t.Fatal("web/index.html missing updated tape pace tooltip")
	}
}

func TestQQQTapeAlertsDoNotRouteSyntheticSoundsContract(t *testing.T) {
	js, err := os.ReadFile(filepath.Join("web", "app.js"))
	if err != nil {
		t.Fatalf("read web/app.js: %v", err)
	}
	src := string(js)
	forbiddenJS := []string{
		"function playTapeAlertSound(kind) {",
		`if (a.kind === "qqq_buy" || a.kind === "qqq_sell") {`,
		`if (kind === "qqq_buy") return "QQQ TAPE BUY";`,
		`if (kind === "qqq_sell") return "QQQ TAPE SELL";`,
	}
	for _, frag := range forbiddenJS {
		if strings.Contains(src, frag) {
			t.Fatalf("web/app.js contains stale qqq tape alert fragment: %q", frag)
		}
	}
}

func TestEssentialsModeKeepsTopbarAndControlsVisibleContract(t *testing.T) {
	css, err := os.ReadFile(filepath.Join("web", "styles.css"))
	if err != nil {
		t.Fatalf("read web/styles.css: %v", err)
	}
	src := string(css)
	forbiddenCSS := []string{
		"body.essentials .topbar{\n  display:none;",
		"body.essentials .controls{\n  display:none !important;",
	}
	for _, frag := range forbiddenCSS {
		if strings.Contains(src, frag) {
			t.Fatalf("web/styles.css contains stale essentials visibility fragment: %q", frag)
		}
	}
}

func mustWriteFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
