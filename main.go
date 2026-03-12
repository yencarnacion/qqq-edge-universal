package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"

	"qqq-edge-universal/internal/marketdata"
	"qqq-edge-universal/internal/providers"
)

const (
	defaultHistoricalSeedTimeout = 90 * time.Second
	defaultWarmupMaxParallelism  = 3
)

type AppConfig struct {
	ServerPort int `yaml:"server_port"`
	MarketData struct {
		Provider string `yaml:"provider"`
	} `yaml:"market_data"`
	Alert struct {
		SoundFile       string `yaml:"sound_file"`
		UpSoundFile     string `yaml:"up_sound_file"`
		DownSoundFile   string `yaml:"down_sound_file"`
		EnableSound     bool   `yaml:"enable_sound"`
		CooldownSeconds int    `yaml:"cooldown_seconds"`
	} `yaml:"alert"`
	Timezone string `yaml:"timezone"`
	UI       struct {
		AutoNowSeconds          int    `yaml:"auto_now_seconds"`
		PaceOfTapeWindowSeconds int    `yaml:"pace_of_tape_window_seconds"`
		ChartOpenerBaseURL      string `yaml:"chart_opener_base_url"`
	} `yaml:"ui"`
}

type WatchEntry struct {
	Symbol string `yaml:"symbol"`
	Name   string `yaml:"name,omitempty"`
}

type WatchlistFile struct {
	Watchlist []WatchEntry `yaml:"watchlist"`
}

func loadYAML(path string, out any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, out)
}

func watchlistSourceLabel(path string) string {
	base := filepath.Base(strings.TrimSpace(path))
	if base == "" {
		return ""
	}
	ext := filepath.Ext(base)
	label := strings.TrimSuffix(base, ext)
	if label == "" {
		return base
	}
	return label
}

func parseWatchlistPaths(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

func isQQQModeWatchlists(paths []string) bool {
	if len(paths) != 1 {
		return false
	}
	base := strings.ToLower(strings.TrimSpace(filepath.Base(paths[0])))
	if base == "" {
		return false
	}
	return strings.TrimSuffix(base, filepath.Ext(base)) == "qqq"
}

func appendUniqueString(slice []string, v string) []string {
	if v == "" {
		return slice
	}
	for _, s := range slice {
		if s == v {
			return slice
		}
	}
	return append(slice, v)
}

func copyStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyStringSliceMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for k, v := range in {
		out[k] = copyStringSlice(v)
	}
	return out
}

func loadWatchlists(paths []string) ([]string, map[string]string, map[string][]string, error) {
	if len(paths) == 0 {
		return nil, nil, nil, fmt.Errorf("no watchlist files provided")
	}
	symbols := make([]string, 0, 256)
	names := make(map[string]string, 256)
	sources := make(map[string][]string, 256)
	seen := make(map[string]struct{}, 256)
	for _, path := range paths {
		var wl WatchlistFile
		if err := loadYAML(path, &wl); err != nil {
			return nil, nil, nil, fmt.Errorf("load %s: %w", path, err)
		}
		source := watchlistSourceLabel(path)
		for _, w := range wl.Watchlist {
			sym := strings.ToUpper(strings.TrimSpace(w.Symbol))
			if sym == "" {
				continue
			}
			if _, ok := seen[sym]; !ok {
				seen[sym] = struct{}{}
				symbols = append(symbols, sym)
			}
			if names[sym] == "" && strings.TrimSpace(w.Name) != "" {
				names[sym] = strings.TrimSpace(w.Name)
			}
			sources[sym] = appendUniqueString(sources[sym], source)
		}
	}
	if len(symbols) == 0 {
		return nil, nil, nil, fmt.Errorf("watchlist is empty")
	}
	return symbols, names, sources, nil
}

func mustET(tz string) *time.Location {
	if strings.TrimSpace(tz) == "" {
		tz = "America/New_York"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		log.Printf("tz load failed (%v); using America/New_York", err)
		loc, _ = time.LoadLocation("America/New_York")
	}
	return loc
}

func tradingDayBounds(et *time.Location, date time.Time) (time.Time, time.Time) {
	y, m, d := date.In(et).Date()
	start := time.Date(y, m, d, 4, 0, 0, 0, et)
	end := time.Date(y, m, d, 20, 0, 0, 0, et)
	return start, end
}

func normalizeClockHHMM(v string) (string, bool) {
	s := strings.TrimSpace(v)
	if s == "" {
		return "", false
	}
	t, err := time.Parse("15:04", s)
	if err != nil {
		return "", false
	}
	return fmt.Sprintf("%02d:%02d", t.Hour(), t.Minute()), true
}

func clockOnDateET(date time.Time, et *time.Location, hhmm string) (time.Time, bool) {
	clock, ok := normalizeClockHHMM(hhmm)
	if !ok {
		return time.Time{}, false
	}
	parts := strings.Split(clock, ":")
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return time.Time{}, false
	}
	y, mo, d := date.In(et).Date()
	return time.Date(y, mo, d, h, m, 0, 0, et), true
}

func sameETDate(a, b time.Time, et *time.Location) bool {
	ay, am, ad := a.In(et).Date()
	by, bm, bd := b.In(et).Date()
	return ay == by && am == bm && ad == bd
}

func etClock(ts time.Time) string {
	return ts.Format("15:04:05") + " ET"
}

func seedBreakoutHiLo(
	ctx context.Context,
	historical marketdata.HistoricalProvider,
	et *time.Location,
	symbols []string,
	names map[string]string,
	sources map[string][]string,
	anchorET, nowET, endET time.Time,
	eng *odEngine,
) {
	if historical == nil || eng == nil {
		return
	}
	if nowET.After(endET) {
		nowET = endET
	}
	if !anchorET.Before(nowET) {
		return
	}

	// Request one extra minute because provider APIs differ on upper-bound handling.
	requestEnd := nowET.Add(time.Minute)
	sessionEnd := endET.Add(time.Minute)
	if requestEnd.After(sessionEnd) {
		requestEnd = sessionEnd
	}

	sem := make(chan struct{}, defaultWarmupMaxParallelism)
	var wg sync.WaitGroup
	for _, sym := range symbols {
		sym := sym
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			bars, err := historical.RangeOhlcv1m(ctx, sym, anchorET.UTC(), requestEnd.UTC())
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[%s seed H/L] %s: %v", historical.Name(), sym, err)
				}
				return
			}

			minLow := math.Inf(1)
			maxHigh := math.Inf(-1)
			for _, bar := range bars {
				barStartET := bar.Start.In(et)
				if barStartET.Before(anchorET) || !barStartET.Before(requestEnd) {
					continue
				}
				if bar.Low > 0 && bar.Low < minLow {
					minLow = bar.Low
				}
				if bar.High > 0 && bar.High > maxHigh {
					maxHigh = bar.High
				}
			}

			if math.IsInf(minLow, 1) && math.IsInf(maxHigh, -1) {
				return
			}
			if math.IsInf(minLow, 1) {
				minLow = maxHigh
			}
			if math.IsInf(maxHigh, -1) {
				maxHigh = minLow
			}
			eng.seedHiLo(sym, names[sym], sources[sym], minLow, maxHigh)
		}()
	}
	wg.Wait()
}

type statusMsg struct {
	Type  string `json:"type"`
	Level string `json:"level"`
	Text  string `json:"text"`
}

type alertMsg struct {
	Type    string   `json:"type"`
	Kind    string   `json:"kind"`
	Time    string   `json:"time"`
	Sym     string   `json:"sym"`
	Name    string   `json:"name,omitempty"`
	Sources []string `json:"sources,omitempty"`
	Price   float64  `json:"price"`
	TSUnix  int64    `json:"ts_unix"`
}

type alertSource string

const (
	alertSourceNBBO    alertSource = "nbbo"
	alertSourceTrades  alertSource = "trades"
	defaultAlertSource alertSource = alertSourceTrades
)

func (s alertSource) String() string {
	if s == alertSourceNBBO {
		return string(alertSourceNBBO)
	}
	return string(defaultAlertSource)
}

func normalizeAlertSource(raw alertSource) alertSource {
	if raw == alertSourceNBBO || raw == alertSourceTrades {
		return raw
	}
	return defaultAlertSource
}

func parseAlertSource(raw string) (alertSource, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return defaultAlertSource, nil
	case string(alertSourceNBBO):
		return alertSourceNBBO, nil
	case string(alertSourceTrades):
		return alertSourceTrades, nil
	default:
		return "", fmt.Errorf("invalid alert source %q", raw)
	}
}

type historyMsg struct {
	Type   string     `json:"type"`
	Alerts []alertMsg `json:"alerts"`
}

type alertSourceMsg struct {
	Type   string `json:"type"`
	Source string `json:"source"`
}

type controlMsg struct {
	Type   string `json:"type"`
	Action string `json:"action"`
	Value  any    `json:"value,omitempty"`
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin:       func(*http.Request) bool { return true },
	EnableCompression: true,
}

type client struct {
	c      *websocket.Conn
	out    chan any
	done   chan struct{}
	paused atomic.Bool
}

type hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
	history []alertMsg
	limit   int
}

func newHub(limit int) *hub {
	return &hub{
		clients: make(map[*client]struct{}),
		history: make([]alertMsg, 0, limit),
		limit:   limit,
	}
}

func (h *hub) addHistory(a alertMsg) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.history = append(h.history, a)
	if h.limit > 0 && len(h.history) > h.limit {
		h.history = h.history[len(h.history)-h.limit:]
	}
}

func (h *hub) getHistory() []alertMsg {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]alertMsg, len(h.history))
	copy(out, h.history)
	return out
}

func (h *hub) resetHistories() {
	h.mu.Lock()
	h.history = h.history[:0]
	h.mu.Unlock()
}

func (h *hub) broadcast(v any) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c.out <- v:
		default:
		}
	}
}

func (h *hub) serveWS(onControl func(*client, controlMsg)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		cl := &client{c: conn, out: make(chan any, 256), done: make(chan struct{})}
		h.mu.Lock()
		h.clients[cl] = struct{}{}
		h.mu.Unlock()

		go func() {
			ping := time.NewTicker(45 * time.Second)
			defer ping.Stop()
			for {
				select {
				case v := <-cl.out:
					if cl.paused.Load() {
						if _, ok := v.(statusMsg); !ok {
							continue
						}
					}
					_ = conn.WriteJSON(v)
				case <-ping.C:
					_ = conn.WriteMessage(websocket.PingMessage, nil)
				case <-cl.done:
					return
				}
			}
		}()

		select {
		case cl.out <- statusMsg{Type: "status", Level: "info", Text: "Connected"}:
		default:
		}
		select {
		case cl.out <- historyMsg{Type: "history", Alerts: h.getHistory()}:
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
			return nil
		})
		for {
			mt, data, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if mt != websocket.TextMessage {
				continue
			}
			var ctrl controlMsg
			if err := json.Unmarshal(data, &ctrl); err != nil || ctrl.Type != "control" {
				continue
			}
			switch strings.ToLower(ctrl.Action) {
			case "pause":
				cl.paused.Store(true)
				select {
				case cl.out <- statusMsg{Type: "status", Level: "info", Text: "Paused (this tab)"}:
				default:
				}
			case "resume":
				cl.paused.Store(false)
				select {
				case cl.out <- statusMsg{Type: "status", Level: "success", Text: "Resumed (this tab)"}:
				default:
				}
			default:
				if onControl != nil {
					onControl(cl, ctrl)
				}
			}
		}

		close(cl.done)
		h.mu.Lock()
		delete(h.clients, cl)
		h.mu.Unlock()
	}
}

type instrumentState struct {
	Symbol  string
	Name    string
	Sources []string
	Quote   breakoutState
	Trade   breakoutState
}

type breakoutState struct {
	LOD         float64
	HOD         float64
	AlertedLow  float64
	AlertedHigh float64
}

func newBreakoutState() breakoutState {
	return breakoutState{
		LOD: math.Inf(1),
		HOD: math.Inf(-1),
	}
}

func newInstrumentState(sym, name string, sources []string) *instrumentState {
	return &instrumentState{
		Symbol:  sym,
		Name:    name,
		Sources: copyStringSlice(sources),
		Quote:   newBreakoutState(),
		Trade:   newBreakoutState(),
	}
}

type odEngine struct {
	mu            sync.RWMutex
	bySymbol      map[string]*instrumentState
	allowed       map[string]struct{}
	et            *time.Location
	startET       time.Time
	endET         time.Time
	alertsAfterET time.Time
	highKind      string
	lowKind       string
	source        alertSource
	enabled       bool
	h             *hub
	eps           float64
}

func newOdEngine(h *hub, et *time.Location, startET, endET, alertsAfterET time.Time, highKind, lowKind string, source alertSource, enabled bool) *odEngine {
	if strings.TrimSpace(highKind) == "" {
		highKind = "lhigh"
	}
	if strings.TrimSpace(lowKind) == "" {
		lowKind = "llow"
	}
	return &odEngine{
		bySymbol:      make(map[string]*instrumentState),
		allowed:       make(map[string]struct{}),
		et:            et,
		startET:       startET,
		endET:         endET,
		alertsAfterET: alertsAfterET,
		highKind:      strings.ToLower(strings.TrimSpace(highKind)),
		lowKind:       strings.ToLower(strings.TrimSpace(lowKind)),
		source:        normalizeAlertSource(source),
		enabled:       enabled,
		h:             h,
		eps:           1e-9,
	}
}

func (e *odEngine) resetWindow(startET, endET, alertsAfterET time.Time) {
	e.mu.Lock()
	e.startET = startET
	e.endET = endET
	e.alertsAfterET = alertsAfterET
	e.bySymbol = make(map[string]*instrumentState)
	e.mu.Unlock()
}

func (e *odEngine) setEnabled(v bool) {
	e.mu.Lock()
	e.enabled = v
	e.mu.Unlock()
}

func (e *odEngine) setSource(source alertSource) alertSource {
	e.mu.Lock()
	e.source = normalizeAlertSource(source)
	out := e.source
	e.mu.Unlock()
	return out
}

func (e *odEngine) setAllowed(symbols []string) {
	allowed := make(map[string]struct{}, len(symbols))
	for _, sym := range symbols {
		sym = strings.ToUpper(strings.TrimSpace(sym))
		if sym != "" {
			allowed[sym] = struct{}{}
		}
	}
	e.mu.Lock()
	e.allowed = allowed
	e.mu.Unlock()
}

func (e *odEngine) upsertSymbol(sym, name string, sources []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sym = strings.ToUpper(strings.TrimSpace(sym))
	st := e.bySymbol[sym]
	if st == nil {
		st = newInstrumentState(sym, name, sources)
		e.bySymbol[sym] = st
		return
	}
	if st.Name == "" && name != "" {
		st.Name = name
	}
	if len(sources) > 0 {
		st.Sources = copyStringSlice(sources)
	}
}

func (e *odEngine) trade(sym string, price float64, tsET time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.enabled {
		return
	}
	if tsET.Before(e.startET) || tsET.After(e.endET) {
		return
	}
	if _, ok := e.allowed[sym]; !ok {
		return
	}

	st := e.bySymbol[sym]
	if st == nil {
		st = newInstrumentState(sym, "", nil)
		e.bySymbol[sym] = st
	}
	e.observeBreakoutLocked(st, &st.Trade, sym, price, price, tsET, e.source == alertSourceTrades)
}

func (e *odEngine) quote(sym string, bid, ask float64, tsET time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.enabled {
		return
	}
	if tsET.Before(e.startET) || tsET.After(e.endET) {
		return
	}
	if _, ok := e.allowed[sym]; !ok {
		return
	}
	if bid > 0 && ask > 0 && ask+e.eps < bid {
		return
	}

	st := e.bySymbol[sym]
	if st == nil {
		st = newInstrumentState(sym, "", nil)
		e.bySymbol[sym] = st
	}
	e.observeBreakoutLocked(st, &st.Quote, sym, bid, ask, tsET, e.source == alertSourceNBBO)
}

func (e *odEngine) observeBreakoutLocked(st *instrumentState, state *breakoutState, sym string, low, high float64, tsET time.Time, emit bool) {
	lowValid := low > 0 && !math.IsNaN(low) && !math.IsInf(low, 0)
	highValid := high > 0 && !math.IsNaN(high) && !math.IsInf(high, 0)
	if !lowValid && !highValid {
		return
	}

	if tsET.Before(e.alertsAfterET) {
		if lowValid && low < state.LOD-e.eps {
			state.LOD = low
		}
		if highValid && high > state.HOD+e.eps {
			state.HOD = high
		}
		return
	}

	initialized := false
	if math.IsInf(state.LOD, 1) && lowValid {
		state.LOD = low
		initialized = true
	}
	if math.IsInf(state.HOD, -1) && highValid {
		state.HOD = high
		initialized = true
	}
	if initialized {
		return
	}

	if lowValid && low < state.LOD-e.eps {
		state.LOD = low
		if emit && (state.AlertedLow == 0 || low < state.AlertedLow-e.eps) {
			state.AlertedLow = low
			e.broadcastAlertLocked(st, sym, e.lowKind, low, tsET)
		}
	}
	if highValid && high > state.HOD+e.eps {
		state.HOD = high
		if emit && (state.AlertedHigh == 0 || high > state.AlertedHigh+e.eps) {
			state.AlertedHigh = high
			e.broadcastAlertLocked(st, sym, e.highKind, high, tsET)
		}
	}
}

func (e *odEngine) broadcastAlertLocked(st *instrumentState, sym, kind string, price float64, tsET time.Time) {
	msg := alertMsg{
		Type:    "alert",
		Kind:    kind,
		Time:    etClock(tsET),
		Sym:     sym,
		Name:    st.Name,
		Sources: copyStringSlice(st.Sources),
		Price:   price,
		TSUnix:  tsET.UnixMilli(),
	}
	e.h.addHistory(msg)
	e.h.broadcast(msg)
}

func (e *odEngine) seedHiLo(sym, name string, sources []string, lod, hod float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.allowed[sym]; !ok {
		return
	}
	st := e.bySymbol[sym]
	if st == nil {
		st = newInstrumentState(sym, "", nil)
		e.bySymbol[sym] = st
	}
	if name != "" && st.Name == "" {
		st.Name = name
	}
	if len(sources) > 0 {
		st.Sources = copyStringSlice(sources)
	}
	if !math.IsInf(lod, 1) && lod > 0 {
		st.Quote.LOD = lod
		st.Quote.AlertedLow = lod
		st.Trade.LOD = lod
		st.Trade.AlertedLow = lod
	}
	if !math.IsInf(hod, -1) && hod > 0 {
		st.Quote.HOD = hod
		st.Quote.AlertedHigh = hod
		st.Trade.HOD = hod
		st.Trade.AlertedHigh = hod
	}
}

func normalizedSoundPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if filepath.IsAbs(p) {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}

var cachedBeepWAV []byte

func synthBeepWAV(durationMs int, freqHz float64, sampleRate int) []byte {
	if durationMs <= 0 {
		durationMs = 350
	}
	if sampleRate <= 0 {
		sampleRate = 44100
	}
	n := int(float64(durationMs) / 1000.0 * float64(sampleRate))
	samples := make([]int16, n)
	amp := 3000.0
	for i := 0; i < n; i++ {
		t := float64(i) / float64(sampleRate)
		val := amp * math.Sin(2*math.Pi*freqHz*t)
		if val > 32767 {
			val = 32767
		}
		if val < -32768 {
			val = -32768
		}
		samples[i] = int16(val)
	}

	var buf bytes.Buffer
	buf.WriteString("RIFF")
	dataSize := len(samples) * 2
	chunkSize := uint32(36 + dataSize)
	_ = binary.Write(&buf, binary.LittleEndian, chunkSize)
	buf.WriteString("WAVE")
	buf.WriteString("fmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate*2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16))
	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(dataSize))
	for _, s := range samples {
		_ = binary.Write(&buf, binary.LittleEndian, s)
	}
	return buf.Bytes()
}

func serveStatic(mux *http.ServeMux, webDir string, upSoundPath string, downSoundPath string) {
	abs, _ := filepath.Abs(webDir)
	log.Printf("Serving static from %s", abs)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
	})

	fs := http.FileServer(http.Dir(webDir))
	mux.Handle("/assets/", http.StripPrefix("/assets/", fs))

	serveSoundFile := func(path string, w http.ResponseWriter, r *http.Request) {
		if p := strings.TrimSpace(path); p != "" {
			if st, err := os.Stat(p); err == nil && !st.IsDir() {
				w.Header().Set("Cache-Control", "public, max-age=864000")
				if strings.HasSuffix(strings.ToLower(p), ".mp3") {
					w.Header().Set("Content-Type", "audio/mpeg")
				}
				http.ServeFile(w, r, p)
				return
			}
		}
		if cachedBeepWAV == nil {
			cachedBeepWAV = synthBeepWAV(400, 880.0, 44100)
		}
		w.Header().Set("Content-Type", "audio/wav")
		w.Header().Set("Cache-Control", "public, max-age=864000")
		_, _ = w.Write(cachedBeepWAV)
	}

	mux.HandleFunc("/alert.mp3", func(w http.ResponseWriter, r *http.Request) {
		serveSoundFile(upSoundPath, w, r)
	})
	mux.HandleFunc("/alert-up.mp3", func(w http.ResponseWriter, r *http.Request) {
		serveSoundFile(upSoundPath, w, r)
	})
	mux.HandleFunc("/alert-down.mp3", func(w http.ResponseWriter, r *http.Request) {
		serveSoundFile(downSoundPath, w, r)
	})
}

func main() {
	portOverride := flag.Int("port", 0, "override server_port")
	watchlistsRaw := flag.String("watchlists", "", "comma-separated watchlist files")
	flag.Parse()

	_ = godotenv.Load(".env")

	var cfg AppConfig
	if err := loadYAML("config.yaml", &cfg); err != nil {
		log.Fatalf("load config.yaml: %v", err)
	}
	marketDataProviderName := providers.ResolveMarketDataProvider(cfg.MarketData.Provider, os.Getenv("MARKET_DATA_PROVIDER"))
	if cfg.ServerPort == 0 {
		if p := strings.TrimSpace(os.Getenv("PORT")); p != "" {
			if v, _ := strconv.Atoi(p); v > 0 {
				cfg.ServerPort = v
			}
		}
		if cfg.ServerPort == 0 {
			cfg.ServerPort = 8089
		}
	}
	if *portOverride != 0 {
		cfg.ServerPort = *portOverride
	}
	if cfg.UI.AutoNowSeconds <= 0 {
		cfg.UI.AutoNowSeconds = 10
	}
	if cfg.UI.PaceOfTapeWindowSeconds <= 0 {
		cfg.UI.PaceOfTapeWindowSeconds = 60
	}

	watchlistFiles := []string{"watchlist.yaml"}
	if strings.TrimSpace(*watchlistsRaw) != "" {
		watchlistFiles = parseWatchlistPaths(*watchlistsRaw)
		if len(watchlistFiles) == 0 {
			log.Fatal("watchlists flag is set but no valid files were provided")
		}
	}
	qqqMode := isQQQModeWatchlists(watchlistFiles)
	symbols, nameBySymbol, sourcesBySymbol, err := loadWatchlists(watchlistFiles)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("watchlists: %s (%d symbols)", strings.Join(watchlistFiles, ", "), len(symbols))
	log.Printf("market data provider: %s", marketDataProviderName)

	var watchMu sync.RWMutex
	getWatchSnapshot := func() ([]string, map[string]string, map[string][]string) {
		watchMu.RLock()
		defer watchMu.RUnlock()
		return copyStringSlice(symbols), copyStringMap(nameBySymbol), copyStringSliceMap(sourcesBySymbol)
	}
	getSymbolMeta := func(sym string) (string, []string) {
		watchMu.RLock()
		defer watchMu.RUnlock()
		return nameBySymbol[sym], copyStringSlice(sourcesBySymbol[sym])
	}
	setWatchData := func(nsymbols []string, nnames map[string]string, nsources map[string][]string) {
		watchMu.Lock()
		symbols = copyStringSlice(nsymbols)
		nameBySymbol = copyStringMap(nnames)
		sourcesBySymbol = copyStringSliceMap(nsources)
		watchMu.Unlock()
	}

	et := mustET(cfg.Timezone)
	h := newHub(500)

	qqqLeaders, err := loadQQQHoldings("qqq-etf-holdings.csv", qqqTapeLeaderLimit)
	if err != nil {
		log.Printf("[qqq tape] holdings load warning: %v", err)
	} else {
		log.Printf("[qqq tape] loaded %d weighted leaders from qqq-etf-holdings.csv", len(qqqLeaders))
	}
	qqqTape := newQQQTapeEngine(h, et, qqqLeaders)

	baseSound := normalizedSoundPath(cfg.Alert.SoundFile)
	upSound := normalizedSoundPath(cfg.Alert.UpSoundFile)
	downSound := normalizedSoundPath(cfg.Alert.DownSoundFile)
	if upSound == "" {
		upSound = baseSound
	}
	if downSound == "" {
		downSound = baseSound
	}

	mux := http.NewServeMux()
	serveStatic(mux, "web", upSound, downSound)
	mux.HandleFunc("/ws", h.serveWS(nil))

	type streamReq struct {
		Mode         string `json:"mode"`
		Date         string `json:"date,omitempty"`
		LocalTime    string `json:"local_time,omitempty"`
		LocalEnabled *bool  `json:"local_enabled,omitempty"`
	}
	type streamResp struct {
		OK     bool   `json:"ok"`
		Status string `json:"status"`
	}
	type alertSourceReq struct {
		Source string `json:"source"`
	}
	type alertSourceResp struct {
		OK     bool   `json:"ok"`
		Status string `json:"status"`
		Source string `json:"source"`
	}

	var (
		streamCancel context.CancelFunc
		streamCtx    context.Context
		broker       marketdata.LiveProvider
		eng          *odEngine

		subsMu    sync.Mutex
		watchSubs = make(map[string]*marketdata.Subscription)
		tapeSubs  = make(map[string]*marketdata.Subscription)

		stateMu          sync.RWMutex
		currentDate      time.Time
		localAnchorClock = "09:30"
		localAlertsOn    = true
		currentSource    = defaultAlertSource
	)

	applyState := func(date time.Time, clock string, enabled bool, source alertSource) {
		stateMu.Lock()
		currentDate = date
		localAnchorClock = clock
		localAlertsOn = enabled
		currentSource = normalizeAlertSource(source)
		stateMu.Unlock()
	}
	readState := func() (time.Time, string, bool, alertSource) {
		stateMu.RLock()
		defer stateMu.RUnlock()
		return currentDate, localAnchorClock, localAlertsOn, currentSource
	}

	startWatchConsumer := func(ctx context.Context, sym string) *marketdata.Subscription {
		sub := broker.Subscribe(sym, marketdata.StreamKinds{Trades: true, Quotes: true})
		if sub == nil {
			return nil
		}
		go func(sym string, sub *marketdata.Subscription) {
			for {
				select {
				case t := <-sub.Trades:
					if eng == nil {
						continue
					}
					ts := time.UnixMilli(t.T).In(et)
					name, sources := getSymbolMeta(sym)
					eng.upsertSymbol(sym, name, sources)
					eng.trade(sym, t.P, ts)
				case q := <-sub.Quotes:
					if eng == nil {
						continue
					}
					ts := time.UnixMilli(q.T).In(et)
					name, sources := getSymbolMeta(sym)
					eng.upsertSymbol(sym, name, sources)
					eng.quote(sym, q.Bp, q.Ap, ts)
				case <-sub.Done():
					return
				case <-ctx.Done():
					return
				}
			}
		}(sym, sub)
		return sub
	}

	startTapeConsumer := func(ctx context.Context, sym string) *marketdata.Subscription {
		sub := broker.Subscribe(sym, marketdata.StreamKinds{Trades: true, Quotes: true})
		if sub == nil {
			return nil
		}
		go func(sub *marketdata.Subscription) {
			for {
				select {
				case t := <-sub.Trades:
					qqqTape.OnTrade(t)
				case q := <-sub.Quotes:
					qqqTape.OnQuote(q)
				case <-sub.Done():
					return
				case <-ctx.Done():
					return
				}
			}
		}(sub)
		return sub
	}

	stopStream := func() {
		if streamCancel != nil {
			streamCancel()
			streamCancel = nil
		}
		if eng != nil {
			eng.setEnabled(false)
		}
		if broker != nil {
			subsMu.Lock()
			for _, sub := range watchSubs {
				broker.Unsubscribe(sub)
			}
			for _, sub := range tapeSubs {
				broker.Unsubscribe(sub)
			}
			watchSubs = make(map[string]*marketdata.Subscription)
			tapeSubs = make(map[string]*marketdata.Subscription)
			subsMu.Unlock()
			broker = nil
		}
		qqqTape.Reset()
	}

	parseDate := func(raw string, fallback time.Time) time.Time {
		if strings.TrimSpace(raw) == "" {
			return fallback
		}
		t, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(raw), et)
		if err != nil {
			return fallback
		}
		return t
	}

	mux.HandleFunc("/api/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req streamReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		switch strings.ToLower(req.Mode) {
		case "stop":
			stopStream()
			h.broadcast(statusMsg{Type: "status", Level: "info", Text: "Stopped"})
			_ = json.NewEncoder(w).Encode(streamResp{OK: true, Status: "Stopped"})
			return
		case "start", "update":
		default:
			_ = json.NewEncoder(w).Encode(streamResp{OK: false, Status: "Unknown mode"})
			return
		}

		if strings.ToLower(req.Mode) == "update" && (streamCancel == nil || eng == nil) {
			_ = json.NewEncoder(w).Encode(streamResp{OK: false, Status: "Not running"})
			return
		}

		prevDate, prevClock, prevEnabled, prevSource := readState()
		if prevDate.IsZero() {
			prevDate = time.Now().In(et)
		}
		if prevClock == "" {
			prevClock = "09:30"
		}

		date := parseDate(req.Date, prevDate)
		clock := prevClock
		if strings.TrimSpace(req.LocalTime) != "" {
			norm, ok := normalizeClockHHMM(req.LocalTime)
			if !ok {
				http.Error(w, "invalid local_time (expected HH:MM)", http.StatusBadRequest)
				return
			}
			clock = norm
		} else if strings.ToLower(req.Mode) == "start" {
			clock = time.Now().In(et).Format("15:04")
		}
		enabled := prevEnabled
		if req.LocalEnabled != nil {
			enabled = *req.LocalEnabled
		}
		if strings.ToLower(req.Mode) == "start" && req.LocalEnabled == nil {
			enabled = true
		}

		startET, endET := tradingDayBounds(et, date)
		localStartET, ok := clockOnDateET(date, et, clock)
		if !ok {
			http.Error(w, "invalid local_time (expected HH:MM)", http.StatusBadRequest)
			return
		}
		if localStartET.Before(startET) {
			localStartET = startET
			clock = localStartET.Format("15:04")
		}
		if !localStartET.Before(endET) {
			http.Error(w, "local_time must be before market close", http.StatusBadRequest)
			return
		}

		streamSymbols, streamNames, streamSources := getWatchSnapshot()
		nowET := time.Now().In(et)
		historicalSeed := func(anchorET time.Time) error {
			if !enabled {
				return nil
			}
			historical, err := providers.NewHistoricalProvider(marketDataProviderName, nil)
			if err != nil {
				return err
			}
			seedCtx, seedCancel := context.WithTimeout(context.Background(), defaultHistoricalSeedTimeout)
			defer seedCancel()
			seedBreakoutHiLo(seedCtx, historical, et, streamSymbols, streamNames, streamSources, anchorET, nowET, endET, eng)
			return nil
		}

		if strings.ToLower(req.Mode) == "start" {
			stopStream()
			h.resetHistories()
			qqqTape.Reset()

			eng = newOdEngine(h, et, localStartET, endET, nowET, "lhigh", "llow", prevSource, enabled)
			eng.setAllowed(streamSymbols)
			for _, sym := range streamSymbols {
				eng.upsertSymbol(sym, streamNames[sym], streamSources[sym])
			}
			if err := historicalSeed(localStartET); err != nil {
				http.Error(w, "market data historical provider init failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			streamCtx, streamCancel = context.WithCancel(context.Background())
			nextBroker, err := providers.NewLiveProvider(marketDataProviderName)
			if err != nil {
				streamCancel()
				streamCancel = nil
				http.Error(w, "market data provider init failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			broker = nextBroker

			subsMu.Lock()
			watchSubs = make(map[string]*marketdata.Subscription)
			for _, sym := range streamSymbols {
				watchSubs[sym] = startWatchConsumer(streamCtx, sym)
			}
			tapeSubs = make(map[string]*marketdata.Subscription)
			for _, sym := range qqqTape.Symbols() {
				tapeSubs[sym] = startTapeConsumer(streamCtx, sym)
			}
			subsMu.Unlock()

			go func() {
				if err := broker.Run(streamCtx); err != nil && streamCtx.Err() == nil {
					log.Printf("[%s] live provider stopped: %v", broker.Name(), err)
				}
			}()
		} else {
			eng.resetWindow(localStartET, endET, nowET)
			eng.setEnabled(enabled)
			eng.setAllowed(streamSymbols)
			eng.setSource(prevSource)
			for _, sym := range streamSymbols {
				eng.upsertSymbol(sym, streamNames[sym], streamSources[sym])
			}
			if err := historicalSeed(localStartET); err != nil {
				http.Error(w, "market data historical provider init failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if !sameETDate(prevDate, date, et) {
				qqqTape.Reset()
			}
		}

		applyState(date, clock, enabled, prevSource)
		status := fmt.Sprintf("Started (%s-%s ET), local anchor %s, alerts %s", startET.Format("15:04"), endET.Format("15:04"), clock, map[bool]string{true: "on", false: "off"}[enabled])
		h.broadcast(statusMsg{Type: "status", Level: "success", Text: status})
		_ = json.NewEncoder(w).Encode(streamResp{OK: true, Status: status})
	})

	mux.HandleFunc("/api/alert-source", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req alertSourceReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		source, err := parseAlertSource(req.Source)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		date, clock, enabled, _ := readState()
		applyState(date, clock, enabled, source)
		if eng != nil {
			eng.setSource(source)
		}

		msg := alertSourceMsg{Type: "alert_source", Source: source.String()}
		h.broadcast(msg)
		_ = json.NewEncoder(w).Encode(alertSourceResp{
			OK:     true,
			Status: "Alert source set to " + strings.ToUpper(source.String()),
			Source: source.String(),
		})
	})

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		type resp struct {
			Running            bool        `json:"running"`
			Date               string      `json:"date"`
			StartET            string      `json:"startET"`
			EndET              string      `json:"endET"`
			Port               int         `json:"port"`
			QQQMode            bool        `json:"qqq_mode"`
			WatchlistCount     int         `json:"watchlist_count"`
			Local              interface{} `json:"local"`
			QQQTape            interface{} `json:"qqq_tape"`
			UI                 interface{} `json:"ui"`
			AlertSource        string      `json:"alert_source"`
			MarketDataProvider string      `json:"market_data_provider"`
		}

		date, clock, enabled, source := readState()
		if date.IsZero() {
			date = time.Now().In(et)
		}
		startET, endET := tradingDayBounds(et, date)
		wsyms, _, _ := getWatchSnapshot()
		out := resp{
			Running:            streamCancel != nil,
			Date:               date.Format("2006-01-02"),
			StartET:            startET.Format("15:04"),
			EndET:              endET.Format("15:04"),
			Port:               cfg.ServerPort,
			QQQMode:            qqqMode,
			WatchlistCount:     len(wsyms),
			MarketDataProvider: marketDataProviderName,
			Local: map[string]any{
				"time":    clock,
				"enabled": enabled,
			},
			QQQTape:     qqqTape.Snapshot(),
			AlertSource: source.String(),
			UI: map[string]any{
				"auto_now_seconds":            cfg.UI.AutoNowSeconds,
				"pace_of_tape_window_seconds": cfg.UI.PaceOfTapeWindowSeconds,
			},
		}
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(out)
	})

	mux.HandleFunc("/api/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		h.resetHistories()
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	mux.HandleFunc("/api/watchlist", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		wsyms, _, wsources := getWatchSnapshot()
		_ = json.NewEncoder(w).Encode(map[string]any{"symbols": wsyms, "sources_by_symbol": wsources})
	})

	mux.HandleFunc("/api/watchlist/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}

		nsymbols, nnames, nsources, err := loadWatchlists(watchlistFiles)
		if err != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "status": "Reload failed: " + err.Error()})
			return
		}

		oldSymbols, _, _ := getWatchSnapshot()
		oldSet := make(map[string]struct{}, len(oldSymbols))
		for _, s := range oldSymbols {
			oldSet[s] = struct{}{}
		}
		newSet := make(map[string]struct{}, len(nsymbols))
		for _, s := range nsymbols {
			newSet[s] = struct{}{}
		}

		added := make([]string, 0)
		removed := make([]string, 0)
		kept := make([]string, 0)
		for s := range newSet {
			if _, ok := oldSet[s]; ok {
				kept = append(kept, s)
			} else {
				added = append(added, s)
			}
		}
		for s := range oldSet {
			if _, ok := newSet[s]; !ok {
				removed = append(removed, s)
			}
		}

		setWatchData(nsymbols, nnames, nsources)
		if eng != nil {
			eng.setAllowed(nsymbols)
			for _, s := range nsymbols {
				eng.upsertSymbol(s, nnames[s], nsources[s])
			}
		}
		if broker != nil {
			subsMu.Lock()
			for _, s := range added {
				if _, ok := watchSubs[s]; !ok {
					watchSubs[s] = startWatchConsumer(streamCtx, s)
				}
			}
			for _, s := range removed {
				if sub, ok := watchSubs[s]; ok {
					broker.Unsubscribe(sub)
					delete(watchSubs, s)
				}
			}
			subsMu.Unlock()
		}

		status := fmt.Sprintf("Watchlists reloaded (%d files): +%d / -%d (kept %d)", len(watchlistFiles), len(added), len(removed), len(kept))
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "status": status, "added": added, "removed": removed, "kept": kept})
		h.broadcast(statusMsg{Type: "status", Level: "info", Text: status})
	})

	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.ServerPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("UI: http://localhost:%d (sound: /alert.mp3)", cfg.ServerPort)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}
