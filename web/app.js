const statusPill = document.getElementById("status");
const feed = document.getElementById("feed");
const liveFeed = document.getElementById("live"); // NEW: right column live stream
const liveWrap = document.getElementById("liveWrap");
const alertsWrap = document.getElementById("alertsWrap");
const tabLiveBtn = document.getElementById("tabLiveBtn");
const tabRvolBtn = document.getElementById("tabRvolBtn");
const soundBtn = document.getElementById("soundBtn");
const soundState = document.getElementById("soundState");
const actionFlashEl = document.getElementById("actionFlash");
const actionBadgeEl = document.getElementById("actionBadge");
const chkEssentialsTop = document.getElementById("chkEssentialsTop");
const chkEssentialsQqqTape = document.getElementById("chkEssentialsQqqTape");

// Separate fallbacks for up/down
const fallbackAudioUp = document.getElementById("alertAudioUp");
const fallbackAudioDown = document.getElementById("alertAudioDown");
const dateInput = document.getElementById("dateInput");
const btnStart = document.getElementById("btnStart");
const btnStop = document.getElementById("btnStop");
const btnPause = document.getElementById("btnPause");
const btnClear = document.getElementById("btnClear");
const btnReloadWL = document.getElementById("btnReloadWl"); // NEW
const btnApplyLive = document.getElementById("btnApplyLive");
const chkHod = document.getElementById("chkHod");
const chkLod = document.getElementById("chkLod");
const chkLocalHigh = document.getElementById("chkLocalHigh");
const chkLocalLow = document.getElementById("chkLocalLow");
const chkCompact = document.getElementById("chkCompact"); // NEW
const chkScalps = document.getElementById("chkScalps");
const localTimeInput = document.getElementById("localTimeInput");
const btnLiveAuto = document.getElementById("btnLiveAuto");
const liveAutoCountdown = document.getElementById("liveAutoCountdown");
const liveNowTime = document.getElementById("liveNowTime");
const tapePaceIndicator = document.getElementById("tapePaceIndicator");
const tapePaceCountEl = document.getElementById("tapePaceCount");
const tapePaceWindowEl = document.getElementById("tapePaceWindow");
const levelsModeRadios = document.querySelectorAll('input[name="levelsMode"]');
const sessionQuickBtns = document.querySelectorAll(".sessionQuick");
const localPresetBtns = document.querySelectorAll(".localPreset");
// NEW RVOL controls
const rvolThreshold = document.getElementById("rvolThreshold");
const rvolMethod = document.getElementById("rvolMethod");
const baselineSingle = document.querySelector('input[name="baseline"][value="single"]');
const baselineCumulative = document.querySelector('input[name="baseline"][value="cumulative"]');
const rvolActive = document.getElementById("rvolActive");
const silentMode = document.getElementById("silentMode");
const bucketSize = document.getElementById("bucketSize");
const chkCurrentSounds = document.getElementById("chkCurrentSounds");
const chkSyntheticSounds = document.getElementById("chkSyntheticSounds");
// NEW Recent Alerts
const alertsTableBody = document.querySelector("#alertsTable tbody");
const alertsCount = document.getElementById("alertsCount");
// QQQ Tape panel
const qqqTapeExecEl = document.getElementById("qqqTapeExec");
const qqqTapeStateEl = document.getElementById("qqqTapeState");
const qqqTapePriceEl = document.getElementById("qqqTapePrice");
const qqqTapeFairValueEl = document.getElementById("qqqTapeFairValue");
const qqqTapeFairGapEl = document.getElementById("qqqTapeFairGap");
const qqqTapeExecBpsEl = document.getElementById("qqqTapeExecBps");
const qqqTapeSpreadEl = document.getElementById("qqqTapeSpread");
const qqqTapeFreshnessEl = document.getElementById("qqqTapeFreshness");
const qqqTapeCoverageEl = document.getElementById("qqqTapeCoverage");
const qqqTapeTradeEl = document.getElementById("qqqTapeTrade");
const qqqTapeQuoteEl = document.getElementById("qqqTapeQuote");
const qqqTapeMicroEl = document.getElementById("qqqTapeMicro");
const qqqTapeTopEl = document.getElementById("qqqTapeTop");
// Synthetic TICK panel
const tickValueEl = document.getElementById("tickValue");
const tickModeBadge = document.getElementById("tickModeBadge");
// NEW: table head + sort state (per-minute sort)
const alertsTableHead = document.querySelector("#alertsTable thead");
let alertsSortState = { key: "volume", dir: "desc" }; // default: volume desc per minute
// Pinned UI
const pinnedWrap = document.getElementById("pinnedWrap");
const pinnedList = document.getElementById("pinned");
const btnUnpinAll = document.getElementById("btnUnpinAll");
const pinnedCountEl = document.getElementById("pinnedCount");
let ws = null;
let audioCtx = null;
let audioBufUp = null;
let audioBufDown = null;
let scalpAudioBuf = null;
let soundEnabled = true; // default: Sound ON
let currentAlertSoundsEnabled = true;
let syntheticTapeSoundsEnabled = true;
let lastSyntheticPulseAtMs = 0;
let lastSyntheticDir = 0;
let lastSyntheticLevel = 0;
let syntheticDirStreak = 0;
const activeSyntheticSources = new Set();
const synthBullBuffers = [];
const synthBearBuffers = [];
let paused = false;
let historyLoaded = false;
let streamRunning = false;
let qqqModeEnabled = false;
const HISTORY_LIMIT = 200; // Hard cap on stored alerts to prevent Chrome crash
let allAlerts = []; // [{kind, sym, name, sources, price, time, ts_unix}]
let recentAlerts = []; // for RVOL [{time, symbol, price, volume, baseline, rvol, method}]
let silent = false;
let sessionDateET = ""; // YYYY-MM-DD (from /api/status)
const tickDirsBySymbol = new Map(); // sym -> -1 | 1
const tapePaceDirsBySymbol = new Map(); // sym -> -1 | 1 for pace-impact detection
let tickCurrentValue = 0;
let tickUniverseSize = 0; // total active watchlist symbols across all loaded watchlists
const scalpAudio = document.createElement("audio");
scalpAudio.src = "/scalp.mp3";
scalpAudio.preload = "auto";
// ---- FMP profile cache (in this browser for the day) ----
const profileCache = new Map(); // sym -> { marketCap:number, country:string, industry:string }
const profileInFlight = new Map(); // sym -> Promise<profile|null>
const extraCache = new Map(); // `${sym}|${date}|${days}` -> API payload
const extraInFlight = new Map(); // `${sym}|${date}|${days}` -> Promise<payload>
const PROFILE_CACHE_MAX = 600;
const EXTRA_CACHE_MAX = 1200;
const metaFetchMaxConcurrent = 4;
let metaFetchInFlight = 0;
const metaFetchQueue = [];
let autoNowEnabled = false;
let autoNowTimerId = 0;
let autoNowNextAt = 0;
let tapePaceTimerId = 0;
let tapePaceEventsMs = [];
let autoNowLastEnabled = null;
let autoNowLastIntervalSeconds = 0;
let autoNowLastCountdown = "";
let tapePaceLastState = "";
let tapePaceLastCount = -1;
let tapePaceLastWindowSeconds = 0;
let tapePaceLastTitle = "";

function setBoundedCache(map, key, value, maxSize) {
  map.set(key, value);
  while (map.size > maxSize) {
    const oldest = map.keys().next().value;
    if (oldest == null) break;
    map.delete(oldest);
  }
}

function drainMetaFetchQueue() {
  while (metaFetchInFlight < metaFetchMaxConcurrent && metaFetchQueue.length > 0) {
    const item = metaFetchQueue.shift();
    metaFetchInFlight += 1;
    Promise.resolve()
      .then(item.task)
      .then(item.resolve, item.reject)
      .finally(() => {
        metaFetchInFlight = Math.max(0, metaFetchInFlight - 1);
        drainMetaFetchQueue();
      });
  }
}

function runMetaFetchTask(task) {
  return new Promise((resolve, reject) => {
    metaFetchQueue.push({ task, resolve, reject });
    drainMetaFetchQueue();
  });
}

async function getExtra(sym, date, days = 2) {
  const k = String(sym || "").toUpperCase();
  const d = String(date || "");
  if (!k || !d) return { news: [], filings: [] };
  const key = `${k}|${d}|${days}`;
  if (extraCache.has(key)) return extraCache.get(key);
  if (extraInFlight.has(key)) return extraInFlight.get(key);

  const req = runMetaFetchTask(async () => {
    const res = await fetch(`/api/extra?ticker=${encodeURIComponent(k)}&date=${encodeURIComponent(d)}&days=${encodeURIComponent(days)}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const j = await res.json();
    setBoundedCache(extraCache, key, j, EXTRA_CACHE_MAX);
    return j;
  }).finally(() => {
    extraInFlight.delete(key);
  });

  extraInFlight.set(key, req);
  return req;
}
// ---- UI config (from server via /api/status), with sane defaults ----
let uiConfig = {
  tinyCapMax: 10_000_000,           // $10M
  industryRegex: "(medical|bio)",   // case-insensitive at runtime
  chartOpenerBaseURL: "http://localhost:8081",
  autoNowSeconds: 10,
  paceOfTapeWindowSeconds: 60
};
let industryRe = null;
function compileIndustryRegex(){
  try {
    industryRe = new RegExp(uiConfig.industryRegex, "i"); // force case-insensitive
  } catch {
    industryRe = /(medical|bio)/i;
  }
}
compileIndustryRegex();
// --- Mini chart management ---
let chartsLimit = 5; // (was const) allow dynamic limit in compact mode
const chartsById = new Map();
const activeChartQueue = [];
const chartLookbackMinDefault = 120;
let chartLookbackMin = chartLookbackMinDefault;
const chartRefreshMs = 15000;
const miniBarsMaxConcurrent = 2;
let miniBarsInFlight = 0;
const miniBarsQueue = [];

function drainMiniBarsQueue() {
  while (miniBarsInFlight < miniBarsMaxConcurrent && miniBarsQueue.length > 0) {
    const item = miniBarsQueue.shift();
    miniBarsInFlight += 1;
    Promise.resolve()
      .then(item.task)
      .then(item.resolve, item.reject)
      .finally(() => {
        miniBarsInFlight = Math.max(0, miniBarsInFlight - 1);
        drainMiniBarsQueue();
      });
  }
}

function runMiniBarsTask(task) {
  return new Promise((resolve, reject) => {
    miniBarsQueue.push({ task, resolve, reject });
    drainMiniBarsQueue();
  });
}
const RIGHT_TAB_STORAGE_KEY = "qqq-edge.right_tab";
const ESSENTIALS_STORAGE_KEY = "qqq-edge.essentials";
const CURRENT_SOUNDS_STORAGE_KEY = "qqq-edge.current_sounds";
const SYNTH_SOUNDS_STORAGE_KEY = "qqq-edge.synth_sounds";
// --- Pinning (persisted in this browser) ---
const pinLimit = 6;
let pinnedOrder = []; // array of ids (in pin insertion order)
let pinnedSet = new Set(); // quick membership
function loadPins(){
  try {
    const raw = localStorage.getItem("qqq-edge.pins") || "[]";
    const arr = JSON.parse(raw);
    pinnedOrder = Array.isArray(arr) ? arr.filter(Boolean) : [];
    pinnedSet = new Set(pinnedOrder);
  } catch {
    pinnedOrder = [];
    pinnedSet = new Set();
  }
}
function savePins(){
  localStorage.setItem("qqq-edge.pins", JSON.stringify(pinnedOrder));
}
function isPinnedId(id){ return pinnedSet.has(id); }
function togglePinById(id){
  if (pinnedSet.has(id)) {
    // Unpin
    pinnedSet.delete(id);
    pinnedOrder = pinnedOrder.filter(x => x !== id);
  } else {
    // Pin (respect limit; evict oldest)
    if (pinnedOrder.length >= pinLimit) {
      const evict = pinnedOrder.shift();
      if (evict) pinnedSet.delete(evict);
    }
    pinnedSet.add(id);
    pinnedOrder.push(id);
  }
  savePins();
  renderAll();
}
function setStatus(text, ok=false){
  statusPill.textContent = text;
  statusPill.className = ok ? "pill ok" : "pill";
}
function clamp01(v) {
  if (!Number.isFinite(v)) return 0;
  return Math.min(1, Math.max(0, v));
}
function loadSoundChannelPrefs() {
  try {
    const rawCurrent = localStorage.getItem(CURRENT_SOUNDS_STORAGE_KEY);
    if (rawCurrent === "0" || rawCurrent === "1") {
      currentAlertSoundsEnabled = rawCurrent === "1";
    }
  } catch {}
  try {
    const rawSynth = localStorage.getItem(SYNTH_SOUNDS_STORAGE_KEY);
    if (rawSynth === "0" || rawSynth === "1") {
      syntheticTapeSoundsEnabled = rawSynth === "1";
    }
  } catch {}
  if (chkCurrentSounds) chkCurrentSounds.checked = currentAlertSoundsEnabled;
  if (chkSyntheticSounds) chkSyntheticSounds.checked = syntheticTapeSoundsEnabled;
}
function persistSoundChannelPrefs() {
  try {
    localStorage.setItem(CURRENT_SOUNDS_STORAGE_KEY, currentAlertSoundsEnabled ? "1" : "0");
    localStorage.setItem(SYNTH_SOUNDS_STORAGE_KEY, syntheticTapeSoundsEnabled ? "1" : "0");
  } catch {}
}
function canPlayCurrentSounds() {
  return soundEnabled && !silent && currentAlertSoundsEnabled;
}
function canPlaySyntheticSounds() {
  return soundEnabled && !silent && syntheticTapeSoundsEnabled;
}
function stopCurrentSounds() {
  if (fallbackAudioUp) {
    try { fallbackAudioUp.pause(); fallbackAudioUp.currentTime = 0; } catch {}
  }
  if (fallbackAudioDown) {
    try { fallbackAudioDown.pause(); fallbackAudioDown.currentTime = 0; } catch {}
  }
  try { scalpAudio.pause(); scalpAudio.currentTime = 0; } catch {}
}
function stopSyntheticSounds() {
  for (const src of Array.from(activeSyntheticSources)) {
    try { src.stop(); } catch {}
  }
  activeSyntheticSources.clear();
  lastSyntheticPulseAtMs = 0;
  lastSyntheticDir = 0;
  lastSyntheticLevel = 0;
  syntheticDirStreak = 0;
}
function registerSyntheticSource(src) {
  if (!src) return;
  activeSyntheticSources.add(src);
  src.onended = () => activeSyntheticSources.delete(src);
}
function qqqEdgeIntensityFromMsg(msg) {
  const execEdge = Math.abs(Number(msg?.exec_edge_bps ?? msg?.edge_bps ?? 0));
  const fairGap = Math.abs(Number(msg?.fair_gap_bps ?? 0));
  const impulse = Math.abs(Number(msg?.trade_impulse ?? 0));
  const spread = Math.max(0, Number(msg?.spread_bps ?? 0));
  const freshness = Math.max(0, Number(msg?.freshness_ms ?? 0));
  const tradableBonus = msg?.tradable ? 0.65 : 0;

  if (freshness > 3200) return 0;

  let score = execEdge;
  score += 0.30 * fairGap;
  score += 0.18 * impulse;
  score += tradableBonus;
  score -= Math.max(0, spread - 0.35) * 0.10;

  const freshnessPenalty = freshness <= 1000
    ? 0
    : Math.min(0.75, (freshness - 1000) / 2500);

  return clamp01((score / 6.5) * (1 - freshnessPenalty));
}
function synthPulseGapMs(level, tradable) {
  const base = tradable ? 850 : 1150;
  return Math.round(base - level * 110);
}
function addShapedTone(data, sampleRate, startSec, durSec, f0, f1, amp, harmonic = 0.25) {
  const start = Math.max(0, Math.floor(startSec * sampleRate));
  const len = Math.max(1, Math.floor(durSec * sampleRate));
  let phaseA = 0;
  let phaseB = 0;
  for (let i = 0; i < len && start + i < data.length; i++) {
    const p = len > 1 ? i / (len - 1) : 0;
    const freq = f0 + (f1 - f0) * p;
    phaseA += (2 * Math.PI * freq) / sampleRate;
    phaseB += (2 * Math.PI * freq * 2.0) / sampleRate;
    const attack = Math.min(1, p / 0.15);
    const release = Math.min(1, (1 - p) / 0.22);
    const env = Math.min(attack, release);
    const sample = (Math.sin(phaseA) + harmonic * Math.sin(phaseB)) * amp * env;
    data[start + i] += sample;
  }
}
function finalizeBuffer(data) {
  for (let i = 0; i < data.length; i++) {
    if (data[i] > 1) data[i] = 1;
    else if (data[i] < -1) data[i] = -1;
  }
}
function buildBullSynthBuffer(sampleRate, intensity) {
  if (!audioCtx) return null;
  const i = clamp01(intensity);
  const durSec = 0.24 + 0.03 * i;
  const n = Math.max(1, Math.floor(sampleRate * durSec));
  const data = new Float32Array(n);

  addShapedTone(data, sampleRate, 0.00, 0.085 + 0.01 * i, 760 + 60 * i, 980 + 120 * i, 0.12 + 0.03 * i, 0.22);
  addShapedTone(data, sampleRate, 0.11, 0.080 + 0.015 * i, 910 + 80 * i, 1220 + 150 * i, 0.10 + 0.03 * i, 0.18);

  finalizeBuffer(data);
  const buf = audioCtx.createBuffer(1, n, sampleRate);
  buf.copyToChannel(data, 0, 0);
  return buf;
}
function buildBearSynthBuffer(sampleRate, intensity) {
  if (!audioCtx) return null;
  const i = clamp01(intensity);
  const durSec = 0.26 + 0.04 * i;
  const n = Math.max(1, Math.floor(sampleRate * durSec));
  const data = new Float32Array(n);

  addShapedTone(data, sampleRate, 0.00, 0.095 + 0.015 * i, 430 + 35 * i, 260 - 20 * i, 0.13 + 0.03 * i, 0.30);
  addShapedTone(data, sampleRate, 0.13, 0.085 + 0.015 * i, 300 + 25 * i, 180 - 10 * i, 0.10 + 0.03 * i, 0.36);

  finalizeBuffer(data);
  const buf = audioCtx.createBuffer(1, n, sampleRate);
  buf.copyToChannel(data, 0, 0);
  return buf;
}
function ensureSyntheticBuffers() {
  if (!audioCtx) return;
  if (synthBullBuffers.length > 0 && synthBearBuffers.length > 0) return;
  const sampleRate = Math.max(22050, Math.floor(audioCtx.sampleRate || 44100));
  for (let level = 0; level <= 5; level++) {
    const intensity = level / 5;
    synthBullBuffers[level] = buildBullSynthBuffer(sampleRate, intensity);
    synthBearBuffers[level] = buildBearSynthBuffer(sampleRate, intensity);
  }
}
function playSyntheticBuffer(buf) {
  if (!audioCtx || !buf) return;
  const src = audioCtx.createBufferSource();
  src.buffer = buf;
  src.connect(audioCtx.destination);
  registerSyntheticSource(src);
  src.start();
}
function maybePlaySyntheticTapeSound(msg) {
  if (!canPlaySyntheticSounds()) return;
  if (!audioCtx) return;
  ensureSyntheticBuffers();

  const edge = Number(msg?.exec_edge_bps ?? msg?.edge_bps ?? 0);
  const dir = edge > 0 ? 1 : (edge < 0 ? -1 : 0);
  const intensity = qqqEdgeIntensityFromMsg(msg);
  const level = Math.max(0, Math.min(5, Math.round(intensity * 5)));
  const tradable = !!msg?.tradable;
  const spread = Math.max(0, Number(msg?.spread_bps ?? 0));
  const freshness = Math.max(0, Number(msg?.freshness_ms ?? 0));

  if (!dir || level === 0 || freshness > 3200) {
    lastSyntheticDir = 0;
    lastSyntheticLevel = 0;
    syntheticDirStreak = 0;
    return;
  }

  const watchable = Math.abs(edge) >= Math.max(0.55, spread * 1.20);
  if (!tradable && !watchable) return;

  if (dir === lastSyntheticDir) syntheticDirStreak += 1;
  else syntheticDirStreak = 1;

  // Require stability unless the setup is already tradable.
  if (!tradable && syntheticDirStreak < 2) return;

  const nowMs = (typeof performance !== "undefined" && performance.now)
    ? performance.now()
    : Date.now();
  const forcePulse = dir !== lastSyntheticDir || level > lastSyntheticLevel;
  const minGap = synthPulseGapMs(level, tradable);
  if (!forcePulse && nowMs - lastSyntheticPulseAtMs < minGap) return;

  if (audioCtx.state === "suspended") {
    try { audioCtx.resume(); } catch {}
  }
  const buf = dir > 0 ? synthBullBuffers[level] : synthBearBuffers[level];
  playSyntheticBuffer(buf);
  lastSyntheticPulseAtMs = nowMs;
  lastSyntheticDir = dir;
  lastSyntheticLevel = level;
}
async function loadSound() {
  try {
    const AC = window.AudioContext || window.webkitAudioContext;
    if (!AC) return false;
    audioCtx = new AC();

    // Load UP and DOWN alert sounds (server: /alert.mp3 is "up", /alert-down.mp3 is "down")
    const [upResp, downResp] = await Promise.all([
      fetch("/alert.mp3", { cache: "force-cache" }),         // UP
      fetch("/alert-down.mp3", { cache: "force-cache" }),    // DOWN
    ]);

    const upArr = await upResp.arrayBuffer();
    audioBufUp = await audioCtx.decodeAudioData(upArr);

    const downArr = await downResp.arrayBuffer();
    audioBufDown = await audioCtx.decodeAudioData(downArr);

    // Scalp sound (best-effort)
    try {
      const rs = await fetch("/scalp.mp3", { cache: "force-cache" });
      const ra = await rs.arrayBuffer();
      scalpAudioBuf = await audioCtx.decodeAudioData(ra);
    } catch {}

    return true;
  } catch {
    return false;
  }
}
async function enableSound() {
  if (!audioCtx || audioCtx.state === "suspended") {
    try { await audioCtx.resume(); } catch {}
  }
  soundEnabled = true;
  soundState.textContent = "Sound ON";
}

function playUpSound() {
  if (!canPlayCurrentSounds()) return;

  if (audioCtx && audioBufUp) {
    try {
      const src = audioCtx.createBufferSource();
      src.buffer = audioBufUp;
      src.connect(audioCtx.destination);
      src.start();
      return;
    } catch {}
  }
  // Fallback <audio> element
  if (fallbackAudioUp) {
    try { fallbackAudioUp.currentTime = 0; fallbackAudioUp.play(); } catch {}
  }
}

function playDownSound() {
  if (!canPlayCurrentSounds()) return;

  if (audioCtx && audioBufDown) {
    try {
      const src = audioCtx.createBufferSource();
      src.buffer = audioBufDown;
      src.connect(audioCtx.destination);
      src.start();
      return;
    } catch {}
  }
  // Fallback <audio> element
  if (fallbackAudioDown) {
    try { fallbackAudioDown.currentTime = 0; fallbackAudioDown.play(); } catch {}
  }
}

// Maintain backward compatibility: generic "playSound" = "up"
function playSound() {
  playUpSound();
}
function playScalpSound() {
  if (!canPlayCurrentSounds()) return;
  if (audioCtx && scalpAudioBuf) {
    try {
      const src = audioCtx.createBufferSource();
      src.buffer = scalpAudioBuf;
      src.connect(audioCtx.destination);
      src.start();
      return;
    } catch {}
  }
  // fallback
  try { scalpAudio.currentTime = 0; scalpAudio.play(); } catch {}
}
function todayISO() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth()+1).padStart(2,"0");
  const dd = String(d.getDate()).padStart(2,"0");
  return `${yyyy}-${mm}-${dd}`;
}
function selectedSession() {
  const sessEl = document.querySelector('input[name="session"]:checked');
  return sessEl ? sessEl.value : "rth";
}
function selectedLevelsMode() {
  const m = document.querySelector('input[name="levelsMode"]:checked');
  return m ? m.value : "session";
}
function setLevelsMode(mode) {
  const m = mode === "local" ? "local" : "session";
  const el = document.querySelector(`input[name="levelsMode"][value="${m}"]`);
  if (el) el.checked = true;
  syncLevelsModeUI();
}
function syncLevelsModeUI() {
  const mode = selectedLevelsMode();
  const isSession = mode === "session";
  if (chkHod) {
    chkHod.disabled = !isSession;
    if (!isSession) {
      chkHod.checked = false;
    } else if (!chkHod.checked && !chkLod?.checked) {
      chkHod.checked = true;
    }
  }
  if (chkLod) {
    chkLod.disabled = !isSession;
    if (!isSession) chkLod.checked = false;
  }
  if (chkLocalHigh) {
    chkLocalHigh.disabled = isSession;
    if (isSession) {
      chkLocalHigh.checked = false;
    } else if (!chkLocalHigh.checked && !chkLocalLow?.checked) {
      chkLocalHigh.checked = true;
      if (chkLocalLow) chkLocalLow.checked = true;
    }
  }
  if (chkLocalLow) {
    chkLocalLow.disabled = isSession;
    if (isSession) chkLocalLow.checked = false;
  }
  renderAll();
}
function defaultLocalTimeForSession(session) {
  if (session === "pre") return "04:06";
  if (session === "pm") return "16:06";
  return "09:30";
}
function normalizeHHMM(raw) {
  const s = String(raw || "").trim();
  if (!/^\d{2}:\d{2}$/.test(s)) return "";
  const [h, m] = s.split(":").map(Number);
  if (!Number.isFinite(h) || !Number.isFinite(m)) return "";
  if (h < 0 || h > 23 || m < 0 || m > 59) return "";
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}
function feedbackClockET() {
  try {
    const fmt = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
    return `${fmt.format(new Date())} ET`;
  } catch {
    return "";
  }
}
function showActionFeedback(label) {
  const text = String(label || "APPLIED").trim().toUpperCase();
  if (actionFlashEl) {
    actionFlashEl.classList.remove("show");
    void actionFlashEl.offsetWidth;
    actionFlashEl.classList.add("show");
  }
  if (actionBadgeEl) {
    actionBadgeEl.textContent = `${text} • ${feedbackClockET()}`;
    actionBadgeEl.classList.remove("show");
    void actionBadgeEl.offsetWidth;
    actionBadgeEl.classList.add("show");
  }
}
function isTypingTarget(target) {
  const el = target instanceof Element ? target : null;
  if (!el) return false;
  const tag = (el.tagName || "").toUpperCase();
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || tag === "BUTTON") return true;
  if (el.isContentEditable) return true;
  return !!el.closest("input, textarea, select, button, [contenteditable='true']");
}
function autoNowIntervalMs() {
  return Math.max(1, Number(uiConfig.autoNowSeconds) || 10) * 1000;
}
function tapePaceWindowMs() {
  return Math.max(1, Number(uiConfig.paceOfTapeWindowSeconds) || 60) * 1000;
}
function isTapePaceKind(kind) {
  return kind === "hod" || kind === "lod" || kind === "lhigh" || kind === "llow";
}
function breakoutDirectionForAlert(alertObj, mode = selectedLevelsMode()) {
  const kind = String(alertObj?.kind || "").toLowerCase();
  const sym = String(alertObj?.sym || "").trim().toUpperCase();
  if (!sym || !kind) return null;
  const info = tickModeInfo(mode);
  const dir = kind === info.upKind ? 1 : (kind === info.downKind ? -1 : 0);
  if (!dir) return null;
  return { sym, dir };
}
function applyBreakoutTransition(alertObj, dirsBySymbol, mode = selectedLevelsMode()) {
  const breakout = breakoutDirectionForAlert(alertObj, mode);
  if (!breakout) return null;
  const prevDir = dirsBySymbol.get(breakout.sym) || 0;
  if (prevDir === breakout.dir) return null;
  dirsBySymbol.set(breakout.sym, breakout.dir);
  return {
    sym: breakout.sym,
    prevDir,
    nextDir: breakout.dir,
    delta: breakout.dir - prevDir,
    time: tickEpochSeconds(alertObj),
  };
}
function tapePaceEventTimeMs(alertObj) {
  const tsMs = Number(alertObj?.ts_unix);
  if (Number.isFinite(tsMs) && tsMs > 0) return tsMs;
  return Date.now();
}
function pruneTapePaceEvents(nowMs = Date.now()) {
  const cutoff = nowMs - tapePaceWindowMs();
  let firstValidIdx = 0;
  while (firstValidIdx < tapePaceEventsMs.length && tapePaceEventsMs[firstValidIdx] < cutoff) {
    firstValidIdx += 1;
  }
  if (firstValidIdx > 0) tapePaceEventsMs.splice(0, firstValidIdx);
}
function tapePaceState(count, windowSeconds) {
  const ratePerMinute = count * (60 / Math.max(1, windowSeconds));
  if (count <= 0) return "idle";
  if (ratePerMinute < 4) return "slow";
  if (ratePerMinute < 8) return "active";
  if (ratePerMinute < 14) return "fast";
  return "hot";
}
function refreshTapePaceIndicator(nowMs = Date.now()) {
  if (!tapePaceIndicator || !tapePaceCountEl || !tapePaceWindowEl) return;
  if (tapePaceTimerId) {
    window.clearTimeout(tapePaceTimerId);
    tapePaceTimerId = 0;
  }
  pruneTapePaceEvents(nowMs);
  const windowSeconds = Math.max(1, Number(uiConfig.paceOfTapeWindowSeconds) || 60);
  const count = tapePaceEventsMs.length;
  const state = tapePaceState(count, windowSeconds);
  const info = tickModeInfo();
  const modeLabel = info.label;
  const title = `${count} ${modeLabel} alerts changed breakout breadth in the last ${windowSeconds} seconds`;
  if (tapePaceLastState !== state) {
    tapePaceIndicator.dataset.state = state;
    tapePaceLastState = state;
  }
  if (tapePaceLastCount !== count) {
    tapePaceCountEl.textContent = String(count);
    tapePaceLastCount = count;
  }
  if (tapePaceLastWindowSeconds !== windowSeconds) {
    tapePaceWindowEl.textContent = `${windowSeconds}s`;
    tapePaceLastWindowSeconds = windowSeconds;
  }
  if (tapePaceLastTitle !== title) {
    tapePaceIndicator.title = title;
    tapePaceIndicator.setAttribute("aria-label", `Pace of tape ${state}: ${title}`);
    tapePaceLastTitle = title;
  }
  if (count <= 0) return;
  const nextExpiryAt = tapePaceEventsMs[0] + tapePaceWindowMs();
  const delayMs = Math.max(50, nextExpiryAt - nowMs);
  tapePaceTimerId = window.setTimeout(() => {
    refreshTapePaceIndicator();
  }, delayMs);
}
function rebuildTapePaceEvents(alerts) {
  tapePaceEventsMs = [];
  tapePaceDirsBySymbol.clear();
  const items = Array.isArray(alerts) ? alerts : [];
  const nowMs = Date.now();
  const windowMs = tapePaceWindowMs();
  const mode = selectedLevelsMode();
  const ordered = items.slice().sort((a, b) => Number(a?.ts_unix || 0) - Number(b?.ts_unix || 0));
  for (const alertObj of ordered) {
    if (!isTapePaceKind(alertObj?.kind)) continue;
    const transition = applyBreakoutTransition(alertObj, tapePaceDirsBySymbol, mode);
    if (!transition) continue;
    const tsMs = tapePaceEventTimeMs(alertObj);
    if (nowMs - tsMs > windowMs || tsMs > nowMs) continue;
    tapePaceEventsMs.push(tsMs);
  }
  tapePaceEventsMs.sort((a, b) => a - b);
  refreshTapePaceIndicator();
}
function recordTapePaceEvent(alertObj) {
  if (!isTapePaceKind(alertObj?.kind)) return;
  const transition = applyBreakoutTransition(alertObj, tapePaceDirsBySymbol);
  if (!transition) return;
  const nowMs = Date.now();
  const tsMs = tapePaceEventTimeMs(alertObj);
  if (tsMs > nowMs) return;
  tapePaceEventsMs.push(tsMs);
  if (tapePaceEventsMs.length > 1 && tsMs < tapePaceEventsMs[tapePaceEventsMs.length - 2]) {
    tapePaceEventsMs.sort((a, b) => a - b);
  }
  refreshTapePaceIndicator(nowMs);
}
function formatAutoCountdown(msRemaining) {
  return `${Math.max(0, Math.ceil(msRemaining / 1000))}s`;
}
function autoNowCountdownDelayMs(nowMs = Date.now()) {
  const remainingMs = autoNowNextAt - nowMs;
  if (remainingMs <= 0) return 0;
  const secondsShown = Math.max(1, Math.ceil(remainingMs / 1000));
  return Math.max(50, remainingMs - ((secondsShown - 1) * 1000));
}
function scheduleAutoNowTick(nowMs = Date.now()) {
  if (autoNowTimerId) {
    window.clearTimeout(autoNowTimerId);
    autoNowTimerId = 0;
  }
  if (!autoNowEnabled) return;
  const delayMs = autoNowNextAt <= nowMs ? 0 : autoNowCountdownDelayMs(nowMs);
  autoNowTimerId = window.setTimeout(() => {
    tickAutoNow();
  }, delayMs);
}
function syncAutoNowButton(nowMs = Date.now()) {
  if (!btnLiveAuto) return;
  const intervalSeconds = Math.max(1, Number(uiConfig.autoNowSeconds) || 10);
  const enabledChanged = autoNowLastEnabled !== autoNowEnabled;
  if (enabledChanged) {
    btnLiveAuto.classList.toggle("isActive", autoNowEnabled);
    btnLiveAuto.setAttribute("aria-pressed", autoNowEnabled ? "true" : "false");
    autoNowLastEnabled = autoNowEnabled;
  }
  if (enabledChanged || autoNowLastIntervalSeconds !== intervalSeconds) {
    btnLiveAuto.title = autoNowEnabled
      ? `Disable automatic Now refresh every ${intervalSeconds} seconds`
      : `Enable automatic Now refresh every ${intervalSeconds} seconds`;
    autoNowLastIntervalSeconds = intervalSeconds;
  }
  if (!liveAutoCountdown) return;
  if (!autoNowEnabled) {
    if (!liveAutoCountdown.hidden) liveAutoCountdown.hidden = true;
    if (autoNowLastCountdown !== "") {
      liveAutoCountdown.textContent = "";
      autoNowLastCountdown = "";
    }
    return;
  }
  if (liveAutoCountdown.hidden) liveAutoCountdown.hidden = false;
  const countdownText = formatAutoCountdown(Math.max(0, autoNowNextAt - nowMs));
  if (autoNowLastCountdown !== countdownText) {
    liveAutoCountdown.textContent = countdownText;
    autoNowLastCountdown = countdownText;
  }
}
function resetAutoNowCountdown() {
  if (!autoNowEnabled) return;
  autoNowNextAt = Date.now() + autoNowIntervalMs();
  syncAutoNowButton();
  scheduleAutoNowTick();
}
function stopAutoNow() {
  autoNowEnabled = false;
  autoNowNextAt = 0;
  if (autoNowTimerId) {
    window.clearTimeout(autoNowTimerId);
    autoNowTimerId = 0;
  }
  syncAutoNowButton();
}
function tickAutoNow() {
  if (!autoNowEnabled) return;
  const now = Date.now();
  if (now >= autoNowNextAt) {
    autoNowNextAt = now + autoNowIntervalMs();
    syncAutoNowButton(now);
    scheduleAutoNowTick(now);
    void triggerNowShortcut({ resetAutoCountdown: false });
    return;
  }
  syncAutoNowButton(now);
  scheduleAutoNowTick(now);
}
function startAutoNow() {
  autoNowEnabled = true;
  autoNowNextAt = Date.now() + autoNowIntervalMs();
  syncAutoNowButton();
  scheduleAutoNowTick();
}
function setAutoNow(enabled) {
  if (enabled) {
    startAutoNow();
    showActionFeedback("Auto On");
    return;
  }
  stopAutoNow();
  showActionFeedback("Auto Off");
}
async function applyLocalPresetButton(btn, opts = {}) {
  if (!btn) return;
  const sess = (btn.dataset.session || "").trim();
  const rawTime = String(btn.dataset.time || "").trim().toLowerCase();
  if (rawTime === "now") {
    showActionFeedback("Now");
  }
  let t = "";
  if (rawTime === "now") {
    t = nowHHMMET();
  } else if (rawTime === "prev_half") {
    t = halfHourStartET();
  } else if (rawTime === "prev_hour") {
    t = hourStartET();
  } else {
    t = normalizeHHMM(rawTime);
  }
  setLevelsMode("local");
  if (sess) {
    const radio = document.querySelector(`input[name="session"][value="${sess}"]`);
    if (radio) radio.checked = true;
  }
  if (t && localTimeInput) localTimeInput.value = t;
  if (rawTime === "now" && opts.resetAutoCountdown !== false) {
    resetAutoNowCountdown();
  }
  await applyLiveSettings({ resetTick: true });
}
function triggerNowShortcut(opts = {}) {
  const btn = document.getElementById("btnLiveNow") || document.querySelector(".localPreset[data-time='now']");
  if (btn) {
    return applyLocalPresetButton(btn, opts);
  }
  showActionFeedback("Now");
  if (opts.resetAutoCountdown !== false) resetAutoNowCountdown();
}
function nowHHMMET() {
  try {
    const fmt = new Intl.DateTimeFormat("en-GB", {
      timeZone: "America/New_York",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
    return normalizeHHMM(fmt.format(new Date())) || "09:30";
  } catch {
    return "09:30";
  }
}
function parseHHMM(hhmm) {
  const normalized = normalizeHHMM(hhmm);
  if (!normalized) return null;
  const [h, m] = normalized.split(":").map(Number);
  if (!Number.isFinite(h) || !Number.isFinite(m)) return null;
  return { h, m };
}
function halfHourStartET() {
  const p = parseHHMM(nowHHMMET());
  if (!p) return "09:30";
  const mm = p.m >= 30 ? 30 : 0;
  return `${String(p.h).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
}
function hourStartET() {
  const p = parseHHMM(nowHHMMET());
  if (!p) return "09:00";
  return `${String(p.h).padStart(2, "0")}:00`;
}
function syncLocalTimeMirror(raw) {
  if (!liveNowTime) return;
  const normalized = normalizeHHMM(raw || localTimeInput?.value || "");
  liveNowTime.textContent = normalized || "09:30";
}
function currentLocalTimeInput() {
  const session = selectedSession();
  const normalized = normalizeHHMM(localTimeInput?.value || "");
  if (normalized) {
    syncLocalTimeMirror(normalized);
    return normalized;
  }
  const fallback = defaultLocalTimeForSession(session);
  if (localTimeInput) localTimeInput.value = fallback;
  syncLocalTimeMirror(fallback);
  return fallback;
}
async function applyLiveSettings(opts = {}) {
  const resetTick = !!opts.resetTick;
  const date = (dateInput.value || sessionDateET || todayISO()).trim();
  const session = selectedSession();
  const levelsMode = selectedLevelsMode();
  const localTime = currentLocalTimeInput();
  const localEnabled = !!(chkLocalHigh?.checked || chkLocalLow?.checked);
  sessionDateET = date;
  if (resetTick) resetTickForContextChange();
  if (!streamRunning) return;
  const j = await postJSON("/api/stream", {
    mode: "update",
    date,
    session,
    levels_mode: levelsMode,
    local_time: localTime,
    local_enabled: localEnabled,
  });
  if (j?.ok) {
    streamRunning = true;
  }
}
function getChartOpenerURL(sym) {
  const ticker = String(sym || "").trim().toUpperCase();
  if (!ticker) return "";
  const date = sessionDateET || todayISO();
  const base = String(uiConfig.chartOpenerBaseURL || "http://localhost:8081").trim().replace(/\/+$/g, "");
  if (!base) return "";
  return `${base}/api/open-chart/${encodeURIComponent(ticker)}/${encodeURIComponent(date)}`;
}
function tickerLinkHTML(sym) {
  const ticker = String(sym || "").trim().toUpperCase();
  const href = getChartOpenerURL(ticker);
  if (!href) return escapeHTML(ticker);
  return `<a class="tickerLink" href="${escapeHTML(href)}" target="_blank" rel="noopener">${escapeHTML(ticker)}</a>`;
}
function getParam(name){
  try {
    const u = new URL(location.href);
    return u.searchParams.get(name) || "";
  } catch { return ""; }
}
function setRightTab(tab) {
  const next = tab === "rvol" ? "rvol" : "live";
  const showLive = next === "live";

  if (liveWrap) {
    if (showLive) liveWrap.removeAttribute("hidden");
    else liveWrap.setAttribute("hidden", "");
  }
  if (alertsWrap) {
    if (showLive) alertsWrap.setAttribute("hidden", "");
    else alertsWrap.removeAttribute("hidden");
  }

  if (tabLiveBtn) {
    tabLiveBtn.classList.toggle("isActive", showLive);
    tabLiveBtn.setAttribute("aria-selected", showLive ? "true" : "false");
  }
  if (tabRvolBtn) {
    tabRvolBtn.classList.toggle("isActive", !showLive);
    tabRvolBtn.setAttribute("aria-selected", showLive ? "false" : "true");
  }

  try { localStorage.setItem(RIGHT_TAB_STORAGE_KEY, next); } catch {}
  if (showLive) recomputeLiveCapacity();
}
function initRightTabs() {
  if (!tabLiveBtn || !tabRvolBtn || !liveWrap || !alertsWrap) return;

  let tab = "live";
  try {
    const saved = localStorage.getItem(RIGHT_TAB_STORAGE_KEY);
    if (saved === "live" || saved === "rvol") tab = saved;
  } catch {}

  const paramTab = getParam("tab").toLowerCase();
  if (paramTab === "live" || paramTab === "rvol") tab = paramTab;

  setRightTab(tab);
  tabLiveBtn.addEventListener("click", () => setRightTab("live"));
  tabRvolBtn.addEventListener("click", () => setRightTab("rvol"));
}
function updateQQQTapeReadout(msg) {
  if (!qqqTapeExecEl) return;
  const execEdgeCents = Number(msg?.exec_edge_cents ?? msg?.edge_cents ?? 0);
  const execEdgeBps = Number(msg?.exec_edge_bps ?? msg?.edge_bps ?? 0);
  const fairGapBps = Number(msg?.fair_gap_bps ?? msg?.lead_lag_gap_bps ?? 0);
  const fairValue = Number(msg?.fair_value || 0);
  const qqqPrice = Number(msg?.qqq_price || 0);
  const spread = Number(msg?.spread_bps || 0);
  const freshness = Math.max(0, Number(msg?.freshness_ms || 0));
  const coverage = Number(msg?.basket_coverage || 0);
  const tradable = !!msg?.tradable;

  qqqTapeExecEl.textContent = `${execEdgeCents > 0 ? "+" : ""}${execEdgeCents.toFixed(2)}¢`;
  qqqTapeExecEl.classList.remove("positive", "negative", "neutral");
  qqqTapeExecEl.classList.add(execEdgeCents > 0 ? "positive" : (execEdgeCents < 0 ? "negative" : "neutral"));

  if (qqqTapeStateEl) {
    const watchOnly = !tradable && Math.abs(execEdgeBps) >= Math.max(0.35, spread);
    qqqTapeStateEl.textContent = tradable ? "Tradable" : (watchOnly ? "Watch" : "No Trade");
    qqqTapeStateEl.classList.remove("tradable", "watch", "idle");
    qqqTapeStateEl.classList.add(tradable ? "tradable" : (watchOnly ? "watch" : "idle"));
  }
  if (qqqTapePriceEl) {
    qqqTapePriceEl.textContent = qqqPrice > 0 ? `$${qqqPrice.toFixed(2)}` : "—";
  }
  if (qqqTapeFairValueEl) qqqTapeFairValueEl.textContent = fairValue > 0 ? `$${fairValue.toFixed(2)}` : "—";
  if (qqqTapeFairGapEl) qqqTapeFairGapEl.textContent = `${fairGapBps > 0 ? "+" : ""}${fairGapBps.toFixed(2)} bps`;
  if (qqqTapeExecBpsEl) qqqTapeExecBpsEl.textContent = `${execEdgeBps > 0 ? "+" : ""}${execEdgeBps.toFixed(2)} bps`;
  if (qqqTapeSpreadEl) qqqTapeSpreadEl.textContent = `${spread.toFixed(2)} bps`;
  if (qqqTapeFreshnessEl) qqqTapeFreshnessEl.textContent = `${freshness} ms`;
  if (qqqTapeCoverageEl) qqqTapeCoverageEl.textContent = coverage > 0 ? `${(coverage * 100).toFixed(1)}%` : "—";
  if (qqqTapeTradeEl) qqqTapeTradeEl.textContent = Number(msg?.trade_impulse || 0).toFixed(3);
  if (qqqTapeQuoteEl) qqqTapeQuoteEl.textContent = Number(msg?.quote_imbalance || 0).toFixed(3);
  if (qqqTapeMicroEl) qqqTapeMicroEl.textContent = Number(msg?.micro_edge || 0).toFixed(3);

  if (qqqTapeTopEl) {
    const top = Array.isArray(msg?.top) ? msg.top : [];
    qqqTapeTopEl.innerHTML = "";
    if (top.length === 0) {
      qqqTapeTopEl.innerHTML = `<span class="muted">Waiting for QQQ + leader alignment…</span>`;
    } else {
      top.forEach(item => {
        const contrib = Number(item?.contribution || 0);
        const chip = document.createElement("span");
        chip.className = `tapeChip ${contrib > 0 ? "pos" : (contrib < 0 ? "neg" : "")}`;
        chip.textContent = `${String(item?.sym || "").toUpperCase()} ${contrib > 0 ? "+" : ""}${contrib.toFixed(3)}`;
        qqqTapeTopEl.appendChild(chip);
      });
    }
  }
}
function resetQQQTapeChart() {
  updateQQQTapeReadout({
    exec_edge_bps: 0,
    exec_edge_cents: 0,
    qqq_price: 0,
    fair_value: 0,
    fair_gap_bps: 0,
    spread_bps: 0,
    freshness_ms: 0,
    basket_coverage: 0,
    trade_impulse: 0,
    quote_imbalance: 0,
    micro_edge: 0,
    tradable: false,
    top: [],
  });
}
function ingestQQQTape(msg, withSound = true) {
  updateQQQTapeReadout(msg);
  if (withSound) {
    maybePlaySyntheticTapeSound(msg);
  }
}
function tickModeInfo(mode = selectedLevelsMode()) {
  if (mode === "local") {
    return {
      label: "Local High/Low",
      badgeClass: "local",
      upKind: "lhigh",
      downKind: "llow",
    };
  }
  return {
    label: "Session HOD/LOD",
    badgeClass: "",
    upKind: "hod",
    downKind: "lod",
  };
}
function syncTickModeUI() {
  if (!tickModeBadge) return;
  const info = tickModeInfo();
  tickModeBadge.textContent = info.label;
  tickModeBadge.classList.toggle("local", info.badgeClass === "local");
}
function setTickUniverseSize(n) {
  const v = Number(n);
  if (Number.isFinite(v) && v > 0) {
    tickUniverseSize = Math.max(1, Math.round(v));
  } else {
    tickUniverseSize = 0;
  }
}
async function refreshTickUniverseSize() {
  try {
    const res = await fetch("/api/watchlist", { cache: "no-store" });
    const j = await res.json();
    const syms = Array.isArray(j?.symbols) ? j.symbols : [];
    setTickUniverseSize(syms.length);
  } catch {}
}
function updateTickReadout(value) {
  if (!tickValueEl) return;
  const v = Math.round(Number(value) || 0);
  tickValueEl.textContent = `${v > 0 ? "+" : ""}${v}`;
  tickValueEl.classList.remove("positive", "negative", "neutral");
  tickValueEl.classList.add(v > 0 ? "positive" : (v < 0 ? "negative" : "neutral"));
}
function resetTickChart() {
  tickDirsBySymbol.clear();
  tickCurrentValue = 0;
  updateTickReadout(0);
}
function tickEpochSeconds(alertObj) {
  const ms = Number(alertObj?.ts_unix);
  if (Number.isFinite(ms) && ms > 0) return Math.floor(ms / 1000);
  return Math.floor(Date.now() / 1000);
}
function applyTickTransition(alertObj) {
  const transition = applyBreakoutTransition(alertObj, tickDirsBySymbol);
  if (!transition) return null;
  tickCurrentValue += transition.delta;
  return { time: transition.time, value: tickCurrentValue };
}
function appendTickPoint(point, redraw = true) {
  if (!point) return;
  tickCurrentValue = Math.round(Number(point.value) || 0);
  updateTickReadout(tickCurrentValue);
}
function ingestTickAlert(alertObj, redraw = true) {
  const point = applyTickTransition(alertObj);
  if (!point) return;
  appendTickPoint(point, redraw);
}
function rebuildTickFromAlerts(alerts) {
  tickDirsBySymbol.clear();
  tickCurrentValue = 0;
  if (qqqModeEnabled) {
    resetTickChart();
    return;
  }
  const ordered = Array.isArray(alerts) ? alerts.slice() : [];
  ordered.sort((a, b) => Number(a?.ts_unix || 0) - Number(b?.ts_unix || 0));
  for (const a of ordered) {
    ingestTickAlert(a, false);
  }
  updateTickReadout(tickCurrentValue);
}
function resetTickForContextChange() {
  syncTickModeUI();
  resetTickChart();
}
function shouldShow(kind){
  const hodOn = !!chkHod?.checked;
  const lodOn = !!chkLod?.checked;
  const lhighOn = !!chkLocalHigh?.checked;
  const llowOn = !!chkLocalLow?.checked;
  const scalpsOn = !!chkScalps?.checked;

  // HOD/LOD filters
  if (kind === "hod") return hodOn;
  if (kind === "lod") return lodOn;
  if (kind === "lhigh") return lhighOn;
  if (kind === "llow") return llowOn;

  // Scalp alerts: any kind starting with "scalp_"
  if (kind && kind.startsWith("scalp_")) {
    return scalpsOn;
  }

  // Unknown kinds: hide by default
  return false;
}
function alertId(a){ return `${a.sym}_${a.ts_unix}_${a.kind}`; }
function renderSourceTags(alertObj){
  const arr = Array.isArray(alertObj?.sources) ? alertObj.sources.filter(Boolean) : [];
  if (arr.length === 0) return "";
  const chips = arr.map(src => `<span class="srcTag">${escapeHTML(src)}</span>`).join("");
  return `<span class="sourceWrap" title="Watchlist source">${chips}</span>`;
}
// ----------------- Build & render cards -----------------
function scalpTypeLabel(kind){
  if (typeof kind !== "string" || !kind.startsWith("scalp_")) return "";
  const parts = kind.split("_").slice(1); // drop "scalp"
  const core = (parts[0] || "").toLowerCase();
  if (core === "rubberband" || core === "rubberbandup" || core === "rubberband_up") return "Rubberband";
  if (core === "backside") return "Backside";
  if (core === "fashionably_late" || (core === "fashionably" && (parts[1]||"").toLowerCase()==="late")) return "Fashionably late";
  // fallback: prettify
  const raw = [core, parts[1] && !["setup","trigger"].includes(parts[1]) ? parts[1] : ""].filter(Boolean).join(" ");
  return raw ? raw.charAt(0).toUpperCase() + raw.slice(1) : "";
}
function buildAlertCard(a, autoChart=false, isPinned=false, isLive=false) {
  const id = alertId(a);
  const card = document.createElement("div");
  // Base classes
  let classNames = ["card"];
  if (isPinned) classNames.push("isPinned");
  if (isLive) classNames.push("live");
  // Map kind -> CSS classes
  if (a.kind === "hod" || a.kind === "lod" || a.kind === "lhigh" || a.kind === "llow") {
    classNames.push(a.kind);
  } else if (typeof a.kind === "string" && a.kind.startsWith("scalp_")) {
    // Expected formats: scalp_rubberband_setup, scalp_backside_trigger, etc.
    const parts = a.kind.split("_"); // ["scalp","rubberband","setup"]
    // Always mark as scalp for base styling
    classNames.push("scalp");
    if (parts.length >= 2) {
      // kind: rubberband, rubberband_up, backside, fashionably_late
      const coreKind = parts[1];
      classNames.push(coreKind);
    }
    if (parts.length >= 3) {
      // phase: setup/trigger
      const phase = parts[parts.length - 1];
      classNames.push(phase);
    }
  } else if (a.kind) {
    // Fallback for any future kinds
    classNames.push(a.kind);
  }
  card.className = classNames.join(" ");
  card.dataset.id = id;
  card.dataset.sym = a.sym;
  card.dataset.ts = String(a.ts_unix);
  card.dataset.kind = a.kind;
  card.dataset.live = isLive ? "1" : "0";
  const labelMap = {
    hod: "NEW HOD",
    lod: "NEW LOD",
    lhigh: "LOCAL HIGH",
    llow: "LOCAL LOW",
  };
  const label = labelMap[a.kind] || "ALERT";
  const priceFmt = Number(a.price).toFixed(4).replace(/\.?0+$/, '');
  const scalpTxt = scalpTypeLabel(a.kind);
  const scalpHTML = scalpTxt ? `<span class="scalpType" title="Scalp type">${scalpTxt}</span>` : "";
  const sourceHTML = renderSourceTags(a);
  const tickerHTML = tickerLinkHTML(a.sym);
  if (isLive) {
    // Live variant: no chart row, no "More news" link
    card.innerHTML = `
      <div class="left">
        <span class="badge">${label}</span>
        <span class="sym">${tickerHTML}</span>
        ${sourceHTML}
        <span class="name">${a.name || ""}</span>
        <button class="iconBtn pinBtn" title="Pin/Unpin" aria-pressed="${isPinned ? "true" : "false"}">${isPinned ? "★" : "☆"}</button>
        ${scalpHTML}
      </div>
      <div class="price">
        <span class="cap" title="Market cap">—</span>
        <span class="country" title="Country">—</span>
        <span class="industry" title="Industry">—</span>
        <span class="pv">$${priceFmt}</span>
      </div>
      <div class="time">${a.time}</div>
      <div class="infoRow">
        <div class="infoCol">
          <div class="sectionTitle">News</div>
          <ul class="newsList" id="news_${id}" aria-live="polite"></ul>
        </div>
        <div class="infoCol">
          <div class="sectionTitle">SEC Filings (today)</div>
          <ul class="secList" id="sec_${id}" aria-live="polite"></ul>
        </div>
      </div>
    `;
  } else {
    // Original (feed/pinned) variant
    card.innerHTML = `
      <div class="left">
        <span class="badge">${label}</span>
        <span class="sym">${tickerHTML}</span>
        ${sourceHTML}
        <span class="name">${a.name || ""}</span>
        <button class="iconBtn pinBtn" title="Pin/Unpin" aria-pressed="${isPinned ? "true" : "false"}">${isPinned ? "★" : "☆"}</button>
        ${scalpHTML}
      </div>
      <div class="price">
        <span class="cap" title="Market cap">—</span>
        <span class="country" title="Country">—</span>
        <span class="industry" title="Industry">—</span>
        <span class="pv">$${priceFmt}</span>
      </div>
      <div class="time">${a.time}</div>
      <div class="chartRow">
        <div class="chartBox disabled" id="chart_${id}" aria-hidden="true"></div>
        <div class="chartBtns">
          <button class="toggleChart" data-action="toggle">Enable chart</button>
        </div>
      </div>
      <div class="infoRow">
        <div class="infoCol">
          <div class="sectionTitle">News</div>
          <ul class="newsList" id="news_${id}" aria-live="polite"></ul>
          <a class="moreLink" id="moreNews_${id}" target="_blank" rel="noopener">More news →</a>
        </div>
        <div class="infoCol">
          <div class="sectionTitle">SEC Filings (today)</div>
          <ul class="secList" id="sec_${id}" aria-live="polite"></ul>
        </div>
      </div>
    `;
  }
  // Prefill placeholders so Live cards never look empty while loading.
  const newsUL0 = card.querySelector(`#news_${id}`);
  const secUL0  = card.querySelector(`#sec_${id}`);
  if (newsUL0) newsUL0.innerHTML = `<li class="muted">loading…</li>`;
  if (secUL0)  secUL0.innerHTML  = `<li class="muted">loading…</li>`;
  // Kick off profile fetch (FMP) to fill cap/country/industry inline with price
  setTimeout(() => populateProfileForCard(card, a.sym), 0);
  // Pin button
  const pinBtn = card.querySelector('.pinBtn');
  pinBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    togglePinById(id);
  });
  // Chart toggle
  const btn = card.querySelector('button.toggleChart');
  if (btn) {
    btn.addEventListener('click', async () => {
      if (chartsById.has(id)) {
        destroyChart(id);
        btn.textContent = "Enable chart";
        btn.classList.remove('active');
        card.querySelector('.chartBox').classList.add('disabled');
      } else {
        // Show box BEFORE creating chart so sizing works
        card.querySelector('.chartBox').classList.remove('disabled');
        await ensureUnderLimitThenSpawn(id, a.sym);
        const enabled = chartsById.has(id);
        btn.textContent = enabled ? "Disable chart" : "Enable chart";
        btn.classList.toggle('active', enabled);
        if (!enabled) card.querySelector('.chartBox').classList.add('disabled');
      }
    });
  }
  if (autoChart && btn) {
    setTimeout(async () => {
      if (!card.isConnected) return;
      // Show box BEFORE creating chart so sizing works
      card.querySelector('.chartBox').classList.remove('disabled');
      await ensureUnderLimitThenSpawn(id, a.sym);
      if (!chartsById.has(id)) {
        card.querySelector('.chartBox').classList.add('disabled');
        return;
      }
      const tbtn = card.querySelector('button.toggleChart');
      if (!tbtn) return;
      tbtn.textContent = "Disable chart";
      tbtn.classList.add('active');
    }, 0);
  }
  // Populate News/SEC scoped to THIS card to avoid duplicate-ID collisions
  setTimeout(() => populateNewsAndFilingsForCard(card, a.sym), 0);
  return card;
}
function renderPinned() {
  // Remove charts (fresh render)
  for (const id of Array.from(chartsById.keys())) {
    // we clear all charts in renderAll(); nothing specific here
  }
  if (qqqModeEnabled) {
    if (pinnedList) pinnedList.innerHTML = "";
    pinnedWrap.classList.add("hidden");
    btnUnpinAll.disabled = true;
    pinnedCountEl.textContent = "(0)";
    return;
  }
  // Build a quick index: id -> alert
  const amap = new Map(allAlerts.map(a => [alertId(a), a]));
  // Keep only pins that still exist in history
  const kept = [];
  for (const id of pinnedOrder) {
    if (amap.has(id)) kept.push(id);
  }
  if (kept.length !== pinnedOrder.length) {
    pinnedOrder = kept;
    pinnedSet = new Set(kept);
    savePins();
  }
  pinnedList.innerHTML = "";
  if (kept.length === 0) {
    pinnedWrap.classList.add("hidden");
    btnUnpinAll.disabled = true;
    pinnedCountEl.textContent = "(0)";
    return;
  }
  pinnedWrap.classList.remove("hidden");
  btnUnpinAll.disabled = false;
  pinnedCountEl.textContent = `(${kept.length})`;
  // Show newest pin first (reverse insertion order)
  const frag = document.createDocumentFragment();
  for (let i = kept.length - 1; i >= 0; i--) {
    const id = kept[i];
    const a = amap.get(id);
    if (!a) continue;
    // pinned cards: auto chart by default
    frag.appendChild(buildAlertCard(a, true, true));
  }
  pinnedList.appendChild(frag);
}
function renderFeed() {
  feed.innerHTML = "";
  if (qqqModeEnabled) {
    return;
  }
  // Exclude pinned from the feed and apply HOD/LOD filter
  const filtered = allAlerts.filter(a => !isPinnedId(alertId(a)) && shouldShow(a.kind));
  const frag = document.createDocumentFragment();
  filtered.forEach((a, idx) => {
    const autoChart = idx < chartsLimit; // charts for top N in feed (dynamic)
    frag.appendChild(buildAlertCard(a, autoChart, false));
  });
  feed.appendChild(frag);
}

/* ============ RIGHT: Live stream ============ */
let liveMaxItems = 6; // will be recomputed after first render
function recomputeFeedCapacity() {
  if (!feed) return;
  const boxTop = feed.getBoundingClientRect().top;
  const target = Math.max(180, Math.floor(window.innerHeight - boxTop - 12));
  feed.style.height = target + "px";
}
function trimLive() {
  if (!liveFeed) return;
  while (liveFeed.children.length > liveMaxItems) {
    liveFeed.removeChild(liveFeed.lastElementChild);
  }
}
function recomputeLiveCapacity() {
  if (!liveFeed) return;
  if (liveWrap && liveWrap.hasAttribute("hidden")) return;
  // Make sure the container has a precise height that fits the viewport.
  const boxTop = liveFeed.getBoundingClientRect().top;
  const target = Math.max(120, Math.floor(window.innerHeight - boxTop - 12));
  liveFeed.style.height = target + "px";

  // Estimate card height (including the vertical gap).
  let sample = liveFeed.querySelector(".card") || feed?.querySelector?.(".card");
  let gap = 8;
  try {
    const cs = getComputedStyle(liveFeed);
    gap = parseInt(cs.rowGap || cs.gap || "8", 10) || 8;
  } catch {}
  const cardH = sample ? Math.ceil(sample.getBoundingClientRect().height) + gap : 160;
  liveMaxItems = Math.max(1, Math.floor(target / cardH));
  trimLive();
}
function seedLiveFromHistory() {
  if (!liveFeed) return;
  liveFeed.innerHTML = "";
  if (qqqModeEnabled) {
    return;
  }
  // take last N alerts (newest last in allAlerts) so we can prepend in correct order
  const take = Math.min(liveMaxItems, allAlerts.length);
  for (let i = allAlerts.length - take; i < allAlerts.length; i++) {
    const a = allAlerts[i];
    if (!a) continue;
    if (!shouldShow(a.kind) || isPinnedId(alertId(a))) continue;
    const node = buildAlertCard(a, false, false, true); // LIVE variant
    liveFeed.prepend(node); // newest ends up at top
  }
  trimLive();
}
function addLiveCard(a) {
  if (!liveFeed) return;
  if (qqqModeEnabled) return;
  const node = buildAlertCard(a, false, false, true); // LIVE variant
  liveFeed.prepend(node); // newest to top
  trimLive();
}
function renderAll(){
  // Destroy all charts (both pinned + feed will fully re-render)
  for (const id of Array.from(chartsById.keys())) destroyChart(id);
  renderPinned();
  renderFeed();
  recomputeFeedCapacity();
  recomputeLiveCapacity();
  seedLiveFromHistory();
  refreshTapePaceIndicator();
}
function applyQQQMode(enabled) {
  qqqModeEnabled = !!enabled;
  document.body.classList.toggle("qqqMode", qqqModeEnabled);
  if (qqqModeEnabled) {
    if (feed) feed.innerHTML = "";
    if (liveFeed) liveFeed.innerHTML = "";
    if (pinnedList) pinnedList.innerHTML = "";
    if (pinnedWrap) pinnedWrap.classList.add("hidden");
    resetTickChart();
  } else if (historyLoaded) {
    renderAll();
    rebuildTickFromAlerts(allAlerts);
  }
  recomputeFeedCapacity();
  recomputeLiveCapacity();
}
function addIncomingAlert(a){
  allAlerts.push(a);
  recordTapePaceEvent(a);
  // Prevent infinite memory growth in array
  if (allAlerts.length > HISTORY_LIMIT) {
    allAlerts.shift();
  }
  if (qqqModeEnabled) {
    return;
  }
  ingestTickAlert(a);

  const isScalp = typeof a.kind === "string" && a.kind.startsWith("scalp_");
  const visible = shouldShow(a.kind) && !isPinnedId(alertId(a));
  if (visible) {
    const autoChart = chartsById.size < chartsLimit;
    const node = buildAlertCard(a, autoChart, false);
    feed.appendChild(node);

    // Prevent infinite DOM growth (Memory Leak Fix)
    while (feed.children.length > HISTORY_LIMIT) {
      // In column-reverse flex, the first child in DOM is the oldest visual item
      const toRemove = feed.firstElementChild;
      if (toRemove) {
        // CRITICAL: Destroy associated chart to stop setInterval/ResizeObserver leaks
        const rmId = toRemove.dataset.id;
        if (rmId && chartsById.has(rmId)) {
          destroyChart(rmId);
        }
        toRemove.remove();
      }
    }

    if (!silent) {
      if (isScalp) {
        // scalps keep their own distinct sound
        playScalpSound();
      } else if (a.kind === "lod" || a.kind === "llow") {
        // price making a new low → DOWN sound
        playDownSound();
      } else {
        // highs (and any other non-scalp alerts) → UP sound
        playUpSound();
      }
    }

    addLiveCard(a); // mirror to the live stream on the right
  } else {
    // If it is pinned (very rare on first arrival) still honor sound
    if (isPinnedId(alertId(a)) && !silent) {
      if (isScalp) {
        playScalpSound();
      } else if (a.kind === "lod" || a.kind === "llow") {
        playDownSound();
      } else {
        playUpSound();
      }
    }
  }
}
// NEW: Render recent RVOL alerts (group by minute, sort per minute)
function renderRecentAlerts() {
  if (!alertsTableBody || !alertsCount) return;

  // Group newest minute at the top (we reverse the array to get newest-first chronology)
  const recents = recentAlerts.slice().reverse();

  // Build minute groups while preserving minute order as encountered
  const groups = new Map(); // minuteStr -> array
  const order = [];         // minuteStr in display order
  for (const a of recents) {
    const minuteKey = String(a.time || "").replace(/\s*ET\s*$/i, " ET"); // normalize & keep "ET"
    if (!groups.has(minuteKey)) {
      groups.set(minuteKey, []);
      order.push(minuteKey);
    }
    groups.get(minuteKey).push(a);
  }

  // Comparator per current column + direction
  const dirMul = alertsSortState.dir === "asc" ? 1 : -1;
  const cmp = (a, b) => {
    const k = alertsSortState.key;
    let av, bv;
    switch (k) {
      case "symbol":
        av = String(a.symbol || "").toUpperCase();
        bv = String(b.symbol || "").toUpperCase();
        if (av < bv) return -1 * dirMul;
        if (av > bv) return  1 * dirMul;
        // stable tiebreakers
        return String(a.time).localeCompare(String(b.time));
      case "price":
      case "volume":
      case "baseline":
      case "rvol":
        av = Number(a[k]);
        bv = Number(b[k]);
        if (av < bv) return -1 * dirMul;
        if (av > bv) return  1 * dirMul;
        return String(a.symbol).localeCompare(String(b.symbol));
      default:
        return 0;
    }
  };

  // Build rows
  alertsTableBody.innerHTML = "";
  const frag = document.createDocumentFragment();

  order.forEach((minuteStr, gIdx) => {
    const arr = groups.get(minuteStr) || [];
    // sort this minute only
    if (alertsSortState.key) arr.sort(cmp);

    arr.forEach((a, i) => {
      const tr = document.createElement("tr");
      tr.className = `${gIdx % 2 === 0 ? "groupEven" : "groupOdd"} ${i === 0 ? "minuteStart" : ""}`;
      tr.setAttribute("data-minute", minuteStr);

      const priceTxt = `$${Number(a.price).toFixed(2)}`;
      let deltaHTML = "";
      if (typeof a.delta === "number" && !Number.isNaN(a.delta) && a.delta !== 0) {
        const sign = a.delta > 0 ? "+" : "-";
        const cls  = a.delta > 0 ? "pos" : "neg";
        deltaHTML = ` <span class="delta ${cls}">(${sign}${Math.abs(a.delta).toFixed(2)})</span>`;
      }

      tr.innerHTML = `
        <td>${minuteStr}</td>
        <td>${tickerLinkHTML(a.symbol)}</td>
        <td>${priceTxt}${deltaHTML}</td>
        <td>${a.volume}</td>
        <td>${Number(a.baseline).toFixed(0)}</td>
        <td>${Number(a.rvol).toFixed(2)}</td>
        <td>${a.method}</td>
      `;
      frag.appendChild(tr);
    });
  });

  alertsTableBody.appendChild(frag);
  alertsCount.textContent = `(${recentAlerts.length})`;
  updateAlertsSortIndicators();
}

function updateAlertsSortIndicators(){
  if (!alertsTableHead) return;
  alertsTableHead.querySelectorAll("th.sortable").forEach(th => {
    const key = th.dataset.key || "";
    const dir = alertsSortState.key === key ? alertsSortState.dir : "none";
    th.setAttribute("data-dir", dir);
    th.setAttribute("aria-sort", dir === "none" ? "none" : (dir === "asc" ? "ascending" : "descending"));
  });
}
function addRvolAlert(msg) {
  recentAlerts.push({
    time: msg.time,
    symbol: msg.sym,
    price: msg.price,
    volume: msg.volume,
    baseline: msg.baseline,
    rvol: msg.rvol,
    method: msg.method,
    delta: (typeof msg.delta === "number" ? msg.delta : null) // NEW
  });
  if (recentAlerts.length > 200) recentAlerts.shift();
  renderRecentAlerts();

  if (!silent) {
    // Directional RVOL sounds: up for green (or flat), down for red
    if (typeof msg.delta === "number" && !Number.isNaN(msg.delta) && msg.delta < 0) {
      playDownSound();
    } else {
      playUpSound();
    }
  }
}
// ----------------- Mini charts -----------------
async function ensureUnderLimitThenSpawn(id, sym) {
  if (chartsById.has(id)) return;
  pruneActiveChartQueue();
  if (activeChartQueue.length >= chartsLimit) {
    // Prefer to evict a non-pinned chart first
    let evictId = activeChartQueue.find(x => !isPinnedId(x));
    if (!evictId) {
      // all pinned; evict the oldest anyway (if any)
      evictId = activeChartQueue[0];
    }
    if (evictId) destroyChart(evictId);
  }
  const created = await spawnChart(id, sym);
  if (!created) return;
  if (!activeChartQueue.includes(id)) activeChartQueue.push(id);
}
function pruneActiveChartQueue() {
  for (let i = activeChartQueue.length - 1; i >= 0; i--) {
    if (!chartsById.has(activeChartQueue[i])) {
      activeChartQueue.splice(i, 1);
    }
  }
}
function destroyChart(id) {
  if (!id) return;
  const idx = activeChartQueue.indexOf(id);
  if (idx >= 0) activeChartQueue.splice(idx, 1);
  const rec = chartsById.get(id);
  if (!rec) return;
  try { if (typeof rec.stop === 'function') rec.stop(); } catch {}
  try { if (rec.timer) clearInterval(rec.timer); } catch {}
  try { if (rec.ro) rec.ro.disconnect(); } catch {}
  try { rec.chart.remove(); } catch {}
  chartsById.delete(id);
}
async function spawnChart(id, sym) {
  const box = document.getElementById(`chart_${id}`);
  if (!box) return false;
  // Guard: library present?
  const LWC = (typeof window !== 'undefined') ? window.LightweightCharts : null;
  if (!LWC || typeof LWC.createChart !== 'function') {
    console.error('[mini-chart]', 'LightweightCharts not loaded; check <script> tag or network.');
    setStatus('Charts library failed to load', false);
    return false;
  }
  // Show dimension debug just in case
  const w = box.clientWidth || 150;
  const h = box.clientHeight || 150;
  const chart = LWC.createChart(box, {
    width: w,
    height: h,
    layout: { background: { type: 'solid', color: '#0e1117' }, textColor: '#cbd5e1' },
    grid: { vertLines: { visible: false }, horzLines: { visible: false } },
    crosshair: { mode: 0 },
    timeScale: { borderVisible: false },
    rightPriceScale: { borderVisible: false },
  });
  let series;
  if (typeof chart.addCandlestickSeries === 'function') {
    series = chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    });
  } else if (typeof chart.addSeries === 'function') {
    series = chart.addSeries({
      type: 'Candlestick',
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    });
  } else {
    console.error('[mini-chart]', 'No candlestick API on chart object:', chart);
    setStatus('Chart API mismatch', false);
    return false;
  }
  let destroyed = false;
  let fetchInProgress = false;
  let pendingAtMs = null;
  let barsAbortCtl = null;
  async function loadBars(atMs) {
    if (destroyed) return;
    pendingAtMs = atMs;
    if (fetchInProgress) return;
    fetchInProgress = true;
    while (!destroyed && pendingAtMs != null) {
      const scheduledAtMs = pendingAtMs;
      pendingAtMs = null;
      const lookback = chartLookbackMin || 120;
      const url = `/api/bars?symbol=${encodeURIComponent(sym)}&at=${encodeURIComponent(scheduledAtMs)}&mins=${encodeURIComponent(lookback)}`;
      const ctl = new AbortController();
      barsAbortCtl = ctl;
      try {
        await runMiniBarsTask(async () => {
          if (destroyed || ctl.signal.aborted) return;
          const res = await fetch(url, { cache: 'no-store', signal: ctl.signal });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const j = await res.json();
          if (destroyed || ctl.signal.aborted) return;
          if (Array.isArray(j?.bars)) {
            const data = j.bars.map(b => ({
              time: Number(b.time), // epoch seconds expected
              open: Number(b.open),
              high: Number(b.high),
              low: Number(b.low),
              close: Number(b.close),
            }));
            series.setData(data);
            chart.timeScale().fitContent();
          } else if (j && j.ok === false) {
            console.warn('[mini-chart]', 'bars fetch failed for', sym, j);
          }
        });
      } catch (e) {
        if (!(e && e.name === 'AbortError')) {
          console.error('[mini-chart] fetch bars failed', sym, e);
        }
      } finally {
        if (barsAbortCtl === ctl) barsAbortCtl = null;
      }
    }
    fetchInProgress = false;
  }
  await loadBars(Date.now());
  const timer = setInterval(() => { loadBars(Date.now()); }, chartRefreshMs);
  let ro;
  if (typeof ResizeObserver === 'function') {
    ro = new ResizeObserver(() => {
      const w2 = box.clientWidth || 150;
      const h2 = box.clientHeight || 150;
      chart.applyOptions({ width: w2, height: h2 });
    });
    ro.observe(box);
  }
  chartsById.set(id, {
    chart, series, container: box, timer, ro,
    stop: () => {
      destroyed = true;
      pendingAtMs = null;
      if (barsAbortCtl) barsAbortCtl.abort();
    }
  });
  return true;
}
// ----------------- News + SEC -----------------
function sameETDate(iso, ymd) {
  if (!iso) return false;
  try {
    const d = new Date(iso);
    const s = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year:'numeric', month:'2-digit', day:'2-digit' }).format(d);
    return s === ymd;
  } catch { return false; }
}
// NEW: scope to the specific card; ensures Live cards are populated
async function populateNewsAndFilingsForCard(cardEl, sym) {
  if (!cardEl || !cardEl.isConnected) return;
  const newsUL = cardEl.querySelector('.newsList');
  const secUL  = cardEl.querySelector('.secList');
  if (!newsUL || !secUL) return;

  const isLive = cardEl.classList.contains('live');
  const d = sessionDateET || todayISO();

  const moreA = cardEl.querySelector('.moreLink');
  if (moreA) {
    moreA.href = `/news.html?ticker=${encodeURIComponent(sym)}&date=${encodeURIComponent(d)}`;
  }

  try {
    const j = await getExtra(sym, d, 2);
    if (!cardEl.isConnected) return;

    // ----- News -----
    const news = Array.isArray(j?.news) ? j.news : [];
    newsUL.innerHTML = "";
    if (isLive) {
      const todays = news
        .filter(n => sameETDate(n.published, d))
        .sort((a,b)=> new Date(b.published) - new Date(a.published));
      if (todays.length === 0) {
        newsUL.innerHTML = `<li class="muted">no headlines</li>`;
      } else {
        const n = todays[0];
        const time = n.published ? new Date(n.published).toLocaleString() : "";
        const li = document.createElement("li");
        li.className = "newsItem";
        li.innerHTML = `
          <a href="${n.url}" target="_blank" rel="noopener">
            <span class="headline">${escapeHTML(n.title || "")}</span>
            <span class="meta">${escapeHTML(n.source || "News")} • ${escapeHTML(time)}</span>
          </a>`;
        newsUL.appendChild(li);
      }
    } else {
      if (news.length === 0) {
        newsUL.innerHTML = `<li class="muted">No headlines.</li>`;
      } else {
        news.slice(0, 5).forEach(n => {
          const li = document.createElement("li");
          li.className = "newsItem";
          const time = n.published ? new Date(n.published).toLocaleString() : "";
          li.innerHTML = `
            <a href="${n.url}" target="_blank" rel="noopener">
              <span class="headline">${escapeHTML(n.title || "")}</span>
              <span class="meta">${escapeHTML(n.source || "News")} • ${escapeHTML(time)}</span>
            </a>`;
          newsUL.appendChild(li);
        });
      }
    }

    // ----- SEC filings -----
    const filings = Array.isArray(j?.filings) ? j.filings : [];
    secUL.innerHTML = "";
    if (isLive) {
      const todaysF = filings
        .filter(f => sameETDate(f.filedAt, d))
        .sort((a,b)=> new Date(b.filedAt) - new Date(a.filedAt));
      if (todaysF.length === 0) {
        secUL.innerHTML = `<li class="muted">no sec filings today</li>`;
      } else {
        const f = todaysF[0];
        const filedAt = f.filedAt ? new Date(f.filedAt).toLocaleString() : "";
        const desc = f.description || f.formType || "Filing";
        const link = f.linkToFilingDetails || "#";
        const label = f.formType ? `<span class="formType">${escapeHTML(f.formType)}</span>` : "";
        const li = document.createElement("li");
        li.className = "secItem";
        li.innerHTML = `
          <a href="${link}" target="_blank" rel="noopener">
            <span class="headline">${label} ${escapeHTML(desc)}</span>
            <span class="meta">${escapeHTML(f.companyName || "")} • ${escapeHTML(filedAt)}</span>
          </a>`;
        secUL.appendChild(li);
      }
    } else {
      if (filings.length === 0) {
        secUL.innerHTML = `<li class="muted">No SEC filings today.</li>`;
      } else {
        const limit = 5;
        filings.forEach((f, idx) => {
          const li = document.createElement("li");
          li.className = "secItem" + (idx >= limit ? " hidden" : "");
          const filedAt = f.filedAt ? new Date(f.filedAt).toLocaleString() : "";
          const desc = f.description || f.formType || "Filing";
          const link = f.linkToFilingDetails || "#";
          const label = f.formType ? `<span class="formType">${escapeHTML(f.formType)}</span>` : "";
          li.innerHTML = `
            <a href="${link}" target="_blank" rel="noopener">
              <span class="headline">${label} ${escapeHTML(desc)}</span>
              <span class="meta">${escapeHTML(f.companyName || "")} • ${escapeHTML(filedAt)}</span>
            </a>`;
          secUL.appendChild(li);
        });
        if (filings.length > limit) {
          const more = document.createElement("button");
          more.className = "linkBtn";
          more.textContent = `more… (${filings.length - limit})`;
          more.addEventListener("click", () => {
            secUL.querySelectorAll(".secItem.hidden").forEach(el => el.classList.remove("hidden"));
            more.remove();
          });
          const wrap = document.createElement("div");
          wrap.appendChild(more);
          secUL.parentElement.appendChild(wrap);
        }
      }
    }
  } catch {
    newsUL.innerHTML = `<li class="muted">News unavailable.</li>`;
    secUL.innerHTML = `<li class="muted">SEC unavailable.</li>`;
  }

  // right column might grow: recompute capacity so nothing gets clipped
  if (cardEl.classList.contains('live')) {
    recomputeLiveCapacity();
  }
}
function escapeHTML(s) {
  return String(s || "").replace(/[&<>"'`]/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','`':'&#96;'}[c]));
}
// ----------------- WS & API -----------------
function connectWS() {
  if (ws) { try { ws.close(1000); } catch {} ws = null; }
  const proto = location.protocol === "https:" ? "wss://" : "ws://";
  ws = new WebSocket(proto + location.host + "/ws");
  ws.onopen = () => setStatus("Connected", true);
  ws.onclose = () => setStatus("Disconnected");
  ws.onerror = () => setStatus("Error");
  ws.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);
      if (msg.type === "status") {
        setStatus(msg.text, msg.level === "success" || msg.level === "info");
        return;
      }
      if (msg.type === "history") {
        historyLoaded = true;
        allAlerts = msg.alerts.slice(); // oldest -> newest
        rebuildTapePaceEvents(allAlerts);
        renderAll();
        rebuildTickFromAlerts(allAlerts);
        return;
      }
      if (msg.type === "alert") {
        addIncomingAlert(msg);
        return;
      }
      if (msg.type === "qqq_tape") {
        ingestQQQTape(msg);
        return;
      }
      // NEW: RVOL history and alerts
      if (msg.type === "rvol_history") {
        // Normalize server payload ({sym: "...", ...}) to the UI table shape ({symbol: "...", ...})
        // so that history renders correctly before any new real-time alerts arrive.
        recentAlerts = (Array.isArray(msg.alerts) ? msg.alerts : []).map(a => ({
          time: a.time,
          symbol: a.sym,     // <-- normalize field name
          price: a.price,
          volume: a.volume,
          baseline: a.baseline,
          rvol: a.rvol,
          method: a.method,
          delta: typeof a.delta === "number" ? a.delta : null // NEW
        }));
        renderRecentAlerts();
        return;
      }
      if (msg.type === "rvol_alert") {
        addRvolAlert(msg);
        return;
      }
      if (msg.type === "scalp_alert") {
        // Visuals are handled via mirrored generic alerts ("alert" with kind "scalp_*").
        // Respect the scalps toggle for sound as well.
        if (chkScalps && !chkScalps.checked) return;
        if (!silent) playScalpSound();
        return;
      }
    } catch {}
  };
}
async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify(body)
  });
  try {
    const j = await res.json();
    if (j?.status) setStatus(j.status, !!j.ok);
    return j;
  } catch { return null; }
}
async function initStatus() {
  try {
    const res = await fetch("/api/status", { cache: "no-store" });
    const j = await res.json();
    streamRunning = !!j?.running;
    if (typeof j?.mini_chart_lookback_min === 'number') {
      chartLookbackMin = j.mini_chart_lookback_min;
    }
    if (j?.date) {
      sessionDateET = j.date;
      dateInput.value = j.date;
    } else {
      sessionDateET = todayISO();
    }
    if (j?.running) {
      const map = { pre: "Pre-market", rth: "RTH", pm: "PM" };
      const label = map[j.session] || (j.session ? j.session.toUpperCase() : "Session");
      const suffix = j.startET ? ` from ${j.startET} to ${j.endET} ET` : "";
      setStatus(`${label} running on ${j.date}${suffix}`, true);
    } else {
      setStatus("Stopped");
    }
    if (j?.session) {
      const radio = document.querySelector(`input[name="session"][value="${j.session}"]`);
      if (radio) radio.checked = true;
    }
    if (j?.local) {
      const mode = (j.local.levels_mode === "local") ? "local" : "session";
      setLevelsMode(mode);
      const t = normalizeHHMM(j.local.time || "");
      if (t && localTimeInput) localTimeInput.value = t;
      if (typeof j.local.enabled === "boolean") {
        if (mode === "local") {
          if (chkLocalHigh) chkLocalHigh.checked = j.local.enabled;
          if (chkLocalLow) chkLocalLow.checked = j.local.enabled;
        }
      }
    } else {
      const fallback = defaultLocalTimeForSession(selectedSession());
      if (localTimeInput && !normalizeHHMM(localTimeInput.value)) localTimeInput.value = fallback;
      setLevelsMode("session");
    }
    syncLocalTimeMirror();
    syncTickModeUI();
    if (typeof j?.watchlist_count === "number") {
      setTickUniverseSize(j.watchlist_count);
    }
    if (j?.qqq_tape) {
      ingestQQQTape(j.qqq_tape, false);
    }
    if (historyLoaded) rebuildTickFromAlerts(allAlerts);
    // NEW: Sync RVOL settings
    if (j?.rvol) {
      rvolThreshold.value = j.rvol.threshold || 2.0;
      rvolMethod.value = j.rvol.method || "A";
      if (baselineSingle && baselineCumulative) {
        if (j.rvol.baseline_mode === "cumulative") {
          baselineCumulative.checked = true;
        } else {
          baselineSingle.checked = true;
        }
      }
      rvolActive.checked = !!j.rvol.active;
    }
    // NEW: UI config (tiny cap threshold & industry regex)
    if (j?.ui) {
      if (typeof j.ui.tiny_cap_max === "number" && isFinite(j.ui.tiny_cap_max) && j.ui.tiny_cap_max > 0) {
        uiConfig.tinyCapMax = j.ui.tiny_cap_max;
      }
      if (typeof j.ui.industry_regex === "string" && j.ui.industry_regex.trim() !== "") {
        uiConfig.industryRegex = j.ui.industry_regex;
      }
      if (typeof j.ui.chart_opener_base_url === "string" && j.ui.chart_opener_base_url.trim() !== "") {
        uiConfig.chartOpenerBaseURL = j.ui.chart_opener_base_url.trim();
      }
      if (typeof j.ui.auto_now_seconds === "number" && isFinite(j.ui.auto_now_seconds) && j.ui.auto_now_seconds > 0) {
        uiConfig.autoNowSeconds = Math.max(1, Math.round(j.ui.auto_now_seconds));
      }
      if (typeof j.ui.pace_of_tape_window_seconds === "number" && isFinite(j.ui.pace_of_tape_window_seconds) && j.ui.pace_of_tape_window_seconds > 0) {
        uiConfig.paceOfTapeWindowSeconds = Math.max(1, Math.round(j.ui.pace_of_tape_window_seconds));
      }
      compileIndustryRegex();
      syncAutoNowButton();
      refreshTapePaceIndicator();
    }
    applyQQQMode(!!j?.qqq_mode);
  } catch {
    setStatus("Disconnected");
  }
}
function setPausedUI(v) {
  paused = v;
  btnPause.textContent = paused ? "Resume Alerts (tab)" : "Pause Alerts (tab)";
}
/* ------------ Compact mode toggle (persisted) ------------ */
function setCompact(on){
  document.body.classList.toggle('compact', !!on);
  try { localStorage.setItem('qqq-edge.compact', on ? '1' : '0'); } catch {}
  chartsLimit = on ? 2 : 5; // fewer auto-charts in compact mode
  renderAll(); // re-render to apply density + limits
}
function initCompact(){
  let on = false;
  try {
    const saved = localStorage.getItem('qqq-edge.compact');
    on = saved === '1';
  } catch {}
  // URL param can force compact on first load
  if (getParam('compact') === '1') on = true;
  if (chkCompact) chkCompact.checked = on;
  setCompact(on);
}
/* ------------ Essentials mode toggle (persisted) ------------ */
function setEssentials(on){
  const enabled = !!on;
  document.body.classList.toggle("essentials", enabled);
  if (chkEssentialsTop) chkEssentialsTop.checked = enabled;
  if (chkEssentialsQqqTape) chkEssentialsQqqTape.checked = enabled;
  if (enabled) setRightTab("live");
  try { localStorage.setItem(ESSENTIALS_STORAGE_KEY, enabled ? "1" : "0"); } catch {}
  recomputeFeedCapacity();
  recomputeLiveCapacity();
}
function initEssentials(){
  let on = false;
  try {
    const saved = localStorage.getItem(ESSENTIALS_STORAGE_KEY);
    on = saved === "1";
  } catch {}
  const p = getParam("essentials");
  if (p === "1") on = true;
  if (p === "0") on = false;
  setEssentials(on);
}
// ----------------- Init & wiring -----------------
(function init(){
  setStatus("Loading…");
  loadSound().then(() => {});
  loadPins();
  loadSoundChannelPrefs();
  // Default sound ON; prime/resume audio context on first gesture
  if (soundState) soundState.textContent = "Sound ON";
  soundBtn.addEventListener("click", enableSound);
  window.addEventListener('pointerdown', enableSound, { once:true });
  window.addEventListener('keydown',    enableSound, { once:true });
  window.addEventListener('touchstart', enableSound, { once:true });
  window.addEventListener("keydown", (e) => {
    if (e.repeat) return;
    if (!(e.code === "Space" || e.key === " ")) return;
    if (isTypingTarget(e.target)) return;
    e.preventDefault();
    triggerNowShortcut();
  });
  // Compact mode first so layout is set before initial renders
  initCompact();
  initRightTabs();
  initEssentials();
  syncAutoNowButton();
  refreshTapePaceIndicator();
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) return;
    if (autoNowEnabled) tickAutoNow();
    else syncAutoNowButton();
    refreshTapePaceIndicator();
  });
  syncTickModeUI();
  resetQQQTapeChart();
  resetTickChart();
  if (chkCompact) {
    chkCompact.addEventListener('change', (e) => setCompact(!!e.target.checked));
  }
  if (chkEssentialsTop) {
    chkEssentialsTop.addEventListener("change", (e) => {
      setEssentials(!!e.target.checked);
    });
  }
  if (chkEssentialsQqqTape) {
    chkEssentialsQqqTape.addEventListener("change", (e) => {
      setEssentials(!!e.target.checked);
    });
  }
  dateInput.value = todayISO();
  syncLevelsModeUI();
  syncLocalTimeMirror();
  // Start
  btnStart.addEventListener("click", async () => {
    const date = (dateInput.value || "").trim();
    if (!date) return;
    const session = selectedSession();
    const levelsMode = selectedLevelsMode();
    const localTime = currentLocalTimeInput();
    const localEnabled = !!(chkLocalHigh?.checked || chkLocalLow?.checked);
    const j = await postJSON("/api/stream", {
      mode: "start",
      date,
      session,
      levels_mode: levelsMode,
      local_time: localTime,
      local_enabled: localEnabled,
    });
    streamRunning = !!j?.ok;
    // wipe UI and cancel any chart timers
    for (const id of Array.from(chartsById.keys())) destroyChart(id);
    feed.innerHTML = "";
    activeChartQueue.splice(0, activeChartQueue.length);
    chartsById.clear();
    allAlerts = [];
    tapePaceEventsMs = [];
    tapePaceDirsBySymbol.clear();
    historyLoaded = false;
    recentAlerts = []; // NEW
    renderRecentAlerts(); // NEW
    sessionDateET = date;
    // keep pins; they will re-render when history arrives
    renderAll();
    resetTickForContextChange();
    resetQQQTapeChart();
  });
  // Stop
  btnStop.addEventListener("click", async () => {
    const j = await postJSON("/api/stream", { mode: "stop" });
    streamRunning = !j?.ok ? streamRunning : false;
    if (j?.ok) resetQQQTapeChart();
  });
  // Pause alerts in this tab only
  btnPause.addEventListener("click", () => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    if (paused) {
      ws.send(JSON.stringify({ type: "control", action: "resume" }));
      setPausedUI(false);
    } else {
      ws.send(JSON.stringify({ type: "control", action: "pause" }));
      setPausedUI(true);
    }
  });
  // Clear UI (& server history) — pins are persisted but will be removed if their alerts are gone
  btnClear.addEventListener("click", async () => {
    for (const id of Array.from(chartsById.keys())) destroyChart(id);
    feed.innerHTML = "";
    activeChartQueue.splice(0, activeChartQueue.length);
    chartsById.clear();
    allAlerts = [];
    tapePaceEventsMs = [];
    tapePaceDirsBySymbol.clear();
    historyLoaded = false;
    recentAlerts = []; // NEW
    renderRecentAlerts(); // NEW
    try { await postJSON("/api/clear", {}); } catch {}
    renderAll();
    resetTickForContextChange();
    resetQQQTapeChart();
  });
  // Reload watchlist (NEW)
  if (btnReloadWL) {
    btnReloadWL.addEventListener("click", async () => {
      await postJSON("/api/watchlist/reload", {}); // server reloads active watchlist file set
      await refreshTickUniverseSize();
    });
  }
  // Unpin all
  if (btnUnpinAll) {
    btnUnpinAll.addEventListener("click", () => {
      pinnedOrder = [];
      pinnedSet = new Set();
      savePins();
      renderAll();
    });
  }
  // Filter toggles: re-render on change
  chkHod.addEventListener("change", renderAll);
  chkLod.addEventListener("change", renderAll);
  levelsModeRadios.forEach(el => {
    el.addEventListener("change", async () => {
      syncLevelsModeUI();
      if (historyLoaded) rebuildTapePaceEvents(allAlerts);
      await applyLiveSettings({ resetTick: true });
    });
  });
  if (chkLocalHigh) {
    chkLocalHigh.addEventListener("change", async () => {
      renderAll();
      await applyLiveSettings({ resetTick: true });
    });
  }
  if (chkLocalLow) {
    chkLocalLow.addEventListener("change", async () => {
      renderAll();
      await applyLiveSettings({ resetTick: true });
    });
  }
  if (localTimeInput) {
    localTimeInput.addEventListener("change", async () => {
      localTimeInput.value = currentLocalTimeInput();
      await applyLiveSettings({ resetTick: true });
    });
  }
  if (btnApplyLive) {
    btnApplyLive.addEventListener("click", async () => {
      showActionFeedback("Apply Live");
      await applyLiveSettings({ resetTick: true });
      if (!streamRunning) {
        setStatus("Settings ready. Press Start to run.", true);
      }
    });
  }
  dateInput.addEventListener("change", async () => {
    await applyLiveSettings({ resetTick: true });
  });
  document.querySelectorAll('input[name="session"]').forEach(el => {
    el.addEventListener("change", async () => {
      await applyLiveSettings({ resetTick: true });
    });
  });
  if (sessionQuickBtns && sessionQuickBtns.length > 0) {
    sessionQuickBtns.forEach(btn => {
      btn.addEventListener("click", async () => {
        const sess = (btn.dataset.session || "").trim();
        const t = normalizeHHMM(btn.dataset.local || "");
        setLevelsMode("session");
        if (sess) {
          const radio = document.querySelector(`input[name="session"][value="${sess}"]`);
          if (radio) radio.checked = true;
        }
        if (t && localTimeInput) localTimeInput.value = t;
        await applyLiveSettings({ resetTick: true });
      });
    });
  }
  if (localPresetBtns && localPresetBtns.length > 0) {
    localPresetBtns.forEach(btn => {
      btn.addEventListener("click", async () => {
        await applyLocalPresetButton(btn);
      });
    });
  }
  if (btnLiveAuto) {
    btnLiveAuto.addEventListener("click", () => {
      setAutoNow(!autoNowEnabled);
    });
  }
  // NEW RVOL control listeners
  rvolThreshold.addEventListener("input", () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({type: "control", action: "set_rvol_threshold", value: parseFloat(rvolThreshold.value)}));
    }
  });
  rvolMethod.addEventListener("change", () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({type: "control", action: "set_rvol_method", value: rvolMethod.value}));
    }
  });
  document.querySelectorAll('input[name="baseline"]').forEach(r => {
    r.addEventListener("change", () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({type: "control", action: "set_baseline_mode", value: r.value}));
      }
    });
  });
  rvolActive.addEventListener("change", () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({type: "control", action: "set_rvol_active", value: rvolActive.checked}));
    }
  });
  if (chkCurrentSounds) {
    chkCurrentSounds.addEventListener("change", (e) => {
      currentAlertSoundsEnabled = !!e.target.checked;
      persistSoundChannelPrefs();
      if (!currentAlertSoundsEnabled) stopCurrentSounds();
    });
  }
  if (chkSyntheticSounds) {
    chkSyntheticSounds.addEventListener("change", (e) => {
      syntheticTapeSoundsEnabled = !!e.target.checked;
      persistSoundChannelPrefs();
      if (!syntheticTapeSoundsEnabled) stopSyntheticSounds();
    });
  }
  silentMode.addEventListener("change", (e) => {
    silent = e.target.checked;
    if (silent) stopAllSounds();
  });
  connectWS();
  initStatus().then(() => {});
  refreshTickUniverseSize().then(() => {});
  // Keep live pane sized correctly
  window.addEventListener("resize", () => {
    recomputeFeedCapacity();
    recomputeLiveCapacity();
  });
  // === RVOL table header sorting (per-minute) ===
  if (alertsTableHead) {
    alertsTableHead.addEventListener("click", (e) => {
      const th = e.target.closest("th.sortable");
      if (!th) return;
      const key = th.dataset.key;
      if (!key) return;
      if (alertsSortState.key === key) {
        alertsSortState.dir = (alertsSortState.dir === "asc") ? "desc" : "asc";
      } else {
        alertsSortState.key = key;
        alertsSortState.dir = "asc"; // first click = ascending
      }
      renderRecentAlerts();
    });
    // show default indicator (volume desc) on first paint
    updateAlertsSortIndicators();
  }
})();

function stopAllSounds() {
  stopCurrentSounds();
  stopSyntheticSounds();
}

// Removed custom scalp live card: live visuals come from mirrored generic alerts ("alert" with kind "scalp_*").

// =======================
// FMP profile helpers
// =======================
function formatMarketCap(n) {
  const num = Number(n) || 0;
  const abs = Math.abs(num);
  let val, suf;
  if (abs >= 1e9) { // billions (and trillions expressed as B with thousands separators)
    val = num / 1e9; suf = "B";
  } else if (abs >= 1e6) {
    val = num / 1e6; suf = "M";
  } else {
    val = num / 1e3; suf = "K";
  }
  const rounded = Math.round(val * 10) / 10; // at most 1 decimal place
  // Use locale grouping with at most 1 decimal place
  const txt = rounded.toLocaleString(undefined, {
    minimumFractionDigits: (Math.abs(rounded - Math.trunc(rounded)) > 0) ? 1 : 0,
    maximumFractionDigits: 1,
  });
  return `${txt} ${suf}`;
}
async function getProfile(sym) {
  const k = String(sym || "").toUpperCase();
  if (!k) return null;
  if (profileCache.has(k)) return profileCache.get(k);
  if (profileInFlight.has(k)) return profileInFlight.get(k);

  const req = runMetaFetchTask(async () => {
    const res = await fetch(`/api/profile?ticker=${encodeURIComponent(k)}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const j = await res.json();
    const info = {
      marketCap: Number(j?.marketCap) || 0,
      country: String(j?.country || ""),
      industry: String(j?.industry || ""),
    };
    setBoundedCache(profileCache, k, info, PROFILE_CACHE_MAX);
    return info;
  })
    .catch(() => null)
    .finally(() => {
      profileInFlight.delete(k);
    });

  profileInFlight.set(k, req);
  return req;
}
function applyLiveHighlights(cardEl, info) {
  if (!cardEl || !cardEl.classList.contains('live')) return;
  const capEl = cardEl.querySelector(".cap");
  if (capEl) {
    const mc = Number(info?.marketCap || 0);
    if (mc > 0 && mc < uiConfig.tinyCapMax) {
      capEl.classList.add("isTinyCap");
    } else {
      capEl.classList.remove("isTinyCap");
    }
  }
  const indEl = cardEl.querySelector(".industry");
  if (indEl) {
    const txt = String(info?.industry || "");
    if (industryRe && txt && industryRe.test(txt)) {
      indEl.classList.add("isIndustryFlag");
    } else {
      indEl.classList.remove("isIndustryFlag");
    }
  }
}
async function populateProfileForCard(cardEl, sym) {
  if (!cardEl || !cardEl.isConnected) return;
  const priceEl = cardEl.querySelector(".price");
  if (!priceEl) return;
  try {
    const info = await getProfile(sym);
    if (!cardEl.isConnected) return;
    if (!info) return;
    const capEl = priceEl.querySelector(".cap");
    const ctyEl = priceEl.querySelector(".country");
    const indEl = priceEl.querySelector(".industry");
    if (capEl) capEl.textContent = formatMarketCap(info.marketCap);
    if (ctyEl) ctyEl.textContent = info.country || "";
    if (indEl) indEl.textContent = info.industry || "";
    // Live‑only visual cues
    applyLiveHighlights(cardEl, info);
  } catch {/* no-op */}
}
