const statusPill = document.getElementById("status");
const feed = document.getElementById("feed");
const liveFeed = document.getElementById("live");
const soundBtn = document.getElementById("soundBtn");
const soundState = document.getElementById("soundState");
const actionFlashEl = document.getElementById("actionFlash");
const actionBadgeEl = document.getElementById("actionBadge");
const chkEssentialsTop = document.getElementById("chkEssentialsTop");
const chkEssentialsQqqTape = document.getElementById("chkEssentialsQqqTape");
const fallbackAudioUp = document.getElementById("alertAudioUp");
const fallbackAudioDown = document.getElementById("alertAudioDown");
const dateInput = document.getElementById("dateInput");
const btnStart = document.getElementById("btnStart");
const btnStop = document.getElementById("btnStop");
const btnPause = document.getElementById("btnPause");
const btnClear = document.getElementById("btnClear");
const btnReloadWL = document.getElementById("btnReloadWl");
const btnApplyLive = document.getElementById("btnApplyLive");
const chkLocalHigh = document.getElementById("chkLocalHigh");
const chkLocalLow = document.getElementById("chkLocalLow");
const localTimeInput = document.getElementById("localTimeInput");
const localTimeReadout = document.getElementById("localTimeReadout");
const btnLiveAuto = document.getElementById("btnLiveAuto");
const liveAutoCountdown = document.getElementById("liveAutoCountdown");
const liveNowTime = document.getElementById("liveNowTime");
const btnLiveNow = document.getElementById("btnLiveNow");
const tapePaceIndicator = document.getElementById("tapePaceIndicator");
const tapePaceCountEl = document.getElementById("tapePaceCount");
const tapePaceWindowEl = document.getElementById("tapePaceWindow");
const alertSourceButtons = Array.from(document.querySelectorAll("[data-alert-source]"));
const localPresetBtns = document.querySelectorAll(".localPreset");
const silentMode = document.getElementById("silentMode");
const chkCurrentSounds = document.getElementById("chkCurrentSounds");
const chkSyntheticSounds = document.getElementById("chkSyntheticSounds");
const qqqTapeExecEl = document.getElementById("qqqTapeExec");
const qqqTapeStateEl = document.getElementById("qqqTapeState");
const qqqTapeBiasEl = document.getElementById("qqqTapeBias");
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
const tickValueEl = document.getElementById("tickValue");
const tickModeBadge = document.getElementById("tickModeBadge");

const ESSENTIALS_STORAGE_KEY = "qqq-edge-universal.essentials";
const CURRENT_SOUNDS_STORAGE_KEY = "qqq-edge-universal.current_sounds";
const SYNTH_SOUNDS_STORAGE_KEY = "qqq-edge-universal.synth_sounds";
const HISTORY_LIMIT = 200;
const LIVE_LIMIT = 30;

let ws = null;
let streamRunning = false;
let paused = false;
let soundEnabled = true;
let currentAlertSoundsEnabled = true;
let syntheticTapeSoundsEnabled = true;
let silent = false;
let currentAlertSource = "trades";
let allAlerts = [];
let uiConfig = {
  autoNowSeconds: 10,
  paceOfTapeWindowSeconds: 60,
};
let autoNowEnabled = false;
let autoNowTimerId = 0;
let autoNowNextAt = 0;
let clockTimerId = 0;
let tapePaceTimerId = 0;
let tickDirsBySymbol = new Map();
let tapePaceDirsBySymbol = new Map();
let tapePaceEventsMs = [];
let breakoutBreadthResetAtMs = 0;

function setStatus(text, ok = false) {
  statusPill.textContent = text;
  statusPill.className = ok ? "pill ok" : "pill";
}

function showActionFeedback(label) {
  const text = String(label || "Applied").trim().toUpperCase();
  const timestamp = `${formatETTime()} ET`;
  if (actionFlashEl) {
    actionFlashEl.classList.remove("show");
    void actionFlashEl.offsetWidth;
    actionFlashEl.classList.add("show");
  }
  if (actionBadgeEl) {
    actionBadgeEl.textContent = `${text} • ${timestamp}`;
    actionBadgeEl.classList.remove("show");
    void actionBadgeEl.offsetWidth;
    actionBadgeEl.classList.add("show");
  }
}

function currentETClockHHMM() {
  return formatETTime().slice(0, 5);
}

function syncLocalTimeMirror(raw = localTimeInput?.value || "") {
  const normalized = normalizeClock(raw) || currentETClockHHMM();
  if (localTimeInput) localTimeInput.value = normalized;
  if (localTimeReadout) localTimeReadout.textContent = `${normalized}:00`;
  return normalized;
}

function normalizeClock(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  const m = /^(\d{1,2}):(\d{2})$/.exec(s);
  if (!m) return "";
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isInteger(hh) || !Number.isInteger(mm) || hh < 0 || hh > 23 || mm < 0 || mm > 59) {
    return "";
  }
  return `${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
}

function currentLocalTimeInput() {
  return syncLocalTimeMirror(localTimeInput?.value || "");
}

function currentLocalEnabled() {
  return !!(chkLocalHigh?.checked || chkLocalLow?.checked);
}

function activateLocalBreakouts() {
  let changed = false;
  if (chkLocalHigh && !chkLocalHigh.checked) {
    chkLocalHigh.checked = true;
    changed = true;
  }
  if (chkLocalLow && !chkLocalLow.checked) {
    chkLocalLow.checked = true;
    changed = true;
  }
  if (changed) {
    renderAll();
  }
}

function formatETTime(date = new Date()) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function updateClock() {
  if (liveNowTime) liveNowTime.textContent = formatETTime();
  syncAutoNowButton();
}

function scheduleClockTick(nowMs = Date.now()) {
  if (clockTimerId) window.clearTimeout(clockTimerId);
  const delayMs = Math.max(50, 1000 - (nowMs % 1000));
  clockTimerId = window.setTimeout(() => {
    updateClock();
    scheduleClockTick();
  }, delayMs);
}

function startClock() {
  updateClock();
  scheduleClockTick();
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

function updateSoundUi() {
  soundBtn.textContent = soundEnabled ? "Disable sound" : "Enable sound";
  soundState.textContent = soundEnabled && !silent ? "Sound ON" : (silent ? "Silent" : "Sound OFF");
}

function canPlayCurrentSounds() {
  return soundEnabled && !silent && currentAlertSoundsEnabled;
}

function canPlaySyntheticSounds() {
  return soundEnabled && !silent && syntheticTapeSoundsEnabled;
}

function normalizeAlertSource(value) {
  return String(value || "").toLowerCase() === "nbbo" ? "nbbo" : "trades";
}

function alertMatchesCurrentSource(a) {
  return normalizeAlertSource(a?.source) === currentAlertSource;
}

function applyAlertSourceUi(source) {
  currentAlertSource = normalizeAlertSource(source);
  alertSourceButtons.forEach((btn) => {
    const active = normalizeAlertSource(btn.dataset.alertSource) === currentAlertSource;
    btn.classList.toggle("isActive", active);
    btn.setAttribute("aria-pressed", active ? "true" : "false");
  });
  renderAll();
}

function restartAudio(el) {
  if (!el) return;
  try {
    el.pause();
    el.currentTime = 0;
    const playPromise = el.play();
    if (playPromise && typeof playPromise.catch === "function") {
      playPromise.catch(() => {});
    }
  } catch {}
}

function playCurrentSoundElement(el) {
  if (!canPlayCurrentSounds() || !el) return;
  try {
    const audio = el.cloneNode(true);
    audio.currentTime = 0;
    const playPromise = audio.play();
    if (playPromise && typeof playPromise.catch === "function") {
      playPromise.catch(() => {
        restartAudio(el);
      });
    }
  } catch {
    restartAudio(el);
  }
}

function playUpSound() {
  if (!canPlayCurrentSounds() || !fallbackAudioUp) return;
  playCurrentSoundElement(fallbackAudioUp);
}

function playDownSound() {
  if (!canPlayCurrentSounds() || !fallbackAudioDown) return;
  playCurrentSoundElement(fallbackAudioDown);
}

function playCurrentAlertSound(kind) {
  if (kind === "llow") {
    playDownSound();
    return;
  }
  playUpSound();
}

function maybePlayTapeSound(msg) {
  return;
}

function alertVisible(kind) {
  if (kind === "lhigh") return !!chkLocalHigh?.checked;
  if (kind === "llow") return !!chkLocalLow?.checked;
  return false;
}

function alertShown(a) {
  return !!a && alertMatchesCurrentSource(a) && alertVisible(a.kind);
}

function alertLabel(kind) {
  if (kind === "lhigh") return "NEW LOCAL HIGH";
  if (kind === "llow") return "NEW LOCAL LOW";
  return kind || "ALERT";
}

function buildSourceTags(sources) {
  if (!Array.isArray(sources) || sources.length === 0) return "";
  return `<span class="sourceWrap">${sources.map((s) => `<span class="srcTag">${escapeHtml(s)}</span>`).join("")}</span>`;
}

function buildAlertCard(a, live = false) {
  const classes = ["card", a.kind];
  if (live) classes.push("live");
  return `
    <article class="${classes.join(" ")}">
      <div class="left">
        <span class="badge">${alertLabel(a.kind)}</span>
        <span class="sym">${escapeHtml(a.sym || "")}</span>
        ${buildSourceTags(a.sources)}
        ${a.name ? `<span class="name">${escapeHtml(a.name)}</span>` : ""}
      </div>
      <div class="price"><span class="pv">${formatPrice(a.price)}</span></div>
      <div class="time">${escapeHtml(a.time || "")}</div>
    </article>
  `;
}

function renderAll() {
  const visible = allAlerts.filter((a) => alertShown(a));
  if (feed) {
    feed.innerHTML = visible.length === 0
      ? `<div class="panel muted">No local alerts yet.</div>`
      : visible.map((a) => buildAlertCard(a, false)).join("");
  }
  if (liveFeed) {
    const slice = visible.slice(0, LIVE_LIMIT);
    liveFeed.innerHTML = slice.length === 0
      ? `<div class="panel muted">No live alerts yet.</div>`
      : slice.map((a) => buildAlertCard(a, true)).join("");
  }
  rebuildBreadthAndPace();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formatPrice(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return "—";
  return n.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function breakoutDirectionForAlert(a) {
  const sym = String(a?.sym || "").trim().toUpperCase();
  if (!sym) return null;
  if (a?.kind === "lhigh") return { sym, dir: 1 };
  if (a?.kind === "llow") return { sym, dir: -1 };
  return null;
}

function applyBreakoutTransition(a, dirsBySymbol) {
  const breakout = breakoutDirectionForAlert(a);
  if (!breakout) return null;
  const prevDir = dirsBySymbol.get(breakout.sym) || 0;
  if (prevDir === breakout.dir) return null;
  dirsBySymbol.set(breakout.sym, breakout.dir);
  return {
    sym: breakout.sym,
    prevDir,
    nextDir: breakout.dir,
    delta: breakout.dir - prevDir,
  };
}

function alertTimeMs(a) {
  const ts = Number(a?.ts_unix);
  return Number.isFinite(ts) && ts > 0 ? ts : Date.now();
}

function updateBreakoutBreadthReadout(total) {
  if (tickValueEl) {
    tickValueEl.textContent = `${total > 0 ? "+" : ""}${total}`;
    tickValueEl.className = `tickValue ${total > 0 ? "positive" : total < 0 ? "negative" : "neutral"}`;
  }
  if (tickModeBadge) {
    tickModeBadge.textContent = "Local High/Low";
    tickModeBadge.classList.add("local");
  }
}

function markBreakoutBreadthReset(nowMs = Date.now()) {
  breakoutBreadthResetAtMs = nowMs;
  tickDirsBySymbol = new Map();
  updateBreakoutBreadthReadout(0);
}

function rebuildBreadthAndPace() {
  tickDirsBySymbol = new Map();
  tapePaceDirsBySymbol = new Map();
  tapePaceEventsMs = [];

  const ordered = [...allAlerts].reverse();
  let total = 0;
  for (const a of ordered) {
    if (!alertShown(a)) continue;
    const tsMs = alertTimeMs(a);
    if (tsMs >= breakoutBreadthResetAtMs) {
      const tickTransition = applyBreakoutTransition(a, tickDirsBySymbol);
      if (tickTransition) {
        total += tickTransition.delta;
      }
    }
    const paceTransition = applyBreakoutTransition(a, tapePaceDirsBySymbol);
    if (paceTransition) {
      tapePaceEventsMs.push(alertTimeMs(a));
    }
  }

  updateBreakoutBreadthReadout(total);
  updateTapePaceIndicator();
}

function paceWindowMs() {
  return Math.max(1, Number(uiConfig.paceOfTapeWindowSeconds) || 60) * 1000;
}

function tapePaceState(count, windowSeconds) {
  const ratePerMinute = count * (60 / Math.max(1, windowSeconds));
  if (count <= 0) return "idle";
  if (ratePerMinute < 4) return "slow";
  if (ratePerMinute < 8) return "active";
  if (ratePerMinute < 14) return "fast";
  return "hot";
}

function updateTapePaceIndicator() {
  const cutoff = Date.now() - paceWindowMs();
  tapePaceEventsMs = tapePaceEventsMs.filter((ts) => ts >= cutoff);
  const count = tapePaceEventsMs.length;
  const windowSeconds = Math.max(1, Number(uiConfig.paceOfTapeWindowSeconds) || 60);
  if (tapePaceCountEl) tapePaceCountEl.textContent = String(count);
  if (tapePaceWindowEl) tapePaceWindowEl.textContent = `${windowSeconds}s`;
  if (tapePaceIndicator) {
    const state = tapePaceState(count, windowSeconds);
    tapePaceIndicator.dataset.state = state;
    const label = `Pace of tape: ${count} local high/low alerts changed breakout breadth in the last ${windowSeconds} seconds`;
    tapePaceIndicator.title = label;
    tapePaceIndicator.setAttribute("aria-label", label);
  }
}

function startTapePaceTimer() {
  if (tapePaceTimerId) window.clearInterval(tapePaceTimerId);
  updateTapePaceIndicator();
  tapePaceTimerId = window.setInterval(updateTapePaceIndicator, 1000);
}

function addIncomingAlert(a) {
  if (!a || !a.kind || !a.sym) return;
  allAlerts.unshift(a);
  if (allAlerts.length > HISTORY_LIMIT) {
    allAlerts = allAlerts.slice(0, HISTORY_LIMIT);
  }
  renderAll();
  if (alertShown(a)) {
    playCurrentAlertSound(a.kind);
  }
}

function autoNowIntervalSeconds() {
  return Math.max(1, Number(uiConfig.autoNowSeconds) || 10);
}

function formatAutoCountdown(msRemaining) {
  return `${Math.max(0, Math.ceil(msRemaining / 1000))}s`;
}

function syncAutoNowButton(nowMs = Date.now()) {
  if (!btnLiveAuto) return;
  btnLiveAuto.classList.toggle("isActive", autoNowEnabled);
  btnLiveAuto.setAttribute("aria-pressed", autoNowEnabled ? "true" : "false");
  const intervalSeconds = autoNowIntervalSeconds();
  btnLiveAuto.title = autoNowEnabled
    ? `Disable automatic Now refresh every ${intervalSeconds} seconds`
    : `Enable automatic Now refresh every ${intervalSeconds} seconds`;
  if (!autoNowEnabled || !liveAutoCountdown) {
    if (liveAutoCountdown) {
      liveAutoCountdown.hidden = true;
      liveAutoCountdown.textContent = "";
    }
    return;
  }
  const remainingMs = Math.max(0, autoNowNextAt - nowMs);
  liveAutoCountdown.hidden = false;
  liveAutoCountdown.textContent = formatAutoCountdown(remainingMs);
}

function nextAutoNowBoundaryMs(nowMs = Date.now(), opts = {}) {
  const inclusive = !!opts.inclusive;
  const intervalSeconds = autoNowIntervalSeconds();
  const truncatedNowMs = Math.floor(nowMs / 1000) * 1000;
  const now = new Date(truncatedNowMs);
  const seconds = now.getSeconds();
  const minuteStartMs = truncatedNowMs - (seconds * 1000);
  if (inclusive && seconds % intervalSeconds === 0) {
    return nowMs;
  }
  const nextSecond = Math.floor(seconds / intervalSeconds + 1) * intervalSeconds;
  if (nextSecond < 60) {
    return minuteStartMs + (nextSecond * 1000);
  }
  return minuteStartMs + 60000;
}

async function applyLiveUpdate(opts = {}) {
  if (opts.resetBreadth) {
    markBreakoutBreadthReset();
    renderAll();
  }
  if (!streamRunning) return;
  await postJson("/api/stream", {
    mode: "update",
    date: dateInput?.value || "",
    local_time: currentLocalTimeInput(),
    local_enabled: currentLocalEnabled(),
  });
}

function autoNowCountdownDelayMs(nowMs = Date.now()) {
  const remainingMs = autoNowNextAt - nowMs;
  if (remainingMs <= 0) return 0;
  const secondsShown = Math.max(1, Math.ceil(remainingMs / 1000));
  return Math.max(50, remainingMs - ((secondsShown - 1) * 1000));
}

function scheduleAutoNowTick(nowMs = Date.now()) {
  if (autoNowTimerId) window.clearTimeout(autoNowTimerId);
  if (!autoNowEnabled) {
    syncAutoNowButton(nowMs);
    return;
  }
  const delayMs = autoNowNextAt <= nowMs ? 0 : autoNowCountdownDelayMs(nowMs);
  autoNowTimerId = window.setTimeout(() => {
    void tickAutoNow();
  }, delayMs);
  syncAutoNowButton(nowMs);
}

function resetAutoNowCountdown() {
  if (!autoNowEnabled) return;
  autoNowNextAt = nextAutoNowBoundaryMs(Date.now());
  syncAutoNowButton();
  scheduleAutoNowTick();
}

function stopAutoNow() {
  autoNowEnabled = false;
  autoNowNextAt = 0;
  if (autoNowTimerId) window.clearTimeout(autoNowTimerId);
  autoNowTimerId = 0;
  syncAutoNowButton();
}

async function triggerNowShortcut(opts = {}) {
  showActionFeedback("Now");
  try {
    await applyLocalPreset({ time: "now", applyLive: streamRunning });
  } catch (err) {
    setStatus(String(err?.message || err));
  } finally {
    if (opts.resetAutoCountdown !== false) {
      resetAutoNowCountdown();
    }
  }
}

async function tickAutoNow() {
  if (!autoNowEnabled) return;
  const nowMs = Date.now();
  if (nowMs >= autoNowNextAt) {
    autoNowNextAt = nextAutoNowBoundaryMs(nowMs);
    syncAutoNowButton(nowMs);
    scheduleAutoNowTick(nowMs);
    await triggerNowShortcut({ resetAutoCountdown: false });
    return;
  }
  syncAutoNowButton(nowMs);
  scheduleAutoNowTick(nowMs);
}

function startAutoNow() {
  autoNowEnabled = true;
  autoNowNextAt = nextAutoNowBoundaryMs(Date.now(), { inclusive: true });
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

async function applyLocalPreset({ time, applyLive = false, resetBreadth = true }) {
  let nextTime = time;
  const now = new Date();
  if (time === "now") {
    nextTime = new Intl.DateTimeFormat("en-GB", {
      timeZone: "America/New_York",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(now);
  } else if (time === "prev_half" || time === "prev_hour") {
    const etNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
    const mins = time === "prev_half" ? 30 : 60;
    etNow.setMinutes(etNow.getMinutes() - mins, 0, 0);
    nextTime = `${String(etNow.getHours()).padStart(2, "0")}:${String(etNow.getMinutes()).padStart(2, "0")}`;
  }
  if (localTimeInput && normalizeClock(nextTime)) {
    localTimeInput.value = normalizeClock(nextTime);
  }
  activateLocalBreakouts();
  syncLocalTimeMirror();
  if (resetBreadth && !applyLive) {
    markBreakoutBreadthReset();
    renderAll();
  }
  if (applyLive) {
    await applyLiveUpdate({ resetBreadth });
  }
}

async function postJson(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const text = await res.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = {};
  }
  if (!res.ok) {
    throw new Error(data?.status || text || `HTTP ${res.status}`);
  }
  return data;
}

async function setAlertSource(source) {
  const next = normalizeAlertSource(source);
  const out = await postJson("/api/alert-source", { source: next });
  applyAlertSourceUi(out?.source || next);
  setStatus(out?.status || `Alert source set to ${next.toUpperCase()}`, true);
}

function connectWS() {
  const protocol = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${protocol}://${location.host}/ws`);
  ws.onopen = () => setStatus("Connected", true);
  ws.onclose = () => {
    setStatus("Disconnected");
    window.setTimeout(connectWS, 1000);
  };
  ws.onmessage = (event) => {
    let msg = null;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }
    if (!msg || !msg.type) return;
    if (msg.type === "status") {
      setStatus(msg.text || "Status", msg.level === "success");
      return;
    }
    if (msg.type === "history") {
      const history = Array.isArray(msg.alerts) ? msg.alerts.slice() : [];
      history.sort((a, b) => Number(b.ts_unix || 0) - Number(a.ts_unix || 0));
      allAlerts = history;
      renderAll();
      return;
    }
    if (msg.type === "alert") {
      addIncomingAlert(msg);
      return;
    }
    if (msg.type === "alert_source") {
      applyAlertSourceUi(msg.source);
      return;
    }
    if (msg.type === "qqq_tape") {
      renderQQQTape(msg);
      maybePlayTapeSound(msg);
    }
  };
}

function renderQQQTape(msg) {
  const execEdgeCents = Number(msg?.exec_edge_cents || 0);
  const execEdgeBps = Number(msg?.exec_edge_bps || 0);
  const fairGapBps = Number(msg?.fair_gap_bps || 0);
  const freshnessMs = Math.round(Number(msg?.freshness_ms || 0));
  const biasText = String(msg?.bias || "Balanced");
  const biasClass = /buy/i.test(biasText) ? "buy" : /sell/i.test(biasText) ? "sell" : "neutral";
  const directionalEdge = Math.max(Math.abs(execEdgeBps), Math.abs(fairGapBps));
  const isFresh = freshnessMs <= 2500;
  let stateText = "No Trade";
  let stateClass = "idle";
  if (msg?.tradable) {
    stateText = "Tradable";
    stateClass = "tradable";
  } else if (directionalEdge >= 0.45 && isFresh) {
    stateText = "Watch";
    stateClass = "watch";
  }

  if (qqqTapeStateEl) {
    qqqTapeStateEl.textContent = stateText;
    qqqTapeStateEl.className = `tapeState ${stateClass}`;
  }
  if (qqqTapeBiasEl) {
    qqqTapeBiasEl.textContent = biasText;
    qqqTapeBiasEl.className = `tapeBias ${biasClass}${/strong/i.test(biasText) ? " strong" : ""}`;
  }
  if (qqqTapeExecEl) {
    qqqTapeExecEl.textContent = `${execEdgeCents >= 0 ? "" : "-"}${Math.abs(execEdgeCents).toFixed(2)}¢`;
    qqqTapeExecEl.className = `tickValue ${execEdgeBps > 0 ? "positive" : execEdgeBps < 0 ? "negative" : "neutral"}`;
  }
  if (qqqTapePriceEl) qqqTapePriceEl.textContent = formatPrice(msg?.qqq_price);
  if (qqqTapeFairValueEl) qqqTapeFairValueEl.textContent = formatPrice(msg?.fair_value);
  if (qqqTapeFairGapEl) qqqTapeFairGapEl.textContent = `${fairGapBps.toFixed(2)} bps`;
  if (qqqTapeExecBpsEl) qqqTapeExecBpsEl.textContent = `${execEdgeBps.toFixed(2)} bps`;
  if (qqqTapeSpreadEl) qqqTapeSpreadEl.textContent = `${Number(msg?.spread_bps || 0).toFixed(2)} bps`;
  if (qqqTapeFreshnessEl) qqqTapeFreshnessEl.textContent = `${freshnessMs} ms`;
  if (qqqTapeCoverageEl) qqqTapeCoverageEl.textContent = `${(Number(msg?.basket_coverage || 0) * 100).toFixed(1)}%`;
  if (qqqTapeTradeEl) qqqTapeTradeEl.textContent = Number(msg?.trade_impulse || 0).toFixed(3);
  if (qqqTapeQuoteEl) qqqTapeQuoteEl.textContent = Number(msg?.quote_imbalance || 0).toFixed(3);
  if (qqqTapeMicroEl) qqqTapeMicroEl.textContent = Number(msg?.micro_edge || 0).toFixed(3);
  if (qqqTapeTopEl) {
    const top = Array.isArray(msg?.top) ? msg.top : [];
    qqqTapeTopEl.innerHTML = top
    .filter((item) => Math.abs(Number(item?.contribution || 0)) >= 0.005)
    .map((item) => {
      const cls = Number(item?.contribution || 0) >= 0 ? "tapeChip pos" : "tapeChip neg";
      return `<span class="${cls}">${escapeHtml(item.sym || "")} ${(Number(item?.contribution || 0)).toFixed(2)}</span>`;
    }).join("");
  }
}

async function fetchStatus() {
  const res = await fetch("/api/status", { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const j = await res.json();
  streamRunning = !!j.running;
  if (dateInput && j.date) dateInput.value = j.date;
  if (j.local?.time) {
    localTimeInput.value = normalizeClock(j.local.time) || currentETClockHHMM();
  } else {
    localTimeInput.value = currentETClockHHMM();
  }
  if (chkLocalHigh) chkLocalHigh.checked = !!j.local?.enabled;
  if (chkLocalLow) chkLocalLow.checked = !!j.local?.enabled;
  syncLocalTimeMirror();
  if (typeof j.ui?.auto_now_seconds === "number" && j.ui.auto_now_seconds > 0) {
    uiConfig.autoNowSeconds = Math.round(j.ui.auto_now_seconds);
  }
  if (typeof j.ui?.pace_of_tape_window_seconds === "number" && j.ui.pace_of_tape_window_seconds > 0) {
    uiConfig.paceOfTapeWindowSeconds = Math.round(j.ui.pace_of_tape_window_seconds);
  }
  applyAlertSourceUi(j.alert_source);
  if (j.qqq_tape) renderQQQTape(j.qqq_tape);
  document.body.classList.toggle("qqqMode", !!j.qqq_mode);
  updateTapePaceIndicator();
}

function applyEssentials(enabled) {
  document.body.classList.toggle("essentials", !!enabled);
  if (chkEssentialsTop) chkEssentialsTop.checked = !!enabled;
  if (chkEssentialsQqqTape) chkEssentialsQqqTape.checked = !!enabled;
  try {
    localStorage.setItem(ESSENTIALS_STORAGE_KEY, enabled ? "1" : "0");
  } catch {}
}

function loadEssentialsPref() {
  try {
    applyEssentials(localStorage.getItem(ESSENTIALS_STORAGE_KEY) === "1");
  } catch {
    applyEssentials(false);
  }
}

async function startStream() {
  const localClock = currentLocalTimeInput() || currentETClockHHMM();
  if (localTimeInput) localTimeInput.value = localClock;
  syncLocalTimeMirror(localClock);
  const payload = {
    mode: "start",
    date: dateInput?.value || "",
    local_time: localClock,
    local_enabled: currentLocalEnabled(),
  };
  const out = await postJson("/api/stream", payload);
  streamRunning = true;
  setStatus(out.status || "Started", true);
}

async function stopStream() {
  const out = await postJson("/api/stream", { mode: "stop" });
  streamRunning = false;
  stopAutoNow();
  setStatus(out.status || "Stopped");
}

async function clearAlerts() {
  await postJson("/api/clear", {});
  allAlerts = [];
  tickDirsBySymbol = new Map();
  tapePaceDirsBySymbol = new Map();
  tapePaceEventsMs = [];
  renderAll();
  updateTapePaceIndicator();
}

function wireEvents() {
  soundBtn.addEventListener("click", async () => {
    soundEnabled = !soundEnabled;
    updateSoundUi();
    if (soundEnabled && fallbackAudioUp) {
      try {
        const playPromise = fallbackAudioUp.play();
        if (playPromise && typeof playPromise.then === "function") {
          await playPromise;
        }
        fallbackAudioUp.pause();
        fallbackAudioUp.currentTime = 0;
      } catch {}
    }
  });

  if (chkCurrentSounds) {
    chkCurrentSounds.addEventListener("change", () => {
      currentAlertSoundsEnabled = !!chkCurrentSounds.checked;
      persistSoundChannelPrefs();
    });
  }
  if (chkSyntheticSounds) {
    chkSyntheticSounds.addEventListener("change", () => {
      syntheticTapeSoundsEnabled = !!chkSyntheticSounds.checked;
      persistSoundChannelPrefs();
    });
  }
  if (silentMode) {
    silentMode.addEventListener("change", () => {
      silent = !!silentMode.checked;
      updateSoundUi();
    });
  }

  chkEssentialsTop?.addEventListener("change", () => applyEssentials(!!chkEssentialsTop.checked));
  chkEssentialsQqqTape?.addEventListener("change", () => applyEssentials(!!chkEssentialsQqqTape.checked));

  btnStart?.addEventListener("click", async () => {
    try {
      await startStream();
    } catch (err) {
      setStatus(String(err?.message || err));
    }
  });
  btnStop?.addEventListener("click", async () => {
    try {
      await stopStream();
    } catch (err) {
      setStatus(String(err?.message || err));
    }
  });
  btnPause?.addEventListener("click", () => {
    paused = !paused;
    btnPause.textContent = paused ? "Resume Alerts (tab)" : "Pause Alerts (tab)";
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "control", action: paused ? "pause" : "resume" }));
    }
  });
  btnClear?.addEventListener("click", async () => {
    try {
      await clearAlerts();
    } catch (err) {
      setStatus(String(err?.message || err));
    }
  });
  btnReloadWL?.addEventListener("click", async () => {
    try {
      const out = await postJson("/api/watchlist/reload", {});
      setStatus(out.status || "Watchlist reloaded", true);
    } catch (err) {
      setStatus(String(err?.message || err));
    }
  });
  btnApplyLive?.addEventListener("click", async () => {
    try {
      showActionFeedback("Apply Live");
      await applyLiveUpdate({ resetBreadth: true });
    } catch (err) {
      setStatus(String(err?.message || err));
    }
  });

  chkLocalHigh?.addEventListener("change", () => {
    if (!currentLocalEnabled()) chkLocalLow.checked = true;
    renderAll();
  });
  chkLocalLow?.addEventListener("change", () => {
    if (!currentLocalEnabled()) chkLocalHigh.checked = true;
    renderAll();
  });

  localTimeInput?.addEventListener("change", () => syncLocalTimeMirror());
  localPresetBtns.forEach((btn) => {
    if (btn.id === "btnLiveNow") return;
    btn.addEventListener("click", async () => {
      await applyLocalPreset({
        time: btn.dataset.time || "",
        applyLive: streamRunning,
      });
    });
  });

  btnLiveNow?.addEventListener("click", async () => {
    await triggerNowShortcut();
  });

  btnLiveAuto?.addEventListener("click", () => {
    setAutoNow(!autoNowEnabled);
  });

  alertSourceButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const next = normalizeAlertSource(btn.dataset.alertSource);
      if (next === currentAlertSource) return;
      try {
        await setAlertSource(next);
      } catch (err) {
        setStatus(String(err?.message || err));
      }
    });
  });
}

async function init() {
  loadEssentialsPref();
  loadSoundChannelPrefs();
  updateSoundUi();
  syncLocalTimeMirror();
  startClock();
  startTapePaceTimer();
  wireEvents();
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) return;
    updateClock();
    if (autoNowEnabled) {
      void tickAutoNow();
      return;
    }
    syncAutoNowButton();
  });
  connectWS();
  try {
    await fetchStatus();
  } catch (err) {
    setStatus(String(err?.message || err));
  }
  renderAll();
}

init();
