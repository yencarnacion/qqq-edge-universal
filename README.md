# qqq-edge

`qqq-edge` is a real-time intraday monitoring tool for fast-moving watchlists. It streams Polygon trades/quotes, computes breakout + RVOL + scalp + QQQ tape signals, and serves a low-latency web UI with optional audio alerts.

## What it does

- Streams watchlist symbols from Polygon websocket data.
- Detects and publishes:
  - Session `HOD` / `LOD` alerts
  - Local `High` / `Low` alerts from a user-selected anchor time
  - RVOL surge alerts (mean/median + single/cumulative baselines)
  - Scalp signals (Rubberband, Backside, Fashionably late)
- Computes a QQQ tape model from:
  - Leader return composite (`1s`, `3s`, `8s`) using ETF weights
  - Leader signed notional flow imbalance
  - QQQ confirmation flow (trade flow + quote imbalance + microprice edge)
  - Spread-aware tradability gating and freshness penalties
- Shows intraday cards with mini charts, news, SEC filings, and source tags in multi-watchlist mode.

## Requirements

- Go `1.21+`
- `POLYGON_API_KEY` (required)
- `FMP_API_KEY` (optional; profile/news enrichment)
- `SEC_API_KEY` (optional; filings)

## Quick Start

1. Copy environment template:

```bash
cp env.example .env
```

2. Fill `.env`:

```env
POLYGON_API_KEY=...
FMP_API_KEY=...   # optional
SEC_API_KEY=...   # optional
```

3. Review `config.yaml`, `watchlist.yaml`, and (recommended) `qqq-etf-holdings.csv`.
4. Run:

```bash
go mod tidy
make run
```

5. Open `http://localhost:8089`.

## Run Options

- Default:

```bash
go run .
```

- Override port:

```bash
go run . -port 8099
```

- Merge multiple watchlists:

```bash
go run . -watchlists watchlist.yaml,watchlist-02.yaml,watchlist-03.yaml
```

- Focused QQQ workflow:

```bash
go run . -watchlists qqq.yaml
```

When the single watchlist filename is `qqq.yaml`, the server returns `qqq_mode=true` and the UI switches into QQQ-focused mode.

## QQQ Tape Model

The model now separates fair value from execution edge:

- `fair_gap_bps`:
  - Build tracked leader return composite from ETF leader weights.
  - Keep raw ETF weights for tracked leaders (no forced renormalization when top-`N` coverage is below 100%).
  - Apply residual basket fallback: omitted ETF weight tracks live QQQ return.
- `exec_edge_bps`:
  - `fair_gap_bps`
  - plus leader flow edge (`leader_flow_250/1000/3000`)
  - plus QQQ tape confirmation (`qqq_flow_250/1000`, quote imbalance, micro edge)
  - then freshness penalty + clamp.

Tradability gate:

- `abs(exec_edge_bps) >= spread_bps*1.60 + 0.45`
- `abs(fair_gap_bps) >= 0.45`
- persistence + alignment checks (fair gap/flow/QQQ confirmation)
- `freshness_ms <= 2500`
- valid QQQ mid/quote

Coverage fields:

- `basket_coverage`: total tracked ETF weight represented by loaded leaders
- `residual_weight`: omitted ETF weight routed to QQQ return fallback

For usage guidance, see `qqq-tape-use.md`.

## Key UI Signals

- **QQQ Tape panel**
  - `Fair Value` and `Fair Gap` show the leader-vs-QQQ valuation component.
  - `Exec Edge` is the execution-facing edge after tape confirmation.
  - State badge: `Tradable`, `Watch`, or `No Trade`.
  - `Coverage` shows tracked ETF weight represented in the model.
- **Breakout Breadth panel**
  - Net upside vs downside breakout state across the watchlist.
- **RVOL table**
  - Minute-level volume expansion vs baseline.
- **Live Alerts**
  - Time-ordered actionable alerts with optional directional sound.

## QQQ Tape Payload Fields

`/api/status` and websocket `qqq_tape` include:

- Core: `score`, `bias`, `qqq_price`, `mid`, `tradable`, `freshness_ms`
- Fair-value: `fair_value`, `fair_gap_bps`, `fair_gap_cents`
- Execution edge: `exec_edge_bps`, `exec_edge_cents` (also mirrored as `edge_bps`, `edge_cents`)
- Structure: `spread_bps`, `basket_coverage`, `residual_weight`
- Return context: `leader_ret_bps`, `qqq_ret_bps`, `lead_lag_gap_bps`
- Flow context: `leader_flow_250/1000/3000`, `qqq_flow_250/1000`, `trade_impulse`, `quote_imbalance`, `micro_edge`
  - compatibility note: these field names are retained for UI stability but represent short/medium/slow human windows in the 1-10s model
- Attribution: `top` (largest weighted leader contributors)

## Configuration Notes

- `config.yaml`
  - `server_port`: UI/API port
  - `rvol.lookback_days`: baseline history depth
  - `ui.chart_opener_base_url`: ticker click target base URL
  - `ui.auto_now_seconds`: Auto button interval/countdown in seconds
  - `alert.up_sound_file` / `alert.down_sound_file`: directional sounds
- `qqq-etf-holdings.csv`
  - Used to build the leader basket.
  - Top `25` valid symbols are loaded by weight.
  - Weights are preserved as ETF weights unless supplied weights are invalidly above 100% aggregate.
  - If missing/invalid, app still runs; QQQ tape degrades to QQQ-driven confirmation with low/zero basket coverage.

## Testing

```bash
make test
# or
go test ./... -v
```

## Notes

- `POLYGON_API_KEY` is required; app exits without it.
- FMP/SEC keys are optional; related UI sections degrade gracefully when missing.
