# QQQ Tape: How To Read It For Execution

The most important shift is this:

- `fair_gap_bps` is the valuation signal.
- `exec_edge_bps` is the execution signal.
- `score` is a normalized convenience view of execution edge quality.

Treat `exec_edge_bps` + `tradable` as primary for entries.

## What The Model Is Doing

`qqq_tape` combines:

- leader return composite (`1s`, `3s`, `8s`)
- leader signed notional flow
- QQQ confirmation flow (`qqq_flow_*`, quote imbalance, micro edge)
- spread/freshness gating

Fair-value construction now preserves ETF realism:

- tracked leaders keep raw ETF weights
- untracked ETF weight is routed to live QQQ return fallback
- this is exposed as:
  - `basket_coverage`
  - `residual_weight`

So top-`N` leader truncation no longer inflates fair value by renormalizing to 100%.

## Field Cheat Sheet

- `fair_gap_bps`: leader-vs-QQQ valuation gap
- `fair_gap_cents`: fair gap in cents from mid
- `exec_edge_bps`: fair gap plus flow/micro confirmation, with freshness penalty
- `exec_edge_cents`: execution edge in cents
- `spread_bps`: current QQQ spread in bps
- `freshness_ms`: age of latest tape inputs
- `tradable`: strict gate for execution-quality setups
- `score` / `bias`: quick normalized readout; useful, but secondary to edge/tradable

Compatibility note:

- `edge_bps`/`edge_cents` mirror execution edge (`exec_edge_*`).
- `leader_flow_250/1000/3000` and `qqq_flow_250/1000` are preserved for UI compatibility, but now map to short/medium/slow human windows (`1s/3s/8s` weighting), not literal millisecond buckets.

## Tradable Gate (Critical)

`tradable=true` requires all of:

- `abs(exec_edge_bps) >= spread_bps*1.60 + 0.45`
- `abs(fair_gap_bps) >= 0.45`
- fair-gap persistence across short/medium/slow windows
- fair gap and leader flow alignment
- QQQ confirmation not strongly fighting the move
- `freshness_ms <= 2500`
- valid QQQ quote/mid

If this fails, treat it as information, not an execution green light.

## Practical Scalper Workflow

1. Set directional lean from `exec_edge_bps` sign.
2. Check valuation context with `fair_gap_bps`.
3. Require `tradable=true` for normal size.
4. Confirm with `trade_impulse`, `qqq_flow_250/1000`, and micro/quote fields.
5. Exit or downsize when `exec_edge_bps` decays, flips, or freshness degrades.

## Long/Short Checklist

Long-lean conditions:

- `exec_edge_bps > 0`
- `fair_gap_bps > 0`
- `trade_impulse > 0`
- `qqq_flow_250` and `qqq_flow_1000` not fighting
- `tradable=true` for full-size entries

Short-lean conditions (mirror image):

- `exec_edge_bps < 0`
- `fair_gap_bps < 0`
- `trade_impulse < 0`
- QQQ flow confirms downside
- `tradable=true` for full-size entries

## Sizing Heuristic

- Full size: `tradable=true` and strong `|exec_edge_bps|`
- Reduced size/watch: directional edge present but `tradable=false`
- Avoid: near-zero edge, stale tape, or conflicting leader/QQQ flow

## Quick UI Interpretation

- `Tradable`: gate passed, setup quality acceptable
- `Watch`: edge exists but gate failed
- `No Trade`: mixed/weak edge

Use the state badge for speed, but validate with `exec_edge_bps`, spread, and freshness before pressing size.
