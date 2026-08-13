# NEXUS PRE-REGISTRATION — CRYPTO PHASE 2 SHADOW (D1 LIVE OBSERVER)
**Written Aug 13 2026, after the P1 batch (D1/D3 KEEP) and diagnostics
(56 episodes, +1.355% episode-level, 82% positive, Jul 23-29 regime
concentration), BEFORE any live shadow data exists. Verdicts bind.**
**CRYPTO_BUYS_DISABLED stays true throughout. Zero order paths.**

## What this tests
Whether D1's multi-day mean reversion exists FORWARD, outside the
July-capitulation week that dominates the discovery cell (74% of rows in
5 days), and whether A7's maker-fill assumption survives contact with
live tape. Both must pass before Phase 3 paper.

## Signal (frozen from the batch cell — no live re-derivation)
- Trailing-48h return <= -3.75% (the batch's decile-0 edge, FROZEN as an
  absolute threshold so live signals match the tested cell)
- Frame filters live: skip if pair funding >= rolling-30d p90 (computed
  from the Thorn tape at signal time); skip hour 20:00 CDT
- Episode dedup: max ONE signal per pair per 24h (mirrors the episode
  structure; prevents 400-row episodes becoming 400 signals)
- D3 covariate: rel-strength (pair t48 minus BTC t48) LOGGED on every
  signal row, not acted on — D3's independent contribution is measured
  at verdict time for free

## Maker-fill simulation (the A7 test)
- Shadow limit = signal price * (1 - spread_bps/2/10000) (post-only at
  the touch, half-spread inside)
- FILLED if the pair trades at or below the limit within 30 minutes
  (determined from the Thorn tape by the evaluation script; nothing
  updates rows live)
- Exit for evaluation: 48h forward from the FILL price

## Instrumentation (crypto.py, next minor version)
- Signal check piggybacks the existing Thorn observation cycle; writes
  one row per signal to crypto_shadow_signals (ts, pair, price, limit,
  spread_bps, funding, t48, rel_strength) via a dedicated short-lived
  autocommit connection (Phase B pattern). Fire-and-forget, wrapped;
  no trading-loop path can be delayed or broken by it.
- T-Bone alert per signal (👻 D1 SHADOW: <pair> ...) so signals are
  visible in real time. Alert failure never blocks the row.

## Pre-registered gates (evaluated at 3 weeks OR n>=25 filled, whichever later)
- **FILL VIABILITY:** fill rate >= 40% of signals. Below -> the maker
  economics don't exist; D1 returns to the queue re-costed at the 2.4%
  taker bar (which its cell cleared at only 28.8% — likely fatal, and
  that honesty is the point).
- **EDGE KEEP:** filled-shadow mean 48h forward >= +1.00% (episode-level
  +1.355% minus tolerance) AND > 0 absolute AND positive rate >= 60%
  (vs 82% observed) on n >= 25 filled.
- **REGIME CHECK:** if a capitulation cluster (>=3 signal days in one
  week) occurs, results reported with and without it. The edge must be
  > 0 excluding the single largest cluster — one crash week cannot
  carry the verdict, per the discovery cell's own concentration.
- **INSUFFICIENT** at 6 weeks with n < 25 filled -> shadow continues,
  no verdict; the market didn't offer enough signals.
- KEEP on all gates -> Phase 3 paper leg per CRYPTO_REVIVAL_PLAN.md.
  Any KILL -> D1 dies or re-registers per the batch clock. The lock
  does not move in any branch of this document.
