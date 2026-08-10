# NEXUS PRE-REGISTRATION — CRYPTO PHASE 1 DISCOVERY BATCH
**Written Jul 29 2026 (evening), BEFORE looking at any data below.**
**Run date: ~Aug 12 2026** (chosen so 48h self-join coverage spans 40+ days).
Ratified by Matthew Jul 29 2026 ("ratify it as you see fit") with the
absolute-positive floor amendment. Verdicts bind.

Governing doc: CRYPTO_REVIVAL_PLAN.md v1.0. All tests are READ-ONLY
self-join queries against the Thorn crypto tape. No crypto.py changes.
CRYPTO_BUYS_DISABLED stays true regardless of outcome.

---

## The Frame (applied to every thesis below)

- Horizon: 48h primary, 24h reported for shape.
- Baseline filters pre-applied in every cell: funding top-decile
  EXCLUDED, worst-hour block (8PM CDT / OFFPEAK band) EXCLUDED.
- Fee bar: 1.6% effective (maker + 25% adverse-selection haircut).
  2.4% taker reported as robustness line.
- All candidate mechanisms are contrarian (maker-route requirement
  from A7's fill-adverse-selection constraint).

## KEEP gate (identical for D1–D3; D4 has its own, below)

A thesis KEEPs only if its selected cell satisfies ALL of:
1. 48h mean >= pooled 48h baseline + 0.6%
2. 48h mean > 0 ABSOLUTE (floor amendment: "loses less than the
   market" does not bank)
3. Fee clearance >= 30% in-cell (vs 1.6% bar)
4. n >= 300
5. Monotone ordering across the conditioning variable's deciles/bins
   (no isolated middle cell)
Placebo for each: shuffled conditioning assignment; effect must exceed
placebo spread. Anything short of all five: KILL (or PARK only if a
cell is empty for instrument reasons, per the B1 precedent).

## Scope honesty (in the doc, not the gate)

A passing cell is a DISCOVERY SCREEN result, not a profitable strategy:
+0.18% gross at current baseline vs 1.6% effective fees does not trade.
The gate locates raw material; Phase 2 (maker-fill shadow) and Phase 3
(isolated paper) decide whether managed exits extract more than the
window mean. No phase-skipping on an impressive P1 print.

---

## D1. Multi-day mean reversion
Mechanism: multi-day overreaction; alt pairs overshoot on 48h moves and
partially revert. Loser: late momentum chasers entering after the move.
Claim: bottom decile of trailing-48h return shows 48h forward returns
satisfying the KEEP gate; ordering monotone across deciles (worst
trailing decile -> best forward, degrading toward the top decile).

## D2. Slow RSI extremes
Mechanism: same overreaction family at oscillator scale; rsi_5m is too
fast (killed families lived there) — compute multi-hour RSI (~14x1h
equivalent) from tape prices via self-join.
Claim: oversold band (slow RSI <= 30) satisfies the KEEP gate; monotone
across RSI bands. Redundancy check reported: if D1 and D2 select >70%
overlapping observations, they count as ONE finding, not two.

## D3. Cross-pair relative strength
Mechanism: alt-vs-BTC divergence converges; pairs-style contrarian.
Loser: single-leg momentum traders ignoring the anchor.
Claim: bottom decile of (alt trailing-48h return minus BTC trailing-48h
return) satisfies the KEEP gate on the alt's forward 48h; monotone
across divergence deciles.

## D4. BTC-vol avoid-filter (parked Jul 29 observation, registered fresh)
Mechanism: BTC short-horizon volatility marks regime stress; alts
underperform during and after. This is a FILTER (subtractive), so the
D1–D3 gate does not apply.
Claim: observations with |btc_ret_5m| > 0.15% underperform the flat
band by >= 0.15% at BOTH 4h and 24h, both tails independently.
KEEP -> joins the frame's baseline filters for all future batches.
KILL if either tail fails either horizon.

---

## Multiple-comparisons discipline
Four registered theses, no additions after this commit, no post-hoc
cells. Any interesting unregistered pattern found during the run gets
PARKED by name (BTC-vol precedent) and registers in the NEXT batch or
dies.

## Program clock
This is discovery batch 1 of 2 under the kill clause: two consecutive
batches with zero KEEPs among D1–D3 (D4 is a filter, doesn't count
toward program survival) -> crypto formally SHELVED per
CRYPTO_REVIVAL_PLAN.md.
