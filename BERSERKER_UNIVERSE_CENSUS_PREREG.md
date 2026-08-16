# NEXUS PRE-REGISTRATION — BERSERKER UNIVERSE CENSUS
**Written Aug 15 2026, before the census runs. Selection bars bind.**

## Question
Does Berserker's entry logic (V10.19 signal engine, exactly as replayed by
backtester V3.3) find edge outside its 12-symbol home universe? This is a
DISCOVERY SCREEN across 40 pre-listed candidates — a funnel into paper
rotation, never a direct live add.

## Universe (frozen — 40 symbols, listed in berserker_universe_census.py)
Miners/crypto-adjacent (7), semis (6), high-beta tech (8), EV/energy/
nuclear (8), space (3), uranium (2), quantum/spec (4), misc (2).
No additions after this commit. Symbols with no IEX bars are SKIPPED and
reported, not replaced.

## Method constraints
- Engine: deployed V3.3, driven directly; fingerprint writes NEVER called
  (the live C-thesis census stays uncontaminated).
- Candidates run the DEFAULT recipe (tp 1.5%/sl 1.0%, no avoid-hours):
  the census measures raw entry-logic fit and deliberately understates
  tuned potential. No per-symbol tuning before or during the run.
- Every batch includes the 7 TRUMP_THEME anchors so the sector-health
  regime gate behaves exactly as production. ANCHOR VALIDITY: if any
  anchor's full-train WR drifts more than 3pts from the most recent
  Sunday suite, that batch is VOID (instrument problem) and reruns.

## Selection bars (a candidate PASSES only if ALL FOUR hold)
1. OOS win rate >= 42%
2. OOS avg PnL per trade > 0
3. OOS n >= 100 trades
4. |train WR − OOS WR| <= 6 pts (walk-forward consistency)

## Multiple-comparisons honesty
At these bars across 40 symbols, ~1-2 false positives are EXPECTED by
chance. Therefore: passing is necessary but not sufficient. Every PASS
goes to the paper Berserker rotation (its own capacity, own fingerprints,
is_paper=TRUE) and must independently clear the existing win-follower
promotion machinery on live-paper data before any live-rotation
discussion — which would then get its own pre-registration.

## What this census cannot say
Nothing here touches live rotation, sizing, budgets, or the experiment
queue. A great census number is a ticket to the paper lane, full stop.
