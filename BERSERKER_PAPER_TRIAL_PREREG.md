# NEXUS PRE-REGISTRATION — BERSERKER PAPER TRIAL (CENSUS COHORT 1)
**Written Aug 17 2026, before any trial trades exist. Gates bind.**

## Cohort and provenance
CRWD, ARM, MU — the three PASSes from the Aug 16 universe census
(BERSERKER_UNIVERSE_CENSUS_PREREG.md; all four bars, anchors valid).
Stated up front: ~1-2 false positives were EXPECTED at those bars across
40 symbols, and all three names are correlated (semis / enterprise
tech). This trial exists to separate signal from sector-draw with live
paper evidence. No conclusion is carried in from the census.

## Mechanism (main.py V10.66)
- PAPER_TRIAL_SYMBOLS = {CRWD, ARM, MU}; PAPER_UNIVERSE = SYMBOLS + trial.
- The paper Berserker twin trades PAPER_UNIVERSE. The LIVE entry path
  iterates SYMBOLS and structurally cannot see trial names. Verified:
  zero PAPER_UNIVERSE references on live paths.
- Trial symbols run DEFAULT tp/sl with no recipes and no avoid-hours —
  identical to their census conditions. NO TUNING during the trial;
  a tuned recipe would invalidate the census comparison.
- Fingerprints flow to berserker_trade_fingerprints as is_paper rows,
  same as all paper trades.
- NOTE (A/B control): the paper twin runs old-trail logic as the
  trailing experiment's control. Trial symbols inherit that. Acceptable:
  the promotion gate below is WR/expectancy-based and the census engine
  used plain TP/SL anyway; recorded here so nobody is surprised later.

## Promotion gate (per symbol, evaluated when n reached — no deadline)
PROMOTE to live-rotation REGISTRATION (not to live directly) if:
- n >= 25 paper trades AND paper WR >= 45% AND paper avg PnL > 0
  AND performance not concentrated in one week (>0 excluding best week).
Promotion produces a NEW pre-registration for the live add (sizing,
recipe derivation, bench rules). Nothing enters live rotation from this
document alone.

## Removal gate (per symbol)
REMOVE from PAPER_TRIAL_SYMBOLS if:
- n >= 25 AND (paper WR < 40% OR paper avg PnL <= -0.10%), OR
- n >= 15 AND paper avg PnL <= -0.25% (fast-fail for clear losers).
Removed symbols return to the general census pool; re-entry requires a
future census pass on fresh tape.

## Ambient rules
- Cohort is frozen at these three until each resolves (promote/remove).
  Future census passes queue for cohort 2; no mid-trial additions.
- Paper capacity note: trial symbols compete for the twin's 3-position
  cap with the 12 home symbols. This throttles trial n accumulation —
  expected, accepted, and why the gate has no deadline.
- Weekly read in the Sunday ledger; no mid-week reactions to trial P&L.
