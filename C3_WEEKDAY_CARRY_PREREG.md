# NEXUS PRE-REGISTRATION — C3 WEEKDAY-CARRY SHADOW (BERSERKER)
**Written Aug 10 2026, before any new data. Binds at evaluation.**

## Evidence base (three independent backtest windows)
Crypto-correlated subset = {MSTR, MARA, CLSK} (as defined in
berserker_session_thesis.py, the instrument that produced the evidence).

| read | Thu->Fri mean/WR (n) | Fri->Mon mean | spread | control |
|------|----------------------|---------------|--------|---------|
| Jul 29 | +0.653% / 55.2% (194) | -0.288% | +0.940% | reversed, clean |
| Aug 2  | +0.665% / 55.2% (194) | +0.040% | +0.625% | reversed, clean |
| Aug 9  | +0.700% / 55.7% (194) | +0.095% | +0.604% | reversed, clean |

Honest read: the HARVEST leg (Thu->Fri carry) is the stable, strengthening
cell. The AVOID leg (Fri->Mon) has decayed toward flat across reads. The
live-confirm cells are EMPTY (n<=1 after 5+ weeks) because Berserker's
current EOD rule (carry winners only) almost never carries these names on
a Thursday. Therefore:

## What this registers: a SHADOW, not a live rule
Per the pipeline (discovery on tape -> shadow -> dollars last), and
because the live cells are empty, the next step is a counterfactual
observer, NOT a behavior change. Zero order paths change. This can
deploy independently of the experiment queue (order-inert, same class
as Phase4 V2.18); the LIVE rule, if earned, queues normally behind the
trailing re-registration.

## Instrumentation (main.py, next minor version)
At EOD auto-close on THURSDAYS and FRIDAYS (control day), for every open
Berserker position in {MSTR, MARA, CLSK} plus every open non-subset
position (control rows):
- log symbol, weekday, P&L at 2:58, the current rule's action
  (CARRIED as winner / CLOSED as non-winner), and the 2:58 price
  to a berserker_c3_shadow table via the Phase B dedicated-connection
  pattern. Fire-and-forget, wrapped, zero order paths.
- Counterfactuals (next-open return of CLOSED positions had they been
  carried; next-open return of CARRIED ones is already realized) are
  computed at verdict time from bars. Nothing updates rows later.

## Pre-registered gates (evaluated at 8 Thursdays logged, ~2 months)
- **KEEP (advance to live-rule registration)** if pooled Thursday
  counterfactual+realized carries on the subset show mean >= +0.40%
  (backtest 0.65-0.70 minus tolerance) AND positive rate >= 50% AND
  n >= 12, AND the Friday control pool shows less than half the
  Thursday effect (day-specificity preserved).
- **KILL** if Thursday pool mean <= 0 at n >= 12, or if Friday control
  matches/exceeds Thursday (effect is not day-specific).
- **INSUFFICIENT** at 8 Thursdays with n < 12 -> extend 4 weeks once;
  still short -> shadow continues, no verdict, no live rule.
- Gap-through-earnings observations are excluded only if the symbol was
  earnings-blocked at carry time (post-V10.62 this should be
  structurally impossible; the exclusion documents the edge).

## The live rule this would earn (registered now so it can't drift)
"On Thursdays, the EOD auto-close carries open {MSTR, MARA, CLSK}
positions with P&L > -0.30% (near-flat and winners), instead of
winners-only. All other days unchanged. Fri->Mon behavior UNCHANGED
(the avoid leg's evidence decayed; it is not part of the rule)."
That rule gets its own pre-registration, gates, and revert clause at
deployment time, in its queue slot: opening-delay verdict -> trailing
re-registration -> this.

## Sunday replication note
The backtest cells re-print every Sunday for free. If the Thu->Fri leg
drops below +0.40% on two consecutive Sunday reads while the shadow
runs, the shadow continues but the live-rule registration is frozen
until the leg recovers -- tape decay outranks an old verdict.
