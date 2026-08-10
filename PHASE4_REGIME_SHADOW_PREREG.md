# NEXUS PRE-REGISTRATION — PHASE4 REGIME SHADOW (B1+B3 JOINT RULE)
**Written Aug 2 2026, after B1/B3 survived independent replication
(Jul 29 discovery run + Aug 2 Sunday cron: B1 chop +0.448%/+0.371%,
trend −0.702%/−0.706%; B3 gap +14.8/+14.7pt). Shadow only — phase4.py
places ZERO new orders under this design. Current trading logic is
untouched; the shadow observes and logs.**

## B3 registration ruling (Matthew, Aug 2 2026)
Banked AS-TESTED: the gate variable is the ETF's OWN 20-bar MA on
5-min replay prices ("ETF-MA20"), not the underlying's 50d MA that the
original registration named. Mismatch flagged in ledger; the tested
variable is what two independent runs validated, so it is what ships.
The underlying-50d variant is dead unless separately re-registered.

## The collision this design resolves
B3 alone: block below-ETF-MA20 entries (+14.7pt WR gap).
B1 alone: below-MA20 (chop) positions held OVERNIGHT are the only
positive cell (+0.37..0.45%); above-MA20 (trend) overnight holds are
the most stable negative number on the board (−0.70% both runs).
Naive B3 deletes B1's entire edge. The joint rule splits by intent:

## JOINT RULE (what the shadow evaluates)
- **R1 — TREND (ETF above MA20):** entries ALLOWED (B3), overnight
  holds FORBIDDEN — force EOD close regardless of winner status.
- **R2 — CHOP (ETF below MA20):** intraday-style entries BLOCKED (B3);
  entries permitted ONLY as designated overnight carries (B1's cell).
  A designated carry is held to next open minimum, normal stops active.
- Caveats carried into evaluation: B1's chop edge is TAIL-DRIVEN
  (median ≈ 0, mean positive) — evaluation uses means with outliers
  reported, never trimmed silently. Universe is 85% TQQQ; verdicts are
  TQQQ-weighted and say so.

## Shadow instrumentation (phase4.py, V10.61 pattern)
- Every entry signal logs: ts, symbol, ETF-MA20 regime, joint-rule
  action (ALLOW / ALLOW-AS-CARRY / BLOCK), actual bot action, price.
- Every EOD with an open position logs: regime, rule branch
  (close vs carry), actual action, EOD price; next-open price appended
  the following morning for the counterfactual.
- Writes via dedicated short-lived autocommit connection to
  phase4_shadow_signals (Phase B pattern — no shared-connection wedge).
- NO order paths touched. The shadow disagrees on paper only.

## Pre-registered gates (evaluated at 30 sessions)
- **R1 KEEP** if: among trend-regime positions the live bot held (or
  would hold) overnight, EOD-close counterfactual saves >= 0.30%/event
  mean, n >= 8. (Bar sits well under the replicated −0.70% so an
  honest-but-weaker live effect still passes; a sign flip fails.)
- **R2 KEEP** if: designated-carry counterfactuals (chop entries held
  to next open) show mean > 0 AND beat same-window chop-intraday mean,
  n >= 8.
- Any cell with n < 8 at 30 sessions: extend 15 sessions once; still
  short -> INSUFFICIENT, rule does not ship, shadow continues.
- KEEP outcomes -> pre-registered live experiment (one at a time,
  behind the Berserker queue). KILL -> rule dies, B1/B3 stay banked as
  tape facts that didn't survive contact.

## Sequencing
Shadow deploys independently of the Berserker experiment queue (no
orders = no attribution contamination). Live implementation, if
earned, queues behind: opening-delay verdict -> trailing re-reg ->
C3 carry -> this.
