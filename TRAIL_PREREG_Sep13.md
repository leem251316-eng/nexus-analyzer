# PRE-REGISTRATION — Berserker trail geometry (DRAFT, Sep 13 2026)

Status: DRAFT. Becomes BINDING when committed to nexus-analyzer with the
primary config confirmed by Matthew. No bar has been fetched under this
registration. Deploys nothing. Runs after Gate B (Sep 21) — one WR-touching
experiment at a time.

## 1. Mechanism (sourced to main.py V10.67)

L885-889: TRAILING_STOP 0.015, RATCHET_PROFIT 0.015, RATCHET_TRAIL_TIGHT
0.005, TAKE_PROFIT_PCT 0.015, STOP_LOSS_PCT 0.01 (SPCX sl 0.015).
L6552: `trailing = TIGHT if profit_pct >= RATCHET_PROFIT else TRAILING_STOP`
L6574: TP check runs first. L6611: SL. L6652: trail on (peak-price)/peak.

Two defects, not one:

(a) RATCHET_PROFIT == TP. Known (handoff #1).

(b) NEW — the ratchet arms on CURRENT profit, not PEAK. For the tight
trail to fire at all, the same tick must satisfy price <= peak*(1-T) AND
price >= entry*(1+A). That forces peak >= entry*(1+A)/(1-T), i.e. roughly
peak >= A + T. With T = 0.5%, any A >= 1.0% needs peak >= 1.5% = TP, and TP
fires first. So lowering RATCHET_PROFIT to 1.0% — the shape in the Sep 13
handoff — changes NOTHING. Under current semantics the tight trail is
reachable only with A <= ~0.9%, and even then only in the narrow band
peak in [A+T, TP).

Confidence: high — it is algebra on three source lines. Consequence: the
candidate must specify ARMING SEMANTICS, not just a constant. V10.53 armed
on peak; it was reverted Jul 23 on a WR gate (33.3% vs 35%, n=27) with a
dead A/B control. WR was the wrong metric then; this registration uses
expectancy and a live control.

Named loser: trailing-stop exits. 0-for-16 over two weeks; Aug 31–Sep 4
(IEX): 9 trails, MFE +0.64..+1.38, giveback 1.27–1.74, sum MFE +9.76% vs
realized −4.30%.

## 2. Claim (falsifiable)

Arming the tight trail on PEAK profit at threshold A < TP converts a
fraction of red trail exits into small green exits, and the resulting
change in per-trade expectancy across ALL exits (not just trails), net of
0.05%/exit fee+slip, is >= +0.10%/trade on the train window and > 0 on OOS.

Cost side, declared up front: some trades that would have reached TP
(+1.5%) will instead trail out at ~A−T (e.g. +0.5%). The claim is about the
NET across the whole exit distribution; the sim scores every trade.

## 3. Candidate and grid

PRIMARY (one config, named now):
  arm on PEAK; A = 0.010 (RATCHET_PROFIT); T = 0.005 (RATCHET_TRAIL_TIGHT).
  Everything else unchanged: base trail 1.5%, TP 1.5%, SL 1.0%,
  MIN_HOLD_MINUTES 20 trail suppression, RTH-only management.

Declared grid (2x2, ridge coherence required — an isolated winning cell is
luck): A in {0.008, 0.010} x T in {0.004, 0.005}, all arm-on-peak.

Null/sanity: A = 0.015 arm-on-peak must reproduce the control on every
trade (the ratchet is unreachable either way). If it does not, the sim is
defective and VOID.

Out of scope (would be a second experiment): removing the base trail below
the ratchet (V10.53's "SL is the only downside exit below ratchet").

## 4. Data and windows

Source of trades: berserker_trade_fingerprints, is_paper=FALSE, no bt_,
won IS NOT NULL. Fields used: symbol, entry_ts, entry_price, pnl_pct,
exit_reason. Bars: Alpaca IEX 1-min, adjustment=all, from entry_ts to the
realized exit or EOD 14:58 CT, whichever the candidate reaches first.

  TRAIN   = live trades entered Aug 17–28 (unseen at minute level)
  EXCLUDED= Aug 31–Sep 4 (inspected post-hoc in fill_reconcile V1.0) —
            reported for completeness, never in the verdict
  OOS     = live trades entered Sep 8–18

Floors: n >= 30 armed trades pooled (peak >= A at some point), OOS >= 12
armed. Below either -> EXTEND, no verdict.

## 5. Replica fidelity precondition (backtester-defect rule)

Before any candidate number is read, the CONTROL replay (current code
semantics on the same bars) must reproduce the realized tape:
  - exit_reason match on >= 80% of trades, AND
  - realized pnl within +-0.25 pct-pts on >= 80% of trades.
Fail either -> sim VOID; investigate before reading anything else.

Known non-replicable class, declared: DynTP trades. main.py V10.67 does NOT
persist the per-trade dynamic_tp on the fingerprint row (set in memory at
execute_trade, gone at exit). The control replay assumes TP 1.5% for every
trade; a DynTP trade will show as a fidelity mismatch (e.g. realized TP
+2.14 vs replica +1.50). Rule: trades with realized exit 'take-profit' and
pnl >= 1.85% are tagged DYNTP and excluded from BOTH arms; count reported.
If DYNTP exclusions exceed 15% of the window, the sim is VOID pending the
V10.68 ledger item "persist dynamic_tp on the fingerprint row".

Within-bar ambiguity (declared, pessimistic-against-candidate): lows
resolve before highs. If a bar's low reaches a stop/trail level and its
high reaches TP, the stop/trail wins. The candidate has more trail levels
exposed to lows than the control, so this ordering hurts the candidate
more — anti-validation.

Ticks vs bars: live evaluates every ~30s on last price; the sim evaluates
on 1-min bar extremes. Peak is tracked on bar highs. This overstates peaks
slightly for both arms equally; it is not a candidate-favoring bias
because the same peak feeds both.

## 6. Verdict bars (bind)

  KILL   : train delta (candidate − control expectancy, net) < +0.10%/trade
  KEEP   : train delta >= +0.10% AND OOS delta > 0 AND at least 3 of the 4
           grid cells positive on train (ridge) -> proceed to SHADOW
  PARK   : floors unmet after OOS window closes
Per-symbol breakdown printed; a verdict carried by one symbol is PARK, not
KEEP.

## 7. Path to dollars

KEEP -> SHADOW logger (V10.68 batch): for every live trade, log what the
primary config WOULD have done (armed? exit reason, exit price, pnl) into
berserker_trail_shadow with verified writes + heartbeat + one alert/day on
failure. n = distinct trades. 15 sessions. Shadow verdict: shadow delta
sign agrees with sim delta sign, else REVERT to research.
Only then a live constant+semantics change, with a 15-trade kill switch
(rolling expectancy of armed trades <= control's -> revert).

## 8. What this touches

Nothing until Sep 21. Then: one research script (trail_prereg_sim.py,
nexus-analyzer), then a shadow observer in main.py. The live exit path
changes only after the shadow verdict.
