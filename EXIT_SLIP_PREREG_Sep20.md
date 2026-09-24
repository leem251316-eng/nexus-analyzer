# PRE-REGISTRATION — Exit execution slip (Sep 20 2026)

Status: binds on commit to nexus-analyzer. Deploys nothing.

## Mechanism
Berserker exits are evaluated on a ~30s sweep and sent as market orders after
the price is already through the level. On thin-IEX names (NUE, GEO, MARA,
SMCI) the next print can be well past it. The trail pre-reg sim (Sep 13)
surfaced it as fidelity mismatches: modeled trail fills at the level, real
fills 0.4–0.6 worse; stops 0.1–1.0 through.

## Discovery set (SEEN — never in the verdict)
fill_reconcile V1.1/V1.2 output, Sep 8–11 and Sep 14–18:
  Sep 14–18: 17 stops overshoot 2.39% (0.14/stop); 7 trails 0.66% (0.10/trail)
             = 0.073%/trade over 42 trades.
  Sep 8–11:  stops 0.09/stop, trails ~0.3/trail (4) ≈ 0.05%/trade (rougher).

## Claim
Over a fresh two-week window, SLIP (modeled-level pnl minus realized pnl,
summed over stop/trail/tp exits, divided by all trades) is >= +0.05%/trade.

## Metric and windows
Metric: the SLIP line printed by fill_reconcile.py V1.3 (definition in its
header). n = trades. Validation = trades closed Sep 21 – Oct 2, read from the
two Monday reconciles. No other window counts.

## Bars (bind)
KEEP  : validation SLIP >= +0.05%/trade  -> the mechanism is worth
        engineering; a SEPARATE registration covers the fix (server-side
        stop orders at Alpaca: what it removes is the sweep latency, not the
        thin-market component; that split gets measured before any live
        change). Nothing about exits changes on this KEEP alone.
KILL  : validation SLIP <  +0.05%/trade  -> 30s sampling is noise at this
        float; close the item.
PARK  : fewer than 30 trades in the window.

## What this touches
One research script (fill_reconcile V1.3, already the Monday routine).
Nothing in main.py. Runs alongside Gate B without conflict: it observes exits
it does not change.
