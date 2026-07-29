#!/usr/bin/env python3
"""
phase4_temporal_thesis.py — READ-ONLY. Jul 29 batch, section B (B1–B3),
per NEXUS_Jul29_Preregistration.md.

B1  Temporal leveraged-ETF anomaly: overnight vs intraday P&L, conditioned on
    regime (above_ma20 at entry — the field is stored, so the pre-registered
    definition runs verbatim). KILL if no regime-conditional diff >= 0.3%/trade.
B2  Close-rebalance tailwind: 13:00–14:55 entries on strong-trend days vs flat
    days (proxy for underlying day-move: spy_momentum sign+size at entry).
    Placebo: morning entries must NOT show the same conditioning.
B3  Trend-gate: reversal entries above vs below MA20 — WR gap >= 8pts.

Universe note (pre-declared): fingerprints are ~86% TQQQ; per-bot cuts on
SOXL/LABU will read INSUFFICIENT by construction. Verdicts are effectively
TQQQ verdicts and are labeled as such.

USAGE:  python3 phase4_temporal_thesis.py
"""
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    import psycopg2
except ImportError:
    print("psycopg2 missing — run on a NEXUS console.")
    sys.exit(1)

CENTRAL = ZoneInfo("America/Chicago")


def stats(v):
    if not v:
        return None
    v = sorted(v)
    n = len(v)
    return {"n": n, "mean": sum(v) / n, "med": v[n // 2],
            "wr": sum(1 for x in v if x > 0) * 100.0 / n}


def line(tag, s):
    if not s:
        return f"  {tag:34s} n=0"
    return (f"  {tag:34s} n={s['n']:5d}  WR={s['wr']:5.1f}%  "
            f"mean={s['mean']:+.3f}%  med={s['med']:+.3f}%")


def main():
    db = os.environ.get("DATABASE_URL", "")
    if not db:
        print("DATABASE_URL not set."); sys.exit(1)
    conn = psycopg2.connect(db, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, is_bear_trade, entry_ts, exit_ts, pnl_pct, won,
                   above_ma20, hour_cdt, day_of_week, spy_momentum,
                   reversal_quality, exit_reason
            FROM phase4_trade_fingerprints
            WHERE pnl_pct IS NOT NULL AND won IS NOT NULL
              AND entry_ts IS NOT NULL AND exit_ts IS NOT NULL
            ORDER BY entry_ts
        """)
        rows = cur.fetchall()
    conn.close()
    print(f"loaded {len(rows)} phase4 fingerprints")

    T = []
    for sym, bear, ent, ext, pnl, won, ma20, hour, dow, spym, revq, reason in rows:
        d_in  = datetime.fromtimestamp(ent, CENTRAL).date()
        d_out = datetime.fromtimestamp(ext, CENTRAL).date()
        T.append({"sym": sym, "bear": bear, "pnl": pnl, "won": won,
                  "overnight": d_out > d_in, "ma20": ma20, "hour": hour,
                  "spym": spym, "revq": revq})
    tq = sum(1 for t in T if t["sym"] == "TQQQ")
    print(f"universe mix: TQQQ {tq}/{len(T)} ({tq * 100 // max(len(T),1)}%) — verdicts are TQQQ-weighted\n")

    # ---------- B1 ----------
    print("=" * 74); print("B1. OVERNIGHT vs INTRADAY, BY REGIME (above_ma20 at entry)")
    cells = {}
    for regime, rlbl in ((True, "TREND (above MA20)"), (False, "CHOP (below MA20)")):
        for ov, olbl in ((True, "overnight"), (False, "intraday")):
            s = stats([t["pnl"] for t in T if t["ma20"] is regime and t["overnight"] is ov])
            cells[(regime, ov)] = s
            print(line(f"{rlbl:20s} {olbl}", s))
    try:
        chop_d  = cells[(False, True)]["mean"] - cells[(False, False)]["mean"]
        trend_d = cells[(True, True)]["mean"] - cells[(True, False)]["mean"]
        print(f"  chop:  overnight - intraday = {chop_d:+.3f}%   |   trend: {trend_d:+.3f}%")
        claim = chop_d >= 0.3 and trend_d <= chop_d - 0.3
        anyd  = abs(chop_d) >= 0.3 or abs(trend_d) >= 0.3
        print(f"  VERDICT B1: "
              f"{'KEEP — pre-registered pattern (overnight pays in chop, reverses in trend)' if claim else 'PARK — a regime-conditional difference exists but not the registered shape' if anyd else 'KILL — no regime-conditional difference >= 0.3%'}")
    except (TypeError, KeyError):
        print("  VERDICT B1: INSUFFICIENT — a cell is empty")

    # ---------- B2 ----------
    print("=" * 74); print("B2. CLOSE-REBALANCE TAILWIND (late entries on strong vs flat days)")
    spyvals = [t["spym"] for t in T if t["spym"] is not None]
    if spyvals:
        hi = sorted(spyvals)[int(len(spyvals) * 0.8)]     # top-quintile momentum = 'strong day'
        late_strong = stats([t["pnl"] for t in T if t["hour"] in (13, 14) and t["spym"] is not None and t["spym"] >= hi])
        late_flat   = stats([t["pnl"] for t in T if t["hour"] in (13, 14) and t["spym"] is not None and abs(t["spym"]) < hi / 2])
        am_strong   = stats([t["pnl"] for t in T if t["hour"] in (8, 9)  and t["spym"] is not None and t["spym"] >= hi])
        am_flat     = stats([t["pnl"] for t in T if t["hour"] in (8, 9)  and t["spym"] is not None and abs(t["spym"]) < hi / 2])
        print(line("late (13-14) strong-momentum", late_strong)); print(line("late (13-14) flat", late_flat))
        print(line("morning (8-9) strong (placebo)", am_strong)); print(line("morning (8-9) flat (placebo)", am_flat))
        try:
            eff = late_strong["mean"] - late_flat["mean"]
            pla = am_strong["mean"] - am_flat["mean"]
            ok = eff >= 0.4 and pla < eff - 0.2
            print(f"  late-day conditioning {eff:+.3f}% | placebo {pla:+.3f}%")
            print(f"  VERDICT B2: {'KEEP — tailwind present, placebo clean' if ok else 'KILL — conditioning absent or placebo-contaminated'}")
        except (TypeError, KeyError):
            print("  VERDICT B2: INSUFFICIENT — a cell is empty")
    else:
        print("  VERDICT B2: INSUFFICIENT — spy_momentum not populated")

    # ---------- B3 ----------
    print("=" * 74); print("B3. TREND-GATE FOR REVERSAL ENTRIES (WR above vs below MA20)")
    above = stats([t["pnl"] for t in T if t["ma20"] is True])
    below = stats([t["pnl"] for t in T if t["ma20"] is False])
    print(line("entries above MA20", above)); print(line("entries below MA20", below))
    if above and below:
        gap = above["wr"] - below["wr"]
        print(f"  WR gap: {gap:+.1f} pts (bar: +8)")
        print(f"  VERDICT B3: {'KEEP — trend gate earns its keep' if gap >= 8 else 'KILL — gate not supported'}")
    else:
        print("  VERDICT B3: INSUFFICIENT")

    print("=" * 74)
    print("Any KEEP -> shadow-mode design precedes implementation (no backtest->live leaps).")


if __name__ == "__main__":
    main()
