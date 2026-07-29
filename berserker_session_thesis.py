#!/usr/bin/env python3
"""
berserker_session_thesis.py — READ-ONLY. Jul 29 batch, section C, per
NEXUS_Jul29_Preregistration.md.

C1  Opening-window entries: 8:30–9:00 CT WR vs later. Placebo: 9:00–9:30 vs
    9:30–10:00 must NOT show a comparable gap. Bar: >= 6pts WR deficit.
C3  Day-of-week overnight carry, crypto-correlated (MSTR/MARA/CLSK) vs rest.
    Bar: Thu->Fri beats Fri->Mon by >= 0.6% on crypto-correlated only.
C5  Overnight holds: open-flat proxy — of overnight holds, compare the trades
    where the OPEN would have banked profit vs how TP/SL actually resolved.
    Pure-DB version: exits within 15 min of open (gap resolution) vs exits
    later in the day (drift resolution) — do late resolutions underperform?
C2  First-half-hour market signal — requires minute bars (not in DB). The
    script SKIPS it with a note unless run where alpaca-py + keys exist.

Runs on BACKTEST fingerprints (bt_ prefix, n~17k) as the discovery set and
prints the LIVE cohort (n~100) beside it as the confirmation cut.

USAGE:  python3 berserker_session_thesis.py
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
CRYPTO_CORR = {"MSTR", "MARA", "CLSK"}


def stats(v):
    if not v:
        return None
    v = sorted(v)
    n = len(v)
    return {"n": n, "mean": sum(v) / n, "med": v[n // 2],
            "wr": sum(1 for x in v if x > 0) * 100.0 / n}


def line(tag, s):
    if not s:
        return f"  {tag:36s} n=0"
    return (f"  {tag:36s} n={s['n']:6d}  WR={s['wr']:5.1f}%  "
            f"mean={s['mean']:+.3f}%  med={s['med']:+.3f}%")


def load(conn, live):
    cohort = ("is_paper IS NOT TRUE AND trade_id NOT LIKE 'bt_%%' "
              "AND exit_reason NOT IN ('trail','timeout') AND entry_ts >= 1783314000"
              ) if live else "trade_id LIKE 'bt_%%'"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT symbol, entry_ts, exit_ts, pnl_pct, won, exit_reason
            FROM berserker_trade_fingerprints
            WHERE pnl_pct IS NOT NULL AND won IS NOT NULL
              AND entry_ts IS NOT NULL AND exit_ts IS NOT NULL AND {cohort}
        """)
        out = []
        for sym, ent, ext, pnl, won, reason in cur.fetchall():
            din  = datetime.fromtimestamp(ent, CENTRAL)
            dout = datetime.fromtimestamp(ext, CENTRAL)
            out.append({"sym": sym, "pnl": pnl, "won": won, "reason": reason or "",
                        "e_hm": din.hour * 60 + din.minute,
                        "x_hm": dout.hour * 60 + dout.minute,
                        "e_dow": din.weekday(), "overnight": dout.date() > din.date()})
    return out


def run_c1(T, label):
    print(f"--- C1 [{label}] opening-window entries ---")
    open_w  = stats([t["pnl"] for t in T if 510 <= t["e_hm"] < 540])     # 8:30-9:00
    later   = stats([t["pnl"] for t in T if 540 <= t["e_hm"] < 895])     # 9:00-14:55
    pl_a    = stats([t["pnl"] for t in T if 540 <= t["e_hm"] < 570])     # 9:00-9:30
    pl_b    = stats([t["pnl"] for t in T if 570 <= t["e_hm"] < 600])     # 9:30-10:00
    print(line("entries 8:30-9:00", open_w)); print(line("entries 9:00-14:55", later))
    print(line("placebo 9:00-9:30", pl_a)); print(line("placebo 9:30-10:00", pl_b))
    if open_w and later and pl_a and pl_b:
        gap = later["wr"] - open_w["wr"]
        pgap = abs(pl_a["wr"] - pl_b["wr"])
        print(f"  opening WR deficit: {gap:+.1f} pts (bar >= 6) | placebo gap {pgap:.1f}")
        return gap >= 6 and pgap < gap / 2
    return None


def run_c3(T, label):
    print(f"--- C3 [{label}] day-of-week overnight carry ---")
    def cut(syms, dow):
        return stats([t["pnl"] for t in T
                      if t["overnight"] and t["sym"] in syms and t["e_dow"] == dow]) \
            if syms else None
    non = set(t["sym"] for t in T) - CRYPTO_CORR
    cc_thu, cc_fri = cut(CRYPTO_CORR, 3), cut(CRYPTO_CORR, 4)
    nc_thu, nc_fri = cut(non, 3), cut(non, 4)
    print(line("crypto-corr Thu->Fri carry", cc_thu)); print(line("crypto-corr Fri->Mon carry", cc_fri))
    print(line("others      Thu->Fri carry", nc_thu)); print(line("others      Fri->Mon carry", nc_fri))
    if cc_thu and cc_fri and cc_thu["n"] >= 15 and cc_fri["n"] >= 15:
        d = cc_thu["mean"] - cc_fri["mean"]
        clean = not (nc_thu and nc_fri and (nc_thu["mean"] - nc_fri["mean"]) >= 0.6)
        print(f"  crypto-corr Thu-vs-Fri spread {d:+.3f}% (bar >= 0.6) | control clean: {clean}")
        return d >= 0.6 and clean
    print("  n < 15 in a crypto-corr cell")
    return None


def run_c5(T, label):
    print(f"--- C5 [{label}] overnight resolution timing ---")
    ov = [t for t in T if t["overnight"]]
    at_open = stats([t["pnl"] for t in ov if t["x_hm"] <= 555])          # exit by 9:15
    drift   = stats([t["pnl"] for t in ov if t["x_hm"] > 555])
    print(line("resolved at/near open (<=9:15)", at_open))
    print(line("resolved later in day", drift))
    if at_open and drift and min(at_open["n"], drift["n"]) >= 15:
        d = at_open["mean"] - drift["mean"]
        print(f"  open-resolution advantage {d:+.3f}%")
        print("  reading: a strongly positive gap says the OPEN carries the payoff "
              "and letting carries drift intraday costs money — the sell-at-open "
              "instinct earns a pre-registered live experiment. A flat/negative "
              "gap says patience past the open is fine.")
        return d
    print("  n < 15 in a cell")
    return None


def main():
    db = os.environ.get("DATABASE_URL", "")
    if not db:
        print("DATABASE_URL not set."); sys.exit(1)
    conn = psycopg2.connect(db, connect_timeout=10)
    BT, LIVE = load(conn, live=False), load(conn, live=True)
    conn.close()
    print(f"backtest cohort n={len(BT)} | live cohort n={len(LIVE)}")
    print("=" * 74)
    c1 = run_c1(BT, "backtest"); run_c1(LIVE, "live confirm")
    print("=" * 74)
    c3 = run_c3(BT, "backtest"); run_c3(LIVE, "live confirm")
    print("=" * 74)
    c5 = run_c5(BT, "backtest"); run_c5(LIVE, "live confirm")
    print("=" * 74)
    print("C2 (first-half-hour market signal): SKIPPED — needs minute bars; "
          "queued for the backtester container where alpaca-py + keys exist.")
    print("=" * 74)
    v1 = "KEEP" if c1 else ("KILL/placebo-fail" if c1 is not None else "INSUFFICIENT")
    v3 = "KEEP" if c3 else ("KILL" if c3 is not None else "INSUFFICIENT")
    v5 = f"gap {c5:+.3f}% -> " + ("REGISTER live experiment" if c5 is not None and c5 >= 0.5
         else "KEEP current rule") if c5 is not None else "INSUFFICIENT"
    print(f"VERDICTS: C1 opening-window: {v1} | C3 weekday carry: {v3} | C5 open-resolution: {v5}")
    print("Any KEEP -> pre-registered live experiment with working control; no silent tunes.")


if __name__ == "__main__":
    main()
