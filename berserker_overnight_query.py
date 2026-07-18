#!/usr/bin/env python3
"""
berserker_overnight_query.py — READ-ONLY. Do Berserker overnight holds pay?

QUESTION UNDER TEST
-------------------
The EOD auto-close leaves any position with pnl > 0 (and rising last-6-bars)
running overnight. Jul 16: NVDA carried at +0.14%, gapped, stopped −2.16% —
the week's worst loss, which then tripped the circuit breaker as "3
consecutive stops" on a fresh morning. Is the keep-winners-running rule
earning its gap risk, or should overnight carry be /stayopen-only?

METHOD
------
Split live Berserker fingerprints (is_paper = FALSE, trade_id NOT LIKE
'bt_%') into INTRADAY (entry and exit on the same CDT date) and OVERNIGHT
(exit date > entry date). Compare n, WR, mean/median pnl, worst-decile,
stop-loss rate. List every overnight trade individually — n will be small,
so the individual rows ARE the evidence, not just the aggregates.

Pre-registered decision rule:
  - n(overnight) < 5  -> INSUFFICIENT: keep the rule, keep counting, but
    the interim option (stayopen-only carry) is a judgment call, not data.
  - n >= 5 AND overnight mean pnl < intraday mean AND overnight p10 worse
    -> RECOMMEND stayopen-only (auto-carry off; explicit /stayopen still
    works). This removes a rule, not tunes a number — no threshold to pick.
  - n >= 5 AND overnight holds outperform -> keep the rule; the NVDA gap
    was tuition, not a pattern.

READ-ONLY: SELECTs only. Safe on the live DB.

USAGE
-----
  python3 berserker_overnight_query.py
  python3 berserker_overnight_query.py --days 30
"""
import os
import sys
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. On the Railway console it's already present; "
          "locally: pip install psycopg2-binary")
    sys.exit(1)

CENTRAL = ZoneInfo("America/Chicago")


def stats(vals):
    if not vals:
        return None
    v = sorted(vals)
    n = len(v)
    return dict(
        n=n,
        mean=sum(v) / n,
        median=v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2,
        wr=sum(1 for x in v if x > 0) / n * 100,
        p10=v[max(0, int(n * 0.10) - 1)],
        worst=v[0],
    )


def line(label, s):
    if s is None:
        return f"  {label:10s} n=0"
    return (f"  {label:10s} n={s['n']:4d}  WR={s['wr']:3.0f}%  "
            f"mean={s['mean'] * 100:+.2f}%  median={s['median'] * 100:+.2f}%  "
            f"p10={s['p10'] * 100:+.2f}%  worst={s['worst'] * 100:+.2f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=None)
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set. Run from a NEXUS service console.")
        sys.exit(1)

    where = ["is_paper = FALSE", "trade_id NOT LIKE 'bt_%%'",
             "exit_ts IS NOT NULL", "entry_ts IS NOT NULL",
             "pnl_pct IS NOT NULL"]
    params = []
    if args.days:
        import time as _t
        where.append("entry_ts >= %s")
        params.append(int(_t.time()) - args.days * 86400)

    conn = psycopg2.connect(db_url, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT symbol, entry_ts, exit_ts, pnl_pct, exit_reason, won
            FROM berserker_trade_fingerprints
            WHERE {' AND '.join(where)}
            ORDER BY entry_ts
        """, params)
        rows = cur.fetchall()
    conn.close()

    intraday, overnight = [], []
    for sym, ent, ext, pnl, reason, won in rows:
        d_in  = datetime.fromtimestamp(ent, tz=CENTRAL).date()
        d_out = datetime.fromtimestamp(ext, tz=CENTRAL).date()
        (overnight if d_out > d_in else intraday).append(
            (sym, ent, ext, pnl, reason))

    print("=" * 74)
    print("BERSERKER OVERNIGHT-HOLD QUERY")
    print(f"live fingerprints: {len(rows)}  |  intraday: {len(intraday)}  |  "
          f"overnight: {len(overnight)}")
    print("=" * 74)

    si = stats([r[3] for r in intraday])
    so = stats([r[3] for r in overnight])
    print("\nCOHORTS (pnl_pct):")
    print(line("INTRADAY", si))
    print(line("OVERNIGHT", so))

    if overnight:
        stop_rate = sum(1 for r in overnight if "stop" in (r[4] or "").lower()) \
                    / len(overnight) * 100
        print(f"\n  overnight stop-loss exit rate: {stop_rate:.0f}%")
        print("\nEVERY OVERNIGHT TRADE (small n — the rows are the evidence):")
        for sym, ent, ext, pnl, reason in overnight:
            e = datetime.fromtimestamp(ent, tz=CENTRAL).strftime("%m-%d %H:%M")
            x = datetime.fromtimestamp(ext, tz=CENTRAL).strftime("%m-%d %H:%M")
            flag = "+" if pnl > 0 else "-"
            print(f"  {flag} {sym:6s} {e} -> {x}  {pnl * 100:+.2f}%  [{reason}]")

    print("\nVERDICT:")
    if so is None or so["n"] < 5:
        n = 0 if so is None else so["n"]
        print(f"  INSUFFICIENT — only {n} overnight holds on record (< 5). "
              f"The aggregates can't decide this yet. Interim carry policy is "
              f"a judgment call; re-run as holds accrue.")
    elif so["mean"] < (si["mean"] if si else 0) and so["p10"] < (si["p10"] if si else 0):
        print("  RECOMMEND stayopen-only — overnight holds underperform intraday "
              "on both mean and tail. Auto-carry removes the operator from a "
              "gap-risk decision the data says isn't paying. Explicit /stayopen "
              "remains available per position.")
    else:
        print("  KEEP the rule — overnight holds are not underperforming. "
              "The NVDA gap loss was tuition, not a pattern.")


if __name__ == "__main__":
    main()
