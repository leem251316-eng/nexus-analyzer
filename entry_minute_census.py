#!/usr/bin/env python3
"""
entry_minute_census.py  V1.0  -- read-only DISCOVERY  (Sep 24 2026)
=====================================================================
Buckets every LIVE Berserker trade by entry minute-of-day (CT) and prints
n, WR, expectancy, exit mix, carry rate, and the share of entries that never
saw +0.25% MFE.  Writes NOTHING.  Runs in the nexus-analyst console.

  python3 entry_minute_census.py                 # since Jul 29 2026 (wall live)
  python3 entry_minute_census.py --since 2026-06-01

DISCOVERY, not validation: every number here has been looked at, so none of
it can be the verdict of the registration it motivates.  The registration
names ONE band and a FRESH window (or a shadow gate, wall-style), then reads.
Two bands are already PARK-grade from the Sep 14-18 reconcile: 09:00-09:22
(9 stops / 14 entries) and >=14:45 (entries with <15 min to auto-close).
This census exists so the choice between them -- or something else -- is
made on ~400 trades, not 42.

n = trades (one row per trade_id).  Expectancy = mean pnl_pct, fill-anchored
as recorded (see the fill-confirm caveat in the Sep 24 ledger).
"""
import os
import sys
import argparse
from datetime import datetime, date, time as dtime
from zoneinfo import ZoneInfo

import psycopg2

CT = ZoneInfo("America/Chicago")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

BANDS = [                       # (label, start_min, end_min) in minutes after midnight CT
    ("08:30-09:00", 510, 540),
    ("09:00-09:05", 540, 545),
    ("09:05-09:10", 545, 550),
    ("09:10-09:15", 550, 555),
    ("09:15-09:22", 555, 562),
    ("09:22-09:30", 562, 570),
    ("09:30-10:00", 570, 600),
    ("10:00-11:00", 600, 660),
    ("11:00-12:00", 660, 720),
    ("12:00-13:00", 720, 780),
    ("13:00-14:00", 780, 840),
    ("14:00-14:30", 840, 870),
    ("14:30-14:45", 870, 885),
    ("14:45-14:58", 885, 898),
]
COARSE = [("09:00-09:22", 540, 562), ("09:22-14:30", 562, 870), ("14:30-14:58", 870, 898)]
REASON = {"take-profit": "tp", "stop-loss": "sl", "trailing-stop": "tr",
          "eod-autoclose": "eod", "manual-close": "man"}


def q(sql, params=()):
    c = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    c.close()
    return rows


def load(since):
    start = int(datetime.combine(since, dtime(0, 0), CT).timestamp())
    rows = q("""
        SELECT symbol, entry_ts, exit_ts, pnl_pct, exit_reason, mfe
        FROM berserker_trade_fingerprints
        WHERE is_paper = FALSE AND trade_id NOT LIKE 'bt_%%' AND won IS NOT NULL
          AND entry_ts >= %s
        ORDER BY entry_ts
    """, (start,))
    out = []
    for sym, e, x, pnl, r, mfe in rows:
        ed = datetime.fromtimestamp(int(e), CT)
        xd = datetime.fromtimestamp(int(x), CT)
        out.append({"sym": sym, "min": ed.hour * 60 + ed.minute, "date": ed.date(),
                    "carry": xd.date() != ed.date(), "pnl": float(pnl),
                    "type": REASON.get(r or "", "?"),
                    "mfe": float(mfe) if mfe is not None else None})
    return out


def row(label, grp, total_n):
    n = len(grp)
    if n == 0:
        return f"  {label:12} {'-':>4}"
    w = [t["pnl"] for t in grp if t["pnl"] > 0]
    l = [t["pnl"] for t in grp if t["pnl"] <= 0]
    exp = sum(t["pnl"] for t in grp) / n
    avg_w = sum(w) / len(w) if w else 0.0
    avg_l = sum(l) / len(l) if l else 0.0
    sl = sum(1 for t in grp if t["type"] == "sl")
    tr = sum(1 for t in grp if t["type"] == "tr")
    eod = sum(1 for t in grp if t["type"] == "eod")
    carry = sum(1 for t in grp if t["carry"])
    seen = [t for t in grp if t["mfe"] is not None]
    dead = sum(1 for t in seen if t["mfe"] < 0.25)
    dead_pct = f"{100 * dead / len(seen):3.0f}%" if seen else "  -"
    return (f"  {label:12} {n:4d} {100 * n / total_n:5.1f}%  WR {100 * len(w) / n:4.1f}%  "
            f"exp {exp:+.3f}  W {avg_w:+.2f} L {avg_l:+.2f}  "
            f"sl {100 * sl / n:3.0f}% tr {100 * tr / n:3.0f}% eod {100 * eod / n:3.0f}%  "
            f"carry {100 * carry / n:3.0f}%  MFE<.25 {dead_pct}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-07-29")
    a = ap.parse_args()
    if not DATABASE_URL:
        sys.exit("DATABASE_URL missing -- run in the nexus-analyst console.")
    since = date.fromisoformat(a.since)
    trades = load(since)
    n = len(trades)
    print(f"ENTRY-MINUTE CENSUS V1.0 | live Berserker trades entered since {since} | n={n} | "
          f"sessions={len({t['date'] for t in trades})}")
    if not n:
        return
    print(row("ALL", trades, n))
    print("\nby entry band (CT):")
    for label, lo, hi in BANDS:
        print(row(label, [t for t in trades if lo <= t["min"] < hi], n))
    print("\ncoarse:")
    for label, lo, hi in COARSE:
        print(row(label, [t for t in trades if lo <= t["min"] < hi], n))
    print("\nby symbol, 09:00-09:22 only:")
    early = [t for t in trades if 540 <= t["min"] < 562]
    for sym in sorted({t["sym"] for t in early}):
        print(row(sym, [t for t in early if t["sym"] == sym], n))
    print("\ncarried trades (exit date != entry date), by entry band:")
    carried = [t for t in trades if t["carry"]]
    for label, lo, hi in COARSE:
        print(row(label, [t for t in carried if lo <= t["min"] < hi], n))
    print("\nReminder: discovery only. Register one band + a fresh window before reading a verdict.")


if __name__ == "__main__":
    main()
