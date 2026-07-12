#!/usr/bin/env python3
"""
thorn_hour_thesis.py — READ-ONLY time-of-day thesis query against Thorn's tape.

THESIS UNDER TEST
-----------------
"Crypto has a daily rhythm: certain hours reliably precede rising prices
(a 'low' you'd buy) and others precede falling prices (a 'high' you'd
sell), driven by session structure (Asia/Europe/US open & handoffs)."

Named mechanism: humans trade on clocks; liquidity spikes at session
opens and dies in the gaps. This is a REAL structural cause, not mined
noise -- but the pattern only counts if it's STABLE across the whole tape,
not just the last few days on the chart.

Kill condition: no hour shows forward returns meaningfully different from
the others, OR the "good" hours' edge is under the round-trip fee bar (a
real-but-worthless rhythm -- same trap that killed F&G).

Likely outcome: time-of-day survives as a FILTER ("only take signals in
the good hours") more often than as a standalone buy-low/sell-high trade,
because intraday swings are usually smaller than the 2.4% fee bar.

WHAT IT DOES
------------
Buckets every RESOLVED observation by hour_cdt (0-23) and reports mean
forward return + hit-rate at 15m/1h/4h. Flags the best/worst hours and
whether any hour's average clears the fee bar. Read-only: SELECT only,
never writes, never trades.

USAGE
-----
  python3 thorn_hour_thesis.py                 # all pairs, 1h headline
  python3 thorn_hour_thesis.py --pair BTC-USDC
  python3 thorn_hour_thesis.py --horizon 4h
"""
import os
import sys
import argparse

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed (present on the Railway console).")
    sys.exit(1)

TAKER = float(os.environ.get("CB_TAKER_FEE_PCT", "0.012"))
FEE_BAR = 2 * TAKER * 100          # percent, round trip

# Session labels for readability (CDT). Matches crypto.py PEAK/CAUTIOUS sets.
def session_tag(h):
    if h in (8, 9, 10, 11):
        return "US-peak"
    if h in (12, 13, 14, 15, 16, 17, 19):
        return "US-cautious"
    if h in (2, 3, 4, 5):
        return "EU-open"
    if h in (20, 21, 22, 23):
        return "Asia"
    return "off"

HORIZONS = [("fwd_15m", "15m"), ("fwd_1h", "1h"), ("fwd_4h", "4h")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default=None)
    ap.add_argument("--horizon", default="1h", choices=["15m", "1h", "4h"])
    ap.add_argument("--min-n", type=int, default=20,
                    help="minimum rows per hour to trust the bucket (default 20)")
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set. Run from a NEXUS service console.")
        sys.exit(1)

    hcol = {"15m": "fwd_15m", "1h": "fwd_1h", "4h": "fwd_4h"}[args.horizon]
    where = [f"{hcol} IS NOT NULL", "hour_cdt IS NOT NULL"]
    params = []
    if args.pair:
        where.append("pair = %s")
        params.append(args.pair)
    where_sql = " AND ".join(where)

    conn = psycopg2.connect(db_url, connect_timeout=10)

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*), MIN(ts), MAX(ts) FROM crypto_thorn_observations WHERE {where_sql}", params)
        total, tmin, tmax = cur.fetchone()

    print("=" * 78)
    print(f"THORN TIME-OF-DAY THESIS  —  {args.horizon} forward  —  read-only")
    print("=" * 78)
    scope = args.pair if args.pair else "ALL pairs"
    print(f"Scope: {scope} | resolved rows: {total:,} | fee bar: {FEE_BAR:.2f}%")
    if total == 0:
        print("No resolved rows under this filter yet. Let the tape fill.")
        conn.close()
        return
    if tmin and tmax:
        print(f"Tape span: ~{(tmax - tmin)/86400.0:.1f} days")
    print("=" * 78)
    print(f"{'Hr':>3} {'session':<12} {'n':>6} {'avg fwd':>9} {'>0%':>6} {'>fee%':>6}  bar")
    print("-" * 78)

    rows = []
    for h in range(24):
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*), AVG({hcol}),
                       AVG(CASE WHEN {hcol} > 0 THEN 1.0 ELSE 0.0 END),
                       AVG(CASE WHEN {hcol} > %s THEN 1.0 ELSE 0.0 END)
                FROM crypto_thorn_observations
                WHERE {where_sql} AND hour_cdt = %s
            """, [FEE_BAR] + params + [h])
            n, avg, hp, hf = cur.fetchone()
        if not n:
            continue
        avg = avg or 0.0
        hp = (hp or 0) * 100
        hf = (hf or 0) * 100
        rows.append((h, n, avg, hp, hf))
        # simple text bar centered on zero
        mag = max(-1.0, min(1.0, avg / 0.5))   # scale: 0.5% = full bar
        if avg >= 0:
            bar = " " * 8 + "|" + "#" * int(mag * 8)
        else:
            bar = " " * (8 + int(mag * 8)) + "#" * int(-mag * 8) + "|"
        thin = "  (thin)" if n < args.min_n else ""
        print(f"{h:>3} {session_tag(h):<12} {n:>6,} {avg:>+8.3f}% {hp:>5.1f} {hf:>5.1f}  {bar}{thin}")

    conn.close()

    # Verdict: best & worst TRUSTWORTHY hours (n >= min_n)
    trust = [r for r in rows if r[1] >= args.min_n]
    print("=" * 78)
    print(f"VERDICT — {args.horizon} forward, hours with n >= {args.min_n}")
    print("=" * 78)
    if len(trust) < 3:
        print(f"Only {len(trust)} hour-buckets have enough rows yet. Thin tape --")
        print("re-run in a week or two once hours fill. First look only.")
        return
    best  = max(trust, key=lambda r: r[2])
    worst = min(trust, key=lambda r: r[2])
    spread = best[2] - worst[2]
    print(f"Best  hour: {best[0]:02d}:00 CDT ({session_tag(best[0])}) "
          f"avg {best[2]:+.3f}% (n={best[1]:,})")
    print(f"Worst hour: {worst[0]:02d}:00 CDT ({session_tag(worst[0])}) "
          f"avg {worst[2]:+.3f}% (n={worst[1]:,})")
    print(f"Best-worst spread: {spread:.3f}%  |  fee bar: {FEE_BAR:.2f}%")
    print("-" * 78)
    if best[2] > FEE_BAR:
        print("STANDALONE-VIABLE (provisional): the best hour's forward return clears")
        print("the fee bar on its own. Rare and strong -- verify per-pair and confirm")
        print("it isn't one big window doing all the work before trusting it.")
    elif spread > FEE_BAR:
        print("FILTER-VIABLE (provisional): no single hour clears fees standalone, but")
        print("the best-vs-worst SPREAD exceeds the fee bar. That means time-of-day is")
        print("worth using as a BIAS -- prefer entries in the strong hours, avoid the")
        print("weak ones -- layered on your existing signals. Not a blind clock trade.")
    else:
        print("KILL: the hour-to-hour spread is under the fee bar. The 'humps' on the")
        print("chart are real to the eye but too small to trade net of costs, and not")
        print("stable enough across the tape to matter. Time-of-day is not an edge here.")
    print("=" * 78)
    print("Note: thin per-hour n early on. A KILL now can flip to FILTER-VIABLE as the")
    print("tape deepens -- re-run periodically. A FILTER result is the useful outcome:")
    print("it tunes WHEN your real signals fire, it doesn't trade the clock blindly.")


if __name__ == "__main__":
    main()
