#!/usr/bin/env python3
"""
thorn_weekend_thesis.py — READ-ONLY thesis query against Thorn's tape.

THESIS UNDER TEST: "WEEKEND GHOST TOWN"
---------------------------------------
"Large moves that happen on weekends occur on structurally thin books —
CME futures are closed, institutional desks are dark, market-maker
inventory appetite is cut — so a weekend dump of a given size carries
LESS information and MORE forced/retail flow than the identical-size
weekday dump, and it mean-reverts harder over the next 1-4 hours."

Named loser: the weekend panic seller (or liquidation cascade) hitting a
book with nobody home — paying enormous impact for a fill that Monday's
liquidity would have absorbed quietly.

Why it survives: the thinness is STRUCTURAL and scheduled. Institutions
cannot staff weekend desks into existence, CME cannot open, and the
arbitrage capital that would fade the overshoot is precisely the capital
that's offline. The inefficiency renews every Friday night.

Falsifiable claim (pre-registered):
  Among resolved observations where the pair's trailing-1h return
  <= -2.0% (a real dump, not noise):
    WEEKEND rows show mean fwd_4h positive, above the +2.4% fee bar at
    some dump-size threshold, AND materially above the SAME dump-size
    band on weekdays.
  The weekday band is the built-in placebo: if weekday dumps bounce just
  as hard, this is generic dip-buying, not a liquidity-structure edge,
  and the thesis dies.

Weekend definition: CME is closed from Fri 16:00 CT to Sun 17:00 CT.
We use that exact window — the mechanism IS the CME/desk closure, so the
test honors the mechanism's own clock, not the calendar's.

Kill conditions (any one kills):
  - Weekend dump bands don't clear the fee bar at any horizon
  - Weekend bands don't beat same-size weekday bands (placebo)
  - Effect concentrated in one pair or one single weekend
    (per-weekend breakdown mandatory — with ~4 weekends of tape, one
    lucky Saturday can fake the whole result)
  - n < 60 in the signal band (weekends are ~29% of hours; the bar is
    lower than the divergence thesis's 100 but still real)

EXPECTED VERDICT ON FIRST RUN: "INSUFFICIENT" is the honest favorite —
30 days of tape holds only ~4 weekends. The script is built to be re-run
as the tape deepens; pre-registering it NOW is the point, so the
threshold isn't chosen after peeking.

This script only SELECTs. Never writes, never trades, never touches
gates. Safe to run against the live DB.

USAGE
-----
  python3 thorn_weekend_thesis.py
  # DATABASE_URL / CB_TAKER_FEE_PCT inherit from any NEXUS service console.

  Optional:
    --pair BTC-USDC     restrict to one pair (default: all)
    --dump -2.0         headline dump threshold in % (default -2.0)
    --days 30           lookback window (default: all retained tape)
"""
import os
import sys
import argparse
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. On the Railway console it's already present; "
          "locally: pip install psycopg2-binary")
    sys.exit(1)

TAKER = float(os.environ.get("CB_TAKER_FEE_PCT", "0.012"))
FEE_BAR = 2 * TAKER * 100

CENTRAL       = ZoneInfo("America/Chicago")
LOOKBACK_SECS = 3600
LOOKBACK_TOL  = 420

# Trailing-1h-return bands, percent
BANDS = [
    ("ret <= -3.0",        None, -3.0),
    ("-3.0 < ret <= -2.0", -3.0, -2.0),
    ("-2.0 < ret <= -1.0", -2.0, -1.0),
    ("neutral (+/-1.0)",   -1.0,  1.0),
    ("ret >= +2.0 (ref)",   2.0, None),
]
HORIZONS = [("fwd_15m", "15m"), ("fwd_1h", "1h"), ("fwd_4h", "4h")]


def is_cme_closed(ts: int) -> bool:
    """True inside the weekly CME closure: Fri 16:00 CT -> Sun 17:00 CT."""
    dt = datetime.fromtimestamp(ts, tz=CENTRAL)
    wd, mins = dt.weekday(), dt.hour * 60 + dt.minute
    if wd == 4:                       # Friday
        return mins >= 16 * 60
    if wd == 5:                       # Saturday
        return True
    if wd == 6:                       # Sunday
        return mins < 17 * 60
    return False


def weekend_id(ts: int) -> str:
    """Label each closure window by the Friday date it starts on, so the
    per-weekend breakdown can expose a single lucky weekend."""
    dt = datetime.fromtimestamp(ts, tz=CENTRAL)
    days_back = {4: 0, 5: 1, 6: 2}.get(dt.weekday(), 0)
    fri = (dt - timedelta(days=days_back)).date()
    return fri.isoformat()


def fmt(x, w=7):
    return f"{x:+{w}.3f}" if x is not None else " " * (w - 3) + "n/a"


def band_of(r):
    for name, lo, hi in BANDS:
        if (lo is None or r > lo) and (hi is None or r <= hi):
            return name
    return None


def stats(rows, col_idx):
    vals = [r[col_idx] for r in rows if r[col_idx] is not None]
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    mean = sum(vals) / n
    median = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
    hit = sum(1 for v in vals if v > 0) / n * 100
    clear = sum(1 for v in vals if v > FEE_BAR) / n * 100
    p10 = vals[max(0, int(n * 0.10) - 1)]
    return dict(n=n, mean=mean, median=median, hit=hit, clear=clear, p10=p10)


def trailing_ret(series_ts, series_px, ts, px):
    target = ts - LOOKBACK_SECS
    i = bisect_right(series_ts, target) - 1
    best = None
    for j in (i, i + 1):
        if 0 <= j < len(series_ts) and abs(series_ts[j] - target) <= LOOKBACK_TOL:
            if best is None or abs(series_ts[j] - target) < abs(series_ts[best] - target):
                best = j
    if best is None or not series_px[best]:
        return None
    return (px - series_px[best]) / series_px[best] * 100


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default=None)
    ap.add_argument("--dump", type=float, default=-2.0)
    ap.add_argument("--days", type=int, default=None)
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set. Run from a NEXUS service console, or export it.")
        sys.exit(1)

    where, params = ["price > 0"], []
    if args.pair:
        where.append("pair = %s")
        params.append(args.pair)
    if args.days:
        import time as _t
        where.append("ts >= %s")
        params.append(int(_t.time()) - args.days * 86400)

    conn = psycopg2.connect(db_url, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT ts, pair, price, fwd_15m, fwd_1h, fwd_4h
            FROM crypto_thorn_observations
            WHERE {' AND '.join(where)}
            ORDER BY pair, ts
        """, params)
        raw = cur.fetchall()
    conn.close()

    ts_by, px_by = defaultdict(list), defaultdict(list)
    for ts, pair, px, *_ in raw:
        ts_by[pair].append(ts)
        px_by[pair].append(px)

    grid = defaultdict(list)              # (regime, band) -> rows
    per_weekend = defaultdict(list)       # weekend_id -> headline rows
    per_pair = defaultdict(list)          # pair -> headline rows
    skipped = total = 0

    for ts, pair, px, f15, f1h, f4h in raw:
        if f4h is None:
            continue
        total += 1
        r1h = trailing_ret(ts_by[pair], px_by[pair], ts, px)
        if r1h is None:
            skipped += 1
            continue
        band = band_of(r1h)
        if band is None:
            continue
        regime = "WKND" if is_cme_closed(ts) else "WEEK"
        grid[(regime, band)].append((f15, f1h, f4h))
        if regime == "WKND" and r1h <= args.dump:
            per_weekend[weekend_id(ts)].append((f15, f1h, f4h))
            per_pair[pair].append((f15, f1h, f4h))

    print("=" * 78)
    print("THORN THESIS: WEEKEND GHOST TOWN (thin-book overshoot reversion)")
    print(f"fee bar (round-trip): {FEE_BAR:.2f}%   |   resolved obs: {total}"
          f"   |   skipped (no 1h anchor): {skipped}")
    print("weekend = CME closure window: Fri 16:00 CT -> Sun 17:00 CT")
    print("=" * 78)

    for regime, label in (("WKND", "WEEKEND (CME closed)  — THE THESIS"),
                          ("WEEK", "WEEKDAY (CME open)  — PLACEBO, must NOT match")):
        print("\n" + "-" * 78)
        print(label)
        print(f"{'trailing-1h band':22s} {'n':>5s}  " +
              "  ".join(f"{h:>26s}" for _, h in HORIZONS))
        print(f"{'':22s} {'':>5s}  " +
              "  ".join(f"{'mean/med/hit%/clr%':>26s}" for _ in HORIZONS))
        for name, lo, hi in BANDS:
            rows = grid.get((regime, name), [])
            if not rows:
                continue
            cells = []
            for i, (_, h) in enumerate(HORIZONS):
                s = stats(rows, i)
                cells.append(f"{fmt(s['mean'],6)}/{fmt(s['median'],6)}/"
                             f"{s['hit']:4.0f}/{s['clear']:4.0f}" if s else "n/a")
            print(f"{name:22s} {len(rows):5d}  " + "  ".join(f"{c:>26s}" for c in cells))

    head = [r for wid in per_weekend for r in per_weekend[wid]]
    plac = [r for (reg, band), rows in grid.items()
            if reg == "WEEK" and band in ("ret <= -3.0", "-3.0 < ret <= -2.0")
            for r in rows]
    hs, ps = stats(head, 2), stats(plac, 2)

    print("\n" + "=" * 78)
    print(f"HEADLINE BAND: WEEKEND & trailing-1h <= {args.dump}%   (fwd_4h)")
    if hs is None:
        print("VERDICT: INSUFFICIENT — zero weekend dumps in the tape yet. "
              "Re-run as the tape deepens; the thesis is pre-registered.")
        return
    print(f"  n={hs['n']}  mean={fmt(hs['mean'])}%  median={fmt(hs['median'])}%  "
          f"hit={hs['hit']:.0f}%  clears-fee={hs['clear']:.0f}%  p10={fmt(hs['p10'])}%")
    if ps:
        print(f"  placebo (weekday same bands): n={ps['n']}  mean={fmt(ps['mean'])}%  "
              f"median={fmt(ps['median'])}%")

    print("\nPER-WEEKEND (headline band) — one lucky Saturday can fake everything:")
    pos_weekends = 0
    for wid in sorted(per_weekend):
        s = stats(per_weekend[wid], 2)
        if s:
            pos_weekends += 1 if s['mean'] > 0 else 0
            print(f"  {'+' if s['mean'] > 0 else '-'} weekend of {wid}  n={s['n']:4d}  "
                  f"mean={fmt(s['mean'])}%  median={fmt(s['median'])}%")

    print("\nPER-PAIR (headline band):")
    pos_pairs = 0
    for pair in sorted(per_pair):
        s = stats(per_pair[pair], 2)
        if s:
            pos_pairs += 1 if s['mean'] > 0 else 0
            print(f"  {'+' if s['mean'] > 0 else '-'} {pair:10s} n={s['n']:4d}  "
                  f"mean={fmt(s['mean'])}%  median={fmt(s['median'])}%  hit={s['hit']:.0f}%")

    print("\nVERDICT:")
    if hs['n'] < 60:
        print(f"  INSUFFICIENT — n={hs['n']} < 60. Park; re-run when more weekends "
              f"are on tape. (Expected outcome on a ~30-day tape.)")
    elif hs['mean'] <= FEE_BAR:
        print(f"  KILL — weekend dumps mean fwd_4h {hs['mean']:+.3f}% does not clear "
              f"the {FEE_BAR:.2f}% fee bar.")
    elif ps and hs['mean'] <= (ps['mean'] or 0):
        print("  KILL — weekend bounce doesn't beat the weekday placebo: generic "
              "dip behavior, not a liquidity-structure edge.")
    elif hs['median'] <= 0 or hs['hit'] < 50:
        print("  KILL — mean clears but median/hit-rate says knife-catching.")
    elif pos_weekends < 3 or pos_pairs < 2:
        print(f"  KILL — effect not broad: positive weekends={pos_weekends} (<3) or "
              f"positive pairs={pos_pairs} (<2). One lucky window, not a structure.")
    else:
        print("  KEEP (provisional) — clears fee bar, beats weekday placebo, "
              "median-positive, broad across weekends and pairs. Next: hold the "
              "verdict through 2 more fresh weekends before any code is written.")


if __name__ == "__main__":
    main()
