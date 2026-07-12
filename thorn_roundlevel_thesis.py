#!/usr/bin/env python3
"""
thorn_roundlevel_thesis.py — READ-ONLY thesis query against Thorn's tape.

THESIS UNDER TEST: "ROUND-NUMBER STOP SWEEP"
--------------------------------------------
"Retail stop-losses cluster at psychologically round prices (BTC 64,000;
SOL 80; DOGE 0.075). When price crosses DOWN through a major round level,
the clustered stops fire as market sells — a burst of purely MECHANICAL,
price-insensitive flow. Once that pocket of forced supply is exhausted,
price snaps back above the level."

Named loser: everyone who parked a stop at the obvious round number.
Their stop placement was chosen for cognitive comfort, not market
structure — and its very obviousness is what gets it harvested.

Why it survives: it's behavioral, not informational. Round-number
anchoring has survived decades of being documented because humans keep
placing stops at numbers that feel clean. New retail cohorts arrive
every cycle and park stops in the same places.

Falsifiable claim (pre-registered):
  Among resolved observations where price sits just BELOW a major round
  level (within 0.6% below it) AND the pair traded ABOVE that level
  within the trailing hour (i.e., the level was just swept):
    mean fwd_4h is positive, exceeds the pair's unconditional baseline,
    and at some depth band clears the +2.4% round-trip fee.

Round levels, defined mechanically (no hand-picking): the {1, 2, 5}
significant-digit grid at the scale where consecutive levels are 2-10%
apart. Examples: BTC -> 60000/62000/64000...; SOL -> 76/78/80 or
70/75/80 depending on scale; DOGE -> 0.070/0.072/0.075. One rule, every
pair, no cherry-picking per pair.

Built-in placebo: the MIRROR case — price just ABOVE a round level having
crossed UP through it within the hour. Breakout longs cluster there
instead of stops; the mechanism predicts the snap-back edge exists on the
sweep-DOWN side specifically. If both sides show the same "edge," we're
measuring generic short-horizon mean reversion, not stop harvesting, and
the thesis dies.

Kill conditions (any one kills):
  - No depth band clears the fee bar at any horizon
  - Sweep-down edge doesn't beat the mirror placebo
  - Effect concentrated in one pair (per-pair breakdown mandatory)
  - Median <= 0 or hit-rate < 50% where the mean clears (knife-catching)
  - n < 100 in the signal band

HONEST CAVEATS, DECLARED UP FRONT
---------------------------------
1) Thorn samples every ~300s — a sweep that dips through a level and
   reverts within a couple of minutes never prints on our tape. We only
   see sweeps that HOLD below for minutes, which biases the sample toward
   deeper/slower sweeps. If the verdict is KEEP, the true tradeable edge
   is probably STRONGER than measured (fast sweeps we missed are the best
   snap-backs). If KILL, this bias can't be the excuse — a real edge
   should still show in the slow subset.
2) A level cross can also be a genuine breakdown. That's not a flaw to
   engineer around — it's exactly what the forward returns adjudicate.
   The claim IS that sweeps outnumber/outweigh breakdowns at round
   levels; if they don't, the tape says KILL and we believe it.

This script only SELECTs. Never writes, never trades, never touches
gates. Safe to run against the live DB.

USAGE
-----
  python3 thorn_roundlevel_thesis.py
  # DATABASE_URL / CB_TAKER_FEE_PCT inherit from any NEXUS service console.

  Optional:
    --pair BTC-USDC     restrict to one pair (default: all)
    --depth 0.6         max distance below level, percent (default 0.6)
    --days 30           lookback window (default: all retained tape)
"""
import os
import sys
import math
import argparse
from bisect import bisect_left, bisect_right
from collections import defaultdict

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. On the Railway console it's already present; "
          "locally: pip install psycopg2-binary")
    sys.exit(1)

TAKER = float(os.environ.get("CB_TAKER_FEE_PCT", "0.012"))
FEE_BAR = 2 * TAKER * 100

SWEEP_WINDOW  = 3600          # "recently above/below" = within the trailing hour
MIN_SPACING   = 0.02          # consecutive round levels >= 2% apart
MAX_SPACING   = 0.10          # ... and <= 10% apart (picks the digit scale)
MANTISSAS     = (1.0, 2.0, 5.0)

# Distance-below-level bands, percent (how deep below the swept level we are)
BANDS = [
    ("0.0-0.2% below", 0.0, 0.2),
    ("0.2-0.4% below", 0.2, 0.4),
    ("0.4-0.6% below", 0.4, 0.6),
]
HORIZONS = [("fwd_15m", "15m"), ("fwd_1h", "1h"), ("fwd_4h", "4h")]


def round_levels_near(px: float):
    """Return the nearest round level ABOVE px and BELOW px on the {1,2,5}
    grid, at the digit scale where level spacing lands in [2%, 10%] of px.
    Mechanical, identical rule for every pair."""
    if px <= 0:
        return None, None
    exp = math.floor(math.log10(px))
    candidates = []
    for e in (exp - 1, exp, exp + 1):
        step = 10.0 ** e
        for m in MANTISSAS:
            spacing = m * step / px
            if MIN_SPACING <= spacing <= MAX_SPACING:
                candidates.append(m * step)
    if not candidates:
        return None, None
    grid = min(candidates, key=lambda g: abs(g / px - 0.04))  # prefer ~4% spacing
    below = math.floor(px / grid) * grid
    above = below + grid
    return above, below


def fmt(x, w=7):
    return f"{x:+{w}.3f}" if x is not None else " " * (w - 3) + "n/a"


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


def traded_beyond(series_ts, series_px, ts, level, above: bool):
    """True if the pair printed above (or below) `level` within the
    trailing SWEEP_WINDOW before ts."""
    lo = bisect_left(series_ts, ts - SWEEP_WINDOW)
    hi = bisect_right(series_ts, ts)
    for j in range(lo, hi):
        p = series_px[j]
        if p and ((above and p > level) or (not above and p < level)):
            return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default=None)
    ap.add_argument("--depth", type=float, default=0.6)
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

    sweep = defaultdict(list)      # band -> rows  (price below level, was above)
    mirror = defaultdict(list)     # band -> rows  (price above level, was below)
    per_pair = defaultdict(list)   # pair -> sweep rows (all depth bands)
    baseline_rows = []
    total = 0

    for ts, pair, px, f15, f1h, f4h in raw:
        if f4h is None:
            continue
        total += 1
        baseline_rows.append((f15, f1h, f4h))
        above_lvl, below_lvl = round_levels_near(px)
        if above_lvl is None:
            continue

        # SWEEP case: we are just BELOW `above_lvl`, and traded above it recently
        dist_below = (above_lvl - px) / above_lvl * 100
        if 0 < dist_below <= args.depth and \
                traded_beyond(ts_by[pair], px_by[pair], ts, above_lvl, above=True):
            for name, lo, hi in BANDS:
                if lo < dist_below <= hi:
                    sweep[name].append((f15, f1h, f4h))
                    per_pair[pair].append((f15, f1h, f4h))
                    break

        # MIRROR placebo: just ABOVE `below_lvl`, and traded below it recently
        dist_above = (px - below_lvl) / below_lvl * 100
        if 0 < dist_above <= args.depth and \
                traded_beyond(ts_by[pair], px_by[pair], ts, below_lvl, above=False):
            for name, lo, hi in BANDS:
                if lo < dist_above <= hi:
                    mirror[name].append((f15, f1h, f4h))
                    break

    print("=" * 78)
    print("THORN THESIS: ROUND-NUMBER STOP SWEEP (snap-back after the harvest)")
    print(f"fee bar (round-trip): {FEE_BAR:.2f}%   |   resolved obs: {total}")
    print(f"level grid: {{1,2,5}} x 10^k, spacing 2-10% (pref ~4%)  |  "
          f"sweep = crossed within trailing {SWEEP_WINDOW // 60}min")
    print("=" * 78)

    bs = stats(baseline_rows, 2)
    if bs:
        print(f"\nUNCONDITIONAL BASELINE  n={bs['n']}  "
              f"mean fwd_4h={fmt(bs['mean'])}%  median={fmt(bs['median'])}%")

    for grid_, label in ((sweep, "SWEEP-DOWN (below level, was above)  — THE THESIS"),
                         (mirror, "MIRROR (above level, was below)  — PLACEBO, must NOT match")):
        print("\n" + "-" * 78)
        print(label)
        print(f"{'depth band':18s} {'n':>5s}  " +
              "  ".join(f"{h:>26s}" for _, h in HORIZONS))
        print(f"{'':18s} {'':>5s}  " +
              "  ".join(f"{'mean/med/hit%/clr%':>26s}" for _ in HORIZONS))
        for name, lo, hi in BANDS:
            rows = grid_.get(name, [])
            if not rows:
                continue
            cells = []
            for i, (_, h) in enumerate(HORIZONS):
                s = stats(rows, i)
                cells.append(f"{fmt(s['mean'],6)}/{fmt(s['median'],6)}/"
                             f"{s['hit']:4.0f}/{s['clear']:4.0f}" if s else "n/a")
            print(f"{name:18s} {len(rows):5d}  " + "  ".join(f"{c:>26s}" for c in cells))

    head = [r for rows in sweep.values() for r in rows]
    plac = [r for rows in mirror.values() for r in rows]
    hs, ps = stats(head, 2), stats(plac, 2)

    print("\n" + "=" * 78)
    print(f"HEADLINE: all sweep-down rows within {args.depth}% below a swept level (fwd_4h)")
    if hs is None:
        print("VERDICT: INSUFFICIENT — zero sweep rows in the tape.")
        return
    print(f"  n={hs['n']}  mean={fmt(hs['mean'])}%  median={fmt(hs['median'])}%  "
          f"hit={hs['hit']:.0f}%  clears-fee={hs['clear']:.0f}%  p10={fmt(hs['p10'])}%")
    if ps:
        print(f"  mirror placebo: n={ps['n']}  mean={fmt(ps['mean'])}%  "
              f"median={fmt(ps['median'])}%")
    if bs:
        print(f"  baseline:       n={bs['n']}  mean={fmt(bs['mean'])}%")

    print("\nPER-PAIR (sweep rows):")
    pos_pairs = 0
    for pair in sorted(per_pair):
        s = stats(per_pair[pair], 2)
        if s:
            pos_pairs += 1 if s['mean'] > 0 else 0
            print(f"  {'+' if s['mean'] > 0 else '-'} {pair:10s} n={s['n']:4d}  "
                  f"mean={fmt(s['mean'])}%  median={fmt(s['median'])}%  hit={s['hit']:.0f}%")

    print("\nVERDICT:")
    if hs['n'] < 100:
        print(f"  INSUFFICIENT — n={hs['n']} < 100. Park; re-run when the tape is deeper.")
    elif hs['mean'] <= FEE_BAR:
        print(f"  KILL — sweep rows mean fwd_4h {hs['mean']:+.3f}% does not clear the "
              f"{FEE_BAR:.2f}% fee bar. (Check bands: a single depth band clearing "
              f"with n>=100 keeps a narrower version alive.)")
    elif ps and hs['mean'] <= (ps['mean'] or 0):
        print("  KILL — sweep-down doesn't beat the mirror: generic reversion, "
              "not stop harvesting.")
    elif hs['median'] <= 0 or hs['hit'] < 50:
        print("  KILL — mean clears but median/hit-rate says a few lucky bounces.")
    elif pos_pairs < 2:
        print("  KILL — effect concentrated in a single pair; not behavioral structure.")
    else:
        print("  KEEP (provisional) — clears fee bar, beats mirror placebo, "
              "median-positive, multi-pair. Next: per-week stability, then depth-band "
              "shape check (edge should DECAY with depth if the mechanism is real).")


if __name__ == "__main__":
    main()
