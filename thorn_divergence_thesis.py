#!/usr/bin/env python3
"""
thorn_divergence_thesis.py — READ-ONLY thesis query against Thorn's tape.

THESIS UNDER TEST: "IDIOSYNCRATIC DIVERGENCE SNAP-BACK"
-------------------------------------------------------
"When an alt dumps hard relative to BTC over the trailing hour — the alt
falls while BTC holds — the move is disproportionately MECHANICAL (one
impatient or forced seller paying through a thin spot book) rather than
new information, and it partially reverts over the next 1-4 hours."

Named loser: whoever market-sold an alt into a thin Coinbase book while
BTC did nothing — a liquidation, a fat-fingered exit, a whale who wanted
out NOW. Their impatience is the inefficiency.

Why it survives: correcting a relative-value gap properly requires
shorting BTC against the long alt — impossible on Coinbase spot, and
cross-exchange stat-arb desks won't touch gaps smaller than their own
costs. Divergences under ~2x retail fees are structurally unarbitrageable,
so the ONLY gaps that snap back hard are the big ones — exactly the band
a fee-burdened account needs. The correction happens slowly (drift, not
instant arb), leaving time to enter after the print.

Falsifiable claim (pre-registered):
  Among resolved observations where
      div_1h = ret_1h(alt) - ret_1h(BTC)  <=  -2.0%
  AND |ret_1h(BTC)| < 1.0%   (isolates IDIOSYNCRATIC dumps),
  mean fwd_4h exceeds the unconditional baseline, and at some divergence
  threshold the conditional mean fwd_4h clears the +2.4% round-trip fee.

Built-in placebo (the honesty check): the SYSTEMIC band — alt down THE
SAME AMOUNT but WITH BTC (btc_1h <= -1%). If systemic dumps "bounce" as
much as idiosyncratic ones, the divergence framing is wrong and the
effect is just volatility clustering. The thesis requires the
idiosyncratic band to beat its own placebo, not just zero.

Kill conditions (any one kills):
  - No divergence band clears the fee bar at any horizon
  - Idiosyncratic band does not beat the systemic placebo band
  - Effect concentrated in one pair (per-pair breakdown mandatory)
  - Median <= 0 or hit-rate < 50% in a band whose MEAN clears
    (mean dragged by a few lucky bounces = knife-catching)
  - n < 100 in the signal band (insufficient, park and re-run later)

This script only SELECTs. It never writes, never trades, never touches
gates. Safe to run against the live DB.

USAGE
-----
  python3 thorn_divergence_thesis.py
  # DATABASE_URL and CB_TAKER_FEE_PCT inherit from any NEXUS service
  # console (run it from the crypto service's Railway shell).

  Optional:
    --pair SOL-USDC     restrict to one pair (default: all alts)
    --days 30           lookback window (default: all retained tape)
    --div -2.0          headline divergence threshold in % (default -2.0)
"""
import os
import sys
import argparse
from bisect import bisect_right
from collections import defaultdict

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. On the Railway console it's already present; "
          "locally: pip install psycopg2-binary")
    sys.exit(1)

TAKER = float(os.environ.get("CB_TAKER_FEE_PCT", "0.012"))
FEE_BAR = 2 * TAKER * 100          # percent; a long must clear this to net positive

BTC_PAIR       = "BTC-USDC"
LOOKBACK_SECS  = 3600              # trailing-return window (1h)
LOOKBACK_TOL   = 420               # accept the nearest obs within +/-7min of 1h ago
BTC_MATCH_TOL  = 300               # BTC obs must exist within 5min of the alt obs
IDIO_BTC_LIMIT = 1.0               # |btc_1h| < this  => idiosyncratic regime
SYST_BTC_LIMIT = -1.0              # btc_1h <= this   => systemic (placebo) regime

# Divergence bands, percent. Negative = alt underperformed BTC.
BANDS = [
    ("div <= -3.0",        None,  -3.0),
    ("-3.0 < div <= -2.0", -3.0,  -2.0),
    ("-2.0 < div <= -1.0", -2.0,  -1.0),
    ("-1.0 < div <= -0.5", -1.0,  -0.5),
    ("neutral (+/-0.5)",   -0.5,   0.5),
    ("div >= +2.0 (ref)",   2.0,  None),
]

HORIZONS = [("fwd_15m", "15m"), ("fwd_1h", "1h"), ("fwd_4h", "4h")]


def fmt(x, w=7):
    return f"{x:+{w}.3f}" if x is not None else " " * (w - 3) + "n/a"


def band_of(div):
    for name, lo, hi in BANDS:
        if (lo is None or div > lo) and (hi is None or div <= hi):
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
    """Return over the trailing LOOKBACK_SECS for one pair, or None if no
    observation exists near ts - 1h. series_* are sorted parallel lists."""
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


def nearest(series_ts, series_vals, ts, tol):
    i = bisect_right(series_ts, ts) - 1
    best = None
    for j in (i, i + 1):
        if 0 <= j < len(series_ts) and abs(series_ts[j] - ts) <= tol:
            if best is None or abs(series_ts[j] - ts) < abs(series_ts[best] - ts):
                best = j
    return series_vals[best] if best is not None else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default=None)
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--div", type=float, default=-2.0,
                    help="headline divergence threshold, percent (default -2.0)")
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set. Run from a NEXUS service console, or export it.")
        sys.exit(1)

    where, params = ["price > 0"], []
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

    # per-pair sorted series for trailing-return lookups
    ts_by, px_by = defaultdict(list), defaultdict(list)
    for ts, pair, px, *_ in raw:
        ts_by[pair].append(ts)
        px_by[pair].append(px)

    if BTC_PAIR not in ts_by:
        print(f"No {BTC_PAIR} observations in the window — cannot compute divergence.")
        sys.exit(1)

    # precompute BTC trailing returns at each BTC observation
    btc_ts = ts_by[BTC_PAIR]
    btc_r1h_vals = [trailing_ret(btc_ts, px_by[BTC_PAIR], t, p)
                    for t, p in zip(btc_ts, px_by[BTC_PAIR])]

    # classify every resolved alt observation
    # regime: IDIO (|btc|<1), SYST (btc<=-1), OTHER
    grid = defaultdict(list)          # (regime, band) -> rows of (f15, f1h, f4h)
    per_pair = defaultdict(list)      # headline-band rows per pair
    skipped, total = 0, 0

    for ts, pair, px, f15, f1h, f4h in raw:
        if pair == BTC_PAIR or (args.pair and pair != args.pair):
            continue
        if f4h is None:               # resolved rows only
            continue
        total += 1
        alt_r = trailing_ret(ts_by[pair], px_by[pair], ts, px)
        btc_r = nearest(btc_ts, btc_r1h_vals, ts, BTC_MATCH_TOL)
        if alt_r is None or btc_r is None:
            skipped += 1
            continue
        div = alt_r - btc_r
        band = band_of(div)
        if band is None:
            continue
        regime = ("IDIO" if abs(btc_r) < IDIO_BTC_LIMIT else
                  "SYST" if btc_r <= SYST_BTC_LIMIT else "OTHER")
        grid[(regime, band)].append((f15, f1h, f4h))
        if regime == "IDIO" and div <= args.div:
            per_pair[pair].append((f15, f1h, f4h))

    print("=" * 78)
    print("THORN THESIS: IDIOSYNCRATIC DIVERGENCE SNAP-BACK")
    print(f"fee bar (round-trip): {FEE_BAR:.2f}%   |   resolved alt obs: {total}"
          f"   |   skipped (no 1h anchor): {skipped}")
    print("=" * 78)

    baseline = stats([r for rows in grid.values() for r in rows], 2)
    if baseline:
        print(f"\nUNCONDITIONAL BASELINE  n={baseline['n']}  "
              f"mean fwd_4h={fmt(baseline['mean'])}%  median={fmt(baseline['median'])}%")

    for regime, label in (("IDIO", f"IDIOSYNCRATIC  (|btc_1h| < {IDIO_BTC_LIMIT}%)  — THE THESIS"),
                          ("SYST", f"SYSTEMIC PLACEBO  (btc_1h <= {SYST_BTC_LIMIT}%)  — must NOT match")):
        print("\n" + "-" * 78)
        print(label)
        print(f"{'divergence band':22s} {'n':>5s}  " +
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

    # headline verdict
    head = [r for (reg, band), rows in grid.items() if reg == "IDIO"
            for r in rows if band in ("div <= -3.0", "-3.0 < div <= -2.0")] \
        if args.div == -2.0 else \
        [r for p in per_pair for r in per_pair[p]]
    hs = stats(head, 2)
    ps = stats([r for rows in (grid.get(("SYST", b), []) for b in
                ("div <= -3.0", "-3.0 < div <= -2.0")) for r in rows], 2)

    print("\n" + "=" * 78)
    print(f"HEADLINE BAND: IDIO & div <= {args.div}%   (fwd_4h)")
    if hs is None:
        print("VERDICT: INSUFFICIENT — zero rows in the signal band.")
        return
    print(f"  n={hs['n']}  mean={fmt(hs['mean'])}%  median={fmt(hs['median'])}%  "
          f"hit={hs['hit']:.0f}%  clears-fee={hs['clear']:.0f}%  p10={fmt(hs['p10'])}%")
    if ps:
        print(f"  placebo (SYST same bands): n={ps['n']}  mean={fmt(ps['mean'])}%  "
              f"median={fmt(ps['median'])}%")

    print("\nPER-PAIR (headline band):")
    multi_pair_pos = 0
    for pair in sorted(per_pair):
        s = stats(per_pair[pair], 2)
        if s:
            flag = "+" if s['mean'] > 0 else "-"
            multi_pair_pos += 1 if s['mean'] > 0 else 0
            print(f"  {flag} {pair:10s} n={s['n']:4d}  mean={fmt(s['mean'])}%  "
                  f"median={fmt(s['median'])}%  hit={s['hit']:.0f}%")

    print("\nVERDICT:")
    if hs['n'] < 100:
        print(f"  INSUFFICIENT — n={hs['n']} < 100. Park; re-run when the tape is deeper.")
    elif hs['mean'] <= FEE_BAR:
        print(f"  KILL — mean fwd_4h {hs['mean']:+.3f}% does not clear the "
              f"{FEE_BAR:.2f}% fee bar.")
    elif ps and hs['mean'] <= (ps['mean'] or 0):
        print("  KILL — idiosyncratic band does not beat the systemic placebo: "
              "the bounce is volatility clustering, not divergence.")
    elif hs['median'] <= 0 or hs['hit'] < 50:
        print("  KILL — mean clears but median/hit-rate says knife-catching "
              "(a few lucky bounces dragging the average).")
    elif multi_pair_pos < 2:
        print("  KILL — effect concentrated in a single pair; not a structural edge.")
    else:
        print(f"  KEEP (provisional) — clears fee bar, beats placebo, median-positive, "
              f"multi-pair. Next: per-week stability check before any code is written.")


if __name__ == "__main__":
    main()
