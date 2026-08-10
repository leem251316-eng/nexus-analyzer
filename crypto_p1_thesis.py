#!/usr/bin/env python3
"""
crypto_p1_thesis.py — READ-ONLY. Crypto Phase 1 discovery batch (D1–D4),
executed per CRYPTO_P1_PREREGISTRATION.md (committed before this run;
gates ratified Jul 29 2026 with the absolute-positive floor). Verdicts bind.

THE FRAME (applied to every D1–D3 cell):
  - candidate pool excludes funding top-decile rows and the worst-hour
    block (20:00 CDT), per the banked A4 / hour-thesis filters
  - horizon: 48h primary (24h reported for shape)
  - fee bars: 1.6% effective maker (primary), 2.4% taker (robustness)
  - KEEP gate per thesis: cell mean >= filtered-pool baseline + 0.60
    AND cell mean > 0 absolute AND fee clearance(1.6) >= 30% AND
    n >= 300 AND monotone ordering (rank-corr <= -0.60 across bins)
    AND real effect exceeds max of 20 shuffled placebos
  - D4 (filter, own gate): both |btc_ret_5m|>0.15% tails underperform
    flat by >= 0.15% at BOTH 4h and 24h, raw pool (matches the parked
    Jul 29 observation census)
  - D1/D2 redundancy: >70% row overlap of selected cells = ONE finding

USAGE:  python3 crypto_p1_thesis.py
"""
import os
import sys
import random
from bisect import bisect_left
from collections import defaultdict

try:
    import psycopg2
except ImportError:
    print("psycopg2 missing — run on a NEXUS console.")
    sys.exit(1)

FEE_MAKER, FEE_TAKER = 1.60, 2.40
GATE_EDGE, GATE_CLEAR, GATE_N, GATE_MONO = 0.60, 30.0, 300, -0.60
H1, H4, H24, H48, TOL = 3600, 14400, 86400, 172800, 900
WORST_HOURS = {20}
N_PLACEBO = 20
random.seed(20260729)   # registration date — deterministic placebos


def stats(v, bar=None):
    if not v:
        return None
    v = sorted(v)
    n = len(v)
    out = {"n": n, "mean": sum(v) / n, "med": v[n // 2],
           "pos": sum(1 for x in v if x > 0) * 100.0 / n}
    if bar is not None:
        out["clear"] = sum(1 for x in v if x > bar) * 100.0 / n
    return out


def line(tag, s, bar=False):
    if not s:
        return f"  {tag:30s} n=0"
    extra = f"  >fee={s['clear']:4.1f}%" if bar and "clear" in s else ""
    return (f"  {tag:30s} n={s['n']:6d}  mean={s['mean']:+.3f}%  "
            f"med={s['med']:+.3f}%  pos={s['pos']:4.1f}%{extra}")


def rank_corr(xs, ys):
    """Spearman on small vectors (bin index vs bin mean)."""
    def ranks(a):
        order = sorted(range(len(a)), key=lambda i: a[i])
        r = [0.0] * len(a)
        for rank, i in enumerate(order):
            r[i] = float(rank)
        return r
    if len(xs) < 3:
        return 0.0
    rx, ry = ranks(xs), ranks(ys)
    mx, my = sum(rx) / len(rx), sum(ry) / len(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = sum((a - mx) ** 2 for a in rx) ** 0.5
    dy = sum((b - my) ** 2 for b in ry) ** 0.5
    return num / (dx * dy) if dx > 0 and dy > 0 else 0.0


def rsi_from_points(pts):
    """Wilder-lite RSI from a list of equally-spaced closes (oldest first)."""
    if len(pts) < 8:
        return None
    gains = losses = 0.0
    for a, b in zip(pts, pts[1:]):
        d = b - a
        if d > 0:
            gains += d
        else:
            losses -= d
    if gains + losses == 0:
        return 50.0
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - 100 / (1 + rs)


def decile_cells(pool, key, fwd_key, bar):
    """Split pool into deciles of key; return (cells, means, labels)."""
    vals = sorted(r[key] for r in pool)
    n = len(vals)
    edges = [vals[int(n * k / 10)] for k in range(1, 10)]
    def dec(x):
        return bisect_left(edges, x)
    cells = defaultdict(list)
    for r in pool:
        cells[dec(r[key])].append(r[fwd_key])
    out, means = [], []
    for d in range(10):
        s = stats(cells.get(d, []), bar)
        out.append(s)
        means.append(s["mean"] if s else 0.0)
    return out, means, cells


def gate_verdict(name, cell, base, mono_rc, placebo_max_edge):
    """Apply the ratified KEEP gate; return (verdict_str, passed)."""
    if not cell:
        return f"VERDICT {name}: PARK — selected cell empty (instrument)", False
    checks = {
        f"edge {cell['mean']-base:+.2f} >= +{GATE_EDGE:.2f}": cell["mean"] - base >= GATE_EDGE,
        f"abs {cell['mean']:+.2f} > 0": cell["mean"] > 0,
        f"clear {cell.get('clear',0):.1f} >= {GATE_CLEAR:.0f}": cell.get("clear", 0) >= GATE_CLEAR,
        f"n {cell['n']} >= {GATE_N}": cell["n"] >= GATE_N,
        f"mono rc {mono_rc:+.2f} <= {GATE_MONO:+.2f}": mono_rc <= GATE_MONO,
        f"placebo max {placebo_max_edge:+.2f} < edge": placebo_max_edge < cell["mean"] - base,
    }
    fails = [k for k, ok in checks.items() if not ok]
    detail = " | ".join(checks.keys())
    if not fails:
        return f"VERDICT {name}: KEEP — all gates pass ({detail})", True
    return f"VERDICT {name}: KILL — failed: {'; '.join(fails)}", False


def main():
    db = os.environ.get("DATABASE_URL", "")
    if not db:
        print("DATABASE_URL not set."); sys.exit(1)
    conn = psycopg2.connect(db, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ts, pair, price, hour_cdt, funding, btc_ret_5m
            FROM crypto_thorn_observations
            WHERE price > 0 ORDER BY pair, ts
        """)
        rows = cur.fetchall()
    conn.close()
    print(f"loaded {len(rows)} observations")

    by_ts, by_px = defaultdict(list), defaultdict(list)
    for ts, pair, px, hour, fund, btc5 in rows:
        by_ts[pair].append(ts)
        by_px[pair].append(px)

    def px_at(pair, ts, offset):
        arr = by_ts[pair]
        t = ts + offset
        i = bisect_left(arr, t - TOL)
        if i < len(arr) and arr[i] <= t + TOL:
            return by_px[pair][i]
        return None

    def ret(pair, ts, px, offset):
        p = px_at(pair, ts, offset)
        return (p / px - 1) * 100 if (p and offset > 0) else \
               ((px / p - 1) * 100 if p else None)

    # decorate: forwards, trailing 48h, BTC trailing 48h, slow RSI (14x1h)
    R = []
    for ts, pair, px, hour, fund, btc5 in rows:
        f24 = ret(pair, ts, px, H24)
        f48 = ret(pair, ts, px, H48)
        t48 = ret(pair, ts, px, -H48)          # trailing 48h return
        hourly = [px_at(pair, ts, -k * H1) for k in range(14, 0, -1)] + [px]
        hourly = [p for p in hourly if p]
        rsi_s  = rsi_from_points(hourly) if len(hourly) >= 12 else None
        R.append({"ts": ts, "pair": pair, "px": px, "hour": hour,
                  "fund": fund, "btc5": btc5, "f4": ret(pair, ts, px, H4),
                  "f24": f24, "f48": f48, "t48": t48, "rsi_s": rsi_s})

    btc_t48 = {r["ts"]: r["t48"] for r in R if r["pair"] == "BTC-USDC"}

    # ---- THE FRAME: candidate pool for D1-D3 ----
    funds = sorted(r["fund"] for r in R if r["fund"] is not None)
    fund_p90 = funds[int(len(funds) * 0.9)] if funds else None
    pool = [r for r in R
            if r["f48"] is not None and r["t48"] is not None
            and r["hour"] not in WORST_HOURS
            and not (fund_p90 is not None and r["fund"] is not None
                     and r["fund"] >= fund_p90)]
    base_raw  = stats([r["f48"] for r in R if r["f48"] is not None])
    base_pool = stats([r["f48"] for r in pool])
    print(f"frame: pool n={len(pool)} (funding top-decile + hour-20 excluded)")
    print(f"baseline 48h: raw {base_raw['mean']:+.3f}%  |  "
          f"filtered pool {base_pool['mean']:+.3f}%  (gates use filtered)")
    BASE = base_pool["mean"]

    def placebo_max(pool_, key, fwd_key):
        """Max bottom-decile edge over N shuffles of the conditioning var."""
        vals = [r[key] for r in pool_]
        worst = -999.0
        for _ in range(N_PLACEBO):
            random.shuffle(vals)
            tmp = [{key: v, fwd_key: r[fwd_key]} for v, r in zip(vals, pool_)]
            _, means, cells = decile_cells(tmp, key, fwd_key, FEE_MAKER)
            s0 = stats(cells.get(0, []))
            if s0:
                worst = max(worst, s0["mean"] - BASE)
        return worst

    # ================= D1: multi-day mean reversion =================
    print("=" * 74)
    print("D1. MULTI-DAY MEAN REVERSION (trailing-48h return deciles -> 48h fwd)")
    d1_cells, d1_means, d1_raw = decile_cells(pool, "t48", "f48", FEE_MAKER)
    for d in range(10):
        print(line(f"trail-48h decile {d} {'(worst)' if d==0 else '(best)' if d==9 else ''}",
                   d1_cells[d], bar=True))
    d1_rc = rank_corr(list(range(10)), d1_means)
    d1_pl = placebo_max(pool, "t48", "f48")
    v, d1_keep = gate_verdict("D1", d1_cells[0], BASE, d1_rc, d1_pl)
    print(f"  monotone rank-corr {d1_rc:+.2f} | placebo max edge {d1_pl:+.2f}")
    print(f"  taker robustness: bottom-decile clear(2.4%) = "
          f"{stats([x for x in d1_raw.get(0, [])], FEE_TAKER)['clear'] if d1_raw.get(0) else 0:.1f}%")
    print(f"  {v}")

    # ================= D2: slow RSI extremes =================
    print("=" * 74)
    print("D2. SLOW RSI EXTREMES (14x1h RSI bands -> 48h fwd)")
    p2 = [r for r in pool if r["rsi_s"] is not None]
    bands = [("rsi<=30", lambda x: x <= 30), ("30-45", lambda x: 30 < x <= 45),
             ("45-55", lambda x: 45 < x <= 55), ("55-70", lambda x: 55 < x <= 70),
             ("rsi>70", lambda x: x > 70)]
    band_stats, band_means = [], []
    d2_sel_rows = []
    for nm, f in bands:
        rows_b = [r for r in p2 if f(r["rsi_s"])]
        if nm == "rsi<=30":
            d2_sel_rows = rows_b
        s = stats([r["f48"] for r in rows_b], FEE_MAKER)
        band_stats.append(s)
        band_means.append(s["mean"] if s else 0.0)
        print(line(nm, s, bar=True))
    d2_rc = rank_corr(list(range(len(bands))), band_means)
    # placebo: shuffle rsi across p2
    vals = [r["rsi_s"] for r in p2]
    d2_pl = -999.0
    for _ in range(N_PLACEBO):
        random.shuffle(vals)
        cell = [r["f48"] for v_, r in zip(vals, p2) if v_ <= 30]
        s = stats(cell)
        if s:
            d2_pl = max(d2_pl, s["mean"] - BASE)
    v, d2_keep = gate_verdict("D2", band_stats[0], BASE, d2_rc, d2_pl)
    print(f"  monotone rank-corr {d2_rc:+.2f} | placebo max edge {d2_pl:+.2f}")
    print(f"  {v}")

    # D1/D2 redundancy — bottom-decile membership overlap
    tvals = sorted(r["t48"] for r in pool)
    edge0 = tvals[len(tvals) // 10]
    d1_set = {(r["pair"], r["ts"]) for r in pool if r["t48"] <= edge0}
    d2_set = {(r["pair"], r["ts"]) for r in d2_sel_rows}
    if d1_set and d2_set:
        ov = len(d1_set & d2_set) / min(len(d1_set), len(d2_set)) * 100
        print(f"  D1/D2 overlap: {ov:.0f}% of smaller cell "
              f"{'-> COUNT AS ONE FINDING' if ov > 70 else '(independent)'}")

    # ================= D3: cross-pair relative strength =================
    print("=" * 74)
    print("D3. CROSS-PAIR RELATIVE STRENGTH (alt t48 minus BTC t48, deciles -> 48h fwd)")
    p3 = []
    for r in pool:
        if r["pair"] == "BTC-USDC":
            continue
        b = btc_t48.get(r["ts"])
        if b is None:
            # nearest BTC ts within TOL
            arr = by_ts["BTC-USDC"]
            i = bisect_left(arr, r["ts"] - TOL)
            b = None
            if i < len(arr) and arr[i] <= r["ts"] + TOL:
                bts = arr[i]
                b = btc_t48.get(bts)
        if b is None:
            continue
        q = dict(r)
        q["rel"] = r["t48"] - b
        p3.append(q)
    print(f"  alt rows with BTC anchor: {len(p3)}")
    d3_cells, d3_means, d3_raw = decile_cells(p3, "rel", "f48", FEE_MAKER)
    for d in (0, 1, 4, 8, 9):
        print(line(f"rel-strength decile {d}", d3_cells[d], bar=True))
    d3_rc = rank_corr(list(range(10)), d3_means)
    d3_pl = placebo_max(p3, "rel", "f48")
    v, d3_keep = gate_verdict("D3", d3_cells[0], BASE, d3_rc, d3_pl)
    print(f"  monotone rank-corr {d3_rc:+.2f} | placebo max edge {d3_pl:+.2f}")
    print(f"  {v}")

    # ================= D4: BTC-vol avoid filter =================
    print("=" * 74)
    print("D4. BTC-VOL AVOID FILTER (|btc_ret_5m|>0.15% tails vs flat, raw pool)")
    alts = [r for r in R if r["pair"] != "BTC-USDC" and r["btc5"] is not None]
    verdict_parts = []
    ok_all = True
    for hz, key in (("4h", "f4"), ("24h", "f24")):
        up = stats([r[key] for r in alts if r[key] is not None and r["btc5"] * 100 > 0.15])
        dn = stats([r[key] for r in alts if r[key] is not None and r["btc5"] * 100 < -0.15])
        fl = stats([r[key] for r in alts if r[key] is not None and -0.15 <= r["btc5"] * 100 <= 0.15])
        print(line(f"{hz} up-tail", up)); print(line(f"{hz} flat", fl)); print(line(f"{hz} down-tail", dn))
        if not (up and dn and fl):
            ok_all = False
            verdict_parts.append(f"{hz}: empty cell")
            continue
        u_ok = fl["mean"] - up["mean"] >= 0.15
        d_ok = fl["mean"] - dn["mean"] >= 0.15
        verdict_parts.append(f"{hz}: up {'PASS' if u_ok else 'FAIL'} "
                             f"({fl['mean']-up['mean']:+.2f}), down {'PASS' if d_ok else 'FAIL'} "
                             f"({fl['mean']-dn['mean']:+.2f})")
        ok_all = ok_all and u_ok and d_ok
    print(f"  {' | '.join(verdict_parts)}")
    print(f"  VERDICT D4: {'KEEP — joins the frame as a baseline filter' if ok_all else 'KILL — a tail fails a horizon'}")

    # ================= program clock =================
    print("=" * 74)
    keeps = sum([d1_keep, d2_keep, d3_keep])
    print(f"BATCH 1 RESULT: {keeps} entry-edge KEEP(s) among D1-D3 "
          f"(D4 is a filter; does not count toward program survival).")
    if keeps == 0:
        print("Program clock: batch 1 of 2 spent with zero KEEPs. One batch "
              "remains before the kill clause shelves crypto per "
              "CRYPTO_REVIVAL_PLAN.md.")
    else:
        print("Any KEEP -> Phase 2 shadow design precedes implementation. "
              "Lock stays until Phase 4 gates clear.")
    print("Verdicts bind per CRYPTO_P1_PREREGISTRATION.md (committed pre-run).")



if __name__ == "__main__":
    main()
