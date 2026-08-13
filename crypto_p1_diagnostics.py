#!/usr/bin/env python3
"""
crypto_p1_diagnostics.py — READ-ONLY, NON-VERDICT. Characterizes the D1
KEEP cell from the Aug 13 P1 batch before Phase 2 shadow design.
NOT a thesis script: no gates, no verdicts, nothing banked from this.
Questions it answers (per the batch-day caveat):
  1. EPISODES — the cell is 5-min rows; how many independent capitulation
     events is that really? (rows <=2h apart in the same pair = 1 episode)
  2. EPISODE-LEVEL EDGE — mean of per-episode mean forwards: the honest
     effective-n read of D1's +1.67%.
  3. CONCENTRATION — per-pair and per-day: is this one coin / one crash?
  4. D1 ∩ D3 OVERLAP — do the two KEEPs select the same rows? (>70% of
     the smaller cell => treat as ONE finding; shadow implements D1 only)
Frame reproduced EXACTLY from crypto_p1_thesis.py so cell membership
matches the batch run (same filters, same decile edges).

USAGE:  python3 crypto_p1_diagnostics.py
"""
import os
import sys
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:
    print("psycopg2 missing — run on a NEXUS console.")
    sys.exit(1)

H4, H24, H48, TOL = 14400, 86400, 172800, 900
WORST_HOURS = {20}
EPISODE_GAP = 7200   # rows <=2h apart in the same pair = same episode


def day_str(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def episodes(rows, gap=EPISODE_GAP):
    """Group (pair-sorted, ts-sorted) rows into episodes per pair."""
    by_pair = defaultdict(list)
    for r in rows:
        by_pair[r["pair"]].append(r)
    eps = []
    for pair, rs in by_pair.items():
        rs.sort(key=lambda r: r["ts"])
        cur = [rs[0]]
        for r in rs[1:]:
            if r["ts"] - cur[-1]["ts"] <= gap:
                cur.append(r)
            else:
                eps.append(cur)
                cur = [r]
        eps.append(cur)
    return eps


def main():
    db = os.environ.get("DATABASE_URL", "")
    if not db:
        print("DATABASE_URL not set."); sys.exit(1)
    conn = psycopg2.connect(db, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ts, pair, price, hour_cdt, funding
            FROM crypto_thorn_observations
            WHERE price > 0 ORDER BY pair, ts
        """)
        rows = cur.fetchall()
    conn.close()
    print(f"loaded {len(rows)} observations")

    by_ts, by_px = defaultdict(list), defaultdict(list)
    for ts, pair, px, hour, fund in rows:
        by_ts[pair].append(ts)
        by_px[pair].append(px)

    def px_at(pair, ts, offset):
        arr = by_ts[pair]
        t = ts + offset
        i = bisect_left(arr, t - TOL)
        if i < len(arr) and arr[i] <= t + TOL:
            return by_px[pair][i]
        return None

    R = []
    for ts, pair, px, hour, fund in rows:
        pf = px_at(pair, ts, H48)
        pb = px_at(pair, ts, -H48)
        R.append({"ts": ts, "pair": pair, "px": px, "hour": hour,
                  "fund": fund,
                  "f48": (pf / px - 1) * 100 if pf else None,
                  "t48": (px / pb - 1) * 100 if pb else None})

    # frame: identical to crypto_p1_thesis.py
    funds = sorted(r["fund"] for r in R if r["fund"] is not None)
    fund_p90 = funds[int(len(funds) * 0.9)] if funds else None
    pool = [r for r in R
            if r["f48"] is not None and r["t48"] is not None
            and r["hour"] not in WORST_HOURS
            and not (fund_p90 is not None and r["fund"] is not None
                     and r["fund"] >= fund_p90)]
    print(f"frame: pool n={len(pool)} (must match batch run)")

    # D1 bottom decile (same edge computation as decile_cells)
    tvals = sorted(r["t48"] for r in pool)
    d1_edge = tvals[len(tvals) // 10]
    cell = [r for r in pool if r["t48"] <= d1_edge]
    cmean = sum(r["f48"] for r in cell) / len(cell)
    print(f"D1 cell: n={len(cell)} rows | trail-48h <= {d1_edge:+.2f}% | "
          f"row-level mean fwd {cmean:+.3f}% (batch printed +1.673%)")

    # 1+2: episodes and episode-level edge
    print("=" * 74)
    eps = episodes(cell)
    ep_means = [sum(r["f48"] for r in e) / len(e) for e in eps]
    ep_pos = sum(1 for m in ep_means if m > 0)
    sizes = sorted((len(e) for e in eps), reverse=True)
    print(f"1. EPISODES: {len(eps)} independent events from {len(cell)} rows")
    print(f"   sizes: max={sizes[0]} rows | median={sizes[len(sizes)//2]} | "
          f"top-5 hold {sum(sizes[:5])*100//len(cell)}% of all cell rows")
    print(f"2. EPISODE-LEVEL EDGE: mean-of-episode-means "
          f"{sum(ep_means)/len(ep_means):+.3f}% | "
          f"{ep_pos}/{len(eps)} episodes positive ({ep_pos*100//len(eps)}%)")
    print(f"   (this is the honest effective-n read; if it diverges far "
          f"from the row-level mean, big episodes are carrying the cell)")

    # 3: concentration
    print("=" * 74)
    print("3. CONCENTRATION")
    per_pair = defaultdict(list)
    for r in cell:
        per_pair[r["pair"]].append(r["f48"])
    for pair, v in sorted(per_pair.items(), key=lambda kv: -len(kv[1])):
        print(f"   {pair:10s} rows={len(v):5d} ({len(v)*100//len(cell):2d}%)  "
              f"mean fwd {sum(v)/len(v):+.3f}%")
    per_day = defaultdict(int)
    for r in cell:
        per_day[day_str(r["ts"])] += 1
    top_days = sorted(per_day.items(), key=lambda kv: -kv[1])[:5]
    top5_share = sum(c for _, c in top_days) * 100 // len(cell)
    print(f"   date span: {min(per_day)} -> {max(per_day)} | "
          f"{len(per_day)} distinct days | top-5 days hold {top5_share}% of rows:")
    for d, c in top_days:
        print(f"     {d}: {c} rows")

    # 4: D1 ∩ D3
    print("=" * 74)
    btc_t48 = {r["ts"]: r["t48"] for r in R if r["pair"] == "BTC-USDC"}
    p3 = []
    for r in pool:
        if r["pair"] == "BTC-USDC":
            continue
        b = btc_t48.get(r["ts"])
        if b is None:
            arr = by_ts["BTC-USDC"]
            i = bisect_left(arr, r["ts"] - TOL)
            if i < len(arr) and arr[i] <= r["ts"] + TOL:
                b = btc_t48.get(arr[i])
        if b is None:
            continue
        p3.append((r, r["t48"] - b))
    rels = sorted(rel for _, rel in p3)
    d3_edge = rels[len(rels) // 10]
    d3_set = {(r["pair"], r["ts"]) for r, rel in p3 if rel <= d3_edge}
    d1_set = {(r["pair"], r["ts"]) for r in cell}
    ov = len(d1_set & d3_set) * 100 // min(len(d1_set), len(d3_set))
    one = ov > 70
    print(f"4. D1 ∩ D3 OVERLAP: {ov}% of the smaller cell "
          f"{'-> ONE finding; shadow implements D1 only' if one else '-> independent; shadow design may use both'}")

    print("=" * 74)
    print("Diagnostics only — nothing here is banked or verdict-bearing.")
    print("Feeds the Phase 2 shadow pre-registration per CRYPTO_REVIVAL_PLAN.md.")


if __name__ == "__main__":
    main()
