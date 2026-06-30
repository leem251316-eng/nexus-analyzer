#!/usr/bin/env python3
"""
scanner_bucket_analysis.py V1.0 — ad hoc exploration of scanner_pattern_stats
================================================================================
One-off analysis script, not part of the regular pipeline. Answers the
specific question raised after the first real scanner_backtester.py V1.1 run
(15,365 fingerprints, 915 buckets, Jun 30 2026): MSTZ and MSTU are the two
highest-volume symbols in Scanner's universe and both are net-negative on
their overall average (-0.046%, -0.036%) -- but the win-rate gate operates
on individual BUCKETS, not the symbol-wide average. This script finds out
whether either symbol has specific bucket conditions (volume-ratio band x
price-move band x SPY trend x hour-of-day) that clear WIN_RATE_GATE_THRESHOLD
(0.45) despite the overall symbol being underwater -- and does the same
sweep across every other symbol in the universe, surfacing the strongest and
weakest buckets system-wide.

Usage:
  python scanner_bucket_analysis.py                  # full report, all symbols
  python scanner_bucket_analysis.py --symbol MSTZ     # one symbol's buckets only
  python scanner_bucket_analysis.py --min-samples 5   # raise the noise floor

Environment:
  DATABASE_URL
"""

import os
import sys
import argparse
from collections import defaultdict

import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "")
WIN_RATE_GATE_THRESHOLD = 0.45   # must match scanner.py's live gate

def fetch_buckets():
    """Pull scanner_pattern_stats directly -- this is the EXACT table the
    live win-rate gate reads from, so this is ground truth, not a
    re-derivation that could drift from what's actually deployed."""
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT bucket_key, win_rate, sample_count, avg_pnl
                FROM scanner_pattern_stats
                ORDER BY bucket_key
            """)
            return cur.fetchall()
    finally:
        conn.close()

def parse_bucket_key(key: str) -> dict:
    """bucket_key format: SYMBOL|vol_band|move_band|spy_b|hr_b"""
    parts = key.split("|")
    if len(parts) != 5:
        return None
    symbol, vol_b, move_b, spy_b, hr_b = parts
    return {
        "symbol": symbol, "vol_band": vol_b, "move_band": move_b,
        "spy": spy_b, "hour": hr_b,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default=None,
                         help="Show only this symbol's buckets")
    parser.add_argument("--min-samples", type=int, default=3,
                         help="Minimum sample_count to display (table itself "
                              "only stores buckets >= PM_MIN_BUCKET_TRADES=3, "
                              "this can raise that floor further for display)")
    args = parser.parse_args()

    if not DATABASE_URL:
        print("DATABASE_URL not set")
        sys.exit(1)

    rows = fetch_buckets()
    print(f"Total buckets in scanner_pattern_stats: {len(rows)}\n")

    by_symbol = defaultdict(list)
    for r in rows:
        parsed = parse_bucket_key(r["bucket_key"])
        if not parsed:
            continue
        if r["sample_count"] < args.min_samples:
            continue
        by_symbol[parsed["symbol"]].append({**parsed, **r})

    if args.symbol:
        symbols_to_show = [args.symbol.upper()]
    else:
        # Sort symbols by total sample count descending -- most-traded first,
        # since that's where the gate's decisions matter most in dollar terms.
        symbols_to_show = sorted(
            by_symbol.keys(),
            key=lambda s: -sum(b["sample_count"] for b in by_symbol[s])
        )

    passing_total = 0
    failing_total = 0

    for sym in symbols_to_show:
        buckets = by_symbol.get(sym, [])
        if not buckets:
            print(f"=== {sym}: no buckets >= {args.min_samples} samples ===\n")
            continue

        buckets_sorted = sorted(buckets, key=lambda b: -b["win_rate"])
        total_samples  = sum(b["sample_count"] for b in buckets)
        passing        = [b for b in buckets if b["win_rate"] >= WIN_RATE_GATE_THRESHOLD]
        failing        = [b for b in buckets if b["win_rate"] < WIN_RATE_GATE_THRESHOLD]
        passing_total += len(passing)
        failing_total += len(failing)
        passing_samples = sum(b["sample_count"] for b in passing)

        print(f"=== {sym} — {len(buckets)} buckets, {total_samples} total samples ===")
        print(f"    {len(passing)}/{len(buckets)} buckets clear the {WIN_RATE_GATE_THRESHOLD:.0%} gate "
              f"({passing_samples}/{total_samples} samples, "
              f"{passing_samples/total_samples*100:.1f}% of this symbol's volume)")

        for b in buckets_sorted:
            gate_mark = "✅ PASS" if b["win_rate"] >= WIN_RATE_GATE_THRESHOLD else "🚫 block"
            avg_pnl_s = f"{b['avg_pnl']:+.3f}%" if b["avg_pnl"] is not None else "  n/a "
            print(f"    {gate_mark} | {b['vol_band']:<10} {b['move_band']:<12} "
                  f"{b['spy']:<9} {b['hour']:<8} | n={b['sample_count']:>4} | "
                  f"WR={b['win_rate']*100:>5.1f}% | avgPnL={avg_pnl_s}")
        print()

    print("=" * 70)
    print(f"SYSTEM-WIDE: {passing_total} buckets pass the gate, "
          f"{failing_total} are blocked ({passing_total + failing_total} total)")
    print("=" * 70)


if __name__ == "__main__":
    main()
