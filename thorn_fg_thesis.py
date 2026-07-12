#!/usr/bin/env python3
"""
thorn_fg_thesis.py — READ-ONLY thesis query against Thorn's tape.

THESIS UNDER TEST
-----------------
"Extreme Fear marks local bottoms: when F&G is low, forward returns over
the next 1h/4h are positive by more than a round-trip fee."

Named loser: panic sellers dumping into fear.
Why it might survive: retail capitulation is emotional and slow to reverse;
the 2.4% fee bar keeps most bots out, so the trade stays uncrowded.
Kill condition: forward returns flat/negative, OR positive but under the
fee bar (a real-but-worthless edge — the same trap that killed the raw
indicator strategy).

WHAT IT DOES
------------
Buckets every RESOLVED Thorn observation by F&G band, and reports the mean
and hit-rate of forward returns at 15m / 1h / 4h. Compares each band's
forward return against the round-trip fee so you can see, at a glance,
whether Extreme Fear entries would have cleared costs.

This script only SELECTs. It never writes, never trades, never touches
gates. Safe to run against the live DB.

USAGE
-----
  DATABASE_URL=postgres://... CB_TAKER_FEE_PCT=0.012 python3 thorn_fg_thesis.py
  # (DATABASE_URL is already set on any NEXUS service — run it from the
  #  crypto service's Railway console and it inherits the env.)

  Optional:
    --pair BTC-USDC     restrict to one pair (default: all)
    --horizon 1h        focus one horizon in the verdict (default: 4h)
"""
import os
import sys
import argparse

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. On the Railway console it's already present; "
          "locally: pip install psycopg2-binary")
    sys.exit(1)

TAKER = float(os.environ.get("CB_TAKER_FEE_PCT", "0.012"))
ROUND_TRIP_PCT = 2 * TAKER * 100          # percent, matches crypto.py
FEE_BAR = ROUND_TRIP_PCT                   # a long must clear this to net positive

# F&G bands. "Extreme Fear" is the thesis's target band.
BANDS = [
    ("Extreme Fear",  0, 25),
    ("Fear",         26, 45),
    ("Neutral",      46, 55),
    ("Greed",        56, 75),
    ("Extreme Greed",76, 100),
]

HORIZONS = [("fwd_15m", "15m"), ("fwd_1h", "1h"), ("fwd_4h", "4h")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default=None, help="restrict to one pair, e.g. BTC-USDC")
    ap.add_argument("--horizon", default="4h", choices=["15m", "1h", "4h"],
                    help="horizon used for the headline verdict")
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set. Run from a NEXUS service console, or export it.")
        sys.exit(1)

    where = ["fwd_4h IS NOT NULL", "fg IS NOT NULL"]   # resolved rows only
    params = []
    if args.pair:
        where.append("pair = %s")
        params.append(args.pair)
    where_sql = " AND ".join(where)

    try:
        conn = psycopg2.connect(db_url, connect_timeout=10)
    except Exception as e:
        print(f"DB connect failed: {e}")
        sys.exit(1)

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM crypto_thorn_observations WHERE {where_sql}", params)
        total = cur.fetchone()[0]
        cur.execute(f"SELECT MIN(ts), MAX(ts) FROM crypto_thorn_observations WHERE {where_sql}", params)
        tmin, tmax = cur.fetchone()

    print("=" * 74)
    print("THORN F&G-BOTTOM THESIS  —  read-only")
    print("=" * 74)
    scope = args.pair if args.pair else "ALL pairs"
    print(f"Scope: {scope} | resolved observations: {total:,}")
    if total == 0:
        print("\nNo RESOLVED rows yet (need observations older than 4h with fwd_4h "
              "filled). Thorn resolves every 5 min; check back once the tape has "
              ">4h of history under this filter.")
        conn.close()
        return
    if tmin and tmax:
        span_days = (tmax - tmin) / 86400.0
        print(f"Tape span: ~{span_days:.1f} days")
    print(f"Round-trip fee bar: {FEE_BAR:.2f}%  (a long's forward return must "
          f"exceed this to net positive)")
    print("=" * 74)

    # Per-band stats for every horizon
    for band_label, lo, hi in BANDS:
        band_where = where_sql + " AND fg >= %s AND fg <= %s"
        band_params = params + [lo, hi]
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM crypto_thorn_observations WHERE {band_where}",
                        band_params)
            n = cur.fetchone()[0]
        if n == 0:
            print(f"\n{band_label:14s} (F&G {lo}-{hi}) — no observations")
            continue

        print(f"\n{band_label:14s} (F&G {lo}-{hi}) — n={n:,}")
        for col, hlabel in HORIZONS:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT AVG({col}),
                           AVG(CASE WHEN {col} > 0 THEN 1.0 ELSE 0.0 END),
                           AVG(CASE WHEN {col} > %s THEN 1.0 ELSE 0.0 END),
                           COUNT({col})
                    FROM crypto_thorn_observations
                    WHERE {band_where} AND {col} IS NOT NULL
                """, [FEE_BAR] + band_params)
                avg_ret, hit_pos, hit_fee, cnt = cur.fetchone()
            if not cnt:
                print(f"    {hlabel:4s}: (unresolved)")
                continue
            avg_ret = avg_ret or 0.0
            hit_pos = (hit_pos or 0) * 100
            hit_fee = (hit_fee or 0) * 100
            clears = "✅ clears fee" if avg_ret > FEE_BAR else \
                     ("+ but under fee" if avg_ret > 0 else "✗ negative")
            print(f"    {hlabel:4s}: avg {avg_ret:+.3f}% | "
                  f">0: {hit_pos:4.1f}% | >fee: {hit_fee:4.1f}% | "
                  f"n={cnt:,} | {clears}")

    # Verdict on the target band + chosen horizon
    hcol = {"15m": "fwd_15m", "1h": "fwd_1h", "4h": "fwd_4h"}[args.horizon]
    ef_where = where_sql + " AND fg <= 25"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT AVG({hcol}),
                   AVG(CASE WHEN {hcol} > %s THEN 1.0 ELSE 0.0 END),
                   COUNT({hcol})
            FROM crypto_thorn_observations
            WHERE {ef_where} AND {hcol} IS NOT NULL
        """, [FEE_BAR] + params)
        ef_avg, ef_hitfee, ef_n = cur.fetchone()

    print("\n" + "=" * 74)
    print(f"VERDICT — Extreme Fear (F&G ≤ 25), {args.horizon} forward")
    print("=" * 74)
    if not ef_n:
        print("Not enough resolved Extreme-Fear observations yet. Let the tape fill.")
    else:
        ef_avg = ef_avg or 0.0
        ef_hitfee = (ef_hitfee or 0) * 100
        print(f"n = {ef_n:,} | avg forward return = {ef_avg:+.3f}% | "
              f"fee bar = {FEE_BAR:.2f}%")
        print(f"Share of Extreme-Fear entries that cleared the fee: {ef_hitfee:.1f}%")
        print("-" * 74)
        if ef_avg > FEE_BAR:
            print("KEEP (provisional): Extreme Fear shows a forward edge that clears")
            print("fees on average. Worth a closer look — check it holds per-pair and")
            print("isn't driven by one violent window. This is a lead, not a green light.")
        elif ef_avg > 0:
            print("KILL: real but worthless. Forward returns are positive but under the")
            print("fee bar — same trap as the raw indicator strategy. No tradeable edge")
            print("here at the 1.2%/side tier.")
        else:
            print("KILL: no edge. Extreme Fear does NOT mark bottoms in this tape —")
            print("forward returns are flat/negative. Thesis dead; cost you nothing.")
    print("=" * 74)
    print("Reminder: this is ONE band on ONE feature. A clean KILL is a good outcome —")
    print("it retires a hypothesis cheaply. A KEEP needs per-pair + per-window checks")
    print("before it ever becomes code.")

    conn.close()


if __name__ == "__main__":
    main()
