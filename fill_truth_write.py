#!/usr/bin/env python3
"""
fill_truth_write.py  V1.0  -- the ONE registered write  (Sep 24 2026)
=====================================================================
Adds pnl_pct_broker (REAL) and entry_price_broker (REAL) to
berserker_trade_fingerprints and fills them for every live fingerprint
that fill_truth.py can match to an Alpaca round trip. pnl_pct is NEVER
touched. Idempotent: re-running rewrites the same broker values.

  python3 fill_truth_write.py            # DRY RUN: shows what would change
  python3 fill_truth_write.py --write    # applies it, then re-reads and
                                         # verifies the count

Approved by Matthew Sep 24 2026 after fill_truth.py V1.1 matched 311/311
rows with 7 off (2 flips) and a 0.003%/trade expectancy gap. Research
scripts switch to COALESCE(pnl_pct_broker, pnl_pct) from here on.
Requires fill_truth.py (same directory) -- fetch it first if missing.
An audit line is written to nexus_config (key fill_truth_write).
"""
import os
import sys
import json
import time
import argparse
from datetime import date

import psycopg2

try:
    from fill_truth import fetch_fills, round_trips, load_fps, MATCH_S, TOL
except ImportError:
    sys.exit("fill_truth.py not found next to this script -- fetch it first:\n"
             "python3 -c \"import urllib.request as u; u.urlretrieve('https://raw.githubusercontent.com/"
             "leem251316-eng/nexus-analyzer/main/fill_truth.py?nocache=1','fill_truth.py')\"")

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def match(fps, trips):
    used, pairs = set(), []
    for fp in fps:
        best, bd = None, None
        for i, tr in enumerate(trips):
            if i in used or tr["sym"] != fp["sym"] or tr["t0"] is None:
                continue
            d = abs((tr["t0"] - fp["t0"]).total_seconds())
            if d <= MATCH_S and (bd is None or d < bd):
                best, bd = i, d
        if best is not None:
            used.add(best)
            pairs.append((fp, trips[best]))
    return pairs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-07-29")
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()
    if not DATABASE_URL:
        sys.exit("DATABASE_URL missing.")
    since = date.fromisoformat(a.since)

    fills, which = fetch_fills(since)
    trips, _ = round_trips(fills)
    fps = load_fps(since)
    pairs = match(fps, trips)
    off = [(fp, tr) for fp, tr in pairs
           if abs(tr["pnl"] - fp["pnl"]) >= TOL or (tr["pnl"] > 0) != (fp["pnl"] > 0)]
    print(f"FILL TRUTH WRITE V1.0 | {'WRITE' if a.write else 'DRY RUN'} | since {since} | key {which}")
    print(f"  fingerprints {len(fps)} | matched {len(pairs)} | unmatched {len(fps) - len(pairs)} | "
          f"rows that change materially (>= {TOL} or flip): {len(off)}")
    for fp, tr in off:
        print(f"    {fp['sym']} {fp['t0'].strftime('%m-%d %H:%M')} {fp['type']}: "
              f"pnl_pct {fp['pnl']:+.2f} -> pnl_pct_broker {tr['pnl']:+.2f}")
    if not a.write:
        print("\nDry run. Nothing written. Re-run with --write to apply.")
        return
    if len(pairs) < len(fps) * 0.95:
        sys.exit(f"Refusing to write: only {len(pairs)}/{len(fps)} matched (< 95%). Investigate first.")

    conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE berserker_trade_fingerprints
                    ADD COLUMN IF NOT EXISTS pnl_pct_broker REAL,
                    ADD COLUMN IF NOT EXISTS entry_price_broker REAL
            """)
            n = 0
            for fp, tr in pairs:
                cur.execute("""
                    UPDATE berserker_trade_fingerprints
                    SET pnl_pct_broker = %s, entry_price_broker = %s
                    WHERE trade_id = %s
                """, (round(float(tr["pnl"]), 3), round(float(tr["entry"]), 4), fp["id"]))
                n += cur.rowcount
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nexus_config (
                    key VARCHAR(100) PRIMARY KEY, value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW())
            """)
            cur.execute("""
                INSERT INTO nexus_config (key, value, updated_at) VALUES ('fill_truth_write', %s, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """, (json.dumps({"ts": int(time.time()), "since": str(since), "rows": n,
                              "off": len(off), "script": "fill_truth_write.py V1.0"}),))
        conn.commit()
        # verify: re-read
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM berserker_trade_fingerprints WHERE pnl_pct_broker IS NOT NULL")
            have = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM berserker_trade_fingerprints
                WHERE pnl_pct_broker IS NOT NULL AND ABS(pnl_pct_broker - pnl_pct) >= %s
            """, (TOL,))
            n_off = cur.fetchone()[0]
        conn.commit()
        print(f"\nWROTE {n} rows. Verified: {have} rows carry pnl_pct_broker; {n_off} differ from pnl_pct by >= {TOL}.")
        print("pnl_pct untouched. Audit line in nexus_config['fill_truth_write'].")
    except Exception as e:
        conn.rollback()
        sys.exit(f"Write FAILED and rolled back: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
