#!/usr/bin/env python3
"""
reclassify_bt_rows.py — prefix orphan backtest rows in berserker_trade_fingerprints.

WHY: the backtester wrote ~16.6k rows with is_paper=FALSE and UNPREFIXED
trade_ids (discovered Jul 18 via the overnight query). ~6.1k use backtester
vocabulary ('trail'/'timeout'); the rest use live-identical exit reasons and
are identifiable only by era — live Berserker fingerprints begin Jul 6 2026.
Prefixing with 'bt_' makes every existing consumer filter correct:
PatternMemory still ingests them through its intended bt_ gate; WinFollower
and recency stats gain real protection instead of timing luck.

SAFE: dry-run by default (prints counts, changes nothing). --apply commits.
Idempotent: prefixed rows never match again. Live rows can't match: none
predate the Jul 6 era cutoff, and is_paper IS NOT TRUE shields paper rows.

USAGE (analyst console):
  python3 reclassify_bt_rows.py           # dry run
  python3 reclassify_bt_rows.py --apply   # do it
"""
import os
import sys
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed — run from a NEXUS service console.")
    sys.exit(1)

CENTRAL = ZoneInfo("America/Chicago")
LIVE_ERA_START = "2026-07-06 00:00"   # first live Berserker fingerprint era

PREDICATE = """
    (entry_ts < %s OR exit_reason IN ('trail', 'timeout'))
    AND trade_id NOT LIKE 'bt_%%'
    AND is_paper IS NOT TRUE
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="actually prefix the rows (default: dry run)")
    args = ap.parse_args()

    cut = int(datetime.strptime(LIVE_ERA_START, "%Y-%m-%d %H:%M")
              .replace(tzinfo=CENTRAL).timestamp())

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set.")
        sys.exit(1)

    conn = psycopg2.connect(db_url, connect_timeout=10)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM berserker_trade_fingerprints WHERE {PREDICATE}",
                (cut,))
    n = cur.fetchone()[0]
    print(f"era cutoff: {LIVE_ERA_START} CDT (ts {cut})")
    print(f"orphan backtest rows matching: {n}")

    if not args.apply:
        print("DRY RUN — nothing changed. Re-run with --apply to prefix them.")
        conn.close()
        return

    cur.execute(f"UPDATE berserker_trade_fingerprints SET trade_id = 'bt_' || trade_id "
                f"WHERE {PREDICATE}", (cut,))
    conn.commit()
    print(f"prefixed: {cur.rowcount}")

    # verify: no unprefixed pre-era rows remain
    cur.execute("""
        SELECT COUNT(*) FROM berserker_trade_fingerprints
        WHERE entry_ts < %s AND trade_id NOT LIKE 'bt_%%' AND is_paper IS NOT TRUE
    """, (cut,))
    print(f"remaining unprefixed pre-era rows: {cur.fetchone()[0]}  (expect 0)")

    # sanity: live rows untouched
    cur.execute("""
        SELECT COUNT(*) FROM berserker_trade_fingerprints
        WHERE entry_ts >= %s AND trade_id NOT LIKE 'bt_%%'
          AND is_paper IS NOT TRUE AND exit_ts IS NOT NULL
    """, (cut,))
    print(f"live-era fingerprints still unprefixed (your real trades): {cur.fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    main()
