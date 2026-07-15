#!/usr/bin/env python3
"""
paper_cooldown_check.py — READ-ONLY verification that the V5.22 paper
re-entry cooldown behaves as designed.

DESIGN UNDER TEST (crypto.py V5.22):
  After a LOSING paper exit on pair X, no paper re-entry on X for
  PAPER_LOSS_COOLDOWN_SECS (default 1800s). Wins impose no cooldown.

METHOD:
  Walk crypto_trade_fingerprints (entry_mode='PAPER') per pair in time
  order. For every entry, measure the gap since the most recent prior
  exit of the same pair, split by whether that exit won or lost.
  - Post-deploy loss->re-entry gaps < cooldown  => VIOLATION (bug)
  - Pre-deploy rows are reported as the contrast group (the old churn)
  - Win->re-entry gaps may legally be short in both eras

Also reports paper trade volume per day (the churn-rate check) and the
five fastest post-deploy loss re-entries for eyeballing.

READ-ONLY: SELECTs only. Safe on the live DB.

USAGE:
  python3 paper_cooldown_check.py
  python3 paper_cooldown_check.py --since "2026-07-15 18:30"   # deploy time, CDT
  python3 paper_cooldown_check.py --cooldown 1800
"""
import os
import sys
import argparse
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed. On the Railway console it's already present; "
          "locally: pip install psycopg2-binary")
    sys.exit(1)

CENTRAL = ZoneInfo("America/Chicago")
DEFAULT_DEPLOY = "2026-07-15 18:30"     # V5.22 boot: Jul 15 2026 6:26 PM CDT


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=DEFAULT_DEPLOY,
                    help="deploy datetime, CDT, 'YYYY-MM-DD HH:MM'")
    ap.add_argument("--cooldown", type=int, default=1800)
    args = ap.parse_args()

    deploy_ts = int(datetime.strptime(args.since, "%Y-%m-%d %H:%M")
                    .replace(tzinfo=CENTRAL).timestamp())

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set. Run from a NEXUS service console.")
        sys.exit(1)

    conn = psycopg2.connect(db_url, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pair, entry_ts, exit_ts, won
            FROM crypto_trade_fingerprints
            WHERE entry_mode = 'PAPER' AND exit_ts IS NOT NULL
            ORDER BY pair, entry_ts
        """)
        rows = cur.fetchall()
    conn.close()

    by_pair = defaultdict(list)
    for pair, ent, ext, won in rows:
        by_pair[pair].append((ent, ext, won))

    # gap since most recent prior exit of the same pair, keyed by
    # (era of the ENTRY, outcome of the PRIOR exit)
    gaps = defaultdict(list)     # (era, prior_won) -> [gap_secs]
    violations = []              # post-deploy loss re-entries under cooldown

    for pair, trades in by_pair.items():
        for i in range(1, len(trades)):
            ent = trades[i][0]
            prev_exit, prev_won = trades[i - 1][1], trades[i - 1][2]
            if prev_exit is None or ent < prev_exit:
                continue                      # overlapping/malformed row
            gap = ent - prev_exit
            era = "POST" if ent >= deploy_ts else "PRE"
            gaps[(era, bool(prev_won))].append(gap)
            if era == "POST" and not prev_won and gap < args.cooldown:
                violations.append((pair, ent, gap))

    def dist(v):
        if not v:
            return "n=0"
        v = sorted(v)
        n = len(v)
        med = v[n // 2]
        return (f"n={n:5d}  min={v[0]:6d}s  median={med:6d}s  "
                f"under-30m={sum(1 for g in v if g < args.cooldown) * 100 // n:3d}%")

    print("=" * 74)
    print("PAPER COOLDOWN COMPLIANCE CHECK (V5.22)")
    print(f"cooldown={args.cooldown}s | deploy cut: {args.since} CDT (ts {deploy_ts})")
    print("=" * 74)
    print("\nRe-entry gap after a LOSING exit (the rule under test):")
    print(f"  PRE-deploy  (old churn, contrast): {dist(gaps[('PRE',  False)])}")
    print(f"  POST-deploy (must be >= cooldown): {dist(gaps[('POST', False)])}")
    print("\nRe-entry gap after a WINNING exit (no cooldown applies, both eras):")
    print(f"  PRE-deploy : {dist(gaps[('PRE',  True)])}")
    print(f"  POST-deploy: {dist(gaps[('POST', True)])}")

    # daily paper volume: the churn-rate view
    print("\nPaper trades per day (volume check — expect a step down post-deploy):")
    per_day = defaultdict(int)
    for pair, trades in by_pair.items():
        for ent, _, _ in trades:
            d = datetime.fromtimestamp(ent, tz=CENTRAL).date().isoformat()
            per_day[d] += 1
    for d in sorted(per_day)[-10:]:
        day_end = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=CENTRAL).timestamp() + 86400
        marker = " <- post-deploy" if day_end > deploy_ts else ""
        print(f"  {d}: {per_day[d]:4d}{marker}")

    print("\nVERDICT:")
    if not gaps[("POST", False)]:
        print("  PENDING — no post-deploy losing exits followed by a re-entry yet. "
              "Re-run after the next red stretch.")
    elif violations:
        print(f"  FAIL — {len(violations)} post-deploy loss re-entries under "
              f"{args.cooldown}s. Fastest five:")
        for pair, ent, gap in sorted(violations, key=lambda x: x[2])[:5]:
            t = datetime.fromtimestamp(ent, tz=CENTRAL).strftime("%m-%d %H:%M")
            print(f"    {pair:10s} {t} CDT  gap={gap}s")
        print("  -> cooldown not enforcing; check that V5.22 actually booted "
              "(boot log 'loss-cooldown=30m') before suspecting the logic.")
    else:
        n = len(gaps[("POST", False)])
        print(f"  PASS — all {n} post-deploy loss re-entries waited >= "
              f"{args.cooldown}s. Cooldown enforcing as designed.")


if __name__ == "__main__":
    main()
