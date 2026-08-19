#!/usr/bin/env python3
"""
tom_gate0.py V1.0 -- PATH 3 Gate 0: turn-of-month flow (SPY)
=============================================================
Pre-registered Aug 19 2026 (PATHS34_PreRegistration_Aug19.md). Verdicts bind.

READ-ONLY. Daily adjusted IEX bars from Alpaca. No NEXUS tables, no DB
writes. One T-Bone summary.

TOM window: last trading session of each month + first 3 of the next.
  C1: TOM avg minus non-TOM avg >= +0.08%/day AND TOM avg > 0  (SPY)
  C2: both walk-forward halves show positive TOM spread
  PLACEBO: 1,000 circular shifts of the TOM mask; real spread beats >= 95%
  REPLICATION (informational): TQQQ TOM spread sign

Verdict: KILL if C1, C2, or placebo fails. KEEP otherwise.

Run: python3 tom_gate0.py --days 2500
"""

import os
import sys
import random
import argparse
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TOM0] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("tom0")

ALPACA_API_KEY = (os.environ.get("ALPACA_PHASE4_API_KEY") or
                  os.environ.get("ALPACA_API_KEY", ""))
ALPACA_SECRET  = (os.environ.get("ALPACA_PHASE4_SECRET_KEY") or
                  os.environ.get("ALPACA_SECRET_KEY", ""))
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ET           = ZoneInfo("America/New_York")
SPREAD_BAR   = 0.08     # %/day, fixed by registration
PLACEBO_N    = 1000
PLACEBO_PCTL = 95.0


def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except Exception:
        pass


def fetch_daily(symbol: str, days: int) -> list:
    """[(ET session date iso, close)] oldest-first, adjusted IEX bars."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 10)
    url   = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY,
               "APCA-API-SECRET-KEY": ALPACA_SECRET}
    out, page_token = [], None
    while True:
        params = {"timeframe": "1Day", "adjustment": "all", "feed": "iex",
                  "limit": 10000,
                  "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "end":   end.strftime("%Y-%m-%dT%H:%M:%SZ")}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        for b in j.get("bars") or []:
            dt = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
            out.append((dt.date(), float(b["c"])))
        page_token = j.get("next_page_token")
        if not page_token:
            break
    log.info(f"  {symbol}: {len(out)} daily bars"
             + (f" ({out[0][0]} -> {out[-1][0]})" if out else ""))
    return out


def build_returns_and_mask(bars: list):
    """Daily close->close returns (%) aligned with a TOM mask.
    TOM = last session of a month + first 3 sessions of the next.
    Return index i covers session i-1 close -> session i close and carries
    session i's TOM flag."""
    n = len(bars)
    tom = [False] * n
    for i, (d, _) in enumerate(bars):
        last_of_month = (i + 1 < n and bars[i + 1][0].month != d.month) or (i + 1 == n)
        if last_of_month and i + 1 < n:
            tom[i] = True
            for k in (1, 2, 3):
                if i + k < n:
                    tom[i + k] = True
    rets, mask = [], []
    for i in range(1, n):
        prev, cur = bars[i - 1][1], bars[i][1]
        if prev > 0:
            rets.append((cur / prev - 1.0) * 100.0)
            mask.append(tom[i])
    return rets, mask


def spread(rets, mask):
    tom_r  = [r for r, m in zip(rets, mask) if m]
    rest_r = [r for r, m in zip(rets, mask) if not m]
    if not tom_r or not rest_r:
        return 0.0, 0.0, 0.0
    a, b = sum(tom_r) / len(tom_r), sum(rest_r) / len(rest_r)
    return a - b, a, b


def main():
    ap = argparse.ArgumentParser(description="TOM Gate 0 V1.0")
    ap.add_argument("--days", type=int, default=2500)
    args = ap.parse_args()
    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("Missing Alpaca keys")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"TOM GATE 0 V1.0 | SPY primary, TQQQ replication | {args.days}d")
    log.info("Read-only. No DB writes. Verdict binds per registration.")
    log.info("=" * 60)

    spy = fetch_daily("SPY", args.days)
    if len(spy) < 500:
        log.error(f"Only {len(spy)} SPY bars — insufficient, aborting")
        sys.exit(1)
    rets, mask = build_returns_and_mask(spy)
    n_tom = sum(mask)
    sp, tom_avg, rest_avg = spread(rets, mask)
    log.info(f"SPY: {len(rets)} return-days | TOM days={n_tom} avg={round(tom_avg,4)}% | "
             f"non-TOM avg={round(rest_avg,4)}% | spread={round(sp,4)}%/day")

    c1 = sp >= SPREAD_BAR and tom_avg > 0

    half = len(rets) // 2
    sp1, t1, _ = spread(rets[:half], mask[:half])
    sp2, t2, _ = spread(rets[half:], mask[half:])
    log.info(f"WalkFwd: H1 spread={round(sp1,4)}% (TOM avg {round(t1,4)}%) | "
             f"H2 spread={round(sp2,4)}% (TOM avg {round(t2,4)}%)")
    c2 = sp1 > 0 and sp2 > 0

    rng, beaten = random.Random(42), 0
    L = len(mask)
    for _ in range(PLACEBO_N):
        k = rng.randint(5, L - 5)
        shifted = mask[k:] + mask[:k]
        if sp > spread(rets, shifted)[0]:
            beaten += 1
    pctl = 100.0 * beaten / PLACEBO_N
    pb = pctl >= PLACEBO_PCTL
    log.info(f"Placebo: real spread beats {pctl}% of {PLACEBO_N} circular shifts "
             f"(need >={PLACEBO_PCTL}%)")

    # Informational replication on TQQQ (sign only, not a kill bar)
    trep = "n/a"
    try:
        tq = fetch_daily("TQQQ", args.days)
        if len(tq) > 300:
            r2, m2 = build_returns_and_mask(tq)
            sp_t, _, _ = spread(r2, m2)
            trep = f"{round(sp_t,4)}%/day ({'+' if sp_t > 0 else '-'})"
    except Exception as e:
        log.info(f"  TQQQ replication skipped: {e}")

    verdict = "KEEP" if (c1 and c2 and pb) else "KILL"

    log.info("=" * 60)
    log.info(f"C1 spread>=+{SPREAD_BAR}%/day: {'PASS' if c1 else 'FAIL'} ({round(sp,4)}%)")
    log.info(f"C2 walk-forward:              {'PASS' if c2 else 'FAIL'}")
    log.info(f"PLACEBO:                      {'PASS' if pb else 'FAIL'} ({pctl}%)")
    log.info(f"TQQQ replication (info):      {trep}")
    log.info(f"VERDICT: {verdict}")
    log.info("=" * 60)

    send_alert(
        f"🧪 TOM GATE 0 — SPY turn-of-month\n"
        f"──────────────────\n"
        f"TOM avg {round(tom_avg,3)}% vs rest {round(rest_avg,3)}% "
        f"(spread {round(sp,3)}%/day, n={n_tom})\n"
        f"WalkFwd {round(sp1,3)}% / {round(sp2,3)}% | placebo {pctl}%\n"
        f"TQQQ replication: {trep}\n"
        f"C1 {'✅' if c1 else '❌'} C2 {'✅' if c2 else '❌'} PLB {'✅' if pb else '❌'}\n"
        f"──────────────────\n"
        f"VERDICT: {verdict}"
    )


if __name__ == "__main__":
    main()
