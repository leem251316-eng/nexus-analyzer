#!/usr/bin/env python3
"""
xsmom_gate0.py V1.0 -- PATH 4 Gate 0: weekly cross-sectional sector momentum
=============================================================================
Pre-registered Aug 19 2026 (PATHS34_PreRegistration_Aug19.md). Verdicts bind.

READ-ONLY. Daily adjusted IEX bars from Alpaca. No NEXUS tables, no DB
writes. One T-Bone summary.

Universe: XLK XLF XLE XLV XLI XLY XLP XLU XLB XLRE XLC. Benchmark: SPY.
Signal: trailing 63-session total return, ranked at each week's final
session close; hold top-1 the following week.

  C1: avg(top-1 weekly ret - SPY weekly ret) >= +0.08%/week
  C2: both walk-forward halves positive on that spread
  C3: avg weekly ret of top-3 > bottom-3
  PLACEBO: 1,000 random-pick-per-week sims; real top-1 avg beats >= 95%

Verdict: KILL if C1, C2, or placebo fails. PARK if only C3 fails.
KEEP otherwise.

Run: python3 xsmom_gate0.py --days 2500
"""

import os
import sys
import random
import argparse
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [XSMOM0] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("xsmom0")

ALPACA_API_KEY = (os.environ.get("ALPACA_PHASE4_API_KEY") or
                  os.environ.get("ALPACA_API_KEY", ""))
ALPACA_SECRET  = (os.environ.get("ALPACA_PHASE4_SECRET_KEY") or
                  os.environ.get("ALPACA_SECRET_KEY", ""))
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ET           = ZoneInfo("America/New_York")
SECTORS      = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY",
                "XLP", "XLU", "XLB", "XLRE", "XLC"]
BENCH        = "SPY"
LOOKBACK     = 63       # sessions, fixed by registration
SPREAD_BAR   = 0.08     # %/week
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


def fetch_closes(symbol: str, days: int) -> dict:
    """{ET date iso: close}, adjusted IEX daily bars."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 10)
    url   = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY,
               "APCA-API-SECRET-KEY": ALPACA_SECRET}
    out, page_token = {}, None
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
            out[dt.date().isoformat()] = float(b["c"])
        page_token = j.get("next_page_token")
        if not page_token:
            break
    log.info(f"  {symbol}: {len(out)} daily closes")
    return out


def week_ends(dates: list) -> list:
    """Indices of each week's final session (next session is a later ISO week)."""
    idx = []
    for i in range(len(dates) - 1):
        d0 = datetime.fromisoformat(dates[i]).isocalendar()[:2]
        d1 = datetime.fromisoformat(dates[i + 1]).isocalendar()[:2]
        if d0 != d1:
            idx.append(i)
    return idx


def build_weeks(closes_by_sym: dict, bench: dict):
    """Weekly records: (rank_date, ranked_sectors_desc, {sym: next-week ret %}, bench ret %).
    Sector universe per week = symbols with full lookback + next-week data."""
    dates = sorted(set(bench.keys()))
    wends = [i for i in week_ends(dates) if i >= LOOKBACK]
    weeks = []
    for wi in range(len(wends) - 1):
        i, j = wends[wi], wends[wi + 1]
        d_i, d_j = dates[i], dates[j]
        d_lb = dates[i - LOOKBACK]
        scores, fwd = {}, {}
        for sym, cl in closes_by_sym.items():
            if d_i in cl and d_j in cl and d_lb in cl and cl[d_lb] > 0 and cl[d_i] > 0:
                scores[sym] = cl[d_i] / cl[d_lb] - 1.0
                fwd[sym]    = (cl[d_j] / cl[d_i] - 1.0) * 100.0
        if len(scores) < 6 or d_j not in bench or d_i not in bench:
            continue
        ranked = sorted(scores, key=scores.get, reverse=True)
        bret   = (bench[d_j] / bench[d_i] - 1.0) * 100.0
        weeks.append((d_i, ranked, fwd, bret))
    return weeks


def main():
    ap = argparse.ArgumentParser(description="XSMOM Gate 0 V1.0")
    ap.add_argument("--days", type=int, default=2500)
    args = ap.parse_args()
    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("Missing Alpaca keys")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"XSMOM GATE 0 V1.0 | {len(SECTORS)} sectors vs {BENCH} | "
             f"lookback {LOOKBACK}d | {args.days}d")
    log.info("Read-only. No DB writes. Verdict binds per registration.")
    log.info("=" * 60)

    closes = {s: fetch_closes(s, args.days) for s in SECTORS}
    bench  = fetch_closes(BENCH, args.days)
    weeks  = build_weeks(closes, bench)
    if len(weeks) < 100:
        log.error(f"Only {len(weeks)} usable weeks — insufficient, aborting")
        sys.exit(1)
    log.info(f"Usable weeks: {len(weeks)} ({weeks[0][0]} -> {weeks[-1][0]})")

    top1_sp = [fwd[ranked[0]] - bret for _, ranked, fwd, bret in weeks]
    top1_r  = [fwd[ranked[0]] for _, ranked, fwd, bret in weeks]
    avg_sp  = sum(top1_sp) / len(top1_sp)
    pos     = 100.0 * sum(1 for x in top1_sp if x > 0) / len(top1_sp)
    log.info(f"Top-1 minus {BENCH}: avg={round(avg_sp,4)}%/week | pos-weeks={round(pos,1)}%")
    c1 = avg_sp >= SPREAD_BAR

    half = len(weeks) // 2
    sp1 = sum(top1_sp[:half]) / half
    sp2 = sum(top1_sp[half:]) / (len(top1_sp) - half)
    log.info(f"WalkFwd spread: H1={round(sp1,4)}%/wk | H2={round(sp2,4)}%/wk")
    c2 = sp1 > 0 and sp2 > 0

    top3, bot3 = [], []
    for _, ranked, fwd, _ in weeks:
        top3.append(sum(fwd[s] for s in ranked[:3]) / 3)
        bot3.append(sum(fwd[s] for s in ranked[-3:]) / 3)
    t3, b3 = sum(top3) / len(top3), sum(bot3) / len(bot3)
    log.info(f"Rank structure: top-3 avg={round(t3,4)}%/wk vs bottom-3 avg={round(b3,4)}%/wk")
    c3 = t3 > b3

    rng, real_avg, beaten = random.Random(42), sum(top1_r) / len(top1_r), 0
    for _ in range(PLACEBO_N):
        sim = [fwd[rng.choice(list(fwd))] for _, _, fwd, _ in weeks]
        if real_avg > sum(sim) / len(sim):
            beaten += 1
    pctl = 100.0 * beaten / PLACEBO_N
    pb = pctl >= PLACEBO_PCTL
    log.info(f"Placebo: real top-1 avg ({round(real_avg,4)}%/wk) beats {pctl}% of "
             f"{PLACEBO_N} random-pick sims (need >={PLACEBO_PCTL}%)")

    if not c1 or not c2 or not pb:
        verdict = "KILL"
    elif not c3:
        verdict = "PARK"
    else:
        verdict = "KEEP"

    log.info("=" * 60)
    log.info(f"C1 spread>=+{SPREAD_BAR}%/wk: {'PASS' if c1 else 'FAIL'} ({round(avg_sp,4)}%)")
    log.info(f"C2 walk-forward:            {'PASS' if c2 else 'FAIL'}")
    log.info(f"C3 rank structure:          {'PASS' if c3 else 'FAIL'}")
    log.info(f"PLACEBO:                    {'PASS' if pb else 'FAIL'} ({pctl}%)")
    log.info(f"VERDICT: {verdict}")
    log.info("=" * 60)

    send_alert(
        f"🧪 XSMOM GATE 0 — sector momentum\n"
        f"──────────────────\n"
        f"Top-1 vs SPY: {round(avg_sp,3)}%/wk over {len(weeks)} weeks\n"
        f"WalkFwd {round(sp1,3)} / {round(sp2,3)} | top3 {round(t3,3)} vs bot3 {round(b3,3)}\n"
        f"Placebo {pctl}%\n"
        f"C1 {'✅' if c1 else '❌'} C2 {'✅' if c2 else '❌'} "
        f"C3 {'✅' if c3 else '❌'} PLB {'✅' if pb else '❌'}\n"
        f"──────────────────\n"
        f"VERDICT: {verdict}"
    )


if __name__ == "__main__":
    main()
