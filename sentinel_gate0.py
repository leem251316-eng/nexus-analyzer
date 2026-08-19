#!/usr/bin/env python3
"""
sentinel_gate0.py V1.0 -- SENTINEL Gate 0: unconditional overnight regime replication
======================================================================================
Pre-registered Aug 19 2026 (SENTINEL_PreRegistration_Aug19.md). Verdicts bind.

READ-ONLY. Fetches daily bars from Alpaca, touches no NEXUS tables, writes
nothing to the DB. Prints the verdict and sends one T-Bone summary.

Tests, exactly as registered:
  CLAIM 1: chop-night (prior close < 20d SMA) avg close->open >= +0.20% gross,
           positive-night share >= 52%.
  CLAIM 2: trend-night avg <= 0 (sign only).
  CLAIM 3: both walk-forward halves agree on BOTH signs.
  PLACEBO: 1,000 regime-label shuffles; real chop-trend spread must beat 95%.

Verdict: KILL if Claim 1 or 3 fails or placebo unbeaten. PARK if only
Claim 2 fails. KEEP otherwise.

Run (nexus-analyst console, single line):
  python3 sentinel_gate0.py --days 730
"""

import os
import sys
import time
import random
import argparse
import logging
from datetime import datetime, timedelta, timezone

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GATE0] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("gate0")

ALPACA_API_KEY = (os.environ.get("ALPACA_PHASE4_API_KEY") or
                  os.environ.get("ALPACA_API_KEY", ""))
ALPACA_SECRET  = (os.environ.get("ALPACA_PHASE4_SECRET_KEY") or
                  os.environ.get("ALPACA_SECRET_KEY", ""))
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SMA_LEN        = 20        # fixed by registration (B3 ETF-MA20)
CHOP_AVG_BAR   = 0.20      # % per night, gross
CHOP_POS_BAR   = 52.0      # % positive-night share
PLACEBO_N      = 1000
PLACEBO_PCTL   = 95.0
FRICTION_RT    = 0.12      # % round trip, declared in registration


def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except Exception:
        pass


def fetch_daily(symbol: str, days: int) -> list:
    """Daily ADJUSTED bars via Alpaca REST. Returns list of dicts oldest-first.
    adjustment=all is mandatory -- raw bars put splits inside the window as
    fake crashes (banked backtester lesson)."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 60)   # pad for SMA warmup
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY,
               "APCA-API-SECRET-KEY": ALPACA_SECRET}
    bars, page_token = [], None
    while True:
        params = {"timeframe": "1Day", "adjustment": "all", "limit": 10000,
                  "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "end":   end.strftime("%Y-%m-%dT%H:%M:%SZ")}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        for b in j.get("bars") or []:
            bars.append({"t": b["t"][:10], "o": float(b["o"]), "c": float(b["c"])})
        page_token = j.get("next_page_token")
        if not page_token:
            break
    log.info(f"  {symbol}: {len(bars)} daily bars ({bars[0]['t']} -> {bars[-1]['t']})"
             if bars else f"  {symbol}: NO BARS")
    return bars


def build_nights(bars: list) -> list:
    """For each session i -> i+1: regime at close(i) vs 20d SMA(close, incl i),
    overnight ret = open(i+1)/close(i) - 1. Returns list of
    (date, regime, overnight_ret_pct)."""
    nights = []
    closes = [b["c"] for b in bars]
    for i in range(SMA_LEN - 1, len(bars) - 1):
        sma = sum(closes[i - SMA_LEN + 1:i + 1]) / SMA_LEN
        regime = "CHOP" if closes[i] < sma else "TREND"
        ret = (bars[i + 1]["o"] / closes[i] - 1.0) * 100.0
        nights.append((bars[i]["t"], regime, ret))
    return nights


def cell(nights, regime):
    rets = [r for _, g, r in nights if g == regime]
    if not rets:
        return {"n": 0, "avg": 0.0, "med": 0.0, "pos": 0.0}
    s = sorted(rets)
    return {"n": len(rets),
            "avg": round(sum(rets) / len(rets), 4),
            "med": round(s[len(s) // 2], 4),
            "pos": round(100.0 * sum(1 for r in rets if r > 0) / len(rets), 1)}


def spread(nights):
    c, t = cell(nights, "CHOP"), cell(nights, "TREND")
    return c["avg"] - t["avg"]


def main():
    ap = argparse.ArgumentParser(description="SENTINEL Gate 0 V1.0")
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--symbol", type=str, default="TQQQ",
                    help="Registration is TQQQ; other symbols are exploration only")
    args = ap.parse_args()

    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("Missing Alpaca keys")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"SENTINEL GATE 0 V1.0 | {args.symbol} | {args.days}d | SMA{SMA_LEN}")
    log.info("Read-only. No DB writes. Verdict binds per registration.")
    log.info("=" * 60)

    bars = fetch_daily(args.symbol, args.days)
    if len(bars) < SMA_LEN + 40:
        log.error("Insufficient bars")
        sys.exit(1)

    nights = build_nights(bars)
    n_all  = len(nights)
    chop   = cell(nights, "CHOP")
    trend  = cell(nights, "TREND")
    log.info(f"Nights: {n_all} | CHOP n={chop['n']} avg={chop['avg']}% med={chop['med']}% "
             f"pos={chop['pos']}% | TREND n={trend['n']} avg={trend['avg']}% "
             f"med={trend['med']}% pos={trend['pos']}%")

    # CLAIM 1
    c1 = chop["avg"] >= CHOP_AVG_BAR and chop["pos"] >= CHOP_POS_BAR and chop["n"] >= 60
    # CLAIM 2
    c2 = trend["avg"] <= 0.0
    # CLAIM 3: walk-forward halves, both signs
    half = n_all // 2
    h1, h2 = nights[:half], nights[half:]
    h1c, h1t = cell(h1, "CHOP"), cell(h1, "TREND")
    h2c, h2t = cell(h2, "CHOP"), cell(h2, "TREND")
    log.info(f"H1: CHOP avg={h1c['avg']}% (n={h1c['n']}) TREND avg={h1t['avg']}% (n={h1t['n']})")
    log.info(f"H2: CHOP avg={h2c['avg']}% (n={h2c['n']}) TREND avg={h2t['avg']}% (n={h2t['n']})")
    c3 = (h1c["avg"] > 0 and h2c["avg"] > 0 and h1t["avg"] <= 0 and h2t["avg"] <= 0)

    # PLACEBO: shuffle regime labels
    real_spread = spread(nights)
    labels = [g for _, g, _ in nights]
    rets   = [r for _, _, r in nights]
    rng    = random.Random(42)
    beaten = 0
    for _ in range(PLACEBO_N):
        rng.shuffle(labels)
        fake = list(zip([""] * n_all, labels, rets))
        if real_spread > spread(fake):
            beaten += 1
    pctl = 100.0 * beaten / PLACEBO_N
    placebo_ok = pctl >= PLACEBO_PCTL
    log.info(f"Spread (CHOP-TREND): {round(real_spread,4)}%/night | beats {pctl}% "
             f"of {PLACEBO_N} shuffles (need >={PLACEBO_PCTL}%)")

    # Verdict per registration
    if not c1 or not c3 or not placebo_ok:
        verdict = "KILL"
    elif not c2:
        verdict = "PARK"
    else:
        verdict = "KEEP"

    net = round(chop["avg"] - FRICTION_RT, 4)
    log.info("=" * 60)
    log.info(f"CLAIM1 chop-edge:   {'PASS' if c1 else 'FAIL'} "
             f"(avg {chop['avg']}% vs {CHOP_AVG_BAR}%, pos {chop['pos']}% vs {CHOP_POS_BAR}%)")
    log.info(f"CLAIM2 trend-sign:  {'PASS' if c2 else 'FAIL'} (avg {trend['avg']}%)")
    log.info(f"CLAIM3 walk-fwd:    {'PASS' if c3 else 'FAIL'}")
    log.info(f"PLACEBO:            {'PASS' if placebo_ok else 'FAIL'} ({pctl}%)")
    log.info(f"VERDICT: {verdict} | chop net of {FRICTION_RT}% friction: {net}%/night")
    log.info("=" * 60)

    send_alert(
        f"🧪 SENTINEL GATE 0 — {args.symbol}\n"
        f"──────────────────\n"
        f"CHOP:  n={chop['n']} avg={chop['avg']}% pos={chop['pos']}%\n"
        f"TREND: n={trend['n']} avg={trend['avg']}% pos={trend['pos']}%\n"
        f"Spread {round(real_spread,3)}%/night | placebo {pctl}%\n"
        f"WalkFwd H1 chop {h1c['avg']}% / H2 chop {h2c['avg']}%\n"
        f"C1 {'✅' if c1 else '❌'} C2 {'✅' if c2 else '❌'} "
        f"C3 {'✅' if c3 else '❌'} PLB {'✅' if placebo_ok else '❌'}\n"
        f"──────────────────\n"
        f"VERDICT: {verdict} | net/night: {net}%"
    )


if __name__ == "__main__":
    main()
