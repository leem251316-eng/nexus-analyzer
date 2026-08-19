#!/usr/bin/env python3
"""
rebal_gate0.py V1.0 -- REBAL Gate 0: leveraged ETF close rebalance flow
========================================================================
Pre-registered Aug 19 2026 (REBAL_PreRegistration_Aug19.md). Verdicts bind.

READ-ONLY. Fetches daily + 30-min bars from Alpaca (feed=iex,
adjustment=all), touches no NEXUS tables, writes nothing to the DB.
Prints the verdict and sends one T-Bone summary.

Signal: QQQ day-move m at 3:30 PM ET vs prior daily close.
Effect: TQQQ final 30-min return r (3:30 bar open -> bar close).
Continuation = sign(m) * r.

  C1: m >= +1%  -> avg r >= +0.20% and >= baseline + 0.10% (n >= 30)
  C2: m <= -1%  -> avg r <= -0.20% and <= baseline - 0.10% (n >= 30)
  C3: continuation strictly increasing across |m| in [0.5-1, 1-2, >2]
  C4: pooled continuation (|m| >= 1%) positive in BOTH halves
  PA: |m| < 0.5% days: |avg r| < half the passing side's effect
  PB: 1,000 shuffles of m labels; real big-vs-mid continuation spread
      beats >= 95%

Verdict: KILL if PB fails, C4 fails, or C1 and C2 both fail.
         PARK if structure passes but best side < 0.20%.
         KEEP otherwise.

Run (nexus-analyst console, single line):
  python3 rebal_gate0.py --days 730
"""

import os
import sys
import random
import argparse
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [REBAL0] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("rebal0")

ALPACA_API_KEY = (os.environ.get("ALPACA_PHASE4_API_KEY") or
                  os.environ.get("ALPACA_API_KEY", ""))
ALPACA_SECRET  = (os.environ.get("ALPACA_PHASE4_SECRET_KEY") or
                  os.environ.get("ALPACA_SECRET_KEY", ""))
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ET             = ZoneInfo("America/New_York")
BIG_MOVE       = 1.0      # % — mandate-flow threshold (fixed by registration)
MID_MOVE       = 0.5      # % — quiet-day placebo ceiling
EFFECT_BAR     = 0.20     # % — gross final-30 effect bar
BASELINE_EDGE  = 0.10     # % — must exceed quiet-day baseline by this
MIN_N          = 30
PLACEBO_N      = 1000
PLACEBO_PCTL   = 95.0
FRICTION_RT    = 0.12     # % round trip, declared in registration


def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except Exception:
        pass


def _get(url, params):
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY,
               "APCA-API-SECRET-KEY": ALPACA_SECRET}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_bars(symbol: str, timeframe: str, days: int) -> list:
    """Adjusted IEX bars, paginated. Returns list of {'t','o','c'} oldest-first."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 10)
    url   = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    bars, page_token = [], None
    while True:
        params = {"timeframe": timeframe, "adjustment": "all", "feed": "iex",
                  "limit": 10000,
                  "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "end":   end.strftime("%Y-%m-%dT%H:%M:%SZ")}
        if page_token:
            params["page_token"] = page_token
        j = _get(url, params)
        for b in j.get("bars") or []:
            bars.append({"t": b["t"], "o": float(b["o"]), "c": float(b["c"])})
        page_token = j.get("next_page_token")
        if not page_token:
            break
    log.info(f"  {symbol} {timeframe}: {len(bars)} bars")
    return bars


def final30_by_date(bars30: list) -> dict:
    """{ET session date: (open_1530, close_1530)} — the 3:30 PM ET bar."""
    out = {}
    for b in bars30:
        dt = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        if dt.hour == 15 and dt.minute == 30 and dt.weekday() < 5:
            out[dt.date().isoformat()] = (b["o"], b["c"])
    return out


def daily_closes(bars_d: list) -> list:
    """[(session date, close)] oldest-first, date from ET conversion."""
    out = []
    for b in bars_d:
        dt = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        out.append((dt.date().isoformat(), b["c"]))
    return out


def build_days(qqq_d, qqq_30, tqqq_30) -> list:
    """[(date, m_pct, r_pct)] where m = QQQ 3:30 open vs prior close,
    r = TQQQ final-30 return."""
    q30 = final30_by_date(qqq_30)
    t30 = final30_by_date(tqqq_30)
    days = []
    closes = daily_closes(qqq_d)
    for i in range(1, len(closes)):
        d, _ = closes[i]
        prev_close = closes[i - 1][1]
        if d in q30 and d in t30 and prev_close > 0:
            m = (q30[d][0] / prev_close - 1.0) * 100.0
            o, c = t30[d]
            if o > 0:
                r = (c / o - 1.0) * 100.0
                days.append((d, m, r))
    return days


def stats(rows):
    if not rows:
        return {"n": 0, "avg": 0.0, "med": 0.0, "pos": 0.0}
    s = sorted(rows)
    return {"n": len(rows),
            "avg": round(sum(rows) / len(rows), 4),
            "med": round(s[len(s) // 2], 4),
            "pos": round(100.0 * sum(1 for x in rows if x > 0) / len(rows), 1)}


def cont_spread(days):
    """Continuation spread: mean sign(m)*r on |m|>=BIG minus on |m|<MID."""
    big = [(1 if m > 0 else -1) * r for _, m, r in days if abs(m) >= BIG_MOVE]
    mid = [(1 if m > 0 else -1) * r for _, m, r in days if abs(m) < MID_MOVE]
    if not big or not mid:
        return 0.0
    return sum(big) / len(big) - sum(mid) / len(mid)


def main():
    ap = argparse.ArgumentParser(description="REBAL Gate 0 V1.0")
    ap.add_argument("--days", type=int, default=730)
    args = ap.parse_args()

    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("Missing Alpaca keys")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"REBAL GATE 0 V1.0 | QQQ signal -> TQQQ final-30 | {args.days}d")
    log.info("Read-only. No DB writes. Verdict binds per registration.")
    log.info("=" * 60)

    qqq_d   = fetch_bars("QQQ",  "1Day",  args.days)
    qqq_30  = fetch_bars("QQQ",  "30Min", args.days)
    tqqq_30 = fetch_bars("TQQQ", "30Min", args.days)
    days = build_days(qqq_d, qqq_30, tqqq_30)
    if len(days) < 200:
        log.error(f"Only {len(days)} matched sessions — data problem, aborting")
        sys.exit(1)
    log.info(f"Matched sessions: {len(days)} ({days[0][0]} -> {days[-1][0]})")

    up_big   = [r for _, m, r in days if m >= BIG_MOVE]
    dn_big   = [r for _, m, r in days if m <= -BIG_MOVE]
    quiet    = [r for _, m, r in days if abs(m) < MID_MOVE]
    s_up, s_dn, s_q = stats(up_big), stats(dn_big), stats(quiet)
    log.info(f"UP   m>=+1%: n={s_up['n']} avg={s_up['avg']}% med={s_up['med']}% pos={s_up['pos']}%")
    log.info(f"DOWN m<=-1%: n={s_dn['n']} avg={s_dn['avg']}% med={s_dn['med']}% pos={s_dn['pos']}%")
    log.info(f"QUIET |m|<0.5%: n={s_q['n']} avg={s_q['avg']}% med={s_q['med']}%")

    # C1 / C2
    c1 = (s_up["n"] >= MIN_N and s_up["avg"] >= EFFECT_BAR
          and s_up["avg"] >= s_q["avg"] + BASELINE_EDGE)
    c2 = (s_dn["n"] >= MIN_N and s_dn["avg"] <= -EFFECT_BAR
          and s_dn["avg"] <= s_q["avg"] - BASELINE_EDGE)

    # C3 dose-response on continuation
    def cont_bucket(lo, hi):
        rows = [(1 if m > 0 else -1) * r for _, m, r in days if lo <= abs(m) < hi]
        return stats(rows)
    b1, b2, b3 = cont_bucket(0.5, 1.0), cont_bucket(1.0, 2.0), cont_bucket(2.0, 999)
    log.info(f"Continuation |m| 0.5-1%: n={b1['n']} avg={b1['avg']}% | "
             f"1-2%: n={b2['n']} avg={b2['avg']}% | >2%: n={b3['n']} avg={b3['avg']}%")
    c3 = b1["avg"] < b2["avg"] < b3["avg"]

    # C4 walk-forward
    half = len(days) // 2
    def pooled_cont(sub):
        rows = [(1 if m > 0 else -1) * r for _, m, r in sub if abs(m) >= BIG_MOVE]
        return stats(rows)
    p1, p2 = pooled_cont(days[:half]), pooled_cont(days[half:])
    log.info(f"WalkFwd pooled continuation |m|>=1%: H1 avg={p1['avg']}% (n={p1['n']}) | "
             f"H2 avg={p2['avg']}% (n={p2['n']})")
    c4 = p1["avg"] > 0 and p2["avg"] > 0

    # Placebo A
    best_side = max(abs(s_up["avg"]) if c1 else 0.0, abs(s_dn["avg"]) if c2 else 0.0)
    ref = best_side if best_side > 0 else max(abs(s_up["avg"]), abs(s_dn["avg"]))
    pa = abs(s_q["avg"]) < ref / 2 if ref > 0 else False
    # Placebo B: shuffle m across days
    real = cont_spread(days)
    ms   = [m for _, m, _ in days]
    rs   = [r for _, _, r in days]
    rng  = random.Random(42)
    beaten = 0
    for _ in range(PLACEBO_N):
        rng.shuffle(ms)
        fake = list(zip([""] * len(days), ms, rs))
        if real > cont_spread(fake):
            beaten += 1
    pctl = 100.0 * beaten / PLACEBO_N
    pb = pctl >= PLACEBO_PCTL
    log.info(f"Continuation spread big-vs-quiet: {round(real,4)}% | beats {pctl}% "
             f"of {PLACEBO_N} shuffles (need >={PLACEBO_PCTL}%)")

    # Verdict per registration
    if not pb or not c4 or (not c1 and not c2):
        verdict = "KILL"
    elif not c3 or not pa:
        verdict = "KILL" if not pa and not c3 else "PARK"
    else:
        verdict = "KEEP"
    # PARK case: structure ok but best side under bar is impossible here since
    # C1/C2 embed the bar; PARK covers a single structural miss (C3 or PA).

    log.info("=" * 60)
    log.info(f"C1 up-flow:    {'PASS' if c1 else 'FAIL'}")
    log.info(f"C2 down-flow:  {'PASS' if c2 else 'FAIL'}")
    log.info(f"C3 dose-resp:  {'PASS' if c3 else 'FAIL'}")
    log.info(f"C4 walk-fwd:   {'PASS' if c4 else 'FAIL'}")
    log.info(f"PLACEBO A:     {'PASS' if pa else 'FAIL'} | PLACEBO B: {'PASS' if pb else 'FAIL'} ({pctl}%)")
    log.info(f"VERDICT: {verdict} | friction bar was {FRICTION_RT}% RT")
    log.info("=" * 60)

    send_alert(
        f"🧪 REBAL GATE 0 — QQQ->TQQQ close flow\n"
        f"──────────────────\n"
        f"UP n={s_up['n']} avg={s_up['avg']}% | DOWN n={s_dn['n']} avg={s_dn['avg']}%\n"
        f"QUIET avg={s_q['avg']}% | dose {b1['avg']}/{b2['avg']}/{b3['avg']}\n"
        f"WalkFwd {p1['avg']}% / {p2['avg']}% | placebo {pctl}%\n"
        f"C1 {'✅' if c1 else '❌'} C2 {'✅' if c2 else '❌'} C3 {'✅' if c3 else '❌'} "
        f"C4 {'✅' if c4 else '❌'} PA {'✅' if pa else '❌'} PB {'✅' if pb else '❌'}\n"
        f"──────────────────\n"
        f"VERDICT: {verdict}"
    )


if __name__ == "__main__":
    main()
