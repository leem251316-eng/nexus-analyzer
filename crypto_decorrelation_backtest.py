#!/usr/bin/env python3
"""
crypto_decorrelation_backtest.py V1.0 -- Regime Override Thesis Backtester
============================================================================
Tests the CORE, actually-testable half of V5.1's regime-gate exception:
during high BTC volatility, does a pair genuinely decorrelated from BTC
show better forward price action than a pair still tracking BTC closely?

NOT a full backtest of the live regime override -- that also requires the
pre-penalty subtotal to be "elite" (>=70 across all 8 other scoring
factors), and two of those factors (L2 order book snapshot, analyst
bridge score) have no retrievable history. Testing the decorrelation
half on its own honest merits instead of pretending to replicate the
whole gate.

Uses the SAME two fixes/behaviors as live crypto.py right now:
  - BTC realized vol: replicates what's ACTUALLY live -- a short
    (~25-equivalent-minute) window scaled to a daily-equivalent figure,
    NOT a hypothetical genuine 7-day window. Honest fidelity to what
    really gates entries today, not what the docstring claims.
  - Pair-BTC correlation: uses the JUST-FIXED alignment -- both series
    at the same 5-minute resolution, same window. The old (pre-fix) live
    version compared mismatched windows entirely; that version is not
    reproduced here since it would just be testing noise.

Multiple correlation bands (not just binary), multiple forward-return
horizons (15min/1hr/4hr -- TIME_FAILSAFE_HOURS=4 caps live hold time),
train/validation split, per-pair AND aggregate reporting.

Usage:
  python crypto_decorrelation_backtest.py              # 365 days, all 8 alts
  python crypto_decorrelation_backtest.py --days 180
  python crypto_decorrelation_backtest.py --pairs SOL-USDC,DOGE-USDC
"""

import os
import sys
import time
import math
import argparse
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DECORR-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("decorr_bt")

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET  = os.environ.get("ALPACA_SECRET_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ALT_PAIRS = ["SOL-USDC", "DOGE-USDC", "XRP-USDC", "DOT-USDC",
             "ADA-USDC", "LTC-USDC", "POL-USDC", "SUI-USDC"]
ALPACA_SYM = {p: p.replace("-USDC", "/USD") for p in ALT_PAIRS + ["BTC-USDC"]}

# ── Matches live crypto.py exactly (see V5.1 regime override) ──────────────
BTC_VOL_HIGH_THRESHOLD    = 5.0
CORR_WINDOW_BARS          = 20
REGIME_OVERRIDE_CORR_MAX  = 0.25

# ── Correlation bands for richer reporting than a binary cut ───────────────
CORR_BANDS = [
    ("decorrelated",   None, 0.25),
    ("weak",           0.25, 0.50),
    ("moderate",       0.50, 0.75),
    ("tracking_btc",   0.75, None),
]

# ── Forward-return horizons in 5-min bars: 15min/1hr/4hr ────────────────────
HORIZONS = {"15min": 3, "1hr": 12, "4hr": 48}


def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.info(f"[ALERT] {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=8
        )
    except Exception as e:
        log.warning(f"Telegram send error: {e}")


def fetch_bars(pairs: List[str], days: int) -> Dict[str, pd.DataFrame]:
    client   = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    result   = {}
    log.info(f"Fetching {days}d 5-min bars for {len(pairs)} pairs...")
    for pair in pairs:
        alpaca_sym = ALPACA_SYM.get(pair, pair.replace("-USDC", "/USD"))
        try:
            bars = client.get_crypto_bars(CryptoBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=TimeFrame(5, TimeFrameUnit.Minute),
                start=start_dt, end=end_dt,
            ))
            df = bars.df
            if hasattr(df.index, "levels"):
                df = df.xs(alpaca_sym, level=0)
            if not df.empty:
                result[pair] = df
                log.info(f"  {pair} ({alpaca_sym}): {len(df):,} bars")
        except Exception as e:
            log.error(f"  {pair}: {e}")
        time.sleep(0.3)
    log.info(f"Fetched {len(result)}/{len(pairs)} pairs")
    return result


def btc_realized_vol_short_window(btc_closes: List[float], idx: int,
                                   window: int = 5) -> float:
    """
    Replicates what's ACTUALLY live in crypto.py's get_btc_realized_vol():
    a short window (live's 30s-cadence deque, maxlen=50 =~ 25 minutes,
    translated to this backtester's 5-minute bars = ~5 bars) of log
    returns, std-dev scaled to a daily-equivalent % via sqrt(bars/day).
    NOT a genuine 7-day measure despite the live docstring's name --
    intentionally testing what really gates entries, not a hypothetical.
    288 = 5-min bars per day (24h * 12), the correct scaling constant for
    THIS backtester's bar interval -- live's 2880 assumes 30s bars, using
    that constant here would overstate vol by sqrt(10)x.
    """
    if idx < window:
        return 0.0
    prices = btc_closes[idx - window + 1:idx + 1]
    try:
        log_returns = [math.log(prices[i] / prices[i-1])
                       for i in range(1, len(prices)) if prices[i-1] > 0]
        if len(log_returns) < 3:
            return 0.0
        n   = len(log_returns)
        mu  = sum(log_returns) / n
        var = sum((r - mu) ** 2 for r in log_returns) / n
        daily_vol = (var ** 0.5) * (288 ** 0.5)
        return round(daily_vol * 100, 2)
    except Exception:
        return 0.0


def pair_btc_correlation_aligned(pair_closes: List[float], btc_closes: List[float],
                                  idx: int, window: int = CORR_WINDOW_BARS) -> Optional[float]:
    """
    Uses the JUST-FIXED live methodology: both series at the SAME 5-minute
    resolution, same window, properly time-aligned. This is what live
    crypto.py does now, post-fix -- not the old mismatched-window version.
    """
    if idx < window:
        return None
    x = pair_closes[idx - window + 1:idx + 1]
    y = btc_closes[idx - window + 1:idx + 1]
    n = min(len(x), len(y))
    if n < 5:
        return None
    x, y = x[-n:], y[-n:]
    try:
        mx, my = sum(x) / n, sum(y) / n
        num   = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
        den_x = sum((xi - mx) ** 2 for xi in x) ** 0.5
        den_y = sum((yi - my) ** 2 for yi in y) ** 0.5
        if den_x == 0 or den_y == 0:
            return None
        return round(num / (den_x * den_y), 3)
    except Exception:
        return None


def corr_band(corr: Optional[float]) -> Optional[str]:
    if corr is None:
        return None
    for name, lo, hi in CORR_BANDS:
        if (lo is None or corr > lo) and (hi is None or corr <= hi):
            return name
    return None


def run_pair(pair: str, pair_df: pd.DataFrame, btc_closes: List[float],
             start_idx: int) -> List[Dict]:
    """One pass over one pair's bars. For every bar where BTC vol is high,
    record the correlation band and forward returns at each horizon."""
    pair_closes = pair_df["close"].tolist()
    n = min(len(pair_closes), len(btc_closes))
    records = []

    max_horizon = max(HORIZONS.values())
    for i in range(CORR_WINDOW_BARS, n - max_horizon):
        btc_vol = btc_realized_vol_short_window(btc_closes, i)
        if btc_vol < BTC_VOL_HIGH_THRESHOLD:
            continue
        band = corr_band(pair_btc_correlation_aligned(pair_closes, btc_closes, i))
        if band is None:
            continue

        entry_price = pair_closes[i]
        if entry_price <= 0:
            continue
        fwd_returns = {}
        for h_name, h_bars in HORIZONS.items():
            fwd_price = pair_closes[i + h_bars]
            fwd_returns[h_name] = (fwd_price - entry_price) / entry_price * 100

        records.append({
            "pair": pair, "idx": i, "band": band,
            "btc_vol": btc_vol, "validate": i >= start_idx,
            **fwd_returns,
        })
    return records


def summarize(records: List[Dict], horizon: str) -> Dict:
    if not records:
        return {"n": 0, "avg": 0.0, "win_rate": 0.0}
    vals = [r[horizon] for r in records]
    wins = sum(1 for v in vals if v > 0)
    return {
        "n": len(records),
        "avg": round(sum(vals) / len(vals), 4),
        "win_rate": round(wins / len(vals) * 100, 1),
    }


def main():
    parser = argparse.ArgumentParser(description="BTC-decorrelation regime backtester")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--pairs", type=str, default=",".join(ALT_PAIRS))
    args = parser.parse_args()
    pairs = [p.strip() for p in args.pairs.split(",")]

    if not ALPACA_API_KEY:
        log.error("Missing ALPACA_API_KEY")
        sys.exit(1)

    log.info("=" * 70)
    log.info("BTC-DECORRELATION REGIME BACKTEST V1.0")
    log.info(f"Pairs: {len(pairs)} | Days: {args.days} | "
             f"Bands: {[b[0] for b in CORR_BANDS]} | Horizons: {list(HORIZONS.keys())}")
    log.info("=" * 70)

    send_alert(
        f"🔬 DECORRELATION BACKTEST STARTING\n"
        f"{len(pairs)} pairs | {args.days} days\n"
        f"Testing: does real BTC-decorrelation during high vol\n"
        f"predict better forward returns?"
    )

    t0 = time.time()
    all_bars = fetch_bars(pairs + ["BTC-USDC"], args.days)
    if "BTC-USDC" not in all_bars:
        log.error("No BTC data -- cannot proceed")
        sys.exit(1)
    btc_closes = all_bars["BTC-USDC"]["close"].tolist()

    all_records: List[Dict] = []
    for pair in pairs:
        if pair not in all_bars:
            continue
        df = all_bars[pair]
        start_idx = int(len(df) * 0.75)
        recs = run_pair(pair, df, btc_closes, start_idx)
        all_records.extend(recs)
        log.info(f"  {pair}: {len(recs):,} high-vol bars captured across all bands")

    log.info(f"Total high-BTC-vol bars across all pairs: {len(all_records):,}")

    # ── Per-band, per-horizon summary (aggregate across all pairs) ─────────
    print(f"\n{'='*100}")
    print(f"AGGREGATE -- ranked by 1hr validation avg return")
    print(f"{'='*100}")
    print(f"{'Band':<16}{'Horizon':<10}{'TRAIN n/wr/avg':<24}{'VAL n/wr/avg':<24}")
    band_names = [b[0] for b in CORR_BANDS]
    ranked_bands = []
    for band in band_names:
        band_recs = [r for r in all_records if r["band"] == band]
        train_recs = [r for r in band_recs if not r["validate"]]
        val_recs   = [r for r in band_recs if r["validate"]]
        val_1hr = summarize(val_recs, "1hr")
        ranked_bands.append((band, val_1hr["avg"], train_recs, val_recs))
    ranked_bands.sort(key=lambda x: x[1], reverse=True)

    for band, _, train_recs, val_recs in ranked_bands:
        for h_name in HORIZONS:
            t = summarize(train_recs, h_name)
            v = summarize(val_recs, h_name)
            print(f"{band:<16}{h_name:<10}"
                  f"{t['n']}/{t['win_rate']}%/{t['avg']:+.3f}%{'':<8}"
                  f"{v['n']}/{v['win_rate']}%/{v['avg']:+.3f}%")
        print(f"{'-'*100}")

    # ── Per-pair breakdown (1hr horizon only, keep it scannable) ───────────
    print(f"\n{'='*100}")
    print(f"PER-PAIR -- 1hr horizon, validation only")
    print(f"{'='*100}")
    print(f"{'Pair':<12}", end="")
    for band in band_names:
        print(f"{band:<18}", end="")
    print()
    for pair in pairs:
        if pair not in all_bars:
            continue
        print(f"{pair:<12}", end="")
        for band in band_names:
            recs = [r for r in all_records
                     if r["pair"] == pair and r["band"] == band and r["validate"]]
            s = summarize(recs, "1hr")
            print(f"n={s['n']} {s['avg']:+.2f}%{'':<6}", end="")
        print()

    elapsed = time.time() - t0
    print(f"\n{'='*100}")
    print(f"Elapsed: {elapsed:.0f}s")
    print(f"{'='*100}")

    best_band, best_avg, _, best_val = ranked_bands[0]
    worst_band, worst_avg, _, worst_val = ranked_bands[-1]
    send_alert(
        f"✅ DECORRELATION BACKTEST COMPLETE\n"
        f"──────────────────\n"
        f"Best band (1hr val): {best_band} | n={len(best_val)} | {best_avg:+.3f}%\n"
        f"Worst band (1hr val): {worst_band} | n={len(worst_val)} | {worst_avg:+.3f}%\n"
        f"──────────────────\n"
        f"Full breakdown in Railway logs\n"
        f"Elapsed: {round(elapsed)}s"
    )
    log.info(f"DONE in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
