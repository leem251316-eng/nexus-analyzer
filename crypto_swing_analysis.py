#!/usr/bin/env python3
"""
crypto_swing_analysis.py V1.0 -- NEXUS Swing Point Condition Miner
====================================================================
Phase 1 of the swing-point idea: find confirmed swing highs/lows in one
pair's real price history, snapshot the technical conditions AT each swing
using the exact same indicator/bucket-key functions crypto_backtester.py
already uses (so results are directly comparable to the 79 buckets already
sitting in crypto_pattern_stats -- not a parallel taxonomy), then compare
those conditions against a random baseline of non-swing bars.

Read-only. Does not trade. Does not write to any DB table -- console
report + optional CSV export only, on purpose: crypto_backtester.py's own
fingerprints already have a known, disclosed contamination risk with live
mean-reversion scoring (different strategies writing into the same table
live scoring reads from). This tool's job is exploratory; it shouldn't
become a second source of that same problem.

THE ONE THING TO KEEP STRAIGHT WHILE READING RESULTS:
  Confirming a bar WAS a swing point is necessarily retrospective -- you
  can't know price is done falling until it turns back up. That's fine;
  hindsight is exactly what labeling swing points requires, same as
  labeling a historical trade a "win" only once you know the exit. What
  has to stay causal is the FEATURE SNAPSHOT at that bar -- every RSI /
  VWAP / trend / regime value below is computed using only bars up to and
  including the swing bar itself, never bars after it. That boundary is
  what makes this useful for a real-time Phase 2 rule instead of just a
  description of the past.

WHAT THIS CAN'T TELL YOU BY ITSELF:
  One coin at a few-percent zigzag over a year will likely land around
  100-300 confirmed swings. Split across the full 6-dimension bucket key
  that's mostly empty cells -- treat individual bucket hits as leads to
  re-test on a second pair or a longer window, not conclusions. The
  marginal (single-dimension) tables below have more real signal per
  sample than the full bucket-key table; read those first.

Environment: ALPACA_API_KEY, ALPACA_SECRET_KEY  (same as crypto_backtester.py)

Usage:
  python crypto_swing_analysis.py
  python crypto_swing_analysis.py --pair ETH-USDC --days 180
  python crypto_swing_analysis.py --zigzag-pct 1.5 --csv swings_btc.csv
"""

import os
import sys
import csv
import random
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any

import pandas as pd
import requests

from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SWING] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("swing")

# ── Environment (matches crypto_backtester.py) ──────────────────────────────
ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ALPACA_SYM_OVERRIDES = {"POL-USDC": "MATIC/USD"}

def to_alpaca_symbol(pair: str) -> str:
    return ALPACA_SYM_OVERRIDES.get(pair, pair.replace("-USDC", "/USD"))

FNG_BASE    = "https://api.alternative.me"
WARMUP_BARS = 60     # matches crypto_backtester.py -- min bars before a snapshot is trusted
WINDOW_BARS = 120    # matches simulate_pair's closes_window/volumes_window size
REGIME_BARS = 1050   # matches simulate_pair's regime_window size


# =============================================================================
# Indicator + bucket-key functions -- copied verbatim from crypto_backtester.py
# so a swing's "rsi_lt25|fg_fear|below|up|hl|wkdy" means exactly what it means
# in crypto_pattern_stats. If those change there, mirror the change here.
# =============================================================================

def calc_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    s        = pd.Series(closes, dtype=float)
    delta    = s.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

def calc_multi_tf_rsi(closes_5m: list) -> dict:
    rsi_5m  = calc_rsi(closes_5m, 14)
    rsi_1m  = calc_rsi(closes_5m[-20:],  7)
    rsi_15m = calc_rsi(closes_5m[-60:], 14)
    rsi_1h  = calc_rsi(closes_5m[-100:], 14)
    rsi_4h  = calc_rsi(closes_5m,        14)
    return {"1m": rsi_1m, "5m": rsi_5m, "15m": rsi_15m, "1h": rsi_1h, "4h": rsi_4h}

def calc_trend_structure(closes: list, lookback: int = 20) -> dict:
    if len(closes) < lookback:
        return {"higher_lows": False, "uptrend": False}
    prices = closes[-lookback:]
    mid    = lookback // 2
    fhl, shl = min(prices[:mid]), min(prices[mid:])
    fhh, shh = max(prices[:mid]), max(prices[mid:])
    return {"higher_lows": shl > fhl, "uptrend": shh > fhh and shl > fhl}

def calc_vwap(closes: list, volumes: list) -> Optional[float]:
    if len(closes) < 5 or len(volumes) < 5:
        return None
    tpv = sum(c * v for c, v in zip(closes, volumes))
    vol = sum(volumes)
    return tpv / vol if vol > 0 else None

def resample_5m_to_4h(closes_5m: list) -> list:
    return closes_5m[::48]

def detect_trend_regime(closes_regime_window: list) -> str:
    """Mirrors crypto_backtester.py detect_trend_regime_bt exactly."""
    closes_4h_approx = resample_5m_to_4h(closes_regime_window)
    t4 = calc_trend_structure(closes_4h_approx, lookback=20)
    t5 = calc_trend_structure(closes_regime_window, lookback=20)
    if t4.get("uptrend") and t5.get("higher_lows"):
        return "TRENDING"
    return "CHOPPY"

def compute_bucket_key(rsi_5m: Optional[float], fg: int, vwap_above: Optional[bool],
                        uptrend: bool, higher_lows: bool, is_weekend: bool) -> str:
    rsi5 = rsi_5m if rsi_5m is not None else 99
    rsi_b = ("rsi_lt25" if rsi5 < 25 else
             "rsi_25_35" if rsi5 < 35 else
             "rsi_35_40" if rsi5 < 40 else "rsi_gt40")
    fg_b   = "fg_fear" if fg < 30 else "fg_neutral" if fg < 60 else "fg_greed"
    vwap_b = "above" if vwap_above else ("below" if vwap_above is not None else "unk")
    return (f"{rsi_b}|{fg_b}|{vwap_b}|"
            f"{'up' if uptrend else 'dn'}|"
            f"{'hl' if higher_lows else 'no'}|"
            f"{'wknd' if is_weekend else 'wkdy'}")


# =============================================================================
# Data fetching -- same client/path as crypto_backtester.py fetch_all_crypto_bars,
# scoped to one pair.
# =============================================================================

def fetch_bars(pair: str, days: int) -> Optional[pd.DataFrame]:
    client   = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    alpaca_sym = to_alpaca_symbol(pair)

    log.info(f"Fetching {days}d 5-min bars for {pair} ({alpaca_sym})...")
    try:
        bars = client.get_crypto_bars(CryptoBarsRequest(
            symbol_or_symbols=alpaca_sym,
            timeframe=TimeFrame(5, TimeFrameUnit.Minute),
            start=start_dt,
            end=end_dt,
        ))
        df = bars.df
        if hasattr(df.index, "levels"):
            df = df.xs(alpaca_sym, level=0)
        if df.empty:
            log.error(f"{pair}: EMPTY")
            return None
        log.info(f"  {pair}: {len(df):,} bars")
        return df
    except Exception as e:
        log.error(f"{pair}: fetch failed: {e}")
        return None


def fetch_historical_fg(days: int) -> Dict[str, int]:
    """Identical to crypto_backtester.py fetch_historical_fg."""
    result: Dict[str, int] = {}
    try:
        r = requests.get(f"{FNG_BASE}/fng/", params={"limit": days + 5}, timeout=15)
        data = r.json().get("data", [])
        for d in data:
            date_str = datetime.fromtimestamp(int(d["timestamp"]), tz=timezone.utc).strftime("%Y-%m-%d")
            result[date_str] = int(d["value"])
        log.info(f"Fetched {len(result)} days of F&G history")
    except Exception as e:
        log.warning(f"F&G history fetch failed, defaulting to neutral (50): {e}")
    return result


# =============================================================================
# Zigzag swing detection
# =============================================================================

def detect_zigzag_swings(closes: List[float], pct_threshold: float) -> List[Dict[str, Any]]:
    """
    Standard %-based zigzag over closes. A swing is confirmed once price
    has retraced pct_threshold% from the running extreme in the opposite
    direction. `idx` on each returned swing is the bar of the actual
    extreme, not the later bar where the retrace confirmed it -- everything
    downstream snapshots conditions AT idx using only bars up to idx, never
    the confirmation bar or anything after it.
    """
    n = len(closes)
    if n < 3:
        return []

    swings: List[Dict[str, Any]] = []
    trend: Optional[str] = None
    extreme_idx = 0
    extreme_price = closes[0]

    for i in range(1, n):
        price = closes[i]

        if trend is None:
            change_pct = (price - extreme_price) / extreme_price * 100
            if change_pct >= pct_threshold:
                trend = "up"
                extreme_idx, extreme_price = i, price
            elif change_pct <= -pct_threshold:
                trend = "down"
                extreme_idx, extreme_price = i, price
            continue

        if trend == "up":
            if price >= extreme_price:
                extreme_idx, extreme_price = i, price
                continue
            retrace_pct = (extreme_price - price) / extreme_price * 100
            if retrace_pct >= pct_threshold:
                swings.append({"idx": extreme_idx, "price": extreme_price, "type": "HIGH"})
                trend = "down"
                extreme_idx, extreme_price = i, price
        else:
            if price <= extreme_price:
                extreme_idx, extreme_price = i, price
                continue
            retrace_pct = (price - extreme_price) / extreme_price * 100
            if retrace_pct >= pct_threshold:
                swings.append({"idx": extreme_idx, "price": extreme_price, "type": "LOW"})
                trend = "up"
                extreme_idx, extreme_price = i, price

    return swings


# =============================================================================
# Causal feature snapshot at a bar index
# =============================================================================

def snapshot_conditions(idx: int, closes: List[float], volumes: List[float],
                         times: list, fg_by_date: Dict[str, int]) -> Optional[Dict[str, Any]]:
    """
    Every value here comes from closes[:idx+1] / volumes[:idx+1] only --
    bars after idx never touch this function. Window sizes (120-bar snapshot,
    1050-bar regime) match simulate_pair's exactly so a swing's numbers mean
    the same thing as the equivalent live/backtest bar would show.
    """
    if idx < WARMUP_BARS:
        return None

    window_closes  = closes[max(0, idx - WINDOW_BARS):idx + 1]
    window_volumes = volumes[max(0, idx - WINDOW_BARS):idx + 1]
    regime_window  = closes[max(0, idx - REGIME_BARS):idx + 1]

    rsi_dict = calc_multi_tf_rsi(window_closes)
    rsi_5m   = rsi_dict.get("5m")
    trend    = calc_trend_structure(window_closes)
    vwap     = calc_vwap(window_closes, window_volumes)
    vwap_above = (window_closes[-1] > vwap) if vwap else None
    regime   = detect_trend_regime(regime_window)

    ts = times[idx]
    try:
        date_str = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else ""
    except Exception:
        date_str = ""
    fg = fg_by_date.get(date_str, 50)
    try:
        is_weekend = ts.weekday() >= 5 if hasattr(ts, "weekday") else False
    except Exception:
        is_weekend = False

    bucket_key = compute_bucket_key(rsi_5m, fg, vwap_above,
                                     trend.get("uptrend", False),
                                     trend.get("higher_lows", False),
                                     is_weekend)

    vwap_dist_pct = ((window_closes[-1] - vwap) / vwap * 100) if vwap else None
    avg_vol_20 = (sum(window_volumes[-20:]) / 20) if len(window_volumes) >= 20 else None
    vol_ratio  = (window_volumes[-1] / avg_vol_20) if (avg_vol_20 and window_volumes) else None

    return {
        "idx":           idx,
        "timestamp":     ts,
        "price":         window_closes[-1],
        "rsi_5m":        round(rsi_5m, 1) if rsi_5m is not None else None,
        "rsi_1h":        round(rsi_dict.get("1h"), 1) if rsi_dict.get("1h") is not None else None,
        "fg":            fg,
        "vwap_above":    vwap_above,
        "vwap_dist_pct": round(vwap_dist_pct, 3) if vwap_dist_pct is not None else None,
        "uptrend":       trend.get("uptrend", False),
        "higher_lows":   trend.get("higher_lows", False),
        "is_weekend":    is_weekend,
        "regime":        regime,
        "vol_ratio_20":  round(vol_ratio, 2) if vol_ratio is not None else None,
        "bucket_key":    bucket_key,
    }


def build_baseline(n_bars: int, swing_indices: set, sample_size: int,
                    seed: int, exclude_radius: int = 12) -> List[int]:
    """
    Random sample of bar indices NOT within exclude_radius bars of any
    confirmed swing -- without this, "baseline" quietly includes the
    shoulders of every swing and understates how distinctive swing
    conditions actually are.
    """
    excluded = set()
    for idx in swing_indices:
        excluded.update(range(max(0, idx - exclude_radius), idx + exclude_radius + 1))

    eligible = [i for i in range(WARMUP_BARS, n_bars) if i not in excluded]
    rng = random.Random(seed)
    if len(eligible) <= sample_size:
        return eligible
    return rng.sample(eligible, sample_size)


# =============================================================================
# Report
# =============================================================================

MIN_BUCKET_SAMPLES = 3  # below this, a bucket's swing frequency is noise, not signal

def _pct(n: int, total: int) -> float:
    return round(n / total * 100, 1) if total else 0.0

def _marginal_table(rows: List[Dict[str, Any]], field: str) -> Dict[Any, int]:
    counts: Dict[Any, int] = {}
    for r in rows:
        v = r.get(field)
        counts[v] = counts.get(v, 0) + 1
    return counts

def print_marginal(title: str, field: str, lows: list, highs: list, baseline: list):
    print(f"\n--- {title} ---", flush=True)
    lc, hc, bc = (_marginal_table(lows, field), _marginal_table(highs, field),
                  _marginal_table(baseline, field))
    all_vals = sorted(set(lc) | set(hc) | set(bc), key=lambda x: str(x))
    nL, nH, nB = len(lows), len(highs), len(baseline)
    print(f"  {'value':<10} {'LOWS':>16} {'HIGHS':>16} {'BASELINE':>16}", flush=True)
    for v in all_vals:
        l, h, b = lc.get(v, 0), hc.get(v, 0), bc.get(v, 0)
        print(f"  {str(v):<10} {l:>5} ({_pct(l,nL):>5.1f}%) {h:>5} ({_pct(h,nH):>5.1f}%) "
              f"{b:>5} ({_pct(b,nB):>5.1f}%)", flush=True)

def print_numeric_summary(title: str, field: str, lows: list, highs: list, baseline: list):
    def stats(rows):
        vals = [r[field] for r in rows if r.get(field) is not None]
        if not vals:
            return None
        return sum(vals) / len(vals), min(vals), max(vals), len(vals)
    print(f"\n--- {title} (mean [min, max], n) ---", flush=True)
    for label, rows in (("LOWS", lows), ("HIGHS", highs), ("BASELINE", baseline)):
        s = stats(rows)
        if s is None:
            print(f"  {label:<10}: no data", flush=True)
        else:
            print(f"  {label:<10}: {s[0]:.2f}  [{s[1]:.2f}, {s[2]:.2f}]  n={s[3]}", flush=True)

def print_bucket_lift(label: str, group: list, baseline: list):
    print(f"\n--- Full bucket key: {label} vs BASELINE (min {MIN_BUCKET_SAMPLES} samples, sorted by lift) ---", flush=True)
    gb = _marginal_table(group, "bucket_key")
    bb = _marginal_table(baseline, "bucket_key")
    nG, nB = len(group), len(baseline)
    rows = []
    for key, g_count in gb.items():
        if g_count < MIN_BUCKET_SAMPLES:
            continue
        g_pct = g_count / nG if nG else 0.0
        b_count = bb.get(key, 0)
        b_pct = b_count / nB if nB else 0.0
        lift = (g_pct / b_pct) if b_pct > 0 else float("inf")
        rows.append((key, g_count, g_pct * 100, b_count, b_pct * 100, lift))
    rows.sort(key=lambda r: (-(r[5] if r[5] != float("inf") else 1e9), -r[1]))
    if not rows:
        print(f"  (no bucket had >= {MIN_BUCKET_SAMPLES} {label} samples -- expected with n={nG} "
              f"points spread across a 6-dimension key; read the marginal tables above instead)", flush=True)
    for key, gc_, gp, bc_, bp, lift in rows[:15]:
        print(f"  {key:<45} {label.lower()}={gc_:>3} ({gp:4.1f}%)  baseline={bc_:>4} ({bp:4.1f}%)  lift={lift:4.2f}x", flush=True)


def send_csv_via_telegram(path: str, caption: str):
    """
    Sends the CSV as a Telegram document via T-Bone instead of writing it to
    /app/output for a retrieve_results.py-style log dump -- a few thousand
    CSV rows copy-pasted out of Deploy Logs isn't a usable spreadsheet.
    Fail-open, same as every other Telegram call in this codebase: missing
    creds or a failed send logs a warning and the run still exits clean.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("TELEGRAM_TOKEN/TELEGRAM_CHAT_ID not set -- skipping CSV delivery")
        return
    try:
        with open(path, "rb") as f:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                files={"document": (os.path.basename(path), f)},
                timeout=30,
            )
        if r.status_code == 200:
            log.info(f"Sent {path} to Telegram")
        else:
            log.warning(f"Telegram send failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        log.warning(f"Telegram send error: {e}")


def export_csv(path: str, lows: list, highs: list, baseline: list):
    fieldnames = ["point_type", "idx", "timestamp", "price", "rsi_5m", "rsi_1h", "fg",
                  "vwap_above", "vwap_dist_pct", "uptrend", "higher_lows", "is_weekend",
                  "regime", "vol_ratio_20", "bucket_key"]
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for point_type, rows in (("LOW", lows), ("HIGH", highs), ("BASELINE", baseline)):
            for r in rows:
                row = dict(r)
                row["point_type"] = point_type
                writer.writerow(row)
    log.info(f"Wrote {len(lows) + len(highs) + len(baseline)} rows to {path}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="NEXUS Crypto Swing Point Condition Miner")
    parser.add_argument("--pair",       default="BTC-USDC")
    parser.add_argument("--days",       type=int, default=365)
    parser.add_argument("--zigzag-pct", type=float, default=2.0,
                         help="Min pct retrace to confirm a swing (default 2.0)")
    parser.add_argument("--baseline-n", type=int, default=3000,
                         help="Random non-swing bars sampled as control group")
    parser.add_argument("--seed",       type=int, default=42)
    parser.add_argument("--csv",        default=None, help="Optional CSV export path")
    args = parser.parse_args()

    if not ALPACA_API_KEY:
        log.error("Missing ALPACA_API_KEY")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"NEXUS SWING ANALYSIS V1.0 -- {args.pair}")
    log.info(f"Days: {args.days} | Zigzag: {args.zigzag_pct}% | Baseline: {args.baseline_n}")
    log.info("=" * 60)

    fg_by_date = fetch_historical_fg(args.days)
    df = fetch_bars(args.pair, args.days)
    if df is None:
        sys.exit(1)

    closes  = df["close"].tolist()
    volumes = df["volume"].tolist() if "volume" in df.columns else [0.0] * len(closes)
    times   = df.index.tolist()

    log.info("Running zigzag swing detection...")
    swings      = detect_zigzag_swings(closes, args.zigzag_pct)
    swing_lows  = [s for s in swings if s["type"] == "LOW"]
    swing_highs = [s for s in swings if s["type"] == "HIGH"]
    log.info(f"Found {len(swing_lows)} swing lows, {len(swing_highs)} swing highs "
             f"({len(swings)} total, {len(closes):,} bars scanned)")

    if len(swings) < 10:
        log.warning("Fewer than 10 swings found -- consider a smaller --zigzag-pct")

    log.info("Snapshotting conditions at each swing (causal-only)...")
    low_features = []
    for s in swing_lows:
        f = snapshot_conditions(s["idx"], closes, volumes, times, fg_by_date)
        if f is not None:
            low_features.append(f)

    high_features = []
    for s in swing_highs:
        f = snapshot_conditions(s["idx"], closes, volumes, times, fg_by_date)
        if f is not None:
            high_features.append(f)

    swing_idx_set = {s["idx"] for s in swings}
    baseline_idx  = build_baseline(len(closes), swing_idx_set, args.baseline_n, args.seed)
    log.info(f"Sampling {len(baseline_idx)} baseline (non-swing) bars...")
    baseline_features = []
    for i in baseline_idx:
        f = snapshot_conditions(i, closes, volumes, times, fg_by_date)
        if f is not None:
            baseline_features.append(f)

    print(f"\n{'='*60}", flush=True)
    print(f"SWING ANALYSIS -- {args.pair} -- {args.days}d @ {args.zigzag_pct}% zigzag", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"Swing lows: {len(low_features)}  |  Swing highs: {len(high_features)}  |  "
          f"Baseline: {len(baseline_features)}", flush=True)

    print_numeric_summary("RSI (5m)", "rsi_5m", low_features, high_features, baseline_features)
    print_numeric_summary("Distance from VWAP (%)", "vwap_dist_pct", low_features, high_features, baseline_features)
    print_numeric_summary("Fear & Greed index", "fg", low_features, high_features, baseline_features)
    print_numeric_summary("Volume vs 20-bar avg (ratio)", "vol_ratio_20", low_features, high_features, baseline_features)
    print_marginal("VWAP position", "vwap_above", low_features, high_features, baseline_features)
    print_marginal("Regime", "regime", low_features, high_features, baseline_features)
    print_marginal("Higher lows", "higher_lows", low_features, high_features, baseline_features)
    print_marginal("Weekend", "is_weekend", low_features, high_features, baseline_features)
    print_bucket_lift("LOWS", low_features, baseline_features)
    print_bucket_lift("HIGHS", high_features, baseline_features)

    print(f"\n{'='*60}", flush=True)

    if args.csv:
        try:
            export_csv(args.csv, low_features, high_features, baseline_features)
            send_csv_via_telegram(
                args.csv,
                f"Swing analysis: {args.pair} {args.days}d @ {args.zigzag_pct}% zigzag\n"
                f"{len(low_features)} lows, {len(high_features)} highs, "
                f"{len(baseline_features)} baseline"
            )
        except Exception as e:
            # Fail-open: the console report above already ran to completion and
            # is sitting in Deploy Logs either way. A CSV/Telegram hiccup here
            # shouldn't turn a successful analysis into a crashed deployment.
            log.error(f"CSV export/delivery failed: {e}")

    log.info("DONE.")


if __name__ == "__main__":
    main()
