#!/usr/bin/env python3
"""
crypto_swing_precision_backtest.py V1.0 -- NEXUS Swing Gate Precision Backtest
================================================================================
Phase 1 (crypto_swing_analysis.py) measured ENRICHMENT: how much more common
a condition is at a confirmed swing low than at a random bar. That's not the
same as knowing what happens if you actually buy every time you SEE the
condition, before knowing whether it's the real bottom or a bar on the way to
a bigger drop. A bucket with 11x lift can still mean a ~2% real hit rate once
every bar that ever matched it is counted, not just the handful that happened
to be the exact low.

This is that test: takes the entry condition Phase 1 actually supports, adds
BTC's existing stop/TP from crypto_backtester.py's RECIPES (reusing an
already-used risk profile instead of inventing a new untested one), and walks
it bar-by-bar through real price action, same discipline as every backtest
in this codebase.

DERIVATION -- why this gate, not the full 6-dimension bucket key:
  The dominant LOWS buckets across all five coins tested in Phase 1 all
  shared the same core -- RSI<40, below VWAP, downtrend, NOT higher-lows --
  while varying freely across F&G band and weekday/weekend. That means F&G
  and weekend weren't doing real discriminating work on top of RSI/VWAP/
  trend; they just rode along because RSI/VWAP/trend already selected for
  fear-correlated conditions. Dropping them isn't a guess, it's what the
  bucket table itself shows once you look at what's actually constant
  across the high-lift rows versus what's just noise.

  Second simplification, worth stating precisely: calc_trend_structure's
  `uptrend` is defined as (higher_high AND higher_low) -- i.e. uptrend
  REQUIRES higher_lows as one of its two components. So "NOT uptrend AND
  NOT higher_lows" collapses to just "NOT higher_lows" -- the "downtrend"
  half of the bucket key ("dn") was never adding independent information
  once "no" (not higher-lows) was already required. The real gate is three
  independent conditions, not four:

      RSI(5m) < 40   AND   price < VWAP(120-bar)   AND   NOT higher_lows(20-bar)

  F&G is still recorded per-trade in the CSV for diagnostics -- not gated
  on, since Phase 1 showed it doesn't discriminate direction -- so it's
  possible to check after the fact whether it correlates with win rate
  among triggered trades even though it isn't part of the decision.

NOT out-of-sample in the strict sense. The gate's SHAPE (which of six
possible dimensions to keep) was chosen after looking at swing_analysis's
lift tables on this same year of BTC data. Two real mitigations, not proof:
the RSI/F&G band boundaries themselves are pre-existing constants from
crypto_backtester.py's compute_bucket_key, not fit to this data, and the
same three-dimension pattern independently replicated on four other coins
(ETH/SOL/ADA/DOGE) that weren't used to pick the gate's shape. First-75%/
last-25% split is reported separately below, same convention
crypto_backtester.py already uses for its own walk-forward check, so the
most recent quarter alone is visible without pretending the full year is
clean holdout.

Runs TWO strategies side by side, same exits, same data:
  SWING_GATE -- the three-condition gate derived above.
  RSI_ONLY   -- BTC's existing live recipe gate (RSI < 38) alone. This is
                what's already deployed; the comparison is the actual point
                of this backtest, not just SWING_GATE's number in isolation.
  (RSI_ONLY is a simplified stand-in for the full live confidence engine --
  it does not replicate F&G gate-widening, sentiment scoring, or the
  historical bucket layer. Testing against the full engine is a bigger,
  separate undertaking; this is a fast sanity comparison, not a claim that
  RSI_ONLY is exactly what's running in production.)

Read-only. Does not trade, does not write to any DB table -- console report
(single write, not the many-small-print()-calls approach that Railway's log
viewer scrambled on the swing analysis script) + Telegram delivery of the
report and, optionally, a per-trade CSV. Same delivery pattern as
crypto_swing_analysis.py throughout, including the lesson from tonight:
--csv is a flag, not a free-text path, so a stale filename from a copy-
pasted command can't silently mislabel a different pair's data again.

Environment: ALPACA_API_KEY, ALPACA_SECRET_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
(same as crypto_swing_analysis.py -- if that's already running on the spare
Railway service, no new variables needed, just a different Start Command.)

Usage:
  python crypto_swing_precision_backtest.py
  python crypto_swing_precision_backtest.py --days 180
  python crypto_swing_precision_backtest.py --csv
"""

import os
import sys
import csv
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any, Tuple

import pandas as pd
import requests

from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SWING-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("swing_precision")

# ── Environment (matches crypto_swing_analysis.py / crypto_backtester.py) ───
ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FNG_BASE    = "https://api.alternative.me"
WARMUP_BARS = 60    # matches crypto_backtester.py / crypto_swing_analysis.py
WINDOW_BARS = 120   # matches simulate_pair's closes_window/volumes_window size
SLIPPAGE_PCT = 0.0005  # 0.05% half-spread, matches crypto_backtester.py

# BTC-USDC's existing recipe -- copied from crypto_backtester.py RECIPES so
# this test uses the SAME risk parameters already validated elsewhere in the
# system, instead of introducing a second untested free parameter alongside
# the entry condition itself.
BTC_STOP_PCT     = 0.012
BTC_TP_PCT       = 0.025
BTC_RSI_ENTRY_MAX = 38   # RECIPES["BTC-USDC"]["rsi_entry_max"] -- the RSI_ONLY comparison gate


# =============================================================================
# Indicator functions -- copied verbatim from crypto_backtester.py /
# crypto_swing_analysis.py. If those change, mirror the change here.
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


# =============================================================================
# Data fetching -- same client/path as crypto_swing_analysis.py
# =============================================================================

def fetch_bars(pair: str, days: int) -> Optional[pd.DataFrame]:
    client   = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    alpaca_sym = pair.replace("-USDC", "/USD")

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
    """Identical to crypto_backtester.py / crypto_swing_analysis.py."""
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
# Entry gates
# =============================================================================

def check_swing_gate(rsi_5m: Optional[float], vwap_above: Optional[bool], higher_lows: bool) -> bool:
    """RSI<40, below VWAP, NOT higher-lows -- see DERIVATION in the module
    docstring for why these three and not the full six-dimension bucket key."""
    return (rsi_5m is not None and rsi_5m < 40
            and vwap_above is False
            and higher_lows is False)

def check_rsi_only_gate(rsi_5m: Optional[float]) -> bool:
    """The existing live recipe gate, in isolation: RSI < 38."""
    return rsi_5m is not None and rsi_5m < BTC_RSI_ENTRY_MAX


# =============================================================================
# Simulation
# =============================================================================

def simulate_strategy(name: str, gate_fn, closes: List[float], volumes: List[float],
                       times: list, fg_by_date: Dict[str, int],
                       stop_pct: float, tp_pct: float) -> Tuple[List[Dict[str, Any]], int]:
    """
    Walks the bar series once. Enters whenever gate_fn(...) is True and flat,
    exits on stop, target, or end-of-data. Mirrors crypto_backtester.py
    simulate_pair's core loop (WARMUP_BARS, 120-bar window, slippage) minus
    partial exits and the BTC-vol alt-restriction gate -- not applicable
    here, this is BTC-only and testing one fixed rule, not the full
    confidence engine.

    Returns (trades, gate_fires) -- gate_fires counts bars where the gate
    condition was true WHILE FLAT (i.e. real entry opportunities), separate
    from the trade count, since one qualifying dip can span many consecutive
    bars but should only ever produce one trade.
    """
    trades: List[Dict[str, Any]] = []
    gate_fires = 0
    in_pos = False
    entry_price = 0.0
    entry_bar = 0
    entry_ts = None
    entry_fg: Optional[int] = None
    total_bars = len(closes)

    for i in range(WARMUP_BARS, total_bars):
        price = closes[i]

        if in_pos:
            profit_pct = (price - entry_price) / entry_price
            exit_reason = None
            if profit_pct <= -stop_pct:
                exit_reason = "STOP_LOSS"
            elif profit_pct >= tp_pct:
                exit_reason = "TAKE_PROFIT"
            elif i == total_bars - 1:
                exit_reason = "END_OF_DATA"

            if exit_reason:
                exit_price = price * (1 - SLIPPAGE_PCT)
                pnl_pct = (exit_price - entry_price) / entry_price * 100
                trades.append({
                    "strategy":    name,
                    "entry_bar":   entry_bar,
                    "entry_ts":    entry_ts,
                    "exit_ts":     times[i],
                    "entry_price": round(entry_price, 2),
                    "exit_price":  round(exit_price, 2),
                    "pnl_pct":     round(pnl_pct, 4),
                    "won":         pnl_pct > 0,
                    "exit_reason": exit_reason,
                    "fg_at_entry": entry_fg,
                })
                in_pos = False
            continue

        window_closes  = closes[max(0, i - WINDOW_BARS):i + 1]
        window_volumes = volumes[max(0, i - WINDOW_BARS):i + 1]

        rsi_5m = calc_rsi(window_closes, 14)
        vwap   = calc_vwap(window_closes, window_volumes)
        vwap_above = (window_closes[-1] > vwap) if vwap else None
        trend  = calc_trend_structure(window_closes)
        higher_lows = trend.get("higher_lows", False)

        fires = gate_fn(rsi_5m, vwap_above, higher_lows) if name == "SWING_GATE" else gate_fn(rsi_5m)
        if fires:
            gate_fires += 1
            entry_price = price * (1 + SLIPPAGE_PCT)
            entry_bar = i
            entry_ts = times[i]
            try:
                date_str = times[i].strftime("%Y-%m-%d") if hasattr(times[i], "strftime") else ""
            except Exception:
                date_str = ""
            entry_fg = fg_by_date.get(date_str, 50)
            in_pos = True

    return trades, gate_fires


# =============================================================================
# Report
# =============================================================================

def summarize(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return {"n": 0, "wr": None, "avg_pnl": None}
    n = len(trades)
    wins = sum(1 for t in trades if t["won"])
    avg_pnl = sum(t["pnl_pct"] for t in trades) / n
    return {"n": n, "wr": round(wins / n * 100, 1), "avg_pnl": round(avg_pnl, 4)}

def exit_breakdown(trades: List[Dict[str, Any]]) -> str:
    if not trades:
        return "no trades"
    counts: Dict[str, int] = {}
    for t in trades:
        counts[t["exit_reason"]] = counts.get(t["exit_reason"], 0) + 1
    return "  ".join(f"{k}x{v}" for k, v in sorted(counts.items()))

def fg_breakdown(trades: List[Dict[str, Any]]) -> List[str]:
    """SWING_GATE win rate by F&G band at entry -- diagnostic only, F&G isn't
    part of the gate decision, this just checks whether it correlates with
    outcome among the trades that did trigger."""
    bands = [("fear (<30)", lambda f: f < 30), ("neutral (30-59)", lambda f: 30 <= f < 60),
             ("greed (60+)", lambda f: f >= 60)]
    lines = []
    for label, pred in bands:
        sub = [t for t in trades if t["fg_at_entry"] is not None and pred(t["fg_at_entry"])]
        s = summarize(sub)
        if s["n"] == 0:
            lines.append(f"  {label:<16}: no trades")
        else:
            lines.append(f"  {label:<16}: n={s['n']:<4} WR={s['wr']}%  avg={s['avg_pnl']:+.3f}%")
    return lines

def format_strategy_block(label: str, trades: List[Dict[str, Any]], gate_fires: int,
                           total_bars: int) -> List[str]:
    split_idx = int(total_bars * 0.75)
    last_q    = [t for t in trades if t["entry_bar"] >= split_idx]

    full = summarize(trades)
    late = summarize(last_q)

    lines = [f"\n--- {label} ---"]
    lines.append(f"  Gate fired while flat: {gate_fires:,} bars")
    if full["n"] == 0:
        lines.append("  Trades: 0 -- gate never fired into a flat position, nothing to report")
        return lines
    lines.append(f"  Trades:      {full['n']}")
    lines.append(f"  Win rate:    {full['wr']}%")
    lines.append(f"  Avg P&L:     {full['avg_pnl']:+.3f}%")
    lines.append(f"  Exits:       {exit_breakdown(trades)}")
    if late["n"] == 0:
        lines.append("  Last 25% only: 0 trades")
    else:
        lines.append(f"  Last 25% only: n={late['n']}  WR={late['wr']}%  avg={late['avg_pnl']:+.3f}%")
    return lines


def export_csv(path: str, all_trades: List[Dict[str, Any]]):
    fieldnames = ["strategy", "entry_bar", "entry_ts", "exit_ts", "entry_price",
                  "exit_price", "pnl_pct", "won", "exit_reason", "fg_at_entry"]
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in all_trades:
            writer.writerow(t)
    log.info(f"Wrote {len(all_trades)} rows to {path}")


def send_file_via_telegram(path: str, caption: str):
    """Identical to crypto_swing_analysis.py's version. Fail-open: missing
    creds or a failed send logs a warning, run still exits clean."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning(f"TELEGRAM_TOKEN/TELEGRAM_CHAT_ID not set -- skipping delivery of {path}")
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


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="NEXUS Swing Gate Precision Backtest")
    parser.add_argument("--pair", default="BTC-USDC", help="BTC-USDC only is validated for this gate")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--csv",  action="store_true", help="Also export and send the per-trade CSV")
    args = parser.parse_args()

    if not ALPACA_API_KEY:
        log.error("Missing ALPACA_API_KEY")
        sys.exit(1)

    if args.pair != "BTC-USDC":
        log.warning(f"{args.pair}: this gate's shape was derived from BTC's dominant buckets -- "
                    f"running it on another pair is a different, unvalidated question, not a repeat of tonight's test")

    log.info("=" * 60)
    log.info(f"NEXUS SWING GATE PRECISION BACKTEST V1.0 -- {args.pair}")
    log.info(f"Days: {args.days}")
    log.info("=" * 60)

    fg_by_date = fetch_historical_fg(args.days)
    df = fetch_bars(args.pair, args.days)
    if df is None:
        sys.exit(1)

    closes  = df["close"].tolist()
    volumes = df["volume"].tolist() if "volume" in df.columns else [0.0] * len(closes)
    times   = df.index.tolist()
    total_bars = len(closes)

    log.info("Running SWING_GATE simulation...")
    swing_trades, swing_fires = simulate_strategy(
        "SWING_GATE", check_swing_gate, closes, volumes, times, fg_by_date, BTC_STOP_PCT, BTC_TP_PCT
    )
    log.info(f"  {len(swing_trades)} trades")

    log.info("Running RSI_ONLY simulation...")
    rsi_trades, rsi_fires = simulate_strategy(
        "RSI_ONLY", check_rsi_only_gate, closes, volumes, times, fg_by_date, BTC_STOP_PCT, BTC_TP_PCT
    )
    log.info(f"  {len(rsi_trades)} trades")

    report: List[str] = []
    report.append(f"\n{'='*60}")
    report.append(f"SWING GATE PRECISION BACKTEST -- {args.pair} -- {args.days}d")
    report.append(f"{'='*60}")
    report.append("Gate: RSI(5m) < 40  AND  below VWAP(120)  AND  NOT higher_lows(20)")
    report.append(f"Exits: stop -{BTC_STOP_PCT*100:.1f}%  /  target +{BTC_TP_PCT*100:.1f}%  "
                   f"(BTC-USDC recipe, crypto_backtester.py)")
    report.append(f"Bars scanned: {total_bars:,}  |  Walk-forward split at bar {int(total_bars*0.75):,} (75%)")

    report += format_strategy_block("SWING_GATE", swing_trades, swing_fires, total_bars)
    report += format_strategy_block("RSI_ONLY (existing live gate, isolated)", rsi_trades, rsi_fires, total_bars)

    if swing_trades:
        report.append(f"\n--- SWING_GATE win rate by F&G at entry (diagnostic -- not gated on) ---")
        report += fg_breakdown(swing_trades)

    report.append(f"\n{'='*60}")
    report.append("NOT strict out-of-sample -- see module docstring DERIVATION note. "
                   "Last-25% split above is the closest thing to a fresh look this run offers.")
    report.append(f"{'='*60}")

    print("\n".join(report), flush=True)

    try:
        report_path = f"/tmp/swing_precision_report_{args.pair.replace('-', '_').lower()}.txt"
        with open(report_path, "w") as f:
            f.write("\n".join(report))
        send_file_via_telegram(
            report_path,
            f"Swing gate precision backtest: {args.pair} {args.days}d\n"
            f"SWING_GATE: {len(swing_trades)} trades  |  RSI_ONLY: {len(rsi_trades)} trades"
        )
    except Exception as e:
        log.error(f"Report file/delivery failed: {e}")

    if args.csv:
        all_trades = swing_trades + rsi_trades
        csv_path = f"/tmp/swing_precision_trades_{args.pair.replace('-', '_').lower()}.csv"
        try:
            export_csv(csv_path, all_trades)
            send_file_via_telegram(
                csv_path,
                f"Swing gate precision trades: {args.pair} {args.days}d\n"
                f"SWING_GATE: {len(swing_trades)}  |  RSI_ONLY: {len(rsi_trades)}"
            )
        except Exception as e:
            log.error(f"CSV export/delivery failed: {e}")

    log.info("DONE.")


if __name__ == "__main__":
    main()
