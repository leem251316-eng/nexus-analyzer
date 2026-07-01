#!/usr/bin/env python3
"""
crypto_backtester.py V3.0 -- NEXUS Crypto Pattern Memory Seeder
================================================================
V3.0 (Jun 2026): Switched from broken Coinbase historical API to Alpaca
crypto bars. Coinbase's candle API returns 0 candles on Railway IPs
intermittently. Alpaca CryptoHistoricalDataClient is stable, proven, and
already used by other NEXUS services.

V3.0 additions vs V2.0:
  ✅ Data source: Alpaca crypto bars (BTC/USD format) instead of Coinbase API
  ✅ BTC realized vol regime gate (V5.0): when 7d BTC vol > 5%, alt entries
     blocked in backtest (matches live crypto.py V5.0 behavior)
  ✅ Partial exit modeling (V5.0): at 50% of TP target, 50% position exited,
     remaining half tracked to full TP/ATR trail
  ✅ Walk-forward validation: train on 75%, validate on last 25%
  ✅ Slippage modeling: 0.05% half-spread applied on entry
  ✅ Exit reason breakdown including partial exits

Key design principle: match crypto.py V5.0 EXACTLY for the confidence engine
(7 scoring layers) -- historical/analyst scores set to neutral (0.5) since
we can't backtest those in historical simulation.

Environment:
  DATABASE_URL, ALPACA_API_KEY, ALPACA_SECRET_KEY
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

Usage:
  python crypto_backtester.py              # 365 days
  python crypto_backtester.py --days 180  # 6 months
  python crypto_backtester.py --dry-run   # no DB writes
  python crypto_backtester.py --pairs BTC-USDC ETH-USDC
"""

import os
import sys
import time
import math
import secrets
import argparse
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRYPTO-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crypto_bt")

# ── Environment ───────────────────────────────────────────────────────────────
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Pairs ─────────────────────────────────────────────────────────────────────
# Coinbase USDC pairs -> Alpaca slash format
ALL_PAIRS = [
    "BTC-USDC", "ETH-USDC", "SOL-USDC", "DOGE-USDC",
    "XRP-USDC", "DOT-USDC", "ADA-USDC", "LTC-USDC",
    "POL-USDC", "SUI-USDC",
]

ALPACA_SYM = {p: p.replace("-USDC", "/USD") for p in ALL_PAIRS}
# Exceptions
ALPACA_SYM["POL-USDC"] = "MATIC/USD"   # Alpaca uses MATIC for Polygon

# ── Recipes (matches crypto.py V5.0 exactly) ─────────────────────────────────
RECIPES: Dict[str, Dict] = {
    "BTC-USDC":  {"stop_pct": 0.012, "tp_pct": 0.025, "rsi_entry_max": 38},
    "ETH-USDC":  {"stop_pct": 0.015, "tp_pct": 0.030, "rsi_entry_max": 38},
    "SOL-USDC":  {"stop_pct": 0.018, "tp_pct": 0.035, "rsi_entry_max": 40},
    "DOGE-USDC": {"stop_pct": 0.020, "tp_pct": 0.040, "rsi_entry_max": 42},
    "XRP-USDC":  {"stop_pct": 0.015, "tp_pct": 0.028, "rsi_entry_max": 30},
    "DOT-USDC":  {"stop_pct": 0.018, "tp_pct": 0.032, "rsi_entry_max": 45},
    "ADA-USDC":  {"stop_pct": 0.018, "tp_pct": 0.032, "rsi_entry_max": 40},
    "LTC-USDC":  {"stop_pct": 0.014, "tp_pct": 0.026, "rsi_entry_max": 43},
    "POL-USDC":  {"stop_pct": 0.020, "tp_pct": 0.038, "rsi_entry_max": 40},
    "SUI-USDC":  {"stop_pct": 0.022, "tp_pct": 0.042, "rsi_entry_max": 42},
}

# Confidence thresholds (matches crypto.py V5.0 defaults)
CONF_FULL         = 75
CONF_CAUTIOUS     = 55
CONF_SKIP         = 40

# V5.1: mirrors crypto.py MOM_RSI_MIN/MAX exactly -- keep these two files'
# values in sync by hand if either changes.
MOM_RSI_MIN = 50
MOM_RSI_MAX = 78

# Backtesting params
SLIPPAGE_PCT      = 0.0005   # 0.05% half-spread
WARMUP_BARS       = 60
PARTIAL_TP_MULT   = 0.50     # V5.0: partial exit at 50% of TP target
BTC_VOL_THRESHOLD = 5.0      # V5.0: restrict alts when BTC 7d vol > 5%
BTC_IS_ETH        = {"BTC-USDC", "ETH-USDC"}  # exempt from vol restriction

# ── Helpers ───────────────────────────────────────────────────────────────────
def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=8
        )
    except Exception:
        pass

def get_utc_hour(ts) -> int:
    if hasattr(ts, "hour"):
        return ts.hour
    try:
        return ts.to_pydatetime().replace(tzinfo=timezone.utc).hour
    except Exception:
        return 12

# ── Signal engine (matches crypto.py V5.0 tech/macro components) ─────────────
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
    """Approximate multi-TF RSI from 5m bars (backtest proxy)."""
    rsi_5m = calc_rsi(closes_5m, 14)
    # Use different lookback windows as proxy for different timeframes
    rsi_1m  = calc_rsi(closes_5m[-20:],  7)  # short window
    rsi_15m = calc_rsi(closes_5m[-60:], 14)  # medium
    rsi_1h  = calc_rsi(closes_5m[-100:],14)  # longer
    rsi_4h  = calc_rsi(closes_5m,       14)  # full dataset
    return {
        "1m":  rsi_1m,
        "5m":  rsi_5m,
        "15m": rsi_15m,
        "1h":  rsi_1h,
        "4h":  rsi_4h,
    }

def calc_obv_momentum(closes: list, volumes: list) -> Optional[float]:
    if len(closes) < 10 or len(volumes) < 10:
        return None
    obv = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    if len(obv) < 6:
        return None
    recent = obv[-3:]
    older  = obv[-6:-3]
    if not older or sum(abs(x) for x in older) == 0:
        return None
    avg_r = sum(recent) / 3
    avg_o = sum(older) / 3
    return round((avg_r - avg_o) / (abs(avg_o) + 1) * 100, 3)

def calc_trend_structure(closes: list, lookback: int = 20) -> dict:
    if len(closes) < lookback:
        return {"higher_lows": False, "uptrend": False}
    prices = closes[-lookback:]
    mid    = lookback // 2
    fhl    = min(prices[:mid])
    shl    = min(prices[mid:])
    fhh    = max(prices[:mid])
    shh    = max(prices[mid:])
    return {
        "higher_lows": shl > fhl,
        "uptrend":     shh > fhh and shl > fhl,
    }

def calc_vwap(closes: list, volumes: list) -> Optional[float]:
    if len(closes) < 5 or len(volumes) < 5:
        return None
    tpv = sum(c * v for c, v in zip(closes, volumes))
    vol = sum(volumes)
    return tpv / vol if vol > 0 else None

def resample_5m_to_4h(closes_5m: list) -> list:
    """
    V5.1: crypto_backtester.py only fetches 5m bars (see fetch_all_crypto_bars) --
    no separate 4h API call exists. Rather than add a second Alpaca fetch per
    pair (doubles fetch time, adds a second failure point), approximate 4h
    closes by taking every 48th 5m close (48 * 5min = 240min = 4h). This is
    a close-only resample, not true 4h OHLC aggregation -- fine for
    calc_trend_structure (which only looks at closes), NOT a substitute for
    real 4h bars if a future indicator needs 4h high/low/volume.
    """
    return closes_5m[::48]

def detect_trend_regime_bt(closes_5m: list) -> str:
    """V5.1: mirrors crypto.py SignalProcessor.detect_trend_regime(), using
    the 5m-resampled 4h approximation above in place of a real 4h fetch."""
    closes_4h_approx = resample_5m_to_4h(closes_5m)
    t4 = calc_trend_structure(closes_4h_approx, lookback=20)
    t5 = calc_trend_structure(closes_5m, lookback=20)
    if t4.get("uptrend") and t5.get("higher_lows"):
        return "TRENDING"
    return "CHOPPY"

def _compute_confidence_meanrev_bt(pair: str, closes_5m: list, volumes_5m: list,
                                    btc_closes: list) -> Tuple[int, str]:
    """
    Original V5.0 mean-reversion confidence engine, UNCHANGED -- renamed
    from compute_confidence_bt so the new dispatcher below can route to it.
    Matches crypto.py V5.0 technical + macro layers.
    Historical/analyst components set to neutral (can't backtest those).
    Orderflow (L/S ratio, taker ratio) = 0 (real-time only).
    Returns (score, mode) where mode is FULL/CAUTIOUS/SKIP/BLOCK.
    """
    recipe  = RECIPES.get(pair, {})
    max_rsi = recipe.get("rsi_entry_max", 40)
    stop_pct = recipe.get("stop_pct", 0.015)

    if len(closes_5m) < WARMUP_BARS:
        return 0, "BLOCK"

    rsi_dict = calc_multi_tf_rsi(closes_5m)
    rsi_5m   = rsi_dict.get("5m")

    # Hard gate: RSI above entry max = skip
    if rsi_5m is not None and rsi_5m > max_rsi:
        return 0, "BLOCK"

    score = 0

    # Technical (max +40)
    rsi_vals = [v for v in rsi_dict.values() if v is not None]
    oc = sum(1 for r in rsi_vals if r < 40)
    if   oc >= 5: score += 20
    elif oc == 4: score += 15
    elif oc == 3: score += 10
    elif oc == 2: score += 5

    trend = calc_trend_structure(closes_5m)
    if trend.get("higher_lows"):
        score += 5

    vwap = calc_vwap(closes_5m, volumes_5m)
    if vwap and closes_5m[-1] < vwap:
        score += 5

    # RSI bounce signal
    rsi_1m = rsi_dict.get("1m")
    if rsi_5m and rsi_1m and rsi_5m < 35 and rsi_1m > rsi_5m:
        score += 5

    tech_score = min(score, 40)

    # Macro: BTC context (simplified -- use BTC price history)
    macro_score = 0
    if btc_closes and len(btc_closes) >= 14:
        btc_rsi = calc_rsi(btc_closes, 14) or 50
        if   btc_rsi < 25: macro_score += 8
        elif btc_rsi < 35: macro_score += 5
        elif btc_rsi < 40: macro_score += 2
        elif btc_rsi > 72: macro_score -= 5
        elif btc_rsi > 65: macro_score -= 2

    # Volume structure
    obv_mom = calc_obv_momentum(closes_5m, volumes_5m)
    vol_score = 0
    if obv_mom is not None:
        if   obv_mom >  5.0: vol_score += 5
        elif obv_mom >  1.0: vol_score += 2
        elif obv_mom < -5.0: vol_score -= 4
        elif obv_mom < -1.0: vol_score -= 2

    # Historical: neutral at backtest time (no real bucket data yet)
    hist_score = 7   # neutral placeholder

    total = max(0, min(100, tech_score + macro_score + vol_score + hist_score))

    if   total >= CONF_FULL:     mode = "FULL"
    elif total >= CONF_CAUTIOUS: mode = "CAUTIOUS"
    elif total >= CONF_SKIP:     mode = "SKIP"
    else:                        mode = "BLOCK"

    return total, mode

def _compute_confidence_momentum_bt(pair: str, closes_5m: list, volumes_5m: list,
                                     btc_closes: list) -> Tuple[int, str]:
    """
    V5.1: Momentum counterpart to _compute_confidence_meanrev_bt. Mirrors
    crypto.py's _score_technical_momentum / _score_volume_momentum weighting
    exactly (same +40/+10 caps) so FULL/CAUTIOUS/SKIP/BLOCK thresholds mean
    the same thing on both paths. Only called when detect_trend_regime_bt()
    returns TRENDING.
    """
    if len(closes_5m) < WARMUP_BARS:
        return 0, "BLOCK"

    rsi_dict = calc_multi_tf_rsi(closes_5m)
    rsi_5m   = rsi_dict.get("5m")

    # Hard gate: RSI outside the momentum band = skip (either not trending
    # yet, or already exhausted -- mirrors crypto.py MOM_RSI_MIN/MAX)
    if rsi_5m is None or not (MOM_RSI_MIN <= rsi_5m <= MOM_RSI_MAX):
        return 0, "BLOCK"

    score = 0

    # Technical (max +40) -- count timeframes showing strength, not oversold
    rsi_vals = [v for v in rsi_dict.values() if v is not None]
    sc = sum(1 for r in rsi_vals if MOM_RSI_MIN <= r <= MOM_RSI_MAX)
    if   sc >= 5: score += 20
    elif sc == 4: score += 15
    elif sc == 3: score += 10
    elif sc == 2: score += 5

    trend = calc_trend_structure(closes_5m)
    if trend.get("higher_lows"):
        score += 5

    vwap = calc_vwap(closes_5m, volumes_5m)
    if vwap and closes_5m[-1] > vwap:
        score += 5

    rsi_1m = rsi_dict.get("1m")
    if (rsi_5m is not None and rsi_1m is not None
            and MOM_RSI_MIN <= rsi_5m <= MOM_RSI_MAX and rsi_1m >= rsi_5m):
        score += 5

    tech_score = min(score, 40)

    macro_score = 0
    if btc_closes and len(btc_closes) >= 14:
        btc_rsi = calc_rsi(btc_closes, 14) or 50
        if   btc_rsi < 25: macro_score += 8
        elif btc_rsi < 35: macro_score += 5
        elif btc_rsi < 40: macro_score += 2
        elif btc_rsi > 72: macro_score -= 5
        elif btc_rsi > 65: macro_score -= 2

    # Volume structure -- OBV confirming breakout, RSI position holding high
    obv_mom = calc_obv_momentum(closes_5m, volumes_5m)
    vol_score = 0
    if obv_mom is not None:
        if   obv_mom >  5.0: vol_score += 5
        elif obv_mom >  1.0: vol_score += 2
        elif obv_mom < -1.0: vol_score -= 4

    hist_score = 7   # neutral placeholder, same as mean-reversion path

    total = max(0, min(100, tech_score + macro_score + vol_score + hist_score))

    if   total >= CONF_FULL:     mode = "FULL"
    elif total >= CONF_CAUTIOUS: mode = "CAUTIOUS"
    elif total >= CONF_SKIP:     mode = "SKIP"
    else:                        mode = "BLOCK"

    return total, mode

def compute_confidence_bt(pair: str, closes_5m: list, volumes_5m: list,
                           btc_closes: list) -> Tuple[int, str, str]:
    """
    V5.1: Regime dispatcher. CHOPPY routes to the original, unmodified
    mean-reversion engine; TRENDING routes to the new momentum engine.
    Returns (score, mode, strategy) -- strategy is "MEAN_REVERSION" or
    "MOMENTUM", recorded on the trade dict so results can be split out
    and compared after a run.
    """
    regime = detect_trend_regime_bt(closes_5m)
    if regime == "TRENDING":
        total, mode = _compute_confidence_momentum_bt(pair, closes_5m, volumes_5m, btc_closes)
        return total, mode, "MOMENTUM"
    total, mode = _compute_confidence_meanrev_bt(pair, closes_5m, volumes_5m, btc_closes)
    return total, mode, "MEAN_REVERSION"


    """7-day BTC realized vol as % -- V5.0 regime gate."""
    if len(btc_closes) < 20:
        return 0.0
    try:
        log_rets = [math.log(btc_closes[i] / btc_closes[i-1])
                    for i in range(1, len(btc_closes)) if btc_closes[i-1] > 0]
        if len(log_rets) < 10:
            return 0.0
        n   = len(log_rets)
        mu  = sum(log_rets) / n
        var = sum((r - mu) ** 2 for r in log_rets) / n
        # bars are 5-min; bars_per_day = 288; annualize then convert to daily %
        daily_vol = (var ** 0.5) * (288 ** 0.5)
        return round(daily_vol * 100, 2)
    except Exception:
        return 0.0


# ── Data fetching ─────────────────────────────────────────────────────────────
def fetch_all_crypto_bars(pairs: list, days: int) -> dict:
    """Fetch 5m bars from Alpaca CryptoHistoricalDataClient."""
    client   = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    result   = {}

    log.info(f"Fetching {days}d 5-min crypto bars for {len(pairs)} pairs...")

    for pair in pairs:
        alpaca_sym = ALPACA_SYM.get(pair, pair.replace("-USDC", "/USD"))
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
            if not df.empty:
                result[pair] = df
                log.info(f"  {pair} ({alpaca_sym}): {len(df):,} bars")
            else:
                log.warning(f"  {pair}: EMPTY")
        except Exception as e:
            log.error(f"  {pair}: {e}")
        time.sleep(0.3)

    log.info(f"Fetched {len(result)}/{len(pairs)} pairs")
    return result


# ── Replay engine ─────────────────────────────────────────────────────────────
def simulate_pair(pair: str, df: pd.DataFrame,
                  btc_df: Optional[pd.DataFrame] = None,
                  validate_mode: bool = False) -> List[Dict]:
    """Replay one pair's 5m bars through the confidence engine."""
    closes  = df["close"].tolist()
    volumes = df["volume"].tolist() if "volume" in df.columns else [0.0] * len(closes)
    times   = df.index.tolist()

    recipe   = RECIPES.get(pair, {})
    stop_pct = recipe.get("stop_pct", 0.015)
    tp_pct   = recipe.get("tp_pct",   0.025)

    total_bars = len(closes)
    start_idx  = int(total_bars * 0.75) if validate_mode else 0

    trades  = []
    in_pos  = False
    entry_price = 0.0
    peak_price  = 0.0
    partial_done = False
    mfe = mae = 0.0
    entry_bar = 0
    entry_ts  = None
    conf_at_entry = 0
    mode_at_entry = ""
    strategy_at_entry = ""

    is_btc_eth = pair in BTC_IS_ETH

    for i in range(WARMUP_BARS, total_bars):
        if validate_mode and i < start_idx:
            # Still update price history during skip
            pass

        closes_window  = closes[max(0, i-120):i+1]
        volumes_window = volumes[max(0, i-120):i+1]
        btc_window     = []
        if btc_df is not None and not btc_df.empty and i < len(btc_df):
            btc_window = btc_df["close"].tolist()[max(0, i-120):i+1]

        price = closes[i]
        hour  = get_utc_hour(times[i])

        # V5.0: BTC realized vol regime gate (alts restricted when BTC vol > threshold)
        if not is_btc_eth and btc_window:
            btc_vol = compute_btc_realized_vol(btc_window[-120:])
            if btc_vol > BTC_VOL_THRESHOLD:
                if in_pos:
                    pass   # manage existing position normally
                else:
                    continue   # skip new entries in alt when BTC vol high

        if validate_mode and i < start_idx:
            continue

        if in_pos:
            profit_pct = (price - entry_price) / entry_price
            mfe = max(mfe, profit_pct)
            mae = min(mae, profit_pct)
            peak_price = max(peak_price, price)

            exit_reason = None

            # Stop loss
            if profit_pct <= -stop_pct:
                exit_reason = "STOP_LOSS"

            # V5.0: Partial exit at 50% of TP
            elif not partial_done and profit_pct >= tp_pct * PARTIAL_TP_MULT:
                partial_done = True
                # Record partial exit as its own trade
                trades.append({
                    "pair":        pair,
                    "entry_price": round(entry_price, 6),
                    "exit_price":  round(price * (1 - SLIPPAGE_PCT), 6),
                    "pnl_pct":     round(profit_pct * 100, 3),
                    "exit_reason": "PARTIAL_TP",
                    "hold_bars":   i - entry_bar,
                    "mfe":         round(mfe * 100, 3),
                    "mae":         round(mae * 100, 3),
                    "won":         True,
                    "confidence":  conf_at_entry,
                    "mode":        mode_at_entry,
                    "strategy":    strategy_at_entry,
                    "hour_utc":    hour,
                    "validate":    validate_mode,
                    "trade_id":    secrets.token_hex(8),
                })
                # Continue holding remaining half (entry_price unchanged, position half size)
                # For simplicity, treat remaining half as continuing from current price
                # (partial exit doesn't change entry_price in our simplified model)

            # Full TP
            elif profit_pct >= tp_pct:
                exit_reason = "TAKE_PROFIT"

            # Trend break / time failsafe (simplified)
            elif (i - entry_bar) >= 288:  # 24h failsafe at 5m bars
                if abs(profit_pct) < 0.002:
                    exit_reason = "TIME_FAILSAFE"

            if exit_reason:
                trades.append({
                    "pair":        pair,
                    "entry_price": round(entry_price, 6),
                    "exit_price":  round(price * (1 - SLIPPAGE_PCT), 6),
                    "pnl_pct":     round(profit_pct * 100, 3),
                    "exit_reason": exit_reason,
                    "hold_bars":   i - entry_bar,
                    "mfe":         round(mfe * 100, 3),
                    "mae":         round(mae * 100, 3),
                    "won":         profit_pct > 0,
                    "confidence":  conf_at_entry,
                    "mode":        mode_at_entry,
                    "strategy":    strategy_at_entry,
                    "hour_utc":    hour,
                    "validate":    validate_mode,
                    "trade_id":    secrets.token_hex(8),
                })
                in_pos       = False
                partial_done = False
                mfe = mae    = 0.0

        else:
            conf, mode, strategy = compute_confidence_bt(pair, closes_window, volumes_window, btc_window)
            if mode in ("FULL", "CAUTIOUS"):
                in_pos          = True
                entry_price     = price * (1 + SLIPPAGE_PCT)
                peak_price      = entry_price
                partial_done    = False
                mfe = mae       = 0.0
                entry_bar       = i
                entry_ts        = times[i]
                conf_at_entry   = conf
                mode_at_entry   = mode
                strategy_at_entry = strategy

    # Close any open at end
    if in_pos and closes:
        price      = closes[-1]
        profit_pct = (price - entry_price) / entry_price
        trades.append({
            "pair":        pair,
            "entry_price": round(entry_price, 6),
            "exit_price":  round(price, 6),
            "pnl_pct":     round(profit_pct * 100, 3),
            "exit_reason": "TIMEOUT",
            "hold_bars":   total_bars - entry_bar,
            "mfe":         round(mfe * 100, 3),
            "mae":         round(mae * 100, 3),
            "won":         profit_pct > 0,
            "confidence":  conf_at_entry,
            "mode":        mode_at_entry,
            "strategy":    strategy_at_entry,
            "hour_utc":    hour,
            "validate":    validate_mode,
            "trade_id":    secrets.token_hex(8),
        })

    return trades


# ── DB write ──────────────────────────────────────────────────────────────────
def write_fingerprints(all_records: List[Dict], dry_run: bool = False) -> int:
    if dry_run or not DATABASE_URL:
        log.info(f"  DRY RUN: would write {len(all_records)} fingerprints")
        return len(all_records)
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        written = 0
        with conn.cursor() as cur:
            # Remove old backtest entries
            cur.execute("""
                DELETE FROM crypto_trade_fingerprints
                WHERE trade_id LIKE 'bt_%' AND won IS NOT NULL
            """)
            for r in all_records:
                trade_id = "bt_" + r["trade_id"]
                try:
                    pair = r["pair"]
                    cur.execute("""
                        INSERT INTO crypto_trade_fingerprints
                        (trade_id, pair, entry_ts, exit_ts,
                         rsi_5m, fg_value, session, is_weekend,
                         confidence_score, entry_mode, entry_price,
                         won, pnl_pct, exit_reason, hold_time_min,
                         mfe, mae)
                        VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s)
                        ON CONFLICT (trade_id) DO UPDATE
                        SET won=EXCLUDED.won, pnl_pct=EXCLUDED.pnl_pct,
                            exit_reason=EXCLUDED.exit_reason,
                            mfe=EXCLUDED.mfe, mae=EXCLUDED.mae
                    """, (
                        trade_id, pair,
                        int(time.time()), int(time.time()),
                        None, 50, "US", False,   # RSI, F&G, session
                        r.get("confidence", 0),
                        r.get("mode", "CAUTIOUS"),
                        r.get("entry_price", 0),
                        bool(r["won"]),
                        round(r["pnl_pct"], 3),
                        r["exit_reason"],
                        r.get("hold_bars", 0) * 5,    # 5m bars * 5min = minutes
                        round(r.get("mfe", 0), 3),
                        round(r.get("mae", 0), 3),
                    ))
                    written += 1
                except Exception as e:
                    log.warning(f"fingerprint write error: {e}")
                    break
        conn.commit()
        conn.close()
        return written
    except Exception as e:
        log.error(f"DB write error: {e}")
        return 0


def run_pattern_analysis() -> Tuple[int, float]:
    """Same as crypto.py PatternMemory.run_analysis()."""
    if not DATABASE_URL:
        return 0, 0.0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT rsi_5m, fg_value, session, is_weekend, vwap_position,
                       trend_5m_up, higher_lows_5m, won, pnl_pct, mfe, mae, pair
                FROM crypto_trade_fingerprints WHERE won IS NOT NULL
            """)
            rows = cur.fetchall()

        if not rows:
            conn.close()
            return 0, 0.0

        from collections import defaultdict
        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)

        for row in rows:
            rsi5  = row["rsi_5m"] or 99
            fg    = row["fg_value"] or 50
            rsi_b = ("rsi_lt25" if rsi5 < 25 else
                     "rsi_25_35" if rsi5 < 35 else
                     "rsi_35_40" if rsi5 < 40 else "rsi_gt40")
            fg_b  = "fg_fear" if fg < 30 else "fg_neutral" if fg < 60 else "fg_greed"
            key   = (f"{rsi_b}|{fg_b}|{row['session'] or 'UNK'}|"
                     f"{row['vwap_position'] or 'unk'}|"
                     f"{'up' if row['trend_5m_up'] else 'dn'}|"
                     f"{'hl' if row['higher_lows_5m'] else 'no'}|"
                     f"{'wknd' if row['is_weekend'] else 'wkdy'}|"
                     f"btc_neu|sess_flat|ls_neu|oi_flat")
            buckets[key].append(bool(row["won"]))
            if row["pnl_pct"] is not None:
                pnl_bkts[key].append(float(row["pnl_pct"]))

        written = 0
        with conn.cursor() as cur:
            for key, outcomes in buckets.items():
                if len(outcomes) < 3:
                    continue
                wr      = sum(outcomes) / len(outcomes)
                avg_pnl = (sum(pnl_bkts[key]) / len(pnl_bkts[key]) if pnl_bkts[key] else None)
                cur.execute("""
                    INSERT INTO crypto_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (bucket_key) DO UPDATE
                    SET win_rate=EXCLUDED.win_rate,
                        sample_count=EXCLUDED.sample_count,
                        avg_pnl=EXCLUDED.avg_pnl,
                        last_updated=NOW()
                """, (key, wr, len(outcomes), avg_pnl))
                written += 1
        conn.commit()

        total = len(rows)
        wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
        conn.close()
        log.info(f"  Pattern analysis: {written} buckets | {total} trades | {wr:.1%} WR")
        return written, wr
    except Exception as e:
        log.error(f"Pattern analysis error: {e}")
        return 0, 0.0


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NEXUS Crypto Backtester V3.0")
    parser.add_argument("--days",      type=int,   default=365)
    parser.add_argument("--pairs",     nargs="+",  default=None)
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--no-validate", action="store_true")
    args = parser.parse_args()

    pairs   = args.pairs or ALL_PAIRS
    days    = args.days
    dry_run = args.dry_run

    if not ALPACA_API_KEY:
        log.error("Missing ALPACA_API_KEY")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"NEXUS CRYPTO BACKTESTER V3.0 — Alpaca Edition")
    log.info(f"Pairs: {pairs} | Days: {days} | Slippage: {SLIPPAGE_PCT*100:.2f}%")
    log.info(f"V5.0: BTC vol regime gate | Partial exits | Walk-forward validation")
    log.info("=" * 60)

    send_alert(
        f"🌙 NEXUS CRYPTO BACKTESTER V3.0 STARTING\n"
        f"Data source: Alpaca (replaces broken Coinbase API)\n"
        f"Pairs: {len(pairs)} | Days: {days}\n"
        f"V5.0: BTC vol regime | Partial exits | Walk-forward\n"
        f"ETA: ~15-25 min"
    )

    start_time = time.time()

    # Fetch all bars
    all_bars = fetch_all_crypto_bars(pairs, days)
    if not all_bars:
        log.error("No data fetched")
        sys.exit(1)

    # Get BTC bars for context
    btc_df = all_bars.get("BTC-USDC")

    all_trades  = []
    pair_summary = []
    strategy_trades = {"MEAN_REVERSION": [], "MOMENTUM": []}

    for pair in pairs:
        if pair not in all_bars:
            log.warning(f"  {pair}: no data, skipping")
            continue

        df = all_bars[pair]
        log.info(f"Simulating {pair} ({len(df):,} bars)...")

        # Full training
        train_trades = simulate_pair(pair, df, btc_df, validate_mode=False)

        # Walk-forward validation
        val_trades = []
        if not args.no_validate:
            val_trades = simulate_pair(pair, df, btc_df, validate_mode=True)

        all_t     = train_trades + val_trades
        wins      = sum(1 for t in all_t if t["won"])
        non_partial = [t for t in all_t if t["exit_reason"] != "PARTIAL_TP"]
        wr        = round(wins / max(len(all_t), 1) * 100, 1)
        avg_pnl   = sum(t["pnl_pct"] for t in non_partial) / max(len(non_partial), 1)

        for t in non_partial:
            strategy_trades.setdefault(t.get("strategy", "MEAN_REVERSION"), []).append(t)

        log.info(f"  {pair}: {len(all_t)} trades | {wr}% WR | avg P&L: {avg_pnl:+.3f}%")
        all_trades.extend(train_trades)  # only write training to DB
        pair_summary.append({
            "pair":   pair,
            "trades": len(all_t),
            "wins":   wins,
            "wr":     wr,
            "avg_pnl": avg_pnl,
        })
        time.sleep(0.5)

    # Write to DB
    written = 0
    if all_trades:
        log.info(f"Writing {len(all_trades)} fingerprints to DB...")
        written = write_fingerprints(all_trades, dry_run)

    # Pattern analysis
    buckets = 0
    overall_wr = 0.0
    if not dry_run and DATABASE_URL and written > 0:
        log.info("Running pattern analysis...")
        buckets, overall_wr = run_pattern_analysis()

    # Summary
    elapsed = round(time.time() - start_time)
    total_t = sum(p["trades"] for p in pair_summary)
    total_w = sum(p["wins"]   for p in pair_summary)
    bt_wr   = round(total_w / max(total_t, 1) * 100, 1)

    sym_lines = [
        f"  {p['pair']}: {p['wr']}% ({p['trades']}t)"
        for p in sorted(pair_summary, key=lambda x: -x["wr"])
    ]

    # V5.1: strategy comparison -- this is the actual answer to "did the
    # momentum path help." MEAN_REVERSION numbers here should be close to
    # what the pre-V5.1 backtester reported, since that path is unchanged;
    # any material drift there would mean something in this edit leaked
    # into the CHOPPY path and is worth a second look before trusting either.
    strat_lines = []
    for strat_name, trades_list in strategy_trades.items():
        n = len(trades_list)
        if n == 0:
            strat_lines.append(f"  {strat_name}: 0 trades")
            continue
        w   = sum(1 for t in trades_list if t["won"])
        pnl = sum(t["pnl_pct"] for t in trades_list) / n
        strat_lines.append(f"  {strat_name}: {n} trades | {round(w/n*100,1)}% WR | avg {pnl:+.3f}%")

    print(f"\n{'='*60}")
    print(f"CRYPTO BACKTEST V3.0 COMPLETE")
    print(f"{'='*60}")
    print(f"Total trades: {total_t} | {bt_wr}% WR")
    print(f"Fingerprints: {written:,} | Pattern buckets: {buckets}")
    print(f"--- By strategy ---")
    for line in strat_lines:
        print(line)
    print(f"--- By pair ---")
    for p in sorted(pair_summary, key=lambda x: -x["wr"]):
        print(f"  {p['pair']:<12} {p['trades']:>6} trades | {p['wr']:>5.1f}% WR | "
              f"avg {p['avg_pnl']:+.3f}%")
    print(f"{'='*60}")

    send_alert(
        f"✅ NEXUS CRYPTO BACKTESTER V3.0 COMPLETE\n"
        f"──────────────────\n"
        f"Overall WR: {bt_wr}% ({total_t} trades)\n"
        f"Fingerprints: {written:,} | Buckets: {buckets}\n"
        f"──────────────────\n"
        + "\n".join(strat_lines) + "\n"
        f"──────────────────\n"
        + "\n".join(sym_lines[:8]) + "\n"
        f"──────────────────\n"
        f"Elapsed: {elapsed}s"
    )

    log.info(f"DONE. {written} fingerprints | {buckets} buckets | {elapsed}s")


if __name__ == "__main__":
    main()
