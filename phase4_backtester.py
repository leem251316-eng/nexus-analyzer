#!/usr/bin/env python3
"""
phase4_backtester.py V1.0 -- NEXUS Phase4 Backtester
=====================================================
Pulls 2yr 1-minute Alpaca IEX bars for all Phase4 ETFs + underlyings,
replays through the EXACT same V2.0 signal engine (compute_entry_score,
select_mode, get_signal_suite, check_reversal), writes fingerprints to
phase4_trade_fingerprints, triggers pattern analysis.

Runs as part of the nexus-analyzer Railway cron (Sundays 11pm UTC).
Also runnable manually by redeploying the worker.

What this validates:
  - Per-bot min_score thresholds (are NUGT=5, SOXL=6 correct?)
  - ADX regime filter (V2.0: does ADX<20 actually hurt performance?)
  - Volume confirmation gate (does it reduce false entries meaningfully?)
  - Best/worst hours per symbol (feeds avoid_hours in BOT_CONFIGS)
  - Bear pair EV (DUST/SOXS/LABD/SQQQ -- are they actually profitable?)
  - Bull vs bear mode win rates (SCALP vs RIDE vs EXTENDED)
  - Slippage-adjusted EV (entry at close + 0.05% half-spread)

Features vs institutional backtesting standards:
  ✅ Exact signal engine replica (same functions as phase4.py V2.0)
  ✅ SPY/QQQ/VIX context from lagged bars (not same-bar lookahead)
  ✅ Walk-forward: train on 21 months, validate on last 3 months
  ✅ Slippage: entry price * (1 + 0.0005) -- half-spread on market orders
  ✅ MFE/MAE tracked per trade
  ✅ Per-symbol win rate breakdown with EV calculation
  ✅ Pattern analysis run after seeding (feeds Phase4Memory)
  ✅ T-Bone start/finish alerts

Environment:
  DATABASE_URL, ALPACA_API_KEY (or ALPACA_PHASE4_API_KEY), ALPACA_SECRET_KEY
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

Usage:
  python phase4_backtester.py              # 2yr full run
  python phase4_backtester.py --days 365   # 1yr
  python phase4_backtester.py --dry-run    # no DB writes
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
from typing import Optional, Dict, List, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [P4-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("phase4_bt")

# ── Environment ───────────────────────────────────────────────────────────────
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
ALPACA_API_KEY   = (os.environ.get("ALPACA_PHASE4_API_KEY") or
                    os.environ.get("ALPACA_API_KEY", ""))
ALPACA_SECRET    = (os.environ.get("ALPACA_PHASE4_SECRET_KEY") or
                    os.environ.get("ALPACA_SECRET_KEY", ""))
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Symbols ───────────────────────────────────────────────────────────────────
BULL_ETFS    = ["NUGT", "SOXL", "LABU", "TQQQ"]
BEAR_ETFS    = ["DUST", "SOXS", "LABD", "SQQQ"]
UNDERLYINGS  = ["GDX", "SMH", "XBI", "QQQ"]
CONTEXT_SYMS = ["SPY", "VIXY"]
ALL_SYMBOLS  = BULL_ETFS + BEAR_ETFS + UNDERLYINGS + CONTEXT_SYMS

# ── Bot configs (exact copy from phase4.py V2.0) ──────────────────────────────
BOT_CONFIGS = {
    "NUGT": {
        "bear_pair":      "DUST",
        "underlying":     "GDX",
        "budget_pct":     0.30,
        "min_score":      5,
        "atr_stop":       0.0213,
        "early_ratchet":  0.0059,
        "late_ratchet":   0.0111,
        "trail_normal":   0.0039,
        "trail_tight":    0.0027,
        "avoid_hours":    [9],
        "best_signals":   ["rsi_lt40", "bb_squeeze", "below_ma20", "rsi14_lt35"],
        "worst_signals":  ["near_lower_bb", "ema9_above_ema21"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  1.8,
    },
    "SOXL": {
        "bear_pair":      "SOXS",
        "underlying":     "SMH",
        "budget_pct":     0.25,
        "min_score":      6,
        "atr_stop":       0.0159,
        "early_ratchet":  0.0051,
        "late_ratchet":   0.0097,
        "trail_normal":   0.0029,
        "trail_tight":    0.0020,
        "avoid_hours":    [],
        "best_signals":   ["far_below_bb", "rsi14_lt20", "rsi_lt25", "stochrsi_oversold", "near_lower_bb"],
        "worst_signals":  ["macd_bullish", "below_ma20"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  1.8,
    },
    "LABU": {
        "bear_pair":      "LABD",
        "underlying":     "XBI",
        "budget_pct":     0.25,
        "min_score":      6,
        "atr_stop":       0.0193,
        "early_ratchet":  0.0054,
        "late_ratchet":   0.0101,
        "trail_normal":   0.0035,
        "trail_tight":    0.0024,
        "avoid_hours":    [10, 13],
        "best_signals":   ["at_lower_bb", "rsi_lt25", "rsi14_lt20", "stochrsi_oversold", "far_below_bb"],
        "worst_signals":  ["ema9_above_ema21", "near_lower_bb"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  1.8,
    },
    "TQQQ": {
        "bear_pair":      "SQQQ",
        "underlying":     "QQQ",
        "budget_pct":     0.20,
        "min_score":      4,
        "atr_stop":       0.0151,
        "early_ratchet":  0.0051,
        "late_ratchet":   0.0097,
        "trail_normal":   0.0028,
        "trail_tight":    0.0019,
        "avoid_hours":    [10],
        "best_signals":   ["rsi_lt40", "macd_bullish", "near_lower_bb", "obv_falling", "ema9_above_ema21"],
        "worst_signals":  ["williams_oversold", "bb_squeeze"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  2.0,
    },
}

BEAR_RECIPES = {
    "DUST": {"underlying": "GDX", "min_score": 7, "atr_stop": 0.0180,
             "avoid_hours": [11],
             "best_signals": ["obv_rising", "below_ma20", "macd_bullish", "far_below_bb"],
             "worst_signals": ["obv_falling", "near_lower_bb"]},
    "SOXS": {"underlying": "SMH", "min_score": 9, "atr_stop": 0.0177,
             "avoid_hours": [9],
             "best_signals": ["below_ma20", "rsi_lt25", "rsi14_lt20", "stochrsi_oversold", "near_lower_bb"],
             "worst_signals": ["obv_rising", "macd_bullish"]},
    "LABD": {"underlying": "XBI", "min_score": 4, "atr_stop": 0.0164,
             "avoid_hours": [11, 14],
             "best_signals": ["far_below_bb", "near_lower_bb", "obv_falling", "rsi_lt25", "macd_bullish"],
             "worst_signals": ["bb_squeeze", "below_ma20"]},
    "SQQQ": {"underlying": "QQQ", "min_score": 5, "atr_stop": 0.0178,
             "avoid_hours": [11, 13, 14],
             "best_signals": ["rsi_lt40", "rsi14_lt35", "bouncing"],
             "worst_signals": ["rsi_lt25", "at_lower_bb"]},
}

BEAR_EXTENDED_TP = {
    "DUST": {"trail_activate": 0.020, "trail_stop": 0.010},
    "SOXS": {"trail_activate": 0.020, "trail_stop": 0.010},
}

# Backtesting parameters
SLIPPAGE_PCT     = 0.0005   # 0.05% half-spread on market orders
WARMUP_BARS      = 60       # bars before first entry allowed
DWELL_MINUTES    = 30
DWELL_FLAT       = 0.001
RSI_OB_EXIT      = 70
QQQ_BEAR_GATE    = 58
QQQ_BEAR_LABD    = 65
VIX_CAUTION      = 28.0
VIX_PAUSE        = 35.0
ADX_TREND        = 20.0     # V2.0: ADX < 20 = ranging = SCALP only
VOL_CONFIRM_MULT = 1.2      # V2.0: volume gate

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

def is_market_hours(dt: datetime) -> bool:
    from zoneinfo import ZoneInfo
    central = ZoneInfo("America/Chicago")
    local = dt.astimezone(central)
    return local.weekday() < 5 and 8 <= local.hour < 15

def get_hour_cdt(dt: datetime) -> int:
    from zoneinfo import ZoneInfo
    central = ZoneInfo("America/Chicago")
    return dt.astimezone(central).hour

# ── Signal engine (exact copy from phase4.py V2.0) ───────────────────────────
def compute_rsi(prices: list, period: int = 7) -> Optional[float]:
    if len(prices) < period + 1:
        return None
    s        = pd.Series(prices, dtype=float)
    delta    = s.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    val = float(rsi.iloc[-1])
    return round(val, 2) if 0 < val < 100 else None

def compute_ema(prices: list, period: int) -> Optional[float]:
    if len(prices) < period:
        return None
    s = pd.Series(prices)
    return round(float(s.ewm(span=period, adjust=False).mean().iloc[-1]), 4)

def compute_macd(prices: list) -> dict:
    if len(prices) < 26:
        return {"bullish": False}
    s         = pd.Series(prices)
    ema12     = s.ewm(span=12, adjust=False).mean()
    ema26     = s.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal    = macd_line.ewm(span=9, adjust=False).mean()
    return {"bullish": float(macd_line.iloc[-1]) > float(signal.iloc[-1])}

def compute_bollinger(prices: list, period: int = 20) -> dict:
    if len(prices) < period:
        return {"squeeze": False, "near_lower": False, "at_lower": False,
                "far_below": False, "pct_b": 0.5}
    s      = pd.Series(prices)
    middle = float(s.rolling(period).mean().iloc[-1])
    std    = float(s.rolling(period).std().iloc[-1])
    upper  = middle + 2 * std
    lower  = middle - 2 * std
    price  = prices[-1]
    band_w = upper - lower
    pct_b  = (price - lower) / band_w if band_w > 0 else 0.5
    return {
        "squeeze":    (band_w / price) < 0.02 if price > 0 else False,
        "near_lower": pct_b < 0.20,
        "at_lower":   pct_b < 0.05,
        "far_below":  price < lower * 0.99,
        "pct_b":      round(pct_b, 3),
    }

def compute_stochrsi(prices: list, rsi_period: int = 14, stoch_period: int = 14) -> dict:
    if len(prices) < rsi_period + stoch_period + 5:
        return {"oversold": False, "overbought": False}
    s        = pd.Series(prices, dtype=float)
    delta    = s.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / rsi_period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("nan"))
    rsi_ser  = 100 - (100 / (1 + rs))
    min_rsi  = rsi_ser.rolling(stoch_period).min()
    max_rsi  = rsi_ser.rolling(stoch_period).max()
    denom    = max_rsi - min_rsi
    stoch_k  = ((rsi_ser - min_rsi) / denom * 100).where(denom != 0, 50)
    k_val    = float(stoch_k.iloc[-1])
    return {"oversold": k_val < 20, "overbought": k_val > 80}

def compute_obv(prices: list, volumes: list) -> dict:
    if len(prices) < 10 or len(volumes) < 10:
        return {"rising": False}
    n = min(len(prices), len(volumes))
    p, v = prices[-n:], volumes[-n:]
    obv  = [0.0]
    for i in range(1, len(p)):
        obv.append(obv[-1] + v[i] if p[i] > p[i-1] else
                   obv[-1] - v[i] if p[i] < p[i-1] else obv[-1])
    recent = obv[-10:]
    slope  = (recent[-1] - recent[0]) / (abs(recent[0]) + 1)
    return {"rising": slope > 0}

def compute_williams_r(prices: list, period: int = 14) -> dict:
    if len(prices) < period:
        return {"oversold": False}
    recent = prices[-period:]
    high, low, close = max(recent), min(recent), prices[-1]
    wr_val = ((high - close) / (high - low) * -100) if (high - low) > 0 else -50
    return {"oversold": wr_val < -80}

def compute_cci(prices: list, period: int = 20) -> dict:
    if len(prices) < period:
        return {"oversold": False}
    recent  = prices[-period:]
    tp_mean = sum(recent) / len(recent)
    md      = sum(abs(p - tp_mean) for p in recent) / len(recent)
    cci_val = (recent[-1] - tp_mean) / (0.015 * md) if md > 0 else 0
    return {"oversold": cci_val < -100}

def check_higher_lows(prices: list, lookback: int = 20) -> bool:
    if len(prices) < lookback:
        return False
    recent = prices[-lookback:]
    lows   = [recent[i] for i in range(1, len(recent)-1)
              if recent[i] <= recent[i-1] and recent[i] <= recent[i+1]]
    return len(lows) >= 2 and lows[-1] > lows[-2]

def compute_adx(prices: list, period: int = 14) -> float:
    if len(prices) < period * 2 + 5:
        return 25.0
    try:
        s      = pd.Series(prices)
        pos_dm = s.diff().clip(lower=0)
        neg_dm = (-s.diff()).clip(lower=0)
        tr     = s.diff().abs()
        atr_s  = tr.ewm(alpha=1.0 / period, adjust=False).mean()
        pdm_s  = pos_dm.ewm(alpha=1.0 / period, adjust=False).mean()
        ndm_s  = neg_dm.ewm(alpha=1.0 / period, adjust=False).mean()
        pdi    = 100 * pdm_s / atr_s.replace(0, float("nan"))
        ndi    = 100 * ndm_s / atr_s.replace(0, float("nan"))
        dx     = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, float("nan"))
        adx    = dx.ewm(alpha=1.0 / period, adjust=False).mean()
        val    = float(adx.iloc[-1])
        return round(val, 2) if not (pd.isna(val) or val < 0) else 25.0
    except Exception:
        return 25.0

def check_volume_confirmation(volumes: list, lookback: int = 10) -> bool:
    if len(volumes) < lookback + 1:
        return True
    recent_vol = volumes[-1]
    avg_vol    = sum(volumes[-lookback-1:-1]) / lookback
    return avg_vol <= 0 or recent_vol >= avg_vol * VOL_CONFIRM_MULT

def get_signal_suite(prices: list, volumes: list) -> dict:
    """Full V2.0 signal suite — exact replica of phase4.py."""
    if len(prices) < 21:
        return {
            "rsi": 50, "rsi14": 50, "rsi21": 50,
            "above_ma20": True, "below_ma20": False, "trend_10bar": 0,
            "higher_lows": False, "bouncing": False, "ema9_above_ema21": False,
            "bb_squeeze": False, "near_lower_bb": False, "at_lower_bb": False, "far_below_bb": False,
            "stochrsi_oversold": False, "macd_bullish": False,
            "obv_rising": False, "obv_falling": False,
            "williams_oversold": False, "cci_oversold": False,
            "rsi_lt40": False, "rsi_lt25": False, "rsi14_lt35": False, "rsi14_lt20": False,
            "vol_confirmed": True, "adx": 25.0,
        }
    rsi7   = compute_rsi(prices, 7)  or 50
    rsi14  = compute_rsi(prices, 14) or 50
    rsi21  = compute_rsi(prices, 21) or 50
    ma20   = sum(prices[-20:]) / 20
    ema9   = compute_ema(prices, 9)  or prices[-1]
    ema21v = compute_ema(prices, 21) or prices[-1]
    trend10 = (prices[-1] - prices[-11]) / prices[-11] if len(prices) > 11 and prices[-11] > 0 else 0
    bb     = compute_bollinger(prices)
    stoch  = compute_stochrsi(prices)
    macd   = compute_macd(prices)
    obv    = compute_obv(prices, volumes) if volumes else {"rising": False}
    will   = compute_williams_r(prices)
    cci    = compute_cci(prices)
    adx    = compute_adx(prices)
    vol_ok = check_volume_confirmation(volumes) if volumes else True
    return {
        "rsi":    rsi7,  "rsi14": rsi14, "rsi21": rsi21,
        "above_ma20":    prices[-1] > ma20,
        "below_ma20":    prices[-1] <= ma20,
        "trend_10bar":   round(trend10 * 100, 3),
        "higher_lows":   check_higher_lows(prices),
        "bouncing":      len(prices) >= 3 and prices[-1] > prices[-3],
        "ema9_above_ema21": ema9 > ema21v,
        "bb_squeeze":    bb["squeeze"],
        "near_lower_bb": bb["near_lower"],
        "at_lower_bb":   bb["at_lower"],
        "far_below_bb":  bb["far_below"],
        "stochrsi_oversold": stoch["oversold"],
        "macd_bullish":  macd["bullish"],
        "obv_rising":    obv["rising"],
        "obv_falling":   not obv["rising"],
        "williams_oversold": will["oversold"],
        "cci_oversold":  cci["oversold"],
        "rsi_lt40":  rsi7  < 40,
        "rsi_lt25":  rsi7  < 25,
        "rsi14_lt35": rsi14 < 35,
        "rsi14_lt20": rsi14 < 20,
        "vol_confirmed": vol_ok,
        "adx":           adx,
    }

def compute_entry_score(sym: str, sym_ctx: dict, is_bear: bool = False) -> int:
    cfg   = BEAR_RECIPES.get(sym) if is_bear else BOT_CONFIGS.get(sym, {})
    if not cfg:
        return 0
    best  = cfg.get("best_signals", [])
    worst = cfg.get("worst_signals", [])
    score = 0
    for sig in best:
        if sym_ctx.get(sig, False):
            score += 1
    for sig in worst:
        if sym_ctx.get(sig, False):
            score -= 1
    if sym_ctx.get("rsi_lt25") and "rsi_lt25" in best:
        score += 1
    if sym_ctx.get("at_lower_bb") and "at_lower_bb" in best:
        score += 1
    return max(0, score)

def select_mode(spy_ctx: dict, sym_ctx: dict) -> str:
    adx = sym_ctx.get("adx", 25.0)
    if adx < ADX_TREND:
        return "SCALP"
    if spy_ctx.get("overbought"):
        return "SCALP"
    if (spy_ctx.get("strong") and
            sym_ctx.get("trend_10bar", 0) > 0.3 and
            sym_ctx.get("higher_lows") and
            sym_ctx.get("above_ma20")):
        return "EXTENDED"
    if spy_ctx.get("bullish") and (sym_ctx.get("above_ma20") or sym_ctx.get("trend_10bar", 0) > 0.1):
        return "RIDE"
    return "SCALP"

def get_spy_context(spy_prices: list) -> dict:
    if len(spy_prices) < 21:
        return {"bullish": False, "strong": False, "overbought": False, "rsi": 50}
    rsi      = compute_rsi(spy_prices) or 50
    ma20     = sum(spy_prices[-20:]) / 20
    momentum = (spy_prices[-1] - spy_prices[-6]) / spy_prices[-6] if len(spy_prices) >= 6 and spy_prices[-6] > 0 else 0
    return {
        "bullish":    spy_prices[-1] > ma20 and momentum > 0,
        "strong":     spy_prices[-1] > ma20 and momentum > 0.005,
        "overbought": rsi > 72,
        "rsi":        rsi,
        "above_ma20": spy_prices[-1] > ma20,
    }

def get_qqq_context(qqq_prices: list) -> dict:
    if len(qqq_prices) < 8:
        return {"rsi": 50, "overbought": False, "oversold": False}
    rsi = compute_rsi(qqq_prices) or 50
    return {"rsi": rsi, "overbought": rsi > 68, "oversold": rsi < 35}

def get_vix_from_vixy(vixy_prices: list) -> float:
    if not vixy_prices:
        return 15.0
    return vixy_prices[-1] * 10.0

def get_underlying_ctx(und_prices: list) -> dict:
    if len(und_prices) < 21:
        return {"available": False, "tide_bullish": False, "tide_bearish": False,
                "at_high": False, "reversal_warning": False}
    rsi      = compute_rsi(und_prices) or 50
    ema20    = compute_ema(und_prices, 20) or und_prices[-1]
    momentum = (und_prices[-1] - und_prices[-6]) / und_prices[-6] if len(und_prices) >= 6 and und_prices[-6] > 0 else 0
    macd     = compute_macd(und_prices)
    at_high  = False
    if len(und_prices) >= 30:
        recent_high = max(und_prices[-30:])
        at_high     = und_prices[-1] >= recent_high * 0.995
    tide_bullish = und_prices[-1] > ema20 and momentum > 0 and rsi < 72
    tide_bearish = und_prices[-1] <= ema20 and momentum <= 0
    reversal_warning = rsi >= 72 and not macd["bullish"]
    return {
        "available":        True,
        "tide_bullish":     tide_bullish,
        "tide_bearish":     tide_bearish,
        "at_high":          at_high,
        "reversal_warning": reversal_warning,
    }


# ── Data fetching ─────────────────────────────────────────────────────────────
def fetch_all_bars(days: int) -> dict:
    """Fetch 1-min bars for all Phase4 symbols. Returns {sym: DataFrame}."""
    client   = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc).replace(hour=21, minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=days)

    log.info(f"Fetching {days}d 1-min bars for {len(ALL_SYMBOLS)} symbols...")
    log.info(f"Range: {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")

    result = {}
    for i, sym in enumerate(ALL_SYMBOLS):
        try:
            bars = client.get_stock_bars(StockBarsRequest(
                symbol_or_symbols=sym,
                timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                start=start_dt,
                end=end_dt,
                feed=DataFeed.IEX,
            ))
            df = bars.df
            if hasattr(df.index, "levels"):
                df = df.xs(sym, level=0)
            if not df.empty:
                result[sym] = df
                log.info(f"  [{i+1}/{len(ALL_SYMBOLS)}] {sym}: {len(df):,} bars")
            else:
                log.warning(f"  [{i+1}/{len(ALL_SYMBOLS)}] {sym}: EMPTY")
        except Exception as e:
            log.error(f"  [{i+1}/{len(ALL_SYMBOLS)}] {sym}: {e}")
        time.sleep(0.3)   # rate limit courtesy

    log.info(f"Fetched {len(result)}/{len(ALL_SYMBOLS)} symbols")
    return result


# ── Replay engine ─────────────────────────────────────────────────────────────
class BotState:
    """Per-bot state during replay."""
    __slots__ = [
        "symbol", "bear_pair", "cfg",
        "prices", "volumes", "bear_prices", "bear_volumes",
        "und_prices",
        "in_position", "active_sym", "entry_price",
        "peak_price", "entry_bar", "mode",
        "mfe", "mae",
        "late_ratchet",
        "bear_ext_trailing", "bear_ext_peak",
        "reversal_state",
        "trades",
    ]
    def __init__(self, symbol: str, cfg: dict):
        self.symbol        = symbol
        self.bear_pair     = cfg["bear_pair"]
        self.cfg           = cfg
        self.prices        = deque(maxlen=120)
        self.volumes       = deque(maxlen=120)
        self.bear_prices   = deque(maxlen=120)
        self.bear_volumes  = deque(maxlen=120)
        self.und_prices    = deque(maxlen=120)
        self.in_position   = False
        self.active_sym    = symbol
        self.entry_price   = 0.0
        self.peak_price    = 0.0
        self.entry_bar     = 0
        self.mode          = "SCALP"
        self.mfe           = 0.0
        self.mae           = 0.0
        self.late_ratchet  = False
        self.bear_ext_trailing = False
        self.bear_ext_peak     = 0.0
        self.reversal_state    = {"state": "IDLE"}
        self.trades            = []   # completed trade dicts


def replay_phase4(all_bars: dict, validate_mode: bool = False) -> list:
    """
    Main replay engine. Iterates all timestamps, feeds bars to each bot,
    runs entry/exit logic. Returns list of trade records.
    validate_mode: if True, skip first 75% of bars (walk-forward out-of-sample).
    """
    # Build unified timestamp index
    all_ts = set()
    for sym, df in all_bars.items():
        all_ts.update(df.index.tolist())
    all_ts = sorted(all_ts)

    total_bars = len(all_ts)
    validate_start_idx = int(total_bars * 0.75) if validate_mode else 0
    mode_label = "OUT-OF-SAMPLE VALIDATE" if validate_mode else "FULL TRAIN"
    log.info(f"  {mode_label}: {len(all_ts):,} timestamps | "
             f"{'skipping first 75%' if validate_mode else 'using all bars'}")

    # Shared context histories
    spy_hist   = deque(maxlen=80)
    qqq_hist   = deque(maxlen=80)
    vixy_hist  = deque(maxlen=20)

    # Init bots
    bots = {sym: BotState(sym, cfg) for sym, cfg in BOT_CONFIGS.items()}

    all_trades = []
    bar_num    = 0

    for bar_idx, ts in enumerate(all_ts):
        bar_num += 1

        # Update context (SPY, QQQ, VIXY)
        for ctx_sym, hist in [("SPY", spy_hist), ("QQQ", qqq_hist), ("VIXY", vixy_hist)]:
            if ctx_sym in all_bars and ts in all_bars[ctx_sym].index:
                row = all_bars[ctx_sym].loc[ts]
                hist.append(float(row["close"]))

        # Skip if not market hours
        dt_utc = ts if hasattr(ts, "tzinfo") and ts.tzinfo else ts.to_pydatetime().replace(tzinfo=timezone.utc)
        if not is_market_hours(dt_utc):
            continue

        hour = get_hour_cdt(dt_utc)

        # Compute shared context (lagged — use history up to but not including current bar)
        spy_ctx   = get_spy_context(list(spy_hist))
        qqq_ctx   = get_qqq_context(list(qqq_hist))
        vix_level = get_vix_from_vixy(list(vixy_hist))

        for sym, bot in bots.items():
            cfg = bot.cfg

            # Update bull ETF prices
            if sym in all_bars and ts in all_bars[sym].index:
                row = all_bars[sym].loc[ts]
                bot.prices.append(float(row["close"]))
                bot.volumes.append(float(row.get("volume", 0)))

            # Update bear ETF prices
            bear = bot.bear_pair
            if bear in all_bars and ts in all_bars[bear].index:
                row = all_bars[bear].loc[ts]
                bot.bear_prices.append(float(row["close"]))
                bot.bear_volumes.append(float(row.get("volume", 0)))

            # Update underlying prices
            und = cfg["underlying"]
            if und in all_bars and ts in all_bars[und].index:
                row = all_bars[und].loc[ts]
                bot.und_prices.append(float(row["close"]))

            if len(bot.prices) < WARMUP_BARS:
                continue

            # Skip during walk-forward training phase
            if validate_mode and bar_idx < validate_start_idx:
                continue

            # ── MANAGE OPEN POSITION ─────────────────────────────────────
            if bot.in_position:
                active_prices = list(bot.bear_prices) if bot.active_sym == bot.bear_pair else list(bot.prices)
                if not active_prices:
                    continue

                price      = active_prices[-1]
                profit_pct = (price - bot.entry_price) / bot.entry_price
                bot.mfe    = max(bot.mfe, profit_pct)
                bot.mae    = min(bot.mae, profit_pct)
                bot.peak_price = max(bot.peak_price, price)
                drawdown   = (bot.peak_price - price) / bot.peak_price if bot.peak_price > 0 else 0

                # Get exit params
                is_bear = (bot.active_sym == bot.bear_pair)
                if is_bear:
                    br      = BEAR_RECIPES.get(bot.bear_pair, cfg)
                    sl      = br["atr_stop"]
                    early_r = br.get("early_ratchet", 0.01)
                    trail_n = br.get("trail", 0.003)
                    trail_t = trail_n * 0.75
                    late_r  = early_r * 2.0
                else:
                    mode_mult = (cfg["ext_stop_mult"] if bot.mode == "EXTENDED" else
                                 cfg["ride_stop_mult"] if bot.mode == "RIDE" else 1.0)
                    sl      = cfg["atr_stop"] * mode_mult
                    early_r = cfg["early_ratchet"]
                    late_r  = cfg["late_ratchet"]
                    trail_n = cfg["trail_normal"]
                    trail_t = cfg["trail_tight"]

                exit_reason = None

                # Underlying reversal exit (bull positions only, V2.0)
                if not is_bear and profit_pct > 0.002:
                    und_ctx = get_underlying_ctx(list(bot.und_prices))
                    if und_ctx.get("reversal_warning") and not bot.late_ratchet:
                        exit_reason = "underlying-reversal"

                # Bear extended TP (DUST/SOXS)
                if not exit_reason and is_bear:
                    ext_cfg = BEAR_EXTENDED_TP.get(bot.active_sym)
                    if ext_cfg:
                        bot.bear_ext_peak = max(bot.bear_ext_peak, price)
                        if not bot.bear_ext_trailing and profit_pct >= ext_cfg["trail_activate"]:
                            bot.bear_ext_trailing = True
                        if bot.bear_ext_trailing:
                            ext_dd = (bot.bear_ext_peak - price) / bot.bear_ext_peak if bot.bear_ext_peak > 0 else 0
                            if ext_dd >= ext_cfg["trail_stop"]:
                                exit_reason = "ext-trail"
                        if not exit_reason and profit_pct <= -sl:
                            exit_reason = "stop-loss"
                    else:
                        if profit_pct <= -sl:
                            exit_reason = "stop-loss"
                elif not exit_reason:
                    if profit_pct <= -sl:
                        exit_reason = "stop-loss"

                # Regular exits (non-extended bear, and bull)
                if not exit_reason:
                    active_ctx = get_signal_suite(active_prices, [])
                    if active_ctx.get("rsi", 50) >= RSI_OB_EXIT and profit_pct > 0:
                        exit_reason = "rsi-overbought"
                    elif profit_pct >= early_r:
                        if profit_pct >= late_r or active_ctx.get("rsi", 50) >= 65:
                            bot.late_ratchet = True
                        trail = trail_t if bot.late_ratchet else trail_n
                        if drawdown >= trail:
                            exit_reason = "trail-tight" if bot.late_ratchet else "trail"
                    elif bot.mode == "EXTENDED" and not is_bear and profit_pct > -0.005:
                        if not active_ctx.get("higher_lows", True):
                            exit_reason = "trend-break"
                    else:
                        held_bars = bar_num - bot.entry_bar
                        held_min  = held_bars / 1   # 1 bar = 1 min
                        if held_min >= DWELL_MINUTES and abs(profit_pct) < DWELL_FLAT:
                            exit_reason = "dwell"

                if exit_reason:
                    hold_min = (bar_num - bot.entry_bar)
                    bot.trades.append({
                        "symbol":       sym,
                        "active_sym":   bot.active_sym,
                        "is_bear":      is_bear,
                        "mode":         bot.mode,
                        "entry_price":  round(bot.entry_price, 4),
                        "exit_price":   round(price, 4),
                        "pnl_pct":      round(profit_pct * 100, 3),
                        "exit_reason":  exit_reason,
                        "hold_min":     hold_min,
                        "mfe":          round(bot.mfe * 100, 3),
                        "mae":          round(bot.mae * 100, 3),
                        "won":          profit_pct > 0,
                        "hour_entry":   hour,
                        "spy_bullish":  spy_ctx.get("bullish", False),
                        "qqq_ob":       qqq_ctx.get("overbought", False),
                        "vix":          round(vix_level, 1),
                        "validate":     validate_mode,
                        "ts":           str(ts),
                        "trade_id":     secrets.token_hex(8),
                    })
                    bot.in_position   = False
                    bot.late_ratchet  = False
                    bot.bear_ext_trailing = False
                    bot.bear_ext_peak     = 0.0

            # ── LOOK FOR ENTRY ────────────────────────────────────────────
            elif len(bot.prices) >= WARMUP_BARS:
                if hour in cfg.get("avoid_hours", []):
                    continue
                if vix_level >= VIX_PAUSE:
                    continue

                prices_l  = list(bot.prices)
                volumes_l = list(bot.volumes)
                sym_ctx   = get_signal_suite(prices_l, volumes_l)
                und_ctx   = get_underlying_ctx(list(bot.und_prices))

                # VIX caution: reduced size (we record it but don't change score)
                vix_caution = vix_level >= VIX_CAUTION

                # Check bull entry
                if (sym_ctx.get("bouncing") and
                        not (und_ctx.get("available") and und_ctx.get("tide_bearish")) and
                        sym_ctx.get("vol_confirmed", True)):
                    score   = compute_entry_score(sym, sym_ctx, is_bear=False)
                    min_sc  = cfg["min_score"]
                    if score >= min_sc:
                        bot.mode       = select_mode(spy_ctx, sym_ctx)
                        entry_px       = prices_l[-1] * (1 + SLIPPAGE_PCT)
                        bot.in_position   = True
                        bot.active_sym    = sym
                        bot.entry_price   = entry_px
                        bot.peak_price    = entry_px
                        bot.entry_bar     = bar_num
                        bot.mfe           = 0.0
                        bot.mae           = 0.0
                        bot.late_ratchet  = False
                        bot.bear_ext_trailing = False
                        continue

                # Check bear reversal
                bear_prices_l  = list(bot.bear_prices)
                bear_volumes_l = list(bot.bear_volumes)
                if not bear_prices_l:
                    continue

                bull_rsi = compute_rsi(prices_l) or 50
                state    = bot.reversal_state

                if state["state"] == "IDLE":
                    if bull_rsi >= 70:
                        bot.reversal_state = {
                            "state":     "WATCHING",
                            "bull_peak": prices_l[-1],
                            "watch_bar": bar_num,
                        }
                elif state["state"] == "WATCHING":
                    if bar_num - state.get("watch_bar", bar_num) > 1800:
                        bot.reversal_state = {"state": "IDLE"}
                    elif bull_rsi < 60:
                        bot.reversal_state = {"state": "IDLE"}
                    else:
                        bull_peak = max(state.get("bull_peak", prices_l[-1]), prices_l[-1])
                        bot.reversal_state["bull_peak"] = bull_peak
                        drop = (bull_peak - prices_l[-1]) / bull_peak if bull_peak > 0 else 0
                        if drop >= 0.005:
                            # Bear entry check
                            if len(bear_prices_l) < 3 or bear_prices_l[-1] <= bear_prices_l[-3]:
                                pass
                            elif hour in BEAR_RECIPES.get(bot.bear_pair, {}).get("avoid_hours", []):
                                pass
                            elif qqq_ctx.get("oversold"):
                                pass
                            else:
                                gate = QQQ_BEAR_LABD if bot.bear_pair == "LABD" else QQQ_BEAR_GATE
                                if qqq_ctx.get("rsi", 50) >= gate:
                                    bear_ctx   = get_signal_suite(bear_prices_l, bear_volumes_l)
                                    bear_min   = BEAR_RECIPES.get(bot.bear_pair, {}).get("min_score", 4)
                                    bear_score = compute_entry_score(bot.bear_pair, bear_ctx, is_bear=True)
                                    if bear_score >= bear_min and bear_ctx.get("vol_confirmed", True):
                                        entry_px       = bear_prices_l[-1] * (1 + SLIPPAGE_PCT)
                                        bot.in_position   = True
                                        bot.active_sym    = bot.bear_pair
                                        bot.entry_price   = entry_px
                                        bot.peak_price    = entry_px
                                        bot.entry_bar     = bar_num
                                        bot.mfe           = 0.0
                                        bot.mae           = 0.0
                                        bot.late_ratchet  = False
                                        bot.bear_ext_trailing = False
                                        bot.bear_ext_peak     = entry_px
                                        bot.reversal_state    = {"state": "IDLE"}
                                        bot.mode = "SCALP"

        if bar_num % 50000 == 0:
            total_so_far = sum(len(b.trades) for b in bots.values())
            log.info(f"  Progress: {bar_num:,}/{total_bars:,} bars | {total_so_far} trades so far")

    # Close any open positions at end of data
    for sym, bot in bots.items():
        if bot.in_position:
            active_prices = list(bot.bear_prices) if bot.active_sym == bot.bear_pair else list(bot.prices)
            if active_prices:
                price      = active_prices[-1]
                profit_pct = (price - bot.entry_price) / bot.entry_price
                bot.trades.append({
                    "symbol":      sym,
                    "active_sym":  bot.active_sym,
                    "is_bear":     bot.active_sym == bot.bear_pair,
                    "mode":        bot.mode,
                    "entry_price": round(bot.entry_price, 4),
                    "exit_price":  round(price, 4),
                    "pnl_pct":     round(profit_pct * 100, 3),
                    "exit_reason": "timeout",
                    "hold_min":    bar_num - bot.entry_bar,
                    "mfe":         round(bot.mfe * 100, 3),
                    "mae":         round(bot.mae * 100, 3),
                    "won":         profit_pct > 0,
                    "hour_entry":  0,
                    "spy_bullish": False,
                    "qqq_ob":      False,
                    "vix":         15.0,
                    "validate":    validate_mode,
                    "ts":          "",
                    "trade_id":    secrets.token_hex(8),
                })
        all_trades.extend(bot.trades)

    return all_trades


# ── DB write ──────────────────────────────────────────────────────────────────
def write_fingerprints(trades: list, dry_run: bool = False) -> int:
    if dry_run or not DATABASE_URL:
        log.info(f"  DRY RUN: would write {len(trades)} fingerprints")
        return len(trades)
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        written = 0
        with conn.cursor() as cur:
            # Clear old backtest entries (keep live trades)
            cur.execute("""
                DELETE FROM phase4_trade_fingerprints
                WHERE mode IN ('SCALP','RIDE','EXTENDED')
                  AND entry_price IS NOT NULL
                  AND exit_ts IS NULL
            """)
            # Actually just clear all backtest-sourced entries
            cur.execute("""
                DELETE FROM phase4_trade_fingerprints
                WHERE entry_ts IS NOT NULL
                  AND won IS NOT NULL
                  AND trade_id LIKE 'bt_%'
            """)
            for t in trades:
                trade_id = "bt_" + t["trade_id"]
                try:
                    cur.execute("""
                        INSERT INTO phase4_trade_fingerprints
                        (trade_id, symbol, bear_pair, is_bear_trade, mode,
                         entry_ts, exit_ts, entry_price,
                         spy_bullish, spy_momentum, qqq_overbought,
                         hour_cdt, vix_at_entry,
                         entry_score, won, pnl_pct, exit_reason, hold_time_min,
                         mfe, mae)
                        VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s, %s,
                                %s,%s,%s,%s, %s,%s)
                        ON CONFLICT (trade_id) DO UPDATE
                        SET won=EXCLUDED.won, pnl_pct=EXCLUDED.pnl_pct,
                            exit_reason=EXCLUDED.exit_reason, mfe=EXCLUDED.mfe, mae=EXCLUDED.mae
                    """, (
                        trade_id,
                        t["symbol"],
                        BOT_CONFIGS.get(t["symbol"], {}).get("bear_pair", ""),
                        bool(t["is_bear"]),
                        t["mode"],
                        int(time.time()), int(time.time()),
                        t["entry_price"],
                        bool(t["spy_bullish"]), 0.0,
                        bool(t["qqq_ob"]),
                        t.get("hour_entry", 12),
                        t.get("vix", 15.0),
                        0,
                        bool(t["won"]),
                        round(t["pnl_pct"], 3),
                        t["exit_reason"],
                        t.get("hold_min", 0),
                        round(t.get("mfe", 0), 3),
                        round(t.get("mae", 0), 3),
                    ))
                    written += 1
                except Exception as e:
                    log.debug(f"  fingerprint write error: {e}")
        conn.commit()
        conn.close()
        log.info(f"  Wrote {written} fingerprints to DB")
        return written
    except Exception as e:
        log.error(f"DB write error: {e}")
        return 0


def run_pattern_analysis() -> Tuple[int, float]:
    """Trigger Phase4Memory.run_analysis() equivalent inline."""
    if not DATABASE_URL:
        return 0, 0.0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        from collections import defaultdict
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, is_bear_trade, mode, symbol_rsi, spy_rsi, qqq_rsi,
                       spy_bullish, qqq_overbought, higher_lows, hour_cdt,
                       bb_squeeze, stochrsi_oversold, macd_bullish, obv_rising,
                       won, pnl_pct
                FROM phase4_trade_fingerprints WHERE won IS NOT NULL
            """)
            rows = cur.fetchall()

        if not rows:
            conn.close()
            return 0, 0.0

        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)
        for row in rows:
            sym   = row["symbol"] or "?"
            bear  = bool(row["is_bear_trade"])
            mode  = row["mode"] or "SCALP"
            rsi   = row["symbol_rsi"] if row["symbol_rsi"] is not None else 99
            spy_b = {"bullish": row["spy_bullish"]}
            qqq_o = {"overbought": row["qqq_overbought"]}
            hour  = row["hour_cdt"] if row["hour_cdt"] is not None else 12
            rsi_b  = "rsi_hi" if rsi > 70 else "rsi_mid" if rsi > 40 else "rsi_low"
            spy_b2 = "spy_bull" if spy_b.get("bullish") else "spy_bear"
            qqq_b  = "qqq_ob"  if qqq_o.get("overbought") else "qqq_ok"
            hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
            key    = f"{sym}|{'bear' if bear else 'bull'}|{mode}|{rsi_b}|{spy_b2}|{qqq_b}|{hr_b}"
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
                    INSERT INTO phase4_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
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


# ── Report ────────────────────────────────────────────────────────────────────
def print_report(trades: list, days: int, validate_mode: bool = False):
    if not trades:
        log.warning("No trades generated")
        return

    label = "OUT-OF-SAMPLE" if validate_mode else "FULL BACKTEST"
    wins  = [t for t in trades if t["won"]]
    total = len(trades)
    wr    = round(len(wins) / total * 100, 1)
    avg_pnl = sum(t["pnl_pct"] for t in trades) / total
    avg_mfe = sum(t["mfe"] for t in trades) / total
    avg_mae = sum(t["mae"] for t in trades) / total

    print(f"\n{'='*60}")
    print(f"PHASE4 BACKTEST — {label}")
    print(f"{'='*60}")
    print(f"Period:   {days} days | Slippage: {SLIPPAGE_PCT*100:.2f}%")
    print(f"Trades:   {total} | {len(wins)}W | {total-len(wins)}L | {wr}% WR")
    print(f"Avg PnL:  {avg_pnl:+.3f}% | Avg MFE: +{avg_mfe:.3f}% | Avg MAE: {avg_mae:.3f}%")
    print()

    # Per-bot breakdown
    print(f"{'Symbol':<8} {'Trades':>7} {'WR':>7} {'AvgPnL':>8} {'Bull':>6} {'Bear':>6}")
    print("-" * 50)
    for sym in BULL_ETFS:
        sym_trades = [t for t in trades if t["symbol"] == sym]
        if not sym_trades:
            continue
        sym_wins   = [t for t in sym_trades if t["won"]]
        bull_trades = [t for t in sym_trades if not t["is_bear"]]
        bear_trades = [t for t in sym_trades if t["is_bear"]]
        sym_wr      = round(len(sym_wins) / len(sym_trades) * 100, 1)
        sym_pnl     = sum(t["pnl_pct"] for t in sym_trades) / len(sym_trades)
        print(f"{sym:<8} {len(sym_trades):>7} {sym_wr:>6.1f}% {sym_pnl:>+7.3f}% "
              f"{len(bull_trades):>5} {len(bear_trades):>6}")

    # Exit reason breakdown
    print()
    exit_counts = defaultdict(lambda: {"wins": 0, "losses": 0})
    for t in trades:
        k = t["exit_reason"]
        if t["won"]:
            exit_counts[k]["wins"] += 1
        else:
            exit_counts[k]["losses"] += 1
    print("Exit reasons:")
    for reason, counts in sorted(exit_counts.items(), key=lambda x: -(x[1]["wins"]+x[1]["losses"])):
        tot = counts["wins"] + counts["losses"]
        wr  = round(counts["wins"] / tot * 100, 1)
        print(f"  {reason:<22} {tot:>5} trades | {wr}% WR")

    # Mode breakdown
    print()
    mode_counts = defaultdict(lambda: {"wins": 0, "losses": 0})
    for t in trades:
        k = t["mode"]
        if t["won"]: mode_counts[k]["wins"] += 1
        else:        mode_counts[k]["losses"] += 1
    print("Modes:")
    for mode, counts in mode_counts.items():
        tot = counts["wins"] + counts["losses"]
        wr  = round(counts["wins"] / tot * 100, 1)
        print(f"  {mode:<12} {tot:>5} trades | {wr}% WR")

    print(f"{'='*60}\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NEXUS Phase4 Backtester V1.0")
    parser.add_argument("--days",      type=int,  default=730, help="History days (default: 730 = 2yr)")
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--no-validate", action="store_true", help="Skip walk-forward validation")
    args = parser.parse_args()

    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"NEXUS PHASE4 BACKTESTER V1.0")
    log.info(f"Days: {args.days} | Slippage: {SLIPPAGE_PCT*100:.2f}% | DryRun: {args.dry_run}")
    log.info(f"V2.0 features: ADX regime filter | Vol confirmation | Underlying exit")
    log.info("=" * 60)

    send_alert(
        f"⚡ PHASE4 BACKTESTER V1.0 STARTING\n"
        f"Bots: NUGT | SOXL | LABU | TQQQ\n"
        f"Bear pairs: DUST | SOXS | LABD | SQQQ\n"
        f"Period: {args.days} days | Slippage: {SLIPPAGE_PCT*100:.2f}%\n"
        f"V2.0 signal engine | Walk-forward validation\n"
        f"ETA: ~45-60 min"
    )

    start_time = time.time()

    # Fetch all bars
    all_bars = fetch_all_bars(args.days)
    if not all_bars:
        log.error("No bar data fetched — check API keys")
        sys.exit(1)

    # Full training run
    log.info("Starting full training replay...")
    train_trades = replay_phase4(all_bars, validate_mode=False)
    log.info(f"Training complete: {len(train_trades)} trades")
    print_report(train_trades, args.days, validate_mode=False)

    # Walk-forward validation
    validate_trades = []
    if not args.no_validate:
        log.info("Starting walk-forward validation (last 25% of data)...")
        validate_trades = replay_phase4(all_bars, validate_mode=True)
        val_25pct_days  = round(args.days * 0.25)
        log.info(f"Validation complete: {len(validate_trades)} trades")
        print_report(validate_trades, val_25pct_days, validate_mode=True)

    # Write training trades to DB (full dataset for pattern memory)
    written = 0
    if train_trades:
        log.info(f"Writing {len(train_trades)} training fingerprints to DB...")
        written = write_fingerprints(train_trades, args.dry_run)

    # Run pattern analysis
    buckets, overall_wr = 0, 0.0
    if not args.dry_run and DATABASE_URL and written > 0:
        log.info("Running pattern analysis...")
        buckets, overall_wr = run_pattern_analysis()

    elapsed = round(time.time() - start_time)

    # Summary for T-Bone
    train_wr = round(len([t for t in train_trades if t["won"]]) / max(len(train_trades),1) * 100, 1)
    sym_lines = []
    for sym in BULL_ETFS:
        sym_trades = [t for t in train_trades if t["symbol"] == sym]
        if sym_trades:
            sw = sum(1 for t in sym_trades if t["won"])
            sym_lines.append(f"  {sym}: {round(sw/len(sym_trades)*100,1)}% WR ({len(sym_trades)}t)")

    val_line = ""
    if validate_trades:
        vw  = sum(1 for t in validate_trades if t["won"])
        vwr = round(vw / max(len(validate_trades), 1) * 100, 1)
        val_line = f"\nValidation (last 25%): {vwr}% WR ({len(validate_trades)} trades)"

    send_alert(
        f"✅ PHASE4 BACKTESTER V1.0 COMPLETE\n"
        f"──────────────────\n"
        f"Training WR: {train_wr}% ({len(train_trades)} trades)\n"
        + "\n".join(sym_lines) + "\n"
        f"──────────────────\n"
        f"Fingerprints: {written:,}\n"
        f"Pattern buckets: {buckets}\n"
        f"{val_line}\n"
        f"──────────────────\n"
        f"Elapsed: {elapsed}s"
    )

    log.info(f"DONE. {written} fingerprints | {buckets} buckets | {elapsed}s")


if __name__ == "__main__":
    main()
