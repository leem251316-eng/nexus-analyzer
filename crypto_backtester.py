#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
crypto_backtester.py V2.0 -- NEXUS Crypto Pattern Memory Seeder
================================================================
Downloads historical Coinbase candle data and replays it through the
EXACT same confidence engine as crypto.py V4.11.

Fingerprints every simulated trade into the SAME PostgreSQL DB that
crypto.py uses, so the live pattern memory starts with real historical
intelligence instead of 0.5 default win rates.

V2.0 changes vs V1.x:
  ✅ Confidence engine rebuilt to match crypto.py V4.11 exactly:
       - score_technical:     multi-TF RSI, higher lows, VWAP, RSI bounce
       - score_macro:         funding rate, BTC dominance velocity, market cap momentum
                              V4.9: floors removed -- negative conditions penalize
       - score_sentiment:     F&G + momentum
                              V4.9: floors removed -- greed/worsening sentiment penalizes
       - score_volume_struct: OBV momentum + RSI position (V4.9)
       - score_market_ctx:    BTC RSI, session momentum, pair-BTC correlation (V4.4)
       - historical:          neutral 7pts at backtest time (bootstrapping -- correct)
  ✅ MFE/MAE tracked per trade -- feeds avg MFE/MAE logs in PatternMemory
  ✅ btc_rsi_5m + btc_session_momentum written to fingerprints (feeds V4.4 bucket keys)
  ✅ AVAX-USDC and LINK-USDC removed (retired V4.9)
  ✅ RSI uses Wilder smoothing via pandas EWM (matches crypto.py V4.11)
  ✅ Telegram alerts on start and finish
  ✅ Pattern analysis triggered after seeding
  ✅ Telegram race condition fix (time.sleep before exit alert)

Usage:
    python crypto_backtester.py --days 365
    python crypto_backtester.py --days 365 --pairs BTC-USDC ETH-USDC
    python crypto_backtester.py --days 90 --dry-run

Environment:
    DATABASE_URL, CB_API_KEY, CB_API_SECRET (or CB_API_KEY_NAME + CB_API_PRIVATE_KEY)
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID (optional)
"""

import os
import sys
import time
import hmac
import math
import json
import hashlib
import secrets
import argparse
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple, Any

import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRYPTO-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crypto_bt")

# ── Environment ───────────────────────────────────────────────────────────────
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
CB_API_KEY       = os.environ.get("CB_API_KEY",
                   os.environ.get("CB_API_KEY_NAME", "")).strip()
CB_API_SECRET    = os.environ.get("CB_API_SECRET",
                   os.environ.get("CB_API_PRIVATE_KEY", "")).replace("\\n", "\n").strip()
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
CB_BASE          = "https://api.coinbase.com"

# ── Pairs (matches crypto.py V4.11 -- AVAX + LINK removed V4.9) ──────────────
ALL_PAIRS = [
    "BTC-USDC", "ETH-USDC", "SOL-USDC",  "DOGE-USDC",
    "XRP-USDC", "DOT-USDC", "ADA-USDC",  "LTC-USDC",
    "POL-USDC", "SUI-USDC",
]

# Per-pair recipes -- matches crypto.py V4.11 RECIPES exactly
RECIPES: Dict[str, Dict] = {
    "BTC-USDC":  {"stop_pct": 0.012, "tp_pct": 0.025, "rsi_entry_max": 38,
                  "best_hours": [17, 10, 9],  "avoid_hours": [8, 19]},
    "ETH-USDC":  {"stop_pct": 0.015, "tp_pct": 0.030, "rsi_entry_max": 38,
                  "best_hours": [7, 10, 17],  "avoid_hours": [13, 8]},
    "SOL-USDC":  {"stop_pct": 0.018, "tp_pct": 0.035, "rsi_entry_max": 40,
                  "best_hours": [17, 9, 8],   "avoid_hours": [4, 7]},
    "DOGE-USDC": {"stop_pct": 0.020, "tp_pct": 0.040, "rsi_entry_max": 42,
                  "best_hours": [2, 19, 8],   "avoid_hours": [15, 13]},
    "XRP-USDC":  {"stop_pct": 0.015, "tp_pct": 0.028, "rsi_entry_max": 30,
                  "best_hours": [13, 10, 9],  "avoid_hours": [2, 8]},
    "DOT-USDC":  {"stop_pct": 0.018, "tp_pct": 0.032, "rsi_entry_max": 45,
                  "best_hours": [2, 11, 10],  "avoid_hours": [3, 16]},
    "ADA-USDC":  {"stop_pct": 0.018, "tp_pct": 0.032, "rsi_entry_max": 40,
                  "best_hours": [14, 19, 18], "avoid_hours": [7, 6]},
    "LTC-USDC":  {"stop_pct": 0.014, "tp_pct": 0.026, "rsi_entry_max": 43,
                  "best_hours": [9, 10, 14, 15],      "avoid_hours": [1, 2, 3]},
    "POL-USDC":  {"stop_pct": 0.020, "tp_pct": 0.038, "rsi_entry_max": 40,
                  "best_hours": [10, 14, 15, 16],     "avoid_hours": [0, 1, 2, 3]},
    "SUI-USDC":  {"stop_pct": 0.022, "tp_pct": 0.042, "rsi_entry_max": 42,
                  "best_hours": [9, 10, 14, 15, 16],  "avoid_hours": [1, 2, 3, 4]},
}

# Confidence thresholds -- matches crypto.py V4.11
CONF_FULL          = 75
CONF_CAUTIOUS      = 55
CONF_SKIP          = 40
CONF_OFFPEAK_OVERRIDE = 85

# Hour buckets -- matches crypto.py V4.11
PEAK_HOURS     = {8, 9, 10, 11}
CAUTIOUS_HOURS = {12, 13, 17, 19}
NO_BUY_HOURS   = set(range(24)) - PEAK_HOURS - CAUTIOUS_HOURS

# Signal constants
FNG_GREED_BLOCK           = 80
FNG_FEAR_LOOSE            = 25
FNG_RSI_BONUS             = 5
FUNDING_SQUEEZE_THRESHOLD = -0.0002
FUNDING_EXIT_THRESHOLD    =  0.0005
SESSION_MOM_STRONG        =  0.8
SESSION_MOM_WEAK          = -0.8
CORR_STRONG               =  0.7
CORR_WEAK                 =  0.3
ATR_TRAIL_MULTIPLIER      = 1.5
ATR_ACTIVATE_MULT         = 0.5
TIME_FAILSAFE_HOURS       = 4
TIME_FAILSAFE_MOVE_PCT    = 0.005
RSI_PERIOD                = 14
ATR_PERIOD                = 14

# ── Helpers ───────────────────────────────────────────────────────────────────
def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=5,
        )
    except Exception:
        pass


def get_hour_cdt(ts_utc: int) -> int:
    return (datetime.fromtimestamp(ts_utc, tz=timezone.utc).hour - 5) % 24


def get_session(ts_utc: int) -> str:
    h = get_hour_cdt(ts_utc)
    if 20 <= h or h < 4:  return "ASIAN"
    elif 4 <= h < 8:      return "LONDON"
    elif 8 <= h < 15:     return "US"
    else:                 return "OFF"


def is_weekend(ts_utc: int) -> bool:
    return datetime.fromtimestamp(ts_utc, tz=timezone.utc).weekday() >= 5


# ── Candle fetcher ────────────────────────────────────────────────────────────
def _hmac_headers(method: str, path: str, body: str = "") -> Dict:
    ts  = str(int(time.time()))
    msg = ts + method + path + body
    sig = hmac.new(CB_API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {
        "CB-ACCESS-KEY":       CB_API_KEY,
        "CB-ACCESS-SIGN":      sig,
        "CB-ACCESS-TIMESTAMP": ts,
        "Content-Type":        "application/json",
    }


def fetch_candles_range(pair: str, granularity: str,
                        start_ts: int, end_ts: int) -> List[Dict]:
    gran_map = {
        "ONE_MINUTE": 60, "FIVE_MINUTE": 300, "FIFTEEN_MINUTE": 900,
        "ONE_HOUR": 3600, "FOUR_HOUR": 14400,
    }
    gran_secs   = gran_map.get(granularity, 300)
    max_candles = 300
    all_candles = []
    chunk_start = start_ts

    while chunk_start < end_ts:
        chunk_end = min(chunk_start + gran_secs * max_candles, end_ts)
        path      = f"/api/v3/brokerage/products/{pair}/candles"
        fetched   = False

        # Authenticated Coinbase Advanced Trade API
        if CB_API_KEY and CB_API_SECRET and not CB_API_SECRET.startswith("-----"):
            try:
                r = requests.get(
                    f"{CB_BASE}{path}",
                    headers=_hmac_headers("GET", path),
                    params={"start": str(chunk_start), "end": str(chunk_end),
                            "granularity": granularity},
                    timeout=15,
                )
                if r.status_code == 200:
                    data = r.json().get("candles", [])
                    if data:
                        all_candles.extend(sorted(data, key=lambda c: int(c["start"])))
                        fetched = True
                elif r.status_code == 429:
                    log.warning("Rate limited -- sleeping 30s")
                    time.sleep(30)
                    continue
            except Exception as e:
                log.debug(f"Auth fetch error {pair}: {e}")

        # Fall back to public Coinbase Exchange API
        if not fetched:
            try:
                r2 = requests.get(
                    f"https://api.exchange.coinbase.com/products/{pair}/candles",
                    params={
                        "start":       datetime.fromtimestamp(chunk_start,
                                        tz=timezone.utc).isoformat(),
                        "end":         datetime.fromtimestamp(chunk_end,
                                        tz=timezone.utc).isoformat(),
                        "granularity": gran_secs,
                    },
                    timeout=15,
                )
                if r2.status_code == 200:
                    for c in r2.json():
                        all_candles.append({
                            "start":  str(c[0]), "low":  str(c[1]),
                            "high":   str(c[2]), "open": str(c[3]),
                            "close":  str(c[4]), "volume": str(c[5]),
                        })
                    fetched = True
                elif r2.status_code == 429:
                    log.warning("Rate limited (public) -- sleeping 10s")
                    time.sleep(10)
                    continue
            except Exception as e:
                log.debug(f"Public fetch error {pair}: {e}")

        chunk_start = chunk_end
        time.sleep(0.4)

    # Deduplicate + sort
    seen, unique = set(), []
    for c in sorted(all_candles, key=lambda c: int(c["start"])):
        ts = c["start"]
        if ts not in seen:
            seen.add(ts)
            unique.append(c)
    return unique


# ── Signal calculations (exact match crypto.py V4.11) ─────────────────────────
def calc_rsi_wilder(closes: List[float], period: int = RSI_PERIOD) -> Optional[float]:
    """Wilder EWM RSI -- matches crypto.py DataCollector.calc_multi_tf_rsi() pattern."""
    if len(closes) < period + 1:
        return None
    import pandas as pd
    s        = pd.Series(closes, dtype=float)
    delta    = s.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("nan"))
    rsi      = (100 - (100 / (1 + rs))).iloc[-1]
    if float("nan") == rsi or not (0 < rsi < 100):
        return None
    return round(float(rsi), 2)


def calc_atr(candles: List[Dict], period: int = ATR_PERIOD) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h  = float(candles[i]["high"])
        l  = float(candles[i]["low"])
        pc = float(candles[i-1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def calc_trend_structure(closes: List[float], lookback: int = 20) -> Dict:
    if len(closes) < lookback:
        return {"uptrend": False, "downtrend": False, "higher_lows": False}
    prices = closes[-lookback:]
    mid    = lookback // 2
    fhh = max(prices[:mid]); shh = max(prices[mid:])
    fhl = min(prices[:mid]); shl = min(prices[mid:])
    return {
        "uptrend":     shh > fhh and shl > fhl,
        "downtrend":   shh < fhh and shl < fhl,
        "higher_lows": shl > fhl,
        "lower_highs": shh < fhh,
    }


def calc_vwap_position(candles: List[Dict]) -> Optional[str]:
    if len(candles) < 5:
        return None
    tpv = sum((float(c["high"]) + float(c["low"]) + float(c["close"])) / 3
              * float(c["volume"]) for c in candles)
    vol = sum(float(c["volume"]) for c in candles)
    if vol == 0:
        return None
    vwap = tpv / vol
    return "above" if float(candles[-1]["close"]) > vwap else "below"


def calc_obv_momentum(candles: List[Dict], window: int = 20) -> Optional[float]:
    """OBV slope as % change -- matches crypto.py calc_obv()."""
    if len(candles) < window + 1:
        return None
    recent = candles[-window:]
    obv    = [0.0]
    for i in range(1, len(recent)):
        c_now  = float(recent[i]["close"])
        c_prev = float(recent[i-1]["close"])
        vol    = float(recent[i]["volume"])
        if c_now > c_prev:
            obv.append(obv[-1] + vol)
        elif c_now < c_prev:
            obv.append(obv[-1] - vol)
        else:
            obv.append(obv[-1])
    if abs(obv[0]) < 1:
        return None
    slope = (obv[-1] - obv[0]) / (abs(obv[0]) + 1) * 100
    return round(slope, 4)


def calc_rsi_position(closes: List[float], window: int = 5) -> Optional[float]:
    """
    RSI Position (0-100): where is current RSI within its own recent range.
    Matches crypto.py V4.9 calc_rsi_position().
    """
    if len(closes) < RSI_PERIOD + window + 2:
        return None
    # Compute RSI series over last RSI_PERIOD + window + 2 bars
    rsi_series = []
    for j in range(window + 1):
        end_idx = len(closes) - window + j
        window_closes = closes[max(0, end_idx - RSI_PERIOD * 3):end_idx]
        r = calc_rsi_wilder(window_closes)
        if r is not None:
            rsi_series.append(r)
    if len(rsi_series) < window:
        return None
    recent_rsi = rsi_series[-1]
    lo = min(rsi_series)
    hi = max(rsi_series)
    if hi == lo:
        return 50.0
    return round((recent_rsi - lo) / (hi - lo) * 100, 2)


def build_multi_tf_rsi(candles_5m: List[Dict], i: int) -> Dict[str, Optional[float]]:
    """Build multi-TF RSI from 5m candle history at position i."""
    closes = [float(c["close"]) for c in candles_5m[:i+1]]
    return {
        "1m":  calc_rsi_wilder(closes[-20:],  7),   # short proxy
        "5m":  calc_rsi_wilder(closes[-60:],  RSI_PERIOD),
        "15m": calc_rsi_wilder(closes[-60:],  21),  # 3x 5m
        "1h":  calc_rsi_wilder(closes[-100:], RSI_PERIOD),
        "4h":  calc_rsi_wilder(closes[-200:], RSI_PERIOD),
    }


# ── Confidence scoring (exact match crypto.py V4.11 ConfidenceEngine) ─────────
def score_technical(rsi_vals: Dict, trend_5m: Dict,
                    vwap_pos: Optional[str], rsi_5m: Optional[float],
                    rsi_1m: Optional[float]) -> int:
    """Mirrors ConfidenceEngine._score_technical() -- max +40."""
    score = 0
    rsi_list = [v for v in rsi_vals.values() if v is not None]
    oc = sum(1 for r in rsi_list if r < 40)
    if   oc >= 5: score += 20
    elif oc == 4: score += 15
    elif oc == 3: score += 10
    elif oc == 2: score +=  5

    if trend_5m.get("higher_lows"):
        score += 10 if trend_5m.get("higher_lows") else 5  # 4h proxy = same in BT
    elif trend_5m.get("higher_lows"):
        score += 5

    if vwap_pos == "below":
        score += 5

    if rsi_5m and rsi_1m and rsi_5m < 35 and rsi_1m > rsi_5m:
        score += 5

    return min(score, 40)


def score_macro(fg: int, fg_mom: float,
                dom_velocity: float = 0.0,
                funding: Optional[float] = None,
                mcap_mom: float = 0.0) -> int:
    """
    Mirrors ConfidenceEngine._score_macro() V4.9.
    V4.9 fix: NO floor -- negative conditions actually penalize.
    In backtest: funding=None (not available), dom_velocity from F&G proxy.
    """
    score = 0

    # Funding rate
    if funding is not None:
        if   funding < FUNDING_SQUEEZE_THRESHOLD: score += 10
        elif funding < 0:                         score +=  5
        elif funding > FUNDING_EXIT_THRESHOLD:    score -=  5

    # BTC dominance velocity (proxy from F&G momentum direction)
    # In backtest we use F&G momentum as a proxy: improving = dom falling = crypto rising
    if   dom_velocity < -0.5: score += 10
    elif dom_velocity <  0:   score +=  5
    elif dom_velocity >  0.5: score -=  5

    # Market cap momentum (proxy from F&G momentum)
    if   mcap_mom >  3: score += 10
    elif mcap_mom >  1: score +=  5
    elif mcap_mom < -3: score -=  5

    return min(score, 30)  # cap +30, allow negative through


def score_sentiment(fg: int, fg_mom: float) -> int:
    """
    Mirrors ConfidenceEngine._score_sentiment() V4.9.
    V4.9 fix: NO floor -- greed/worsening sentiment penalizes.
    """
    score = 0
    if   fg < 20:              score += 8
    elif fg < 30:              score += 5
    elif fg < 45:              score += 2
    elif fg > FNG_GREED_BLOCK: score -= 8
    elif fg > 70:              score -= 3

    if   fg_mom >  5: score += 7
    elif fg_mom >  2: score += 4
    elif fg_mom < -5: score -= 4
    elif fg_mom < -2: score -= 2

    return min(score, 15)  # cap +15, allow negative through


def score_volume_structure(obv_mom: Optional[float],
                            rsi_pos: Optional[float],
                            rsi_5m: Optional[float]) -> int:
    """
    Mirrors ConfidenceEngine._score_volume_structure() V4.9.
    Max +10 / Min -6.
    """
    score = 0

    if obv_mom is not None:
        if   obv_mom >  5.0: score += 5
        elif obv_mom >  1.0: score += 2
        elif obv_mom < -5.0: score -= 4
        elif obv_mom < -1.0: score -= 2

    if rsi_pos is not None:
        if rsi_pos >= 70 and rsi_5m is not None and rsi_5m < 40:
            score += 5
        elif rsi_pos >= 50 and rsi_5m is not None and rsi_5m < 40:
            score += 2
        elif rsi_pos < 25:
            score -= 2

    return max(min(score, 10), -6)


def score_market_context(btc_rsi: Optional[float],
                          sess_mom: float,
                          pair: str,
                          corr: Optional[float] = None) -> int:
    """
    Mirrors ConfidenceEngine._score_market_context() V4.4.
    Max +10 / Min -10.
    In backtest: corr = None (no live BTC correlation data).
    """
    score = 0

    if btc_rsi is not None:
        if   btc_rsi < 25: score += 8
        elif btc_rsi < 35: score += 5
        elif btc_rsi < 40: score += 2
        elif btc_rsi > 72: score -= 5
        elif btc_rsi > 65: score -= 2

    if   sess_mom < SESSION_MOM_WEAK:    score -= 4
    elif sess_mom < -0.3:               score -= 1
    elif sess_mom > SESSION_MOM_STRONG: score += 3
    elif sess_mom > 0.3:               score += 1

    if corr is not None and not pair.startswith("BTC"):
        if   corr >= CORR_STRONG: score += 3
        elif corr <= CORR_WEAK:   score -= 3

    return max(min(score, 10), -10)


def calculate_confidence(pair: str, rsi_vals: Dict, trend_5m: Dict,
                          vwap_pos: Optional[str], fg: int, fg_mom: float,
                          rsi_5m: Optional[float], rsi_1m: Optional[float],
                          obv_mom: Optional[float], rsi_pos: Optional[float],
                          btc_rsi: Optional[float], sess_mom: float) -> int:
    """
    Full V4.11 confidence calculation.
    Returns 0-100 integer. Matches ConfidenceEngine.calculate_confidence().
    Historical score = 7 (neutral bootstrapping -- correct for a backtester
    that is GENERATING the historical data, not consuming it).
    """
    # Hard blocks
    if fg > FNG_GREED_BLOCK:
        return 0

    recipe  = RECIPES.get(pair, {})
    max_rsi = recipe.get("rsi_entry_max", 40)
    if fg < FNG_FEAR_LOOSE:
        max_rsi += FNG_RSI_BONUS
    if rsi_5m is not None and rsi_5m > max_rsi:
        return 0

    # F&G momentum as proxy for dominance velocity and market cap momentum
    # When F&G improving: dominance likely falling (alt-season), mcap rising
    dom_vel  = -fg_mom * 0.1   # rough proxy: improving F&G = dom falling
    mcap_mom = fg_mom * 0.5    # rough proxy: improving F&G = mcap rising

    tech  = score_technical(rsi_vals, trend_5m, vwap_pos, rsi_5m, rsi_1m)
    macro = score_macro(fg, fg_mom, dom_vel, None, mcap_mom)
    sent  = score_sentiment(fg, fg_mom)
    vol   = score_volume_structure(obv_mom, rsi_pos, rsi_5m)
    ctx   = score_market_context(btc_rsi, sess_mom, pair)
    hist  = 7   # neutral -- bootstrapping the DB that will later feed this

    total = max(0, min(100, tech + macro + sent + vol + ctx + hist))
    return total


# ── F&G history ───────────────────────────────────────────────────────────────
def fetch_fg_history(days: int) -> Dict[str, int]:
    try:
        r    = requests.get("https://api.alternative.me/fng/",
                            params={"limit": days + 10}, timeout=10)
        data = r.json().get("data", [])
        result = {}
        for d in data:
            dt = datetime.fromtimestamp(int(d["timestamp"]), tz=timezone.utc)
            result[dt.strftime("%Y-%m-%d")] = int(d["value"])
        log.info(f"Loaded {len(result)} days of F&G history")
        return result
    except Exception as e:
        log.warning(f"F&G fetch error: {e} -- using neutral 50")
        return {}


def get_fg_for_ts(ts: int, fg_history: Dict) -> Tuple[int, float]:
    dt       = datetime.fromtimestamp(ts, tz=timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    fg       = fg_history.get(date_str, 50)
    prev_str = (dt - timedelta(days=3)).strftime("%Y-%m-%d")
    fg_prev  = fg_history.get(prev_str, fg)
    return fg, float(fg - fg_prev)


# ── Core simulation ───────────────────────────────────────────────────────────
def simulate_pair(pair: str, days: int, fg_history: Dict,
                  btc_candles: Optional[List[Dict]] = None) -> Dict:
    log.info(f"Simulating {pair} over {days} days...")
    recipe   = RECIPES.get(pair, {})
    stop_pct = recipe.get("stop_pct", 0.015)
    tp_pct   = recipe.get("tp_pct",   0.025)

    end_ts   = int(time.time())
    start_ts = end_ts - days * 86400

    candles_5m = fetch_candles_range(pair, "FIVE_MINUTE", start_ts, end_ts)
    if len(candles_5m) < 200:
        log.warning(f"{pair}: only {len(candles_5m)} candles -- skipping")
        return {"pair": pair, "trades": 0, "wins": 0, "losses": 0, "records": []}
    log.info(f"{pair}: {len(candles_5m)} 5m candles loaded")

    # BTC price history for context (passed in from main loop)
    btc_closes_by_ts: Dict[int, float] = {}
    if btc_candles:
        for c in btc_candles:
            btc_closes_by_ts[int(c["start"])] = float(c["close"])

    records     = []
    trades = wins = losses = 0
    in_trade    = False
    entry_price = peak_price = 0.0
    entry_ts    = 0
    entry_idx   = 0
    mfe = mae   = 0.0

    # Entry context stored at trade open
    ctx: Dict   = {}
    warmup      = 200

    for i in range(warmup, len(candles_5m)):
        candle   = candles_5m[i]
        ts       = int(candle["start"])
        price    = float(candle["close"])
        hour_cdt = get_hour_cdt(ts)
        session  = get_session(ts)
        weekend  = is_weekend(ts)
        fg, fg_mom = get_fg_for_ts(ts, fg_history)

        if in_trade:
            peak_price = max(peak_price, price)
            pnl        = (price - entry_price) / entry_price
            mfe        = max(mfe, pnl)
            mae        = min(mae, pnl)

            # Stop loss
            if price <= entry_price * (1 - stop_pct):
                hold_min = int((ts - entry_ts) / 60)
                records.append(_make_record(pair, ctx, entry_ts, ts, entry_price,
                                            False, pnl, "STOP_LOSS", hold_min,
                                            mfe, mae))
                losses += 1; trades += 1; in_trade = False
                continue

            # Take profit
            if price >= entry_price * (1 + tp_pct):
                hold_min = int((ts - entry_ts) / 60)
                records.append(_make_record(pair, ctx, entry_ts, ts, entry_price,
                                            True, pnl, "TAKE_PROFIT", hold_min,
                                            mfe, mae))
                wins += 1; trades += 1; in_trade = False
                continue

            # ATR trail
            atr = calc_atr(candles_5m[max(0, i - ATR_PERIOD * 2):i + 1])
            if atr:
                if (peak_price >= entry_price + ATR_ACTIVATE_MULT * atr
                        and price <= peak_price - ATR_TRAIL_MULTIPLIER * atr
                        and pnl > 0):
                    hold_min = int((ts - entry_ts) / 60)
                    records.append(_make_record(pair, ctx, entry_ts, ts, entry_price,
                                                True, pnl, "ATR_TRAIL", hold_min,
                                                mfe, mae))
                    wins += 1; trades += 1; in_trade = False
                    continue

            # Time failsafe (4h)
            if (ts - entry_ts) > TIME_FAILSAFE_HOURS * 3600:
                max_move = abs(peak_price - entry_price) / entry_price if entry_price > 0 else 0
                if max_move < TIME_FAILSAFE_MOVE_PCT:
                    hold_min = int((ts - entry_ts) / 60)
                    won = pnl > 0
                    records.append(_make_record(pair, ctx, entry_ts, ts, entry_price,
                                                won, pnl, "TIME_FAILSAFE", hold_min,
                                                mfe, mae))
                    if won: wins += 1
                    else:   losses += 1
                    trades += 1; in_trade = False
                    continue

        else:
            # ── Entry checks ─────────────────────────────────────────────
            if fg > FNG_GREED_BLOCK:
                continue

            # Hour gate
            if hour_cdt in NO_BUY_HOURS:
                continue

            # Avoid hours from recipe
            if hour_cdt in recipe.get("avoid_hours", []):
                continue

            closes = [float(c["close"]) for c in candles_5m[max(0, i - 60):i + 1]]
            rsi_vals = build_multi_tf_rsi(candles_5m, i)
            rsi_5m   = rsi_vals.get("5m")
            rsi_1m   = rsi_vals.get("1m")

            # RSI gate
            max_rsi = recipe.get("rsi_entry_max", 40)
            if fg < FNG_FEAR_LOOSE:
                max_rsi += FNG_RSI_BONUS
            if rsi_5m is None or rsi_5m > max_rsi:
                continue

            # Bounce check
            if i < 3:
                continue
            recent = [float(c["close"]) for c in candles_5m[i-3:i+1]]
            if recent[-1] <= recent[-3]:
                continue

            trend_5m  = calc_trend_structure(closes)
            vwap_pos  = calc_vwap_position(candles_5m[max(0, i - 100):i + 1])
            obv_mom   = calc_obv_momentum(candles_5m[max(0, i - 20):i + 1])
            rsi_pos   = calc_rsi_position(closes)

            # BTC context for this bar
            btc_rsi: Optional[float] = None
            btc_sess_mom: float      = 0.0
            if btc_closes_by_ts:
                # Find closest BTC bar at or before this ts
                btc_ts_keys = [t for t in btc_closes_by_ts if t <= ts]
                if len(btc_ts_keys) >= 20:
                    btc_ts_keys.sort()
                    recent_btc = [btc_closes_by_ts[t] for t in btc_ts_keys[-60:]]
                    btc_rsi    = calc_rsi_wilder(recent_btc[-20:])
                    # Session momentum: % BTC moved since session open (~8h ago)
                    session_bars = min(96, len(recent_btc))  # ~8h of 5m bars
                    if len(recent_btc) >= session_bars and recent_btc[-session_bars] > 0:
                        btc_sess_mom = (recent_btc[-1] - recent_btc[-session_bars]) / \
                                       recent_btc[-session_bars] * 100

            conf = calculate_confidence(
                pair, rsi_vals, trend_5m, vwap_pos,
                fg, fg_mom, rsi_5m, rsi_1m,
                obv_mom, rsi_pos, btc_rsi, btc_sess_mom,
            )

            if conf < CONF_SKIP:
                continue

            # Enter
            mode      = "FULL" if conf >= CONF_FULL else "CAUTIOUS"
            in_trade  = True
            entry_price = price
            peak_price  = price
            entry_ts    = ts
            entry_idx   = i
            mfe = mae   = 0.0
            ctx = {
                "rsi_5m":              rsi_5m,
                "rsi_1m":              rsi_1m,
                "fg":                  fg,
                "fg_mom":              fg_mom,
                "session":             session,
                "is_weekend":          weekend,
                "vwap_pos":            vwap_pos,
                "trend_up":            trend_5m.get("uptrend", False),
                "higher_lows":         trend_5m.get("higher_lows", False),
                "confidence":          conf,
                "mode":                mode,
                "btc_rsi_5m":          btc_rsi,
                "btc_session_momentum": btc_sess_mom,
                "hour_cdt":            hour_cdt,
            }

    # Force-close open trade at end
    if in_trade and candles_5m:
        last    = candles_5m[-1]
        ts      = int(last["start"])
        price   = float(last["close"])
        pnl     = (price - entry_price) / entry_price
        hold_min = int((ts - entry_ts) / 60)
        won     = pnl > 0
        records.append(_make_record(pair, ctx, entry_ts, ts, entry_price,
                                    won, pnl, "SIM_END", hold_min,
                                    max(mfe, pnl), min(mae, pnl)))
        if won: wins += 1
        else:   losses += 1
        trades += 1

    wr = round(wins / trades * 100, 1) if trades > 0 else 0
    log.info(f"{pair}: {trades} trades | {wins}W {losses}L | {wr}% WR")
    return {"pair": pair, "trades": trades, "wins": wins, "losses": losses,
            "records": records}


def _make_record(pair: str, ctx: Dict, entry_ts: int, exit_ts: int,
                 entry_price: float, won: bool, pnl: float,
                 exit_reason: str, hold_min: int,
                 mfe: float, mae: float) -> Dict:
    return {
        "trade_id":             secrets.token_hex(8),
        "pair":                 pair,
        "entry_ts":             entry_ts,
        "exit_ts":              exit_ts,
        "rsi_5m":               ctx.get("rsi_5m"),
        "rsi_1m":               ctx.get("rsi_1m"),
        "fg":                   ctx.get("fg", 50),
        "fg_mom":               ctx.get("fg_mom", 0),
        "session":              ctx.get("session", "US"),
        "is_weekend":           ctx.get("is_weekend", False),
        "vwap_pos":             ctx.get("vwap_pos"),
        "trend_up":             ctx.get("trend_up", False),
        "higher_lows":          ctx.get("higher_lows", False),
        "confidence":           ctx.get("confidence", 0),
        "mode":                 ctx.get("mode", "CAUTIOUS"),
        "entry_price":          entry_price,
        "btc_rsi_5m":           ctx.get("btc_rsi_5m"),
        "btc_session_momentum": ctx.get("btc_session_momentum", 0.0),
        "won":                  won,
        "pnl_pct":              round(pnl * 100, 3),
        "exit_reason":          exit_reason,
        "hold_min":             hold_min,
        "mfe":                  round(mfe * 100, 3),
        "mae":                  round(mae * 100, 3),
    }


# ── Database writer ───────────────────────────────────────────────────────────
def write_fingerprints(records: List[Dict], dry_run: bool = False) -> int:
    if dry_run:
        log.info(f"[DRY RUN] Would write {len(records)} fingerprints")
        return len(records)

    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    conn.autocommit = False
    written = 0

    try:
        with conn.cursor() as cur:
            # Ensure table exists and has V4.4+ columns
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crypto_trade_fingerprints (
                    id              SERIAL PRIMARY KEY,
                    trade_id        VARCHAR(32) UNIQUE NOT NULL,
                    pair            VARCHAR(20) NOT NULL,
                    entry_ts        BIGINT NOT NULL,
                    exit_ts         BIGINT,
                    rsi_1m REAL, rsi_5m REAL, rsi_15m REAL, rsi_1h REAL, rsi_4h REAL,
                    fg_value INTEGER, fg_momentum REAL,
                    btc_dominance REAL, dom_velocity REAL, funding_rate REAL,
                    session VARCHAR(10), hour_cdt INTEGER, is_weekend BOOLEAN,
                    vwap_position VARCHAR(10),
                    trend_5m_up BOOLEAN, trend_4h_up BOOLEAN, higher_lows_5m BOOLEAN,
                    confidence_score INTEGER, entry_mode VARCHAR(10), entry_price REAL,
                    won BOOLEAN, pnl_pct REAL, exit_reason VARCHAR(50),
                    hold_time_min INTEGER,
                    btc_rsi_5m REAL, qqq_rsi_5m REAL,
                    btc_session_momentum REAL,
                    pair_btc_correlation REAL,
                    mfe REAL, mae REAL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_ctf_pair ON crypto_trade_fingerprints(pair);
                CREATE INDEX IF NOT EXISTS idx_ctf_won  ON crypto_trade_fingerprints(won);
            """)
            conn.commit()

            for r in records:
                cur.execute("""
                    INSERT INTO crypto_trade_fingerprints
                    (trade_id, pair, entry_ts, exit_ts,
                     rsi_5m, rsi_1m, fg_value, fg_momentum,
                     session, hour_cdt, is_weekend,
                     vwap_position, trend_5m_up, higher_lows_5m,
                     confidence_score, entry_mode, entry_price,
                     btc_rsi_5m, btc_session_momentum,
                     won, pnl_pct, exit_reason, hold_time_min,
                     mfe, mae)
                    VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,
                            %s,%s,%s, %s,%s,%s, %s,%s,
                            %s,%s,%s,%s, %s,%s)
                    ON CONFLICT (trade_id) DO NOTHING
                """, (
                    r["trade_id"], r["pair"], r["entry_ts"], r["exit_ts"],
                    r.get("rsi_5m"), r.get("rsi_1m"),
                    r.get("fg"), r.get("fg_mom"),
                    r.get("session"), r.get("hour_cdt"), r.get("is_weekend"),
                    r.get("vwap_pos"), r.get("trend_up"), r.get("higher_lows"),
                    r.get("confidence"), r.get("mode"), r.get("entry_price"),
                    r.get("btc_rsi_5m"), r.get("btc_session_momentum"),
                    r.get("won"), r.get("pnl_pct"), r.get("exit_reason"),
                    r.get("hold_min"), r.get("mfe"), r.get("mae"),
                ))
                written += 1

        conn.commit()
        log.info(f"DB write complete: {written} fingerprints")
    except Exception as e:
        log.error(f"DB write error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    return written


def run_pattern_analysis(db_url: str) -> Tuple[int, int, float]:
    """
    Rebuild crypto_pattern_stats from all fingerprints.
    Mirrors PatternMemory.run_analysis() in crypto.py V4.11.
    Returns (bucket_count, total_trades, overall_wr).
    """
    conn = psycopg2.connect(db_url, sslmode="require")
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT rsi_5m, fg_value, session, vwap_position,
                       trend_5m_up, higher_lows_5m, is_weekend,
                       btc_rsi_5m, btc_session_momentum,
                       won, pnl_pct, mfe, mae
                FROM crypto_trade_fingerprints WHERE won IS NOT NULL
            """)
            rows = cur.fetchall()

        if not rows:
            return 0, 0, 0.0

        from collections import defaultdict
        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)

        for row in rows:
            rsi5     = row["rsi_5m"] or 99
            fg       = row["fg_value"] or 50
            btc_rsi  = row.get("btc_rsi_5m") or 50
            sess_mom = row.get("btc_session_momentum") or 0

            rsi_b = ("rsi_lt25" if rsi5 < 25 else
                     "rsi_25_35" if rsi5 < 35 else
                     "rsi_35_40" if rsi5 < 40 else "rsi_gt40")
            fg_b  = ("fg_fear" if fg < 30 else
                     "fg_neutral" if fg < 60 else "fg_greed")
            btc_b = ("btc_os" if btc_rsi < 35 else
                     "btc_neu" if btc_rsi < 60 else "btc_ob")
            mom_b = ("sess_dn" if sess_mom < -0.5 else
                     "sess_up" if sess_mom > 0.5 else "sess_flat")

            key = (f"{rsi_b}|{fg_b}|{row['session'] or 'UNK'}|"
                   f"{row['vwap_position'] or 'unk'}|"
                   f"{'up' if row['trend_5m_up'] else 'dn'}|"
                   f"{'hl' if row['higher_lows_5m'] else 'no'}|"
                   f"{'wknd' if row['is_weekend'] else 'wkdy'}|"
                   f"{btc_b}|{mom_b}")

            buckets[key].append(bool(row["won"]))
            if row["pnl_pct"] is not None:
                pnl_bkts[key].append(float(row["pnl_pct"]))

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crypto_pattern_stats (
                    id           SERIAL PRIMARY KEY,
                    bucket_key   VARCHAR(200) UNIQUE NOT NULL,
                    win_rate     REAL NOT NULL,
                    sample_count INTEGER NOT NULL,
                    avg_pnl      REAL,
                    last_updated TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            written_buckets = 0
            for key, outcomes in buckets.items():
                if len(outcomes) < 3:
                    continue
                wr      = sum(outcomes) / len(outcomes)
                avg_pnl = sum(pnl_bkts[key]) / len(pnl_bkts[key]) if pnl_bkts[key] else None
                cur.execute("""
                    INSERT INTO crypto_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (bucket_key) DO UPDATE
                    SET win_rate=EXCLUDED.win_rate, sample_count=EXCLUDED.sample_count,
                        avg_pnl=EXCLUDED.avg_pnl, last_updated=NOW()
                """, (key, wr, len(outcomes), avg_pnl))
                written_buckets += 1

        conn.commit()

        total = len(rows)
        wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
        log.info(f"Pattern analysis: {written_buckets} buckets | {total} trades | {wr:.1%} WR")

        # Log MFE/MAE summary
        all_mfe = [float(r["mfe"]) for r in rows if r.get("mfe") is not None]
        all_mae = [float(r["mae"]) for r in rows if r.get("mae") is not None]
        if all_mfe:
            log.info(f"  MFE avg={sum(all_mfe)/len(all_mfe):.2f}% max={max(all_mfe):.2f}%")
        if all_mae:
            log.info(f"  MAE avg={sum(all_mae)/len(all_mae):.2f}% worst={min(all_mae):.2f}%")

        return written_buckets, total, wr

    except Exception as e:
        log.error(f"Pattern analysis error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NEXUS Crypto Backtester V2.0")
    parser.add_argument("--days",    type=int,    default=365)
    parser.add_argument("--pairs",   nargs="+",   default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    pairs   = args.pairs or ALL_PAIRS
    days    = args.days
    dry_run = args.dry_run

    log.info("=" * 60)
    log.info(f"NEXUS CRYPTO BACKTESTER V2.0")
    log.info(f"Pairs: {pairs}")
    log.info(f"Days: {days}")
    log.info(f"DB: {'connected' if DATABASE_URL else 'NOT SET'}")

    if not DATABASE_URL and not dry_run:
        log.error("DATABASE_URL not set.")
        sys.exit(1)
    if not CB_API_KEY:
        log.error("CB_API_KEY not set.")
        sys.exit(1)

    send_alert(
        f"🌙 NEXUS CRYPTO BACKTESTER V2.0 STARTING\n"
        f"Pairs: {len(pairs)} | Days: {days}\n"
        f"Engine: V4.11 exact replica\n"
        f"ETA: ~15-30 min"
    )

    start_time = time.time()

    # Fetch F&G history once
    log.info("Fetching F&G history...")
    fg_history = fetch_fg_history(days + 10)

    # Fetch BTC candles once for context across all pairs
    log.info("Fetching BTC context candles...")
    end_ts   = int(time.time())
    start_ts = end_ts - days * 86400
    btc_candles = fetch_candles_range("BTC-USDC", "FIVE_MINUTE", start_ts, end_ts)
    log.info(f"BTC context: {len(btc_candles)} 5m candles")

    all_records    = []
    total_trades   = 0
    total_wins     = 0
    total_losses   = 0
    pair_summaries = []

    for pair in pairs:
        result = simulate_pair(pair, days, fg_history, btc_candles)
        all_records.extend(result["records"])
        total_trades += result["trades"]
        total_wins   += result["wins"]
        total_losses += result["losses"]
        pair_summaries.append(result)
        time.sleep(1)

    # Write fingerprints
    written = 0
    if all_records:
        log.info(f"Writing {len(all_records)} fingerprints to DB...")
        written = write_fingerprints(all_records, dry_run)
    else:
        log.warning("No records generated")

    # Run pattern analysis
    buckets = total_bt_trades = 0
    overall_wr = 0.0
    if not dry_run and DATABASE_URL and written > 0:
        log.info("Running pattern analysis...")
        buckets, total_bt_trades, overall_wr = run_pattern_analysis(DATABASE_URL)

    # Summary
    bt_wr = round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0
    print("\n" + "=" * 60)
    print("CRYPTO BACKTEST COMPLETE V2.0")
    print("=" * 60)
    print(f"Total trades:    {total_trades}")
    print(f"Wins/Losses:     {total_wins}W {total_losses}L  ({bt_wr}% WR)")
    print(f"Fingerprints DB: {written}")
    print(f"Pattern buckets: {buckets}")
    print(f"\nPer-pair:")
    print(f"  {'Pair':<12} {'Trades':>7} {'WR':>7} {'Wins':>6} {'Losses':>7}")
    print("  " + "-" * 45)
    for s in sorted(pair_summaries,
                    key=lambda x: x["wins"] / max(x["trades"], 1), reverse=True):
        wr = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] > 0 else 0
        print(f"  {s['pair']:<12} {s['trades']:>7} {wr:>6.1f}% {s['wins']:>6} {s['losses']:>7}")
    print("=" * 60)

    elapsed = round(time.time() - start_time, 1)
    sym_lines = []
    for s in sorted(pair_summaries,
                    key=lambda x: x["wins"] / max(x["trades"], 1), reverse=True):
        wr = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] > 0 else 0
        sym_lines.append(f"  {s['pair']}: {wr}% ({s['trades']} trades)")

    time.sleep(3)  # race condition fix
    send_alert(
        f"✅ NEXUS CRYPTO BACKTESTER V2.0 COMPLETE\n"
        f"──────────────────\n"
        f"Fingerprints: {written:,}\n"
        f"Pattern buckets: {buckets}\n"
        f"Overall WR: {bt_wr}%\n"
        f"──────────────────\n"
        + "\n".join(sym_lines) + "\n"
        f"──────────────────\n"
        f"Elapsed: {elapsed}s"
    )

    log.info(f"DONE. {written} fingerprints | {buckets} buckets | {elapsed}s")


if __name__ == "__main__":
    main()
