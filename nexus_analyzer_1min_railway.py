"""
NEXUS MARKET ANALYZER — 1-MIN RAILWAY ONE-SHOT
================================================
Runs once (or on a weekly cron), saves output to /app/output volume,
seeds berserker_trade_fingerprints + berserker_pattern_stats in PostgreSQL,
then exits.

Deploy as a Railway service with:
  ALPACA_API_KEY, ALPACA_SECRET_KEY, DATABASE_URL env vars.

Output:
  /app/output/nexus_analysis_report_1min.txt
  /app/output/nexus_recipes_updated_1min.json

Sections:
  1. Scalper / Phase4 symbol analysis
  2. Scanner volume spike optimization
  3. Recipe recommendations
  4. Berserker backtester — seeds berserker_trade_fingerprints in PostgreSQL
"""

import os
import json
import time
import secrets
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

warnings.filterwarnings("ignore")

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests   import StockBarsRequest
    from alpaca.data.timeframe  import TimeFrame, TimeFrameUnit
    from alpaca.data.enums      import DataFeed, Adjustment
except ImportError:
    print("Install alpaca-py: pip install alpaca-py")
    exit(1)

try:
    import psycopg2
    import psycopg2.extras as pg_extras
    _pg_ok = True
except ImportError:
    _pg_ok = False
    print("WARNING: psycopg2 not installed — DB seeding disabled")

API_KEY      = os.environ.get("ALPACA_API_KEY",    "")
SECRET_KEY   = os.environ.get("ALPACA_SECRET_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL",      "")
CENTRAL      = ZoneInfo("America/Chicago")

if not API_KEY or not SECRET_KEY:
    print("ERROR: ALPACA_API_KEY and ALPACA_SECRET_KEY env vars required")
    exit(1)

LOOKBACK_YEARS = 2
BAR_SIZE       = TimeFrame(1, TimeFrameUnit.Minute)

OUTPUT_DIR  = "/app/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
out_file    = os.path.join(OUTPUT_DIR, "nexus_analysis_report_1min.txt")
recipe_file = os.path.join(OUTPUT_DIR, "nexus_recipes_updated_1min.json")

# ==============================================================================
# SYMBOL LISTS — mirrors main.py exactly
# ==============================================================================
TRUMP_THEME = ["CLSK", "MARA", "PLTR", "GEO", "CXW", "NUE", "MSTR"]
TECH_GROWTH = ["NVDA", "AMD", "TSLA", "AAPL", "MSFT", "META", "SMCI", "SPCX"]
BERSERKER_SYMBOLS = TRUMP_THEME + TECH_GROWTH

SCALPER_SYMBOLS = [
    "SOXL", "SOXS", "TQQQ", "SQQQ", "SPXL", "SPXU",
    "LABU", "LABD", "NUGT", "DUST", "ERX",  "ERY",
]

SCANNER_PRIORITY = [
    "TNA", "TZA", "MSTU", "MSTZ", "NVDL", "NVDS",
    "SOXL","SOXS","TQQQ","LABD","LABU","FNGU","FAS","FAZ",
    "ERX","DUST","SDOW","UDOW","SPXL","SPXU",
]

PAIRS = {
    "SOXS": "SOXL", "SQQQ": "TQQQ", "SPXU": "SPXL",
    "LABD": "LABU", "DUST": "NUGT",  "ERY":  "ERX",
    "FAZ":  "FAS",  "SDOW": "UDOW",  "FNGD": "FNGU",
    "TZA":  "TNA",  "MSTZ": "MSTU",  "NVDS": "NVDL",
}

# Berserker hour/day gates — mirrors BERSERKER_RECIPES in main.py exactly
BERSERKER_RECIPES = {
    "CLSK": {"avoid_hours": [8,9,10,11,12,13], "avoid_days": []},
    "MARA": {"avoid_hours": [8,9,11,13],        "avoid_days": []},
    "PLTR": {"avoid_hours": [8,14],             "avoid_days": []},
    "GEO":  {"avoid_hours": [8,9,11,12,13,14],  "avoid_days": []},
    "CXW":  {"avoid_hours": [8,10,12,13,14],    "avoid_days": []},
    "NUE":  {"avoid_hours": [8,11],             "avoid_days": [3]},
    "MSTR": {"avoid_hours": [8,9,10,13],        "avoid_days": [0]},
    "NVDA": {"avoid_hours": [13],               "avoid_days": [3]},
    "AMD":  {"avoid_hours": [8,14],             "avoid_days": [4]},
    "TSLA": {"avoid_hours": [8,13],             "avoid_days": [3]},
    "AAPL": {"avoid_hours": [],                 "avoid_days": []},
    "MSFT": {"avoid_hours": [],                 "avoid_days": []},
    "META": {"avoid_hours": [14],               "avoid_days": []},
    "SMCI": {"avoid_hours": [8,9,10,11,13,14],  "avoid_days": [1,3,4]},
    "SPCX": {"avoid_hours": [],                 "avoid_days": []},
}

# Berserker signal constants — mirrors main.py exactly
RSI_BUY_TRIGGER  = 62
RSI_PERIOD       = 9
MACD_FAST        = 12
MACD_SLOW        = 26
MACD_SIGNAL_PER  = 9
STOP_LOSS_PCT    = 0.04
TRAILING_STOP    = 0.015
RATCHET_PROFIT   = 0.03
RATCHET_TRAIL    = 0.01     # tighter trail after ratchet activates
MIN_HOLD_BARS    = 40       # ~20 min at 30s; 1-min bars = 20 bars
MAX_HOLD_BARS    = 390      # ~6.5 hours = full trading day on 1-min bars

# Win-rate gate threshold — mirrors main.py
WIN_RATE_GATE    = 0.35
PM_MIN_BUCKET    = 3

ALL_SYMBOLS = list(set(
    SCALPER_SYMBOLS + BERSERKER_SYMBOLS + SCANNER_PRIORITY + ["SPY", "QQQ"]
))


# ==============================================================================
# UTILITIES
# ==============================================================================
def pct(n, d):
    return round(n / d * 100, 1) if d > 0 else 0

def hr_cst(ts):
    if hasattr(ts, "tz_convert"):
        return ts.tz_convert(CENTRAL).hour
    return ts.hour

def day_cst(ts):
    if hasattr(ts, "tz_convert"):
        return ts.tz_convert(CENTRAL).weekday()
    return ts.weekday()

def print_and_log(msg, f):
    print(msg, flush=True)
    f.write(msg + "\n")

def write_section(f, title):
    bar = "=" * 70
    print_and_log(f"\n{bar}", f)
    print_and_log(f"  {title}", f)
    print_and_log(bar, f)

def write_subsection(f, title):
    print_and_log(f"\n-- {title} " + "-" * max(1, 60 - len(title)), f)


# ==============================================================================
# DATA FETCH
# ==============================================================================
def fetch_bars(symbols, client, years=2, log_f=None):
    end   = datetime.now()
    start = end - timedelta(days=years * 365)

    def log(msg):
        print(msg, flush=True)
        if log_f:
            log_f.write(msg + "\n")

    log(f"  Fetching {len(symbols)} symbols | {start.date()} -> {end.date()}")
    data       = {}
    batch_size = 5
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            req  = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=BAR_SIZE,
                start=start,
                end=end,
                feed=DataFeed.IEX,
                adjustment=Adjustment.SPLIT,
            )
            bars = client.get_stock_bars(req).df
            if not bars.empty:
                for sym in batch:
                    if sym in bars.index.get_level_values(0):
                        df = bars.loc[sym].copy()
                        df.index = pd.to_datetime(df.index, utc=True)
                        data[sym] = df
                        log(f"    OK {sym}: {len(df):,} bars")
                    else:
                        log(f"    SKIP {sym}: no data")
            else:
                log(f"    WARN batch {batch}: empty response")
        except Exception as e:
            log(f"    ERR batch {batch}: {e}")
        time.sleep(0.3)   # gentle rate limiting
    return data


# ==============================================================================
# SIGNAL CALCULATIONS — exact mirrors of main.py
# ==============================================================================
def calc_rsi_wilder(prices, period=9):
    """Wilder smoothed RSI — matches how main.py SHOULD be computing it."""
    if len(prices) < period + 1:
        return np.nan
    s     = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0)
    loss  = (-delta.where(delta < 0, 0.0))
    # Wilder smoothing (EWM with alpha = 1/period)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

def calc_rsi_simple(prices, period=9):
    """Simple rolling-mean RSI — matches current main.py implementation exactly."""
    if len(prices) < period + 1:
        return np.nan
    s     = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    val   = rsi.iloc[-1]
    return float(val) if not np.isnan(val) else np.nan

def calc_macd(prices, fast=12, slow=26, signal=9):
    s       = pd.Series(prices, dtype=float)
    ema_f   = s.ewm(span=fast,   adjust=False).mean()
    ema_s   = s.ewm(span=slow,   adjust=False).mean()
    macd    = ema_f - ema_s
    sig     = macd.ewm(span=signal, adjust=False).mean()
    return float(macd.iloc[-1]), float(sig.iloc[-1])

def calc_spy_context(spy_prices):
    """Mirrors _get_spy_context_for_fingerprint() in main.py."""
    if len(spy_prices) < 8:
        return {"rsi": 50.0, "qqq_rsi": 50.0, "momentum": 0.0, "bullish": False}
    spy_rsi  = calc_rsi_simple(spy_prices[-20:])
    if np.isnan(spy_rsi):
        spy_rsi = 50.0
    mom      = (spy_prices[-1] - spy_prices[-6]) / spy_prices[-6] * 100 if len(spy_prices) >= 6 and spy_prices[-6] > 0 else 0
    bullish  = len(spy_prices) >= 8 and spy_prices[-1] > sum(spy_prices[-20:]) / len(spy_prices[-20:])
    return {
        "rsi":      round(spy_rsi, 2),
        "qqq_rsi":  50.0,   # QQQ context — populated separately when available
        "momentum": round(mom, 3),
        "bullish":  bullish,
    }

def get_sector_health(symbol, price_window, trump_theme):
    """Mirrors update_sector_health() in main.py."""
    if symbol not in trump_theme:
        return "STRONG"
    # We don't have cross-symbol sector state in backtest so return STRONG
    # (conservative — doesn't artificially block TRUMP entries)
    return "STRONG"

def bucket_key(symbol, rsi, spy_bullish, sector_health, hour, pdt_used):
    """Mirrors BerserkerMemory._bucket_key() in main.py EXACTLY."""
    sector = "TRUMP" if symbol in TRUMP_THEME else "TECH"
    rsi_b  = "rsi_hi" if rsi > 70 else "rsi_mid" if rsi > 60 else "rsi_low"
    spy_b  = "spy_bull" if spy_bullish else "spy_bear"
    sec_b  = sector_health or "STRONG"
    hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
    pdt_b  = "pdt_ok" if pdt_used < 2 else "pdt_tight"
    return f"{symbol}|{rsi_b}|{spy_b}|{sec_b}|{sector}|{hr_b}|{pdt_b}"


# ==============================================================================
# LEGACY HELPERS (scalper section — unchanged from original)
# ==============================================================================
def calc_rsi_legacy(prices, period=7):
    if len(prices) < period + 1:
        return np.nan
    s     = pd.Series(prices)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs    = gain / loss
    return (100 - 100 / (1 + rs)).iloc[-1]

def simulate_dip_trades(df, rsi_threshold=40, stop_pct=0.02,
                        trail_pct=0.004, ratchet_pct=0.0075):
    prices = df["close"].values
    times  = df.index
    trades = []
    in_pos = False
    entry_price = peak_price = 0.0
    entry_idx = entry_hour = entry_day = 0

    for i in range(20, len(prices)):
        price = prices[i]
        if in_pos:
            peak_price = max(peak_price, price)
            pnl        = (price - entry_price) / entry_price
            mfe_so_far = (peak_price - entry_price) / entry_price
            drawdown   = (peak_price - price) / peak_price if peak_price > 0 else 0
            if pnl >= ratchet_pct and drawdown >= trail_pct:
                trades.append({"pnl": pnl, "entry_hour": entry_hour,
                               "entry_day": entry_day, "mfe": mfe_so_far,
                               "mae": min(0, pnl), "exit": "trail"})
                in_pos = False
            elif pnl <= -stop_pct:
                trades.append({"pnl": pnl, "entry_hour": entry_hour,
                               "entry_day": entry_day, "mfe": mfe_so_far,
                               "mae": pnl, "exit": "stop"})
                in_pos = False
            elif i - entry_idx >= 60:
                trades.append({"pnl": pnl, "entry_hour": entry_hour,
                               "entry_day": entry_day, "mfe": mfe_so_far,
                               "mae": min(0, pnl), "exit": "timeout"})
                in_pos = False
        else:
            window = prices[max(0, i - 14):i + 1].tolist()
            rsi    = calc_rsi_legacy(window)
            if np.isnan(rsi):
                continue
            if rsi < rsi_threshold and i >= 2 and prices[i] > prices[i - 2]:
                in_pos      = True
                entry_price = price
                peak_price  = price
                entry_idx   = i
                entry_hour  = hr_cst(times[i])
                entry_day   = day_cst(times[i])
    return trades

def analyze_hour_day(trades, sym):
    if not trades:
        return {}, {}
    hours     = {}
    days      = {}
    day_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
    for t in trades:
        h = t.get("entry_hour", -1)
        d = t.get("entry_day",  -1)
        if 8 <= h <= 14:
            if h not in hours:
                hours[h] = {"wins": 0, "losses": 0, "pnl": 0.0, "mfe": [], "mae": []}
            if t["pnl"] > 0:
                hours[h]["wins"]   += 1
            else:
                hours[h]["losses"] += 1
            hours[h]["pnl"]  += t["pnl"]
            hours[h]["mfe"].append(t.get("mfe", 0))
            hours[h]["mae"].append(t.get("mae", 0))
        if 0 <= d <= 4:
            dn = day_names[d]
            if dn not in days:
                days[dn] = {"wins": 0, "losses": 0, "pnl": 0.0}
            if t["pnl"] > 0:
                days[dn]["wins"]   += 1
            else:
                days[dn]["losses"] += 1
            days[dn]["pnl"] += t["pnl"]
    return hours, days

def analyze_stop_trail(trades):
    if not trades:
        return {}
    maes = [abs(t.get("mae", 0)) for t in trades if t.get("mae", 0) < 0]
    mfes = [t.get("mfe", 0)      for t in trades if t.get("mfe", 0) > 0]
    if not maes or not mfes:
        return {}
    mae_arr = np.array(maes)
    mfe_arr = np.array(mfes)
    return {
        "avg_mae":         round(float(mae_arr.mean())  * 100, 3),
        "p75_mae":         round(float(np.percentile(mae_arr, 75)) * 100, 3),
        "p90_mae":         round(float(np.percentile(mae_arr, 90)) * 100, 3),
        "avg_mfe":         round(float(mfe_arr.mean())  * 100, 3),
        "p25_mfe":         round(float(np.percentile(mfe_arr, 25)) * 100, 3),
        "p75_mfe":         round(float(np.percentile(mfe_arr, 75)) * 100, 3),
        "optimal_stop":    round(float(np.percentile(mae_arr, 80)) * 100, 2),
        "optimal_ratchet": round(float(np.percentile(mfe_arr, 25)) * 100, 2),
        "optimal_trail":   round(float(mfe_arr.mean()) * 0.15 * 100, 2),
    }

def analyze_reversal_pattern(bear_df, bull_df):
    if bear_df is None or bull_df is None:
        return {}
    common = bear_df.index.intersection(bull_df.index)
    if len(common) < 200:
        return {}
    bear   = bear_df.loc[common, "close"].values
    bull   = bull_df.loc[common, "close"].values
    events = []
    for ob_level in [68, 70, 72, 75]:
        hits = []
        for i in range(20, len(bull) - 30):
            bull_win = bull[max(0, i - 14):i + 1].tolist()
            bull_rsi = calc_rsi_legacy(bull_win)
            if np.isnan(bull_rsi) or bull_rsi < ob_level:
                continue
            reversal_bars = None
            for j in range(1, 31):
                if i + j >= len(bull):
                    break
                if (bull[i] - bull[i + j]) / bull[i] > 0.01:
                    reversal_bars = j
                    break
            bear_start = bear[i]
            if bear_start <= 0:
                continue
            max_bear_bounce = max(
                (bear[i + k] - bear_start) / bear_start
                for k in range(1, min(31, len(bear) - i))
            ) if i + 1 < len(bear) else 0
            hits.append({
                "reversal_bars": reversal_bars,
                "bear_bounce":   max_bear_bounce,
                "win":           max_bear_bounce > 0.005,
            })
        if hits:
            rev_bars = [h["reversal_bars"] for h in hits if h["reversal_bars"]]
            bounces  = [h["bear_bounce"]   for h in hits]
            events.append({
                "ob_level":        ob_level,
                "occurrences":     len(hits),
                "win_rate":        pct(sum(1 for h in hits if h["win"]), len(hits)),
                "avg_bear_bounce": round(np.mean(bounces) * 100, 2),
                "avg_bars_to_rev": round(np.mean(rev_bars), 1) if rev_bars else None,
                "p75_bounce":      round(float(np.percentile(bounces, 75)) * 100, 2),
            })
    return {"reversal_data": events}

def analyze_volume_spikes(df, sym):
    if df is None or "volume" not in df.columns or len(df) < 200:
        return {}
    prices  = df["close"].values
    volumes = df["volume"].values
    results = {}
    for mult in [1.5, 2.0, 2.5, 3.0]:
        trades  = []
        in_pos  = False
        entry_price = peak_price = 0.0
        entry_idx   = 0
        for i in range(20, len(prices) - 12):
            price = prices[i]
            vol   = volumes[i]
            if in_pos:
                peak_price = max(peak_price, price)
                pnl        = (price - entry_price) / entry_price
                drawdown   = (peak_price - price) / peak_price if peak_price > 0 else 0
                if (pnl >= 0.015 and drawdown >= 0.004) or pnl <= -0.015 or i - entry_idx > 12:
                    trades.append({"pnl": pnl, "win": pnl > 0})
                    in_pos = False
            else:
                avg_vol   = np.mean(volumes[max(0, i - 10):i]) if i > 0 else 0
                if avg_vol <= 0:
                    continue
                vol_spike  = vol >= avg_vol * mult
                price_move = (abs(prices[i] - prices[max(0, i - 5)]) / prices[max(0, i - 5)] >= 0.005
                              if prices[max(0, i - 5)] > 0 else False)
                ma20 = np.mean(prices[i - 20:i]) if i >= 20 else price
                if vol_spike and price_move and price > ma20:
                    in_pos      = True
                    entry_price = price
                    peak_price  = price
                    entry_idx   = i
        if trades:
            results[f"mult_{mult}x"] = {
                "multiplier":   mult,
                "trades":       len(trades),
                "win_rate":     pct(sum(1 for t in trades if t["win"]), len(trades)),
                "avg_pnl":      round(np.mean([t["pnl"] for t in trades]) * 100, 3),
                "ev_per_trade": round(np.mean([t["pnl"] for t in trades]) * 100, 4),
            }
    return results


# ==============================================================================
# SECTION 4 — BERSERKER BACKTESTER
# Replays the EXACT entry logic from main.py get_signals() and execute_trade()
# Writes every simulated trade to berserker_trade_fingerprints in PostgreSQL
# ==============================================================================
def simulate_berserker_trades(symbol, df, spy_df, f):
    """
    Replay Berserker entry/exit logic on historical 1-min bars.
    Returns list of trade dicts ready for DB insertion.
    """
    if df is None or len(df) < MACD_SLOW + MACD_SIGNAL_PER + 5:
        print_and_log(f"  {symbol}: insufficient bars ({len(df) if df is not None else 0})", f)
        return []

    prices     = df["close"].values
    times      = df.index
    spy_prices = spy_df["close"].values if spy_df is not None else []
    spy_times  = spy_df.index          if spy_df is not None else []

    # Build a time->index map for SPY so we can align by timestamp
    spy_idx_map = {}
    if spy_df is not None:
        for idx, ts in enumerate(spy_times):
            spy_idx_map[ts] = idx

    recipe      = BERSERKER_RECIPES.get(symbol, {"avoid_hours": [], "avoid_days": []})
    avoid_hours = recipe.get("avoid_hours", [])
    avoid_days  = recipe.get("avoid_days",  [])

    trades      = []
    in_pos      = False
    entry_price = peak_price = 0.0
    entry_ts    = 0
    entry_hour  = entry_day = 0
    entry_rsi   = 50.0
    entry_macd  = False
    entry_above_ma20 = False
    entry_spy_ctx    = {}
    entry_sector     = "STRONG"
    mfe = mae        = 0.0

    # Need enough warmup bars for MACD (slow=26) + signal(9) = 35 + buffer
    warmup = max(RSI_PERIOD + 1, 20, MACD_SLOW + MACD_SIGNAL_PER + 5)

    for i in range(warmup, len(prices)):
        price = prices[i]
        ts    = times[i]
        hour  = hr_cst(ts)
        day   = day_cst(ts)

        # Market hours only (8am-3pm CDT, weekdays)
        if day >= 5 or hour < 8 or hour > 14:
            continue

        if in_pos:
            peak_price = max(peak_price, price)
            profit_pct = (price - entry_price) / entry_price
            mfe        = max(mfe, profit_pct)
            mae        = min(mae, profit_pct)
            bars_held  = i - entry_bar
            drawdown   = (peak_price - price) / peak_price if peak_price > 0 else 0
            trailing   = RATCHET_TRAIL if profit_pct >= RATCHET_PROFIT else TRAILING_STOP

            exit_reason = None

            # Stop loss
            if profit_pct <= -STOP_LOSS_PCT:
                exit_reason = "stop-loss"

            # Trailing stop (with min hold)
            elif drawdown >= trailing and bars_held >= MIN_HOLD_BARS:
                exit_reason = "trailing-stop"

            # EOD close — 2:58pm
            elif hour == 14 and (ts.tz_convert(CENTRAL).minute if hasattr(ts, "tz_convert") else ts.minute) >= 58:
                exit_reason = "eod"

            # Max hold safety valve
            elif bars_held >= MAX_HOLD_BARS:
                exit_reason = "timeout"

            if exit_reason:
                won = profit_pct > 0
                trades.append({
                    "symbol":       symbol,
                    "sector":       "TRUMP" if symbol in TRUMP_THEME else "TECH",
                    "entry_ts":     entry_ts,
                    "exit_ts":      int(ts.timestamp()),
                    "entry_price":  round(entry_price, 4),
                    "symbol_rsi":   round(entry_rsi, 2),
                    "macd_bullish": entry_macd,
                    "above_ma20":   entry_above_ma20,
                    "spy_rsi":      entry_spy_ctx.get("rsi", 50.0),
                    "spy_momentum": entry_spy_ctx.get("momentum", 0.0),
                    "spy_bullish":  entry_spy_ctx.get("bullish", False),
                    "qqq_rsi":      entry_spy_ctx.get("qqq_rsi", 50.0),
                    "sector_health": entry_sector,
                    "hour_cdt":     entry_hour,
                    "day_of_week":  entry_day,
                    "pdt_slots_used": 1,   # conservative assumption
                    "won":          won,
                    "pnl_pct":      round(profit_pct * 100, 3),
                    "exit_reason":  exit_reason,
                    "hold_time_min": int(bars_held),
                    "mfe":          round(mfe * 100, 3),
                    "mae":          round(mae * 100, 3),
                })
                in_pos = False
                mfe = mae = 0.0

        else:
            # Check recipe gates
            if hour in avoid_hours or day in avoid_days:
                continue

            # Need enough bars for all indicators
            window = prices[max(0, i - max(RSI_PERIOD + 1, MACD_SLOW + MACD_SIGNAL_PER)):i + 1]
            if len(window) < MACD_SLOW + MACD_SIGNAL_PER:
                continue

            # RSI
            rsi_window = prices[max(0, i - (RSI_PERIOD * 3)):i + 1].tolist()
            rsi = calc_rsi_simple(rsi_window, period=RSI_PERIOD)
            if np.isnan(rsi):
                continue

            # Trigger threshold — mirrors main.py exactly
            sector_health = "STRONG"  # conservative
            required_rsi  = 72 if (symbol in TRUMP_THEME and sector_health == "WEAK") else RSI_BUY_TRIGGER

            if rsi <= required_rsi:
                continue

            # MACD
            macd_window = prices[max(0, i - (MACD_SLOW + MACD_SIGNAL_PER + 5)):i + 1].tolist()
            if len(macd_window) < MACD_SLOW + MACD_SIGNAL_PER:
                continue
            macd_val, macd_sig = calc_macd(macd_window)
            macd_bull = macd_val > macd_sig
            if not macd_bull:
                continue

            # MA20
            if i < 20:
                continue
            ma20       = float(np.mean(prices[i - 20:i]))
            above_ma20 = price > ma20
            if not above_ma20:
                continue

            # All conditions met — build SPY context
            spy_ctx = {"rsi": 50.0, "qqq_rsi": 50.0, "momentum": 0.0, "bullish": False}
            if len(spy_prices) > 0:
                # Find closest SPY bar at or before this timestamp
                spy_i = spy_idx_map.get(ts)
                if spy_i is None:
                    # Find nearest — use the closest prior index
                    prior = [idx for t2, idx in spy_idx_map.items() if t2 <= ts]
                    spy_i = prior[-1] if prior else None
                if spy_i is not None and spy_i >= 8:
                    spy_window = spy_prices[max(0, spy_i - 20):spy_i + 1].tolist()
                    spy_ctx    = calc_spy_context(spy_window)

            in_pos           = True
            entry_price      = price
            peak_price       = price
            entry_ts         = int(ts.timestamp())
            entry_bar        = i
            entry_hour       = hour
            entry_day        = day
            entry_rsi        = rsi
            entry_macd       = macd_bull
            entry_above_ma20 = above_ma20
            entry_spy_ctx    = spy_ctx
            entry_sector     = sector_health
            mfe = mae        = 0.0

    return trades


# ==============================================================================
# DATABASE — seed berserker_trade_fingerprints
# ==============================================================================
def get_db_conn():
    if not _pg_ok or not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        return conn
    except Exception as e:
        print(f"[DB] Connection error: {e}", flush=True)
        return None

def init_db_tables(conn):
    """Ensure tables exist — same DDL as BerserkerMemory.init_tables() in main.py."""
    ddl = """
    CREATE TABLE IF NOT EXISTS berserker_trade_fingerprints (
        id              SERIAL PRIMARY KEY,
        trade_id        VARCHAR(32) UNIQUE NOT NULL,
        symbol          VARCHAR(10) NOT NULL,
        sector          VARCHAR(20),
        entry_ts        BIGINT,
        exit_ts         BIGINT,
        entry_price     REAL,
        symbol_rsi      REAL,
        macd_bullish    BOOLEAN,
        above_ma20      BOOLEAN,
        spy_rsi         REAL,
        spy_momentum    REAL,
        spy_bullish     BOOLEAN,
        qqq_rsi         REAL,
        sector_health   VARCHAR(10),
        hour_cdt        INTEGER,
        day_of_week     INTEGER,
        pdt_slots_used  INTEGER,
        won             BOOLEAN,
        pnl_pct         REAL,
        exit_reason     VARCHAR(50),
        hold_time_min   INTEGER,
        mfe             REAL,
        mae             REAL,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_btf_symbol ON berserker_trade_fingerprints(symbol);
    CREATE INDEX IF NOT EXISTS idx_btf_won    ON berserker_trade_fingerprints(won);
    CREATE TABLE IF NOT EXISTS berserker_pattern_stats (
        id           SERIAL PRIMARY KEY,
        bucket_key   VARCHAR(200) UNIQUE NOT NULL,
        win_rate     REAL NOT NULL,
        sample_count INTEGER NOT NULL,
        avg_pnl      REAL,
        last_updated TIMESTAMPTZ DEFAULT NOW()
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        print("[DB] Tables ready", flush=True)
        return True
    except Exception as e:
        print(f"[DB] init_tables error: {e}", flush=True)
        return False

def clear_backtest_data(conn):
    """Remove existing fingerprints — clean slate before seeding."""
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM berserker_trade_fingerprints;")
            cur.execute("DELETE FROM berserker_pattern_stats;")
        conn.commit()
        print("[DB] Cleared existing fingerprints and pattern stats", flush=True)
        return True
    except Exception as e:
        print(f"[DB] Clear error: {e}", flush=True)
        conn.rollback()
        return False

def write_fingerprints_batch(conn, trades):
    """Batch insert a list of trade dicts. Returns count inserted."""
    if not trades:
        return 0
    inserted = 0
    try:
        with conn.cursor() as cur:
            for t in trades:
                trade_id = secrets.token_hex(8)
                cur.execute("""
                    INSERT INTO berserker_trade_fingerprints
                    (trade_id, symbol, sector, entry_ts, exit_ts, entry_price,
                     symbol_rsi, macd_bullish, above_ma20,
                     spy_rsi, spy_momentum, spy_bullish, qqq_rsi,
                     sector_health, hour_cdt, day_of_week, pdt_slots_used,
                     won, pnl_pct, exit_reason, hold_time_min, mfe, mae)
                    VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,
                            %s,%s,%s,%s, %s,%s,%s,%s,%s,%s)
                    ON CONFLICT (trade_id) DO NOTHING
                """, (
                    trade_id,
                    t["symbol"], t["sector"], t["entry_ts"], t["exit_ts"], t["entry_price"],
                    t["symbol_rsi"], t["macd_bullish"], t["above_ma20"],
                    t["spy_rsi"], t["spy_momentum"], t["spy_bullish"], t["qqq_rsi"],
                    t["sector_health"], t["hour_cdt"], t["day_of_week"], t["pdt_slots_used"],
                    t["won"], t["pnl_pct"], t["exit_reason"], t["hold_time_min"],
                    t["mfe"], t["mae"],
                ))
                inserted += 1
        conn.commit()
    except Exception as e:
        print(f"[DB] Batch write error: {e}", flush=True)
        conn.rollback()
    return inserted

def run_pattern_analysis(conn, f):
    """
    Mirrors BerserkerMemory.run_analysis() in main.py EXACTLY.
    Reads all completed fingerprints, buckets them, writes berserker_pattern_stats.
    """
    print_and_log("\n[DB] Running pattern analysis...", f)
    query = """
        SELECT symbol, sector, symbol_rsi, macd_bullish, spy_bullish,
               sector_health, hour_cdt, day_of_week, pdt_slots_used,
               won, pnl_pct, mfe, mae
        FROM berserker_trade_fingerprints WHERE won IS NOT NULL
    """
    try:
        with pg_extras.RealDictCursor(conn) as cur:
            cur.execute(query)
            rows = cur.fetchall()

        total = len(rows)
        if total < 20:
            print_and_log(f"[DB] Only {total} trades — need 20+ for analysis", f)
            return

        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)

        for row in rows:
            key = bucket_key(
                row["symbol"],
                float(row["symbol_rsi"]) if row["symbol_rsi"] is not None else 50,
                bool(row["spy_bullish"]),
                row["sector_health"] or "STRONG",
                int(row["hour_cdt"]) if row["hour_cdt"] is not None else 12,
                int(row["pdt_slots_used"]) if row["pdt_slots_used"] is not None else 0,
            )
            buckets[key].append(bool(row["won"]))
            if row["pnl_pct"] is not None:
                pnl_bkts[key].append(float(row["pnl_pct"]))

        inserted = 0
        with conn.cursor() as cur:
            for key, outcomes in buckets.items():
                if len(outcomes) < PM_MIN_BUCKET:
                    continue
                wr      = sum(outcomes) / len(outcomes)
                avg_pnl = (sum(pnl_bkts[key]) / len(pnl_bkts[key])
                           if pnl_bkts[key] else None)
                cur.execute("""
                    INSERT INTO berserker_pattern_stats
                    (bucket_key, win_rate, sample_count, avg_pnl)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (bucket_key) DO UPDATE
                    SET win_rate=EXCLUDED.win_rate,
                        sample_count=EXCLUDED.sample_count,
                        avg_pnl=EXCLUDED.avg_pnl,
                        last_updated=NOW()
                """, (key, wr, len(outcomes), avg_pnl))
                inserted += 1
        conn.commit()

        overall_wr = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
        print_and_log(f"[DB] Pattern analysis complete:", f)
        print_and_log(f"     {total} trades | {inserted} buckets | {overall_wr:.1%} overall WR", f)

        # Print top and bottom buckets
        with pg_extras.RealDictCursor(conn) as cur:
            cur.execute("""
                SELECT bucket_key, win_rate, sample_count, avg_pnl
                FROM berserker_pattern_stats
                ORDER BY win_rate DESC LIMIT 10
            """)
            top = cur.fetchall()
        print_and_log("\n  Top 10 buckets (best WR):", f)
        for b in top:
            flag = " *** BELOW GATE" if b["win_rate"] < WIN_RATE_GATE else ""
            print_and_log(
                f"    {round(b['win_rate']*100)}% WR | n={b['sample_count']} | "
                f"avg PnL={round(b['avg_pnl'] or 0,2)}% | {b['bucket_key'][:60]}{flag}", f
            )

        with pg_extras.RealDictCursor(conn) as cur:
            cur.execute("""
                SELECT bucket_key, win_rate, sample_count, avg_pnl
                FROM berserker_pattern_stats
                ORDER BY win_rate ASC LIMIT 10
            """)
            bottom = cur.fetchall()
        print_and_log("\n  Bottom 10 buckets (worst WR — these get blocked by win-rate gate):", f)
        for b in bottom:
            flag = " *** BLOCKED" if b["win_rate"] < WIN_RATE_GATE else ""
            print_and_log(
                f"    {round(b['win_rate']*100)}% WR | n={b['sample_count']} | "
                f"avg PnL={round(b['avg_pnl'] or 0,2)}% | {b['bucket_key'][:60]}{flag}", f
            )

    except Exception as e:
        print_and_log(f"[DB] Pattern analysis error: {e}", f)
        conn.rollback()


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    print("\nNEXUS MARKET ANALYZER — 1-MIN RAILWAY V2.0", flush=True)
    print("=" * 50, flush=True)
    print(f"Output directory: {OUTPUT_DIR}", flush=True)
    print(f"Database: {'connected' if DATABASE_URL else 'NOT SET — DB seeding disabled'}", flush=True)

    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    updated_recipes = {}

    # DB connection — established once, reused throughout
    conn = get_db_conn()
    if conn:
        if init_db_tables(conn):
            clear_backtest_data(conn)
        else:
            conn = None

    with open(out_file, "w", encoding="utf-8") as f:
        ts = datetime.now(tz=CENTRAL).strftime("%Y-%m-%d %H:%M CST")
        print_and_log(f"NEXUS MARKET ANALYZER 1-MIN V2.0 -- {ts}", f)
        print_and_log(f"Data: {LOOKBACK_YEARS} years of 1-min bars from Alpaca (IEX)", f)
        print_and_log(f"DB seeding: {'ENABLED' if conn else 'DISABLED'}", f)

        # ---------------------------------------------------------------
        # DATA FETCH — all symbols in one pass
        # ---------------------------------------------------------------
        print_and_log("\nFetching market data...", f)
        all_needed = list(set(
            ALL_SYMBOLS + list(PAIRS.values()) + ["SPY", "QQQ"]
        ))
        data = fetch_bars(all_needed, client, years=LOOKBACK_YEARS, log_f=f)
        print_and_log(f"\nLoaded {len(data)} symbols", f)

        if len(data) == 0:
            print_and_log("ERROR: No data returned. Check API keys.", f)
            return

        spy_df = data.get("SPY")
        qqq_df = data.get("QQQ")

        # ===================================================================
        # SECTION 1 — SCALPER ANALYSIS (unchanged)
        # ===================================================================
        write_section(f, "1. SCALPER -- FULL SYMBOL ANALYSIS (1-MIN)")
        scalper_summary = {}

        for sym in SCALPER_SYMBOLS:
            if sym not in data:
                continue
            write_subsection(f, sym)
            df     = data[sym]
            trades = simulate_dip_trades(df)
            if not trades:
                print_and_log(f"  {sym}: insufficient signal data", f)
                continue

            wins   = sum(1 for t in trades if t["pnl"] > 0)
            losses = len(trades) - wins
            wr     = pct(wins, len(trades))
            avg_w  = np.mean([t["pnl"] for t in trades if t["pnl"] > 0]) * 100 if wins else 0
            avg_l  = np.mean([t["pnl"] for t in trades if t["pnl"] <= 0]) * 100 if losses else 0
            ev     = round((wins / len(trades)) * avg_w + (losses / len(trades)) * avg_l, 3) if trades else 0

            print_and_log(
                f"  Base: {len(trades)} trades | {wr}% WR | "
                f"avg W: +{round(avg_w,2)}% | avg L: {round(avg_l,2)}% | EV: {ev}%", f
            )

            st = analyze_stop_trail(trades)
            if st:
                print_and_log(f"  MAE: avg={st['avg_mae']}% p75={st['p75_mae']}% p90={st['p90_mae']}%", f)
                print_and_log(f"  MFE: avg={st['avg_mfe']}% p25={st['p25_mfe']}% p75={st['p75_mfe']}%", f)
                print_and_log(
                    f"  Optimal: stop={st['optimal_stop']}%  "
                    f"ratchet={st['optimal_ratchet']}%  trail={st['optimal_trail']}%", f
                )

            hours, days = analyze_hour_day(trades, sym)
            if hours:
                print_and_log("  Hour breakdown (CDT):", f)
                for h in sorted(hours):
                    hd   = hours[h]
                    t    = hd["wins"] + hd["losses"]
                    hr   = pct(hd["wins"], t)
                    flag = " AVOID" if hr < 45 else " BEST" if hr > 60 else ""
                    print_and_log(f"    {h}:00 -- {t:3d} trades | {hr:4.1f}% WR{flag}", f)

            if days:
                print_and_log("  Day breakdown:", f)
                for dn in ["Mon", "Tue", "Wed", "Thu", "Fri"]:
                    if dn in days:
                        dd   = days[dn]
                        t    = dd["wins"] + dd["losses"]
                        dr   = pct(dd["wins"], t)
                        flag = " AVOID" if dr < 45 else " BEST" if dr > 62 else ""
                        print_and_log(f"    {dn} -- {t:3d} trades | {dr:4.1f}% WR{flag}", f)

            bull_sym = PAIRS.get(sym)
            if bull_sym and bull_sym in data:
                rev = analyze_reversal_pattern(data[sym], data[bull_sym])
                if rev.get("reversal_data"):
                    print_and_log(f"  Reversal (when {bull_sym} overbought):", f)
                    for rd in rev["reversal_data"]:
                        print_and_log(
                            f"    RSI>{rd['ob_level']}: {rd['occurrences']} events | "
                            f"{rd['win_rate']}% WR | avg bounce +{rd['avg_bear_bounce']}% | "
                            f"p75 +{rd['p75_bounce']}% | avg bars to rev: {rd['avg_bars_to_rev']}", f
                        )

            scalper_summary[sym] = {"trades": len(trades), "wr": wr, "ev": ev,
                                    "stop_trail": st, "hours": hours, "days": days}

        # ===================================================================
        # SECTION 2 — SCANNER VOLUME SPIKE OPTIMIZATION (unchanged)
        # ===================================================================
        write_section(f, "2. SCANNER -- VOLUME SPIKE OPTIMIZATION (1-MIN)")
        for sym in SCANNER_PRIORITY:
            if sym not in data:
                continue
            vol_results = analyze_volume_spikes(data[sym], sym)
            if vol_results:
                best     = max(vol_results.values(), key=lambda x: x["ev_per_trade"])
                best_key = [k for k, v in vol_results.items() if v == best][0]
                print_and_log(
                    f"  {sym}: optimal={best['multiplier']}x | "
                    f"{best['trades']} trades | {best['win_rate']}% WR | "
                    f"EV: {best['ev_per_trade']}%/trade", f
                )
                for mk, mv in sorted(vol_results.items(), key=lambda x: x[1]["multiplier"]):
                    flag = " <- OPTIMAL" if mk == best_key else ""
                    print_and_log(
                        f"    {mv['multiplier']}x: {mv['trades']} trades | "
                        f"{mv['win_rate']}% WR | avg PnL: {mv['avg_pnl']}%{flag}", f
                    )

        # ===================================================================
        # SECTION 3 — RECIPE RECOMMENDATIONS (unchanged)
        # ===================================================================
        write_section(f, "3. RECOMMENDED RECIPE UPDATES (1-MIN DATA)")
        print_and_log("  Compare vs 5-min recipes -- use tighter values where 1-min confirms", f)

        for sym in SCALPER_SYMBOLS:
            s = scalper_summary.get(sym)
            if not s:
                continue
            st         = s.get("stop_trail", {})
            hours_data = s.get("hours", {})
            days_data  = s.get("days",  {})

            avoid_h = [h for h, hd in hours_data.items()
                       if pct(hd["wins"], hd["wins"] + hd["losses"]) < 45
                       and hd["wins"] + hd["losses"] >= 10]
            avoid_d = [{"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4}[dn]
                       for dn, dd in days_data.items()
                       if pct(dd["wins"], dd["wins"] + dd["losses"]) < 45
                       and dd["wins"] + dd["losses"] >= 10]

            recipe_rec = {
                "stop_loss":      round(st.get("optimal_stop",    2.0) / 100, 3) if st else 0.020,
                "profit_ratchet": round(st.get("optimal_ratchet", 0.75) / 100, 4) if st else 0.0075,
                "trailing_stop":  round(st.get("optimal_trail",   0.4) / 100, 4) if st else 0.004,
                "avoid_hours":    avoid_h,
                "avoid_days":     avoid_d,
            }
            updated_recipes[sym] = recipe_rec
            print_and_log(f"\n  {sym}:", f)
            print_and_log(f"    stop_loss:      {recipe_rec['stop_loss']}", f)
            print_and_log(f"    profit_ratchet: {recipe_rec['profit_ratchet']}", f)
            print_and_log(f"    trailing_stop:  {recipe_rec['trailing_stop']}", f)
            print_and_log(f"    avoid_hours:    {recipe_rec['avoid_hours']}", f)
            print_and_log(f"    avoid_days:     {recipe_rec['avoid_days']}", f)

        # ===================================================================
        # SECTION 4 — BERSERKER BACKTESTER + DB SEED
        # ===================================================================
        write_section(f, "4. BERSERKER BACKTESTER -- PATTERN MEMORY SEED")
        print_and_log(
            f"  Replaying {LOOKBACK_YEARS}-year signal history for {len(BERSERKER_SYMBOLS)} symbols\n"
            f"  Entry logic: RSI>{RSI_BUY_TRIGGER} + MACD bull + above MA20 + recipe gates\n"
            f"  Exit logic:  {STOP_LOSS_PCT*100}% stop | {TRAILING_STOP*100}% trail "
            f"({RATCHET_TRAIL*100}% after {RATCHET_PROFIT*100}% ratchet) | EOD\n"
            f"  DB write: {'ENABLED' if conn else 'DISABLED'}", f
        )

        total_inserted  = 0
        symbol_summary  = {}

        for sym in BERSERKER_SYMBOLS:
            if sym not in data:
                print_and_log(f"  {sym}: no data — skipping", f)
                continue

            write_subsection(f, sym)
            sym_trades = simulate_berserker_trades(sym, data[sym], spy_df, f)

            if not sym_trades:
                print_and_log(f"  {sym}: no trades generated", f)
                continue

            wins    = sum(1 for t in sym_trades if t["won"])
            losses  = len(sym_trades) - wins
            wr      = pct(wins, len(sym_trades))
            avg_pnl = round(np.mean([t["pnl_pct"] for t in sym_trades]), 3)
            avg_mfe = round(np.mean([t["mfe"] for t in sym_trades]), 3)
            avg_mae = round(np.mean([t["mae"] for t in sym_trades]), 3)

            exits = defaultdict(int)
            for t in sym_trades:
                exits[t["exit_reason"]] += 1

            print_and_log(
                f"  {sym}: {len(sym_trades)} trades | {wr}% WR | "
                f"avg PnL={avg_pnl}% | MFE={avg_mfe}% | MAE={avg_mae}%", f
            )
            print_and_log(
                f"    Exits: " +
                " | ".join(f"{k}={v}" for k, v in sorted(exits.items())), f
            )

            # Hour breakdown for Berserker symbols
            hours, days = analyze_hour_day(sym_trades, sym)
            if hours:
                print_and_log("    Hour WR:", f)
                for h in sorted(hours):
                    hd  = hours[h]
                    t   = hd["wins"] + hd["losses"]
                    hr  = pct(hd["wins"], t)
                    flag = " !! AVOID" if hr < 40 else " BEST" if hr > 65 else ""
                    print_and_log(f"      {h}:00 -- {t:3d} trades | {hr:4.1f}% WR{flag}", f)

            symbol_summary[sym] = {
                "trades": len(sym_trades), "wr": wr,
                "avg_pnl": avg_pnl, "avg_mfe": avg_mfe, "avg_mae": avg_mae
            }

            # Write to DB
            if conn:
                n = write_fingerprints_batch(conn, sym_trades)
                total_inserted += n
                print_and_log(f"    DB: {n} fingerprints written", f)

        # Summary table
        print_and_log("\n\n  BERSERKER BACKTEST SUMMARY", f)
        print_and_log("  " + "-" * 60, f)
        print_and_log(f"  {'Symbol':<8} {'Trades':>7} {'WR':>6} {'AvgPnL':>8} {'MFE':>7} {'MAE':>7}", f)
        print_and_log("  " + "-" * 60, f)
        for sym, s in sorted(symbol_summary.items(), key=lambda x: -x[1]["wr"]):
            flag = "  <-- low WR" if s["wr"] < 45 else ""
            print_and_log(
                f"  {sym:<8} {s['trades']:>7} {s['wr']:>5.1f}% "
                f"{s['avg_pnl']:>7.3f}% {s['avg_mfe']:>6.3f}% {s['avg_mae']:>6.3f}%{flag}", f
            )

        if conn:
            print_and_log(f"\n  Total fingerprints written to DB: {total_inserted}", f)
            run_pattern_analysis(conn, f)

        # ===================================================================
        # FOOTER
        # ===================================================================
        print_and_log("\n\n" + "=" * 70, f)
        print_and_log("  END OF NEXUS 1-MIN ANALYZER REPORT V2.0", f)
        print_and_log("=" * 70, f)

    # Save recipe file (scalper/phase4 — unchanged)
    with open(recipe_file, "w", encoding="utf-8") as f:
        json.dump(updated_recipes, f, indent=2)

    if conn:
        conn.close()

    print(f"\nReport saved:  {out_file}", flush=True)
    print(f"Recipes saved: {recipe_file}", flush=True)
    print("\nAnalysis complete. Service will exit.", flush=True)


if __name__ == "__main__":
    main()
