"""
NEXUS PHASE 4 — PER-SYMBOL AUTONOMOUS BOTS V2.2
4 dedicated bots: NUGT, SOXL, LABU, TQQQ
Each reads full market context, selects a trading mode, executes independently.

Trading Modes:
  SCALP    — RSI dip + bounce. Quick target. Choppy/uncertain markets.
  RIDE     — Sustained trend entry. Looser trail. Bull market confirmed.
  EXTENDED — Multi-bar uptrend, higher lows locked in. Ride the full wave.

Bear pairs: each bot monitors bull RSI exhaustion -> flips to bear ETF on reversal.
  NUGT -> DUST | SOXL -> SOXS | LABU -> LABD | TQQQ -> SQQQ

Capital allocation (by EV from nexus_analyzer 2yr + 1yr backtest):
  NUGT 30% | SOXL 25% | LABU 25% | TQQQ 20%

V2.2 — Cross-service capital coordination (Jun 30 2026):
  Confirmed: Phase4 and Berserker (main.py, separate Railway service/process,
  nexus-commander/rare-perception) trade against the SAME live Alpaca account
  -- one account was ever created; paper trading was layered on top of it,
  never a second account. Each service was independently calling Alpaca's
  buying_power and sizing trades with zero knowledge of the other's
  outstanding orders. Added capital_coordinator.py, coordinating through
  Postgres (both services already share DATABASE_URL). try_buy() now clamps
  trade_size against get_available() before sizing, reserves immediately
  before order submission, releases in a finally block immediately after --
  whether the order succeeds or fails. Fails open throughout: any DB/
  connection problem falls back to raw buying_power, uncoordinated, exactly
  the pre-V2.2 behavior, rather than blocking a trade. Identical fix applied
  to main.py (V10.25).

V2.1 — Exit priority fix (Jun 30 2026):
  rsi-overbought was checked BEFORE the profit ratchet. On a mean-reversion
  entry (buy oversold, RSI climbs toward overbought as the thesis plays out),
  that ordering let rsi-overbought cut almost every winner the instant profit
  ticked positive -- before the position ever reached early_ratchet.
  Backtest evidence (phase4_backtester.py V1.2, 2yr replay): 298/321 wins
  (93%) were rsi-overbought exits averaging ~+0.24%, while the 61 stop-losses
  averaged ~-1.7% (atr_stop) -- a ~7:1 win/loss size mismatch that made a
  73% WR system net PnL-negative (avg -0.031%/trade) in both training and
  out-of-sample. Ratchet is now checked first; rsi-overbought is the fallback
  for trades that hit RSI=70 without yet clearing early_ratchet -- i.e. take
  the small win/scratch now rather than risk it reversing to red. No change
  to stop-loss priority (still checked first, unconditionally) or to any
  entry-side logic, sizing, or gating.

V2.0 — Full Alpaca migration (Jun 29 2026):
  Webull broker layer completely removed.
  Alpaca TradingClient handles all orders, positions, buying power.
  Alpaca StockHistoricalDataClient replaces yfinance for all price bars.
  yfinance kept as fallback only for ^VIX (not available on Alpaca IEX).
  acct_id param removed throughout — Alpaca uses API key auth, not account IDs.
  Fractional shares supported via Alpaca notional orders.

V1.9 — Alpaca data feed replaces yfinance for price bars.
V1.8 — Score + StochRSI fixes.
V1.7 — RSI Wilder EWM fix.
V1.6 — Complete entry/exit overhaul.
"""

import os
import time
import threading
import traceback
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from capital_coordinator import CapitalCoordinator, NO_COORDINATION

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce
    _alpaca_trade_ok = True
except ImportError:
    _alpaca_trade_ok = False

try:
    from alpaca.data import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    _alpaca_data_ok = True
except ImportError:
    _alpaca_data_ok = False

try:
    import psycopg2
    import psycopg2.extras
    _db_available = True
except ImportError:
    _db_available = False

# ── Env ───────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
ANALYST_URL       = os.environ.get("ANALYST_URL", "").rstrip("/")
NEXUS_TOKEN       = os.environ.get("NEXUS_INTERNAL_TOKEN", "")
SQQQ_ENABLED      = os.environ.get("PHASE4_SQQQ_ENABLED", "false").lower() == "true"
ALPACA_API_KEY    = os.environ.get("APCA_API_KEY_ID", "")
ALPACA_API_SECRET = os.environ.get("APCA_API_SECRET_KEY", "")
PAPER_MODE        = os.environ.get("APCA_PAPER", "false").lower() == "true"

CENTRAL  = ZoneInfo("America/Chicago")
BOT_NAME = "PHASE4"

# ── Alpaca clients ────────────────────────────────────────────────────────────
_trade_client = None
_data_client  = None

if _alpaca_trade_ok and ALPACA_API_KEY and ALPACA_API_SECRET:
    try:
        _trade_client = TradingClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_API_SECRET,
            paper=PAPER_MODE,
        )
        print(f"[PHASE4] Alpaca trading client ready (paper={PAPER_MODE})", flush=True)
    except Exception as _e:
        print(f"[PHASE4] ⚠ Alpaca trading client error: {_e}", flush=True)

if _alpaca_data_ok and ALPACA_API_KEY and ALPACA_API_SECRET:
    try:
        _data_client = StockHistoricalDataClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_API_SECRET,
        )
    except Exception as _e:
        print(f"[PHASE4] ⚠ Alpaca data client error: {_e}", flush=True)

# ── Per-symbol config ─────────────────────────────────────────────────────────
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
        "avoid_days":     [],
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
        "avoid_days":     [],
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
        "avoid_days":     [],
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
        "avoid_days":     [],
        "best_signals":   ["rsi_lt40", "macd_bullish", "near_lower_bb", "obv_falling", "ema9_above_ema21"],
        "worst_signals":  ["williams_oversold", "bb_squeeze"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  2.0,
    },
}

BEAR_RECIPES = {
    "DUST": {
        "underlying":     "GDX",
        "min_score":      7,
        "atr_stop":       0.0180,
        "early_ratchet":  0.0134,
        "trail":          0.0033,
        "avoid_hours":    [11],
        "best_signals":   ["obv_rising", "below_ma20", "macd_bullish", "far_below_bb"],
        "worst_signals":  ["obv_falling", "near_lower_bb"],
    },
    "SOXS": {
        "underlying":     "SMH",
        "min_score":      9,
        "atr_stop":       0.0177,
        "early_ratchet":  0.0120,
        "trail":          0.0032,
        "avoid_hours":    [9],
        "best_signals":   ["below_ma20", "rsi_lt25", "rsi14_lt20", "stochrsi_oversold", "near_lower_bb"],
        "worst_signals":  ["obv_rising", "macd_bullish"],
    },
    "LABD": {
        "underlying":     "XBI",
        "min_score":      4,
        "atr_stop":       0.0164,
        "early_ratchet":  0.0032,
        "trail":          0.0030,
        "avoid_hours":    [11, 14],
        "best_signals":   ["far_below_bb", "near_lower_bb", "obv_falling", "rsi_lt25", "macd_bullish"],
        "worst_signals":  ["bb_squeeze", "below_ma20"],
    },
    "SQQQ": {
        "underlying":     "QQQ",
        "min_score":      5,
        "atr_stop":       0.0178,
        "early_ratchet":  0.0045,
        "trail":          0.0030,
        "avoid_hours":    [11, 13, 14],
        "best_signals":   ["rsi_lt40", "rsi14_lt35", "bouncing"],
        "worst_signals":  ["rsi_lt25", "at_lower_bb"],
    },
}

BEAR_EXTENDED_TP = {
    "DUST": {"trail_activate": 0.020, "trail_stop": 0.010},
    "SOXS": {"trail_activate": 0.020, "trail_stop": 0.010},
}

SIGNAL_COMBO_BOOST_SYMBOLS = {
    "SOXL": [("bb_squeeze", "stochrsi_oversold")],
    "LABU": [("rsi14_lt20", "rsi_lt25")],
    "TQQQ": [("bouncing", "obv_falling")],
    "NUGT": [("bb_squeeze", "macd_bullish")],
    "DUST": [("far_below_bb", "stochrsi_oversold")],
    "SOXS": [("below_ma20", "rsi_lt25")],
    "LABD": [("ema9_above_ema21", "stochrsi_oversold")],
}

VIX_CAUTION  = 28.0
VIX_PAUSE    = 35.0

# V2.4: daily_losses/daily_pnl were tracked and reset at midnight since
# V2.2 but never actually READ anywhere -- boot log claimed "Daily limits"
# as a live feature since V2.2, it wasn't. Per-bot, not account-wide:
# matches how daily_pnl is already tracked (instance attr per SymbolBot),
# and a bad day on one bot shouldn't necessarily silence the other three.
# -6% is a reasoned default (room for normal stop-loss variance across a
# few trades, stops before a bad day compounds), not backtested at this
# specific value -- same caveat as every other first-pass number tonight.
MAX_DAILY_LOSS_PCT = -6.0

REVERSAL_HIGH_RSI    = 75
REVERSAL_HIGH_DROP   = 0.008
REVERSAL_OB_RSI      = 70
REVERSAL_RSI_RESET   = 60
REVERSAL_CONFIRM     = 0.005
REVERSAL_MAX_WATCH   = 1800

DWELL_MINUTES        = 30
DWELL_FLAT_THRESHOLD = 0.001
RSI_OVERBOUGHT_EXIT  = 70
QQQ_BEAR_RSI_GATE      = 58
QQQ_BEAR_RSI_GATE_LABD = 65

PM_MIN_TRADES           = 15
PM_ANALYSIS_INTERVAL    = 86400
PM_MIN_BUCKET_TRADES    = 3
WIN_RATE_GATE_THRESHOLD = 0.35

BUYING_POWER_BUFFER  = 1.05
WIN_COOLDOWN_SECS    = 180
LOSS_COOLDOWN_SECS   = 900
LOOP_INTERVAL        = 12
WARMUP_BARS          = 40

# ── Shared context data ───────────────────────────────────────────────────────
_spy_prices:        list  = []
_qqq_prices:        list  = []
_vix_price:         float = 15.0
_underlying_prices: dict  = {}
_underlying_5m:     dict  = {}
_context_lock               = threading.Lock()
_analyst_scores_cache: dict  = {}
_analyst_scores_ts:    float = 0.0
_analyst_scores_ttl:   float = 20.0
_analyst_lock                = threading.Lock()
_phase4_memory = None
_capital_coordinator = None  # CapitalCoordinator -- initialized in run(), see capital_coordinator.py

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(symbol, msg):
    ts = datetime.now(tz=CENTRAL).strftime("%H:%M:%S")
    print(f"[{symbol} | {ts}] {msg}", flush=True)

def alert(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except Exception:
        pass

def is_market_hours() -> bool:
    now = datetime.now(tz=CENTRAL)
    return now.weekday() < 5 and 8 <= now.hour < 15

# ── Alpaca broker functions ───────────────────────────────────────────────────
def get_buying_power() -> float:
    """Get available buying power from Alpaca account."""
    if _trade_client is None:
        return 0.0
    try:
        acct = _trade_client.get_account()
        return float(acct.buying_power)
    except Exception as e:
        print(f"[P4 BROKER] buying_power error: {e}", flush=True)
        return 0.0

def get_all_positions() -> dict:
    """Returns {symbol: position_object} for all open positions."""
    if _trade_client is None:
        return {}
    try:
        positions = _trade_client.get_all_positions()
        return {p.symbol: p for p in positions}
    except Exception as e:
        print(f"[P4 BROKER] get_positions error: {e}", flush=True)
        return {}

def place_order(symbol: str, side: str, notional: float) -> bool:
    """
    Place a notional market order via Alpaca.
    V2.0: Uses notional (dollar amount) instead of qty — supports fractional shares.
    side: 'BUY' or 'SELL'
    """
    if _trade_client is None:
        print(f"[P4 BROKER] No trade client — order skipped {symbol} {side}", flush=True)
        return False
    try:
        req = MarketOrderRequest(
            symbol=symbol,
            notional=round(notional, 2),
            side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        _trade_client.submit_order(req)
        return True
    except Exception as e:
        print(f"[P4 BROKER] order error {symbol} {side} ${notional}: {e}", flush=True)
        return False

def place_sell_all(symbol: str) -> bool:
    """Close entire position in symbol via Alpaca."""
    if _trade_client is None:
        return False
    try:
        _trade_client.close_position(symbol)
        return True
    except Exception as e:
        print(f"[P4 BROKER] close_position error {symbol}: {e}", flush=True)
        return False

# ── Price data (Alpaca + yfinance fallback) ───────────────────────────────────
def fetch_prices_and_volumes(symbol: str, bars: int = 40, interval: str = "1m") -> tuple:
    alpaca_sym = symbol.replace("^", "")

    if _data_client is not None and not symbol.startswith("^"):
        try:
            tf       = TimeFrame.Minute if interval == "1m" else TimeFrame(5, "Min")
            lookback = timedelta(days=2) if interval == "1m" else timedelta(days=7)
            req      = StockBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=tf,
                start=datetime.now(timezone.utc) - lookback,
                feed="iex",
            )
            df = _data_client.get_stock_bars(req).df
            if df is not None and not df.empty:
                if isinstance(df.index, pd.MultiIndex):
                    df = df.xs(alpaca_sym, level="symbol")
                prices  = df["close"].tail(bars).tolist()
                volumes = df["volume"].tail(bars).tolist()
                if prices:
                    return prices, volumes
        except Exception:
            pass

    # Fallback: yfinance (used for ^VIX and when Alpaca fails)
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        period = "1d" if interval == "1m" else "5d"
        df     = ticker.history(period=period, interval=interval)
        if not df.empty:
            return df["Close"].tail(bars).tolist(), df["Volume"].tail(bars).tolist()
    except Exception:
        pass
    return [], []

def fetch_prices(symbol: str, bars: int = 40) -> list:
    p, _ = fetch_prices_and_volumes(symbol, bars)
    return p

def get_current_price(symbol: str) -> float | None:
    alpaca_sym = symbol.replace("^", "")
    if _data_client is not None and not symbol.startswith("^"):
        try:
            req = StockBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=TimeFrame.Minute,
                start=datetime.now(timezone.utc) - timedelta(minutes=5),
                feed="iex",
            )
            df = _data_client.get_stock_bars(req).df
            if df is not None and not df.empty:
                if isinstance(df.index, pd.MultiIndex):
                    df = df.xs(alpaca_sym, level="symbol")
                if not df.empty:
                    return float(df["close"].iloc[-1])
        except Exception:
            pass
    try:
        import yfinance as yf
        return float(yf.Ticker(symbol).fast_info.last_price)
    except Exception:
        return None

# ── Signal computations ───────────────────────────────────────────────────────
def compute_rsi(prices: list, period: int = 7) -> float | None:
    if len(prices) < period + 1:
        return None
    s     = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0)
    loss  = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    val = float(rsi.iloc[-1])
    if not (0 < val < 100):
        return None
    return round(val, 2)

def compute_ma(prices: list, period: int = 20) -> float | None:
    if len(prices) < period:
        return None
    return round(float(sum(prices[-period:]) / period), 4)

def compute_ema(prices: list, period: int) -> float | None:
    if len(prices) < period:
        return None
    s = pd.Series(prices)
    return round(float(s.ewm(span=period, adjust=False).mean().iloc[-1]), 4)

def compute_atr(prices: list, period: int = 14) -> float:
    if len(prices) < period + 1:
        return abs(prices[-1] * 0.015) if prices else 0.0
    diffs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return round(float(sum(diffs[-period:]) / period), 4)

def compute_macd(prices: list) -> dict:
    if len(prices) < 26:
        return {"bullish": False, "macd_line": 0, "signal_line": 0, "histogram": 0}
    s         = pd.Series(prices)
    ema12     = s.ewm(span=12, adjust=False).mean()
    ema26     = s.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal    = macd_line.ewm(span=9, adjust=False).mean()
    hist      = macd_line - signal
    return {
        "bullish":     float(macd_line.iloc[-1]) > float(signal.iloc[-1]),
        "macd_line":   round(float(macd_line.iloc[-1]), 5),
        "signal_line": round(float(signal.iloc[-1]), 5),
        "histogram":   round(float(hist.iloc[-1]), 5),
    }

def compute_bollinger(prices: list, period: int = 20, std_dev: float = 2.0) -> dict:
    if len(prices) < period:
        return {"upper": 0, "middle": 0, "lower": 0, "pct_b": 0.5,
                "squeeze": False, "near_lower": False, "at_lower": False, "far_below": False}
    s      = pd.Series(prices)
    middle = float(s.rolling(period).mean().iloc[-1])
    std    = float(s.rolling(period).std().iloc[-1])
    upper  = middle + std_dev * std
    lower  = middle - std_dev * std
    price  = prices[-1]
    band_w = upper - lower
    pct_b  = (price - lower) / band_w if band_w > 0 else 0.5
    squeeze = (band_w / price) < 0.02 if price > 0 else False
    return {
        "upper": round(upper, 4), "middle": round(middle, 4), "lower": round(lower, 4),
        "pct_b": round(pct_b, 3), "squeeze": squeeze,
        "near_lower": pct_b < 0.20, "at_lower": pct_b < 0.05,
        "far_below": price < lower * 0.99,
    }

def compute_stochrsi(prices: list, rsi_period: int = 14, stoch_period: int = 14) -> dict:
    if len(prices) < rsi_period + stoch_period + 5:
        return {"k": 50, "d": 50, "oversold": False, "overbought": False}
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
    stoch_d  = stoch_k.rolling(3).mean()
    k_val    = float(stoch_k.iloc[-1])
    d_val    = float(stoch_d.iloc[-1])
    return {
        "k": round(k_val, 2), "d": round(d_val, 2),
        "oversold":   k_val < 20 and d_val < 20,
        "overbought": k_val > 80 and d_val > 80,
    }

def compute_obv(prices: list, volumes: list) -> dict:
    if len(prices) < 10 or len(volumes) < 10:
        return {"rising": False, "obv_slope": 0}
    n    = min(len(prices), len(volumes))
    p, v = prices[-n:], volumes[-n:]
    obv  = [0.0]
    for i in range(1, len(p)):
        obv.append(obv[-1] + v[i] if p[i] > p[i-1] else
                   obv[-1] - v[i] if p[i] < p[i-1] else obv[-1])
    recent = obv[-10:]
    slope  = (recent[-1] - recent[0]) / (abs(recent[0]) + 1)
    return {"rising": slope > 0, "obv_slope": round(slope, 4)}

def compute_williams_r(prices: list, period: int = 14) -> dict:
    if len(prices) < period:
        return {"value": -50, "oversold": False}
    recent = prices[-period:]
    high, low, close = max(recent), min(recent), prices[-1]
    wr_val = ((high - close) / (high - low) * -100) if (high - low) > 0 else -50
    return {"value": round(wr_val, 2), "oversold": wr_val < -80}

def compute_cci(prices: list, period: int = 20) -> dict:
    if len(prices) < period:
        return {"value": 0, "oversold": False}
    recent   = prices[-period:]
    tp_mean  = sum(recent) / len(recent)
    mean_dev = sum(abs(p - tp_mean) for p in recent) / len(recent)
    cci_val  = (recent[-1] - tp_mean) / (0.015 * mean_dev) if mean_dev > 0 else 0
    return {"value": round(cci_val, 2), "oversold": cci_val < -100}

def check_higher_lows(prices: list, lookback: int = 20) -> bool:
    if len(prices) < lookback:
        return False
    recent = prices[-lookback:]
    lows   = [recent[i] for i in range(1, len(recent)-1)
              if recent[i] <= recent[i-1] and recent[i] <= recent[i+1]]
    return len(lows) >= 2 and lows[-1] > lows[-2]

# V2.3: ADX regime filter + volume confirmation. Was claimed live in the
# V2.2 boot log ("ADX regime filter | Vol confirmation") but never actually
# implemented -- only existed in phase4_backtester.py, which is what the
# 298/321-win backtest evidence cited in this file's header was measuring.
# Ported directly from phase4_backtester.py's compute_adx/
# check_volume_confirmation, unchanged, so live finally matches what was
# already backtested instead of drifting from it.
ADX_TREND        = 20.0     # ADX < 20 = ranging = SCALP only
VOL_CONFIRM_MULT = 1.2      # volume gate

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

# ── Context refresh thread ────────────────────────────────────────────────────
def refresh_context_data():
    all_underlyings = ["SMH", "GDX", "XBI", "QQQ", "^VIX"]
    while True:
        try:
            spy_p, _ = fetch_prices_and_volumes("SPY", 40)
            qqq_p, _ = fetch_prices_and_volumes("QQQ", 40)
            with _context_lock:
                if spy_p: _spy_prices[:] = spy_p
                if qqq_p: _qqq_prices[:] = qqq_p
            for sym in all_underlyings:
                p1, _ = fetch_prices_and_volumes(sym, 40, "1m")
                p5, _ = fetch_prices_and_volumes(sym, 60, "5m")
                with _context_lock:
                    if p1:
                        if sym == "^VIX":
                            globals()["_vix_price"] = p1[-1]
                        else:
                            _underlying_prices[sym] = p1
                    if p5:
                        _underlying_5m[sym] = p5
        except Exception:
            pass
        time.sleep(30)

def get_spy_context() -> dict:
    with _context_lock:
        prices = list(_spy_prices)
    if len(prices) < 21:
        return {"bullish": False, "strong": False, "overbought": False,
                "momentum": 0, "rsi": 50, "above_ma20": False}
    rsi        = compute_rsi(prices) or 50
    ma20       = compute_ma(prices, 20) or prices[-1]
    momentum   = (prices[-1] - prices[-6]) / prices[-6] if prices[-6] > 0 else 0
    above_ma20 = prices[-1] > ma20
    bullish    = above_ma20 and momentum > 0
    strong     = above_ma20 and momentum > 0.005
    return {
        "bullish": bullish, "strong": strong,
        "overbought": rsi > 72, "momentum": round(momentum * 100, 3),
        "rsi": rsi, "above_ma20": above_ma20,
    }

def get_qqq_context() -> dict:
    with _context_lock:
        prices = list(_qqq_prices)
    if len(prices) < 8:
        return {"rsi": 50, "momentum": 0, "overbought": False, "oversold": False}
    rsi      = compute_rsi(prices) or 50
    momentum = (prices[-1] - prices[-6]) / prices[-6] if len(prices) >= 6 and prices[-6] > 0 else 0
    return {
        "rsi": rsi, "momentum": round(momentum * 100, 3),
        "overbought": rsi > 68, "oversold": rsi < 35,
    }

def get_vix() -> float:
    with _context_lock:
        return _vix_price

def get_underlying_context(underlying: str) -> dict:
    with _context_lock:
        prices_1m = list(_underlying_prices.get(underlying, []))
        prices_5m = list(_underlying_5m.get(underlying, []))
    result = {
        "available": False, "rsi_1m": 50, "above_ema20_1m": False,
        "trending_up_1m": False, "rsi_5m": 50, "above_ema20_5m": False,
        "trending_up_5m": False, "at_high": False, "vol_expanding": False,
        "tide_bullish": False, "tide_bearish": False,
    }
    if len(prices_1m) >= 21:
        rsi_1m      = compute_rsi(prices_1m) or 50
        ema20_1m    = compute_ema(prices_1m, 20) or prices_1m[-1]
        momentum_1m = (prices_1m[-1] - prices_1m[-6]) / prices_1m[-6] if len(prices_1m) >= 6 and prices_1m[-6] > 0 else 0
        result.update({
            "available": True, "rsi_1m": rsi_1m,
            "above_ema20_1m": prices_1m[-1] > ema20_1m,
            "trending_up_1m": momentum_1m > 0,
        })
    if len(prices_5m) >= 21:
        rsi_5m      = compute_rsi(prices_5m) or 50
        ema20_5m    = compute_ema(prices_5m, 20) or prices_5m[-1]
        momentum_5m = (prices_5m[-1] - prices_5m[-6]) / prices_5m[-6] if len(prices_5m) >= 6 and prices_5m[-6] > 0 else 0
        hl_5m       = check_higher_lows(prices_5m, 15)
        result.update({
            "rsi_5m": rsi_5m,
            "above_ema20_5m": prices_5m[-1] > ema20_5m,
            "trending_up_5m": momentum_5m > 0 and hl_5m,
        })
        if len(prices_5m) >= 30:
            recent_high   = max(prices_5m[-30:])
            result["at_high"] = prices_5m[-1] >= recent_high * 0.995
    if result["available"]:
        result["tide_bullish"] = (
            result["above_ema20_1m"] and result["trending_up_1m"] and
            result.get("rsi_1m", 50) < 72
        )
        result["tide_bearish"] = (
            not result["above_ema20_1m"] and not result["trending_up_1m"]
        )
    return result

# ── Analyst bridge ────────────────────────────────────────────────────────────
def fetch_analyst_scores() -> dict:
    global _analyst_scores_cache, _analyst_scores_ts
    now = time.time()
    with _analyst_lock:
        if now - _analyst_scores_ts < _analyst_scores_ttl and _analyst_scores_cache:
            return dict(_analyst_scores_cache)
    if not ANALYST_URL:
        return {}
    try:
        headers = {"X-Nexus-Token": NEXUS_TOKEN} if NEXUS_TOKEN else {}
        res     = requests.get(f"{ANALYST_URL}/scores", headers=headers, timeout=5)
        if res.status_code == 200:
            data = res.json()
            with _analyst_lock:
                _analyst_scores_cache = data
                _analyst_scores_ts    = now
            return data
    except Exception:
        pass
    with _analyst_lock:
        return dict(_analyst_scores_cache)

def get_analyst_signal_boost(symbol: str, analyst_scores: dict) -> tuple:
    sym_data = analyst_scores.get(symbol)
    if not sym_data:
        return 0, []
    signals = sym_data.get("signals", [])
    for combo_pair in SIGNAL_COMBO_BOOST_SYMBOLS.get(symbol, []):
        sig_a, sig_b = combo_pair
        a = any(sig_a.lower() in s.lower() for s in signals)
        b = any(sig_b.lower() in s.lower() for s in signals)
        if a and b:
            return 2, signals
        if a or b:
            return 1, signals
    return 0, signals

# ── Phase4Memory ─────────────────────────────────────────────────────────────
class Phase4Memory:
    def __init__(self, db_url: str):
        self.db_url           = db_url
        self._conn            = None
        self._lock            = threading.Lock()
        self._win_rates       = {}
        self._last_analysis   = 0.0
        self._enabled         = bool(db_url) and _db_available

    def _get_conn(self):
        if not self._enabled:
            return None
        try:
            if self._conn is None or self._conn.closed:
                self._conn = psycopg2.connect(self.db_url)
                self._conn.autocommit = False
            return self._conn
        except Exception as e:
            print(f"[PM] DB connect error: {e}", flush=True)
            return None

    def init_tables(self):
        if not self._enabled:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS phase4_trade_fingerprints (
            id              SERIAL PRIMARY KEY,
            trade_id        VARCHAR(32) UNIQUE NOT NULL,
            symbol          VARCHAR(10) NOT NULL,
            bear_pair       VARCHAR(10),
            is_bear_trade   BOOLEAN DEFAULT FALSE,
            mode            VARCHAR(12),
            entry_ts        BIGINT,
            exit_ts         BIGINT,
            entry_price     REAL,
            symbol_rsi      REAL,
            spy_rsi         REAL,
            qqq_rsi         REAL,
            spy_bullish     BOOLEAN,
            spy_momentum    REAL,
            qqq_overbought  BOOLEAN,
            higher_lows     BOOLEAN,
            above_ma20      BOOLEAN,
            bb_squeeze      BOOLEAN,
            stochrsi_oversold BOOLEAN,
            macd_bullish    BOOLEAN,
            obv_rising      BOOLEAN,
            hour_cdt        INTEGER,
            day_of_week     INTEGER,
            analyst_score   INTEGER,
            signal_boost    INTEGER,
            entry_score     INTEGER,
            reversal_quality INTEGER,
            underlying_tide  BOOLEAN,
            vix_at_entry    REAL,
            won             BOOLEAN,
            pnl_pct         REAL,
            exit_reason     VARCHAR(50),
            hold_time_min   INTEGER,
            mfe             REAL,
            mae             REAL,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_p4fp_symbol ON phase4_trade_fingerprints(symbol);
        CREATE INDEX IF NOT EXISTS idx_p4fp_won    ON phase4_trade_fingerprints(won);
        CREATE TABLE IF NOT EXISTS phase4_pattern_stats (
            id           SERIAL PRIMARY KEY,
            bucket_key   VARCHAR(200) UNIQUE NOT NULL,
            win_rate     REAL NOT NULL,
            sample_count INTEGER NOT NULL,
            avg_pnl      REAL,
            last_updated TIMESTAMPTZ DEFAULT NOW()
        );
        """
        alter_ddl = """
        ALTER TABLE phase4_trade_fingerprints
            ADD COLUMN IF NOT EXISTS entry_score      INTEGER,
            ADD COLUMN IF NOT EXISTS reversal_quality INTEGER,
            ADD COLUMN IF NOT EXISTS underlying_tide  BOOLEAN,
            ADD COLUMN IF NOT EXISTS vix_at_entry     REAL;
        """
        try:
            with self._lock:
                conn = self._get_conn()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(ddl)
                        cur.execute(alter_ddl)
                    conn.commit()
                    print("[PM] Phase4 pattern memory tables ready (V2.0)", flush=True)
        except Exception as e:
            print(f"[PM] init_tables error: {e}", flush=True)

    def record_entry(self, trade_id, symbol, bear_pair, is_bear, mode,
                     entry_price, sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
                     analyst_score=0, signal_boost=0, entry_score=0,
                     reversal_quality=0, underlying_tide=False, vix=15.0):
        if not self._enabled:
            return
        threading.Thread(target=self._write_entry, daemon=True, args=(
            trade_id, symbol, bear_pair, is_bear, mode, entry_price,
            sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
            analyst_score, signal_boost, entry_score, reversal_quality,
            underlying_tide, vix
        )).start()

    def _write_entry(self, trade_id, symbol, bear_pair, is_bear, mode,
                     entry_price, sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
                     analyst_score, signal_boost, entry_score, reversal_quality,
                     underlying_tide, vix):
        now = datetime.now(tz=CENTRAL)
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO phase4_trade_fingerprints
                        (trade_id, symbol, bear_pair, is_bear_trade, mode,
                         entry_ts, entry_price,
                         symbol_rsi, spy_rsi, qqq_rsi,
                         spy_bullish, spy_momentum, qqq_overbought,
                         higher_lows, above_ma20,
                         bb_squeeze, stochrsi_oversold, macd_bullish, obv_rising,
                         hour_cdt, day_of_week,
                         analyst_score, signal_boost,
                         entry_score, reversal_quality, underlying_tide, vix_at_entry)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        trade_id, symbol, bear_pair, is_bear, mode,
                        int(time.time()), entry_price,
                        sym_rsi, spy_ctx.get("rsi"), qqq_ctx.get("rsi"),
                        spy_ctx.get("bullish"), spy_ctx.get("momentum"), qqq_ctx.get("overbought"),
                        sym_ctx.get("higher_lows"), sym_ctx.get("above_ma20"),
                        sym_ctx.get("bb_squeeze"), sym_ctx.get("stochrsi_oversold"),
                        sym_ctx.get("macd_bullish"), sym_ctx.get("obv_rising"),
                        now.hour, now.weekday(),
                        analyst_score, signal_boost,
                        entry_score, reversal_quality, underlying_tide, vix,
                    ))
                conn.commit()
        except Exception as e:
            print(f"[PM] write_entry error {trade_id}: {e}", flush=True)

    def record_exit(self, trade_id, won, pnl_pct, exit_reason, hold_min,
                    mfe=0.0, mae=0.0):
        if not self._enabled:
            return
        threading.Thread(target=self._write_exit, daemon=True, args=(
            trade_id, won, pnl_pct, exit_reason, hold_min, mfe, mae
        )).start()

    def _write_exit(self, trade_id, won, pnl_pct, exit_reason, hold_min, mfe, mae):
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE phase4_trade_fingerprints
                        SET won=%s, pnl_pct=%s, exit_reason=%s,
                            hold_time_min=%s, exit_ts=%s, mfe=%s, mae=%s
                        WHERE trade_id=%s
                    """, (won, round(pnl_pct * 100, 3), exit_reason,
                          hold_min, int(time.time()),
                          round(mfe * 100, 3), round(mae * 100, 3), trade_id))
                conn.commit()
        except Exception as e:
            print(f"[PM] write_exit error {trade_id}: {e}", flush=True)

    def run_analysis(self):
        if not self._enabled:
            return
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT symbol, is_bear_trade, mode, symbol_rsi,
                               spy_bullish, qqq_overbought, hour_cdt,
                               won, pnl_pct
                        FROM phase4_trade_fingerprints WHERE won IS NOT NULL
                    """)
                    rows = cur.fetchall()

            if len(rows) < PM_MIN_TRADES:
                return

            from collections import defaultdict
            buckets  = defaultdict(list)
            pnl_bkts = defaultdict(list)

            for row in rows:
                key = Phase4Memory._bucket_key(
                    row["symbol"], row["is_bear_trade"], row["mode"] or "SCALP",
                    row["symbol_rsi"] if row["symbol_rsi"] is not None else 99,
                    {"bullish": row["spy_bullish"]},
                    {"overbought": row["qqq_overbought"]},
                    row["hour_cdt"] if row["hour_cdt"] is not None else 12,
                )
                buckets[key].append(bool(row["won"]))
                if row["pnl_pct"] is not None:
                    pnl_bkts[key].append(float(row["pnl_pct"]))

            new_cache = {}
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    for key, outcomes in buckets.items():
                        if len(outcomes) < PM_MIN_BUCKET_TRADES:
                            continue
                        wr      = sum(outcomes) / len(outcomes)
                        avg_pnl = sum(pnl_bkts[key]) / len(pnl_bkts[key]) if pnl_bkts[key] else None
                        cur.execute("""
                            INSERT INTO phase4_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                            VALUES (%s,%s,%s,%s)
                            ON CONFLICT (bucket_key) DO UPDATE
                            SET win_rate=EXCLUDED.win_rate, sample_count=EXCLUDED.sample_count,
                                avg_pnl=EXCLUDED.avg_pnl, last_updated=NOW()
                        """, (key, wr, len(outcomes), avg_pnl))
                        new_cache[key] = wr
                conn.commit()

            self._win_rates     = new_cache
            self._last_analysis = time.time()
            total = len(rows)
            wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
            print(f"[PM] Analysis: {len(new_cache)} buckets | {total} trades | {wr:.1%} WR", flush=True)
        except Exception as e:
            print(f"[PM] analysis error: {e}", flush=True)

    @staticmethod
    def _bucket_key(symbol, is_bear, mode, sym_rsi, spy_ctx, qqq_ctx, hour):
        rsi_b  = ("rsi_lt30" if sym_rsi < 30 else "rsi_30_40" if sym_rsi < 40 else
                  "rsi_40_55" if sym_rsi < 55 else "rsi_gt55")
        spy_b  = "spy_bull" if spy_ctx.get("bullish") else "spy_bear"
        qqq_b  = "qqq_ob"   if qqq_ctx.get("overbought") else "qqq_ok"
        hr_b   = "hr_open"  if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
        return f"{symbol}|{'bear' if is_bear else 'bull'}|{mode or 'SCALP'}|{rsi_b}|{spy_b}|{qqq_b}|{hr_b}"

    def should_skip_entry(self, symbol, is_bear, mode, sym_rsi, spy_ctx, qqq_ctx, hour):
        key = self._bucket_key(symbol, is_bear, mode, sym_rsi, spy_ctx, qqq_ctx, hour)
        if key not in self._win_rates:
            return False, 0.5, False
        wr = self._win_rates[key]
        return (wr < WIN_RATE_GATE_THRESHOLD), wr, True

    def start_scheduler(self):
        def _run():
            time.sleep(300)
            self.run_analysis()
            while True:
                time.sleep(PM_ANALYSIS_INTERVAL)
                self.run_analysis()
        threading.Thread(target=_run, daemon=True, name="p4-pattern-memory").start()


# ── SymbolBot ─────────────────────────────────────────────────────────────────
class SymbolBot:
    def __init__(self, symbol: str, config: dict):
        self.symbol      = symbol
        self.bear_pair   = config["bear_pair"]
        self.underlying  = config["underlying"]
        self.budget_pct  = config["budget_pct"]
        self.cfg         = config

        self.prices:       list  = []
        self.volumes:      list  = []
        self.bear_prices:  list  = []
        self.bear_volumes: list  = []
        self.peak_price:   float = 0.0
        self.entry_price:  float = 0.0
        self.entry_time:   float = 0.0
        self.trade_id:     str   = ""
        self.mfe:          float = 0.0
        self.mae:          float = 0.0
        self.in_position:  bool  = False
        self.active_sym:   str   = symbol
        self.mode:         str   = "SCALP"
        self.cooldown_until: float = 0.0

        self._bear_ext_trailing:   bool  = False
        self._bear_ext_peak:       float = 0.0
        self._late_ratchet_active: bool  = False

        self._entry_spy_ctx:       dict  = {}
        self._entry_qqq_ctx:       dict  = {}
        self._entry_sym_ctx:       dict  = {}
        self._entry_rsi:           float = 50.0
        self._entry_analyst_score: int   = 0
        self._entry_signal_boost:  int   = 0
        self._entry_score:         int   = 0
        self._entry_rev_quality:   int   = 0
        self._entry_tide:          bool  = False
        self._entry_vix:           float = 15.0

        self.reversal_state: dict = {"state": "IDLE"}
        self.daily_wins:   int   = 0
        self.daily_losses: int   = 0
        self.daily_pnl:    float = 0.0

    def is_on_cooldown(self) -> bool:
        return time.time() < self.cooldown_until

    def set_cooldown(self, secs: int):
        self.cooldown_until = time.time() + secs

    def refresh_prices(self):
        p, v = fetch_prices_and_volumes(self.symbol, WARMUP_BARS + 5)
        if p:
            self.prices  = p
            self.volumes = v
        bp, bv = fetch_prices_and_volumes(self.bear_pair, WARMUP_BARS + 5)
        if bp:
            self.bear_prices  = bp
            self.bear_volumes = bv

    def get_signal_suite(self, prices: list, volumes: list) -> dict:
        if len(prices) < 21:
            return {
                "rsi": 50, "rsi14": 50, "rsi21": 50,
                "ma20": 0, "above_ma20": True, "below_ma20": False,
                "trend_10bar": 0, "higher_lows": False, "bouncing": False,
                "ema9": 0, "ema21": 0, "ema9_above_ema21": False,
                "bb": {}, "bb_squeeze": False, "near_lower_bb": False,
                "at_lower_bb": False, "far_below_bb": False,
                "stochrsi": {}, "stochrsi_oversold": False,
                "macd": {}, "macd_bullish": False,
                "obv": {}, "obv_rising": False, "obv_falling": False,
                "williams_r": {}, "williams_oversold": False,
                "cci": {}, "cci_oversold": False,
                "rsi_lt40": False, "rsi_lt25": False,
                "rsi14_lt35": False, "rsi14_lt20": False, "rsi21_lt45": False,
                "vol_confirmed": True, "adx": 25.0,
            }
        rsi7   = compute_rsi(prices, 7)  or 50
        rsi14  = compute_rsi(prices, 14) or 50
        rsi21  = compute_rsi(prices, 21) or 50
        ma20   = compute_ma(prices, 20)  or prices[-1]
        ema9   = compute_ema(prices, 9)  or prices[-1]
        ema21v = compute_ema(prices, 21) or prices[-1]
        trend10       = (prices[-1] - prices[-11]) / prices[-11] if len(prices) > 11 and prices[-11] > 0 else 0
        above_ma20    = prices[-1] > ma20
        ema9_above_21 = ema9 > ema21v
        higher_l      = check_higher_lows(prices)
        bouncing      = len(prices) >= 3 and prices[-1] > prices[-3]
        bb       = compute_bollinger(prices)
        stochrsi = compute_stochrsi(prices)
        macd     = compute_macd(prices)
        obv      = compute_obv(prices, volumes) if volumes else {"rising": False, "obv_slope": 0}
        williams = compute_williams_r(prices)
        cci      = compute_cci(prices)
        adx      = compute_adx(prices)
        vol_ok   = check_volume_confirmation(volumes) if volumes else True
        return {
            "rsi": rsi7, "rsi14": rsi14, "rsi21": rsi21,
            "ma20": ma20, "above_ma20": above_ma20, "below_ma20": not above_ma20,
            "trend_10bar": round(trend10 * 100, 3),
            "higher_lows": higher_l, "bouncing": bouncing,
            "ema9": ema9, "ema21": ema21v, "ema9_above_ema21": ema9_above_21,
            "bb": bb, "bb_squeeze": bb.get("squeeze", False),
            "near_lower_bb": bb.get("near_lower", False),
            "at_lower_bb":   bb.get("at_lower", False),
            "far_below_bb":  bb.get("far_below", False),
            "stochrsi": stochrsi, "stochrsi_oversold": stochrsi.get("oversold", False),
            "macd": macd,         "macd_bullish":      macd.get("bullish", False),
            "obv": obv,           "obv_rising":        obv.get("rising", False),
            "obv_falling": not obv.get("rising", True),
            "williams_r": williams, "williams_oversold": williams.get("oversold", False),
            "cci": cci,           "cci_oversold":       cci.get("oversold", False),
            "rsi_lt40":   rsi7  < 40, "rsi_lt25":   rsi7  < 25,
            "rsi14_lt35": rsi14 < 35, "rsi14_lt20": rsi14 < 20,
            "rsi21_lt45": rsi21 < 45,
            "vol_confirmed": vol_ok, "adx": adx,
        }

    def compute_entry_score(self, sym: str, sym_ctx: dict) -> int:
        cfg   = self.cfg if sym == self.symbol else BEAR_RECIPES.get(sym, self.cfg)
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

    def select_mode(self, spy_ctx: dict, sym_ctx: dict) -> str:
        # V2.3: ranging market (ADX < 20) forces SCALP regardless of SPY
        # context -- checked first, matching phase4_backtester.py's order.
        if sym_ctx.get("adx", 25.0) < ADX_TREND:
            return "SCALP"
        if spy_ctx.get("overbought"):
            return "SCALP"
        if (spy_ctx.get("strong") and sym_ctx.get("trend_10bar", 0) > 0.3 and
                sym_ctx.get("higher_lows") and sym_ctx.get("above_ma20")):
            return "EXTENDED"
        if spy_ctx.get("bullish") and (sym_ctx.get("above_ma20") or sym_ctx.get("trend_10bar", 0) > 0.1):
            return "RIDE"
        return "SCALP"

    def get_exit_params(self) -> tuple:
        if self.active_sym == self.bear_pair:
            br      = BEAR_RECIPES.get(self.bear_pair, self.cfg)
            sl      = br["atr_stop"]
            early_r = br["early_ratchet"]
            late_r  = early_r * 1.8
            trail_n = br["trail"]
            trail_t = round(trail_n * 0.7, 4)
            return sl, early_r, late_r, trail_n, trail_t
        sl      = self.cfg["atr_stop"]
        early_r = self.cfg["early_ratchet"]
        late_r  = self.cfg["late_ratchet"]
        trail_n = self.cfg["trail_normal"]
        trail_t = self.cfg["trail_tight"]
        mult    = (self.cfg.get("ride_stop_mult", 1.3) if self.mode == "RIDE" else
                   self.cfg.get("ext_stop_mult",  1.8) if self.mode == "EXTENDED" else 1.0)
        return round(sl * mult, 4), early_r, late_r, trail_n, trail_t

    def should_enter_bull(self, spy_ctx: dict, sym_ctx: dict, underlying_ctx: dict) -> tuple:
        now = datetime.now(tz=CENTRAL)
        if now.hour in self.cfg.get("avoid_hours", []):
            return False, 0, "avoid_hour"
        if now.weekday() in self.cfg.get("avoid_days", []):
            return False, 0, "avoid_day"
        vix = get_vix()
        if vix >= VIX_PAUSE:
            return False, 0, f"vix_pause({vix:.1f})"
        if self.daily_pnl <= MAX_DAILY_LOSS_PCT:
            return False, 0, f"daily_loss_limit({self.daily_pnl:.2f}%)"
        if not sym_ctx.get("bouncing"):
            return False, 0, "no_bounce"
        if underlying_ctx.get("available") and underlying_ctx.get("tide_bearish"):
            return False, 0, "tide_bearish"
        if not sym_ctx.get("vol_confirmed", True):
            return False, 0, "vol_not_confirmed"
        score  = self.compute_entry_score(self.symbol, sym_ctx)
        min_sc = self.cfg["min_score"]
        if score < min_sc:
            return False, score, f"score_{score}<{min_sc}"
        return True, score, "ok"

    def score_reversal_quality(self, bull_rsi: float, drop: float, bear_ctx: dict) -> int:
        score = 0
        if bull_rsi >= REVERSAL_HIGH_RSI:
            score += 2
        elif bull_rsi >= REVERSAL_OB_RSI:
            score += 1
        if drop >= REVERSAL_HIGH_DROP:
            score += 1
        if bear_ctx.get("rsi", 50) < 45:
            score += 1
        if bear_ctx.get("obv_rising"):
            score += 1
        underlying_ctx = get_underlying_context(
            BEAR_RECIPES.get(self.bear_pair, {}).get("underlying", "QQQ"))
        if underlying_ctx.get("at_high") and underlying_ctx.get("tide_bullish"):
            score -= 2
        return max(0, min(3, score))

    def check_reversal(self) -> tuple:
        if self.daily_pnl <= MAX_DAILY_LOSS_PCT:
            return False, 0
        if len(self.prices) < 8:
            return False, 0
        bull_rsi = compute_rsi(self.prices)
        if bull_rsi is None:
            return False, 0
        state = self.reversal_state
        now_t = time.time()
        if state["state"] == "IDLE":
            if bull_rsi >= REVERSAL_OB_RSI:
                self.reversal_state = {"state": "WATCHING", "bull_peak": self.prices[-1], "watch_start": now_t}
                log(self.symbol, f"👁 REVERSAL WATCH -> {self.bear_pair} | RSI={bull_rsi:.1f}")
            return False, 0
        if state["state"] == "WATCHING":
            if now_t - state.get("watch_start", now_t) > REVERSAL_MAX_WATCH:
                self.reversal_state = {"state": "IDLE"}
                return False, 0
            if bull_rsi < REVERSAL_RSI_RESET:
                self.reversal_state = {"state": "IDLE"}
                return False, 0
            bull_peak = max(state.get("bull_peak", self.prices[-1]), self.prices[-1])
            self.reversal_state["bull_peak"] = bull_peak
            drop = (bull_peak - self.prices[-1]) / bull_peak if bull_peak > 0 else 0
            if drop >= REVERSAL_CONFIRM:
                if len(self.bear_prices) < 3 or self.bear_prices[-1] <= self.bear_prices[-3]:
                    return False, 0
                now_hour   = datetime.now(tz=CENTRAL).hour
                bear_avoid = BEAR_RECIPES.get(self.bear_pair, {}).get("avoid_hours", [])
                if now_hour in bear_avoid:
                    return False, 0
                if self.bear_pair == "SQQQ" and not SQQQ_ENABLED:
                    return False, 0
                qqq_ctx = get_qqq_context()
                if qqq_ctx.get("oversold"):
                    return False, 0
                gate = QQQ_BEAR_RSI_GATE_LABD if self.bear_pair == "LABD" else QQQ_BEAR_RSI_GATE
                if qqq_ctx.get("rsi", 50) < gate:
                    return False, 0
                bear_ctx   = self.get_signal_suite(self.bear_prices, self.bear_volumes)
                bear_min   = BEAR_RECIPES.get(self.bear_pair, {}).get("min_score", 4)
                bear_score = self.compute_entry_score(self.bear_pair, bear_ctx)
                if bear_score < bear_min:
                    return False, 0
                if not bear_ctx.get("vol_confirmed", True):
                    return False, 0
                quality = self.score_reversal_quality(bull_rsi, drop, bear_ctx)
                if quality == 0:
                    return False, 0
                log(self.symbol,
                    f"🔁 REVERSAL -> {self.bear_pair} | drop={round(drop*100,2)}% | "
                    f"bull_rsi={bull_rsi:.1f} | bear_score={bear_score} | quality={quality}")
                self.reversal_state = {"state": "IDLE"}
                return True, quality
        return False, 0

    def try_buy(self, sym: str, prices: list, volumes: list,
                spy_ctx: dict, sym_ctx: dict, reversal_quality: int = 0) -> bool:
        bp        = get_buying_power()
        base_size = round(bp * self.budget_pct, 2)
        if base_size < 1.00:
            return False

        is_bear     = (sym == self.bear_pair)
        entry_score = self.compute_entry_score(sym, sym_ctx)
        self.mode   = self.select_mode(spy_ctx, sym_ctx)

        analyst_scores = fetch_analyst_scores()
        analyst_entry  = analyst_scores.get(sym, {})
        analyst_score  = analyst_entry.get("score", 0)
        signal_boost, _ = get_analyst_signal_boost(sym, analyst_scores)

        underlying     = self.cfg["underlying"] if not is_bear else BEAR_RECIPES.get(sym, {}).get("underlying", "QQQ")
        underlying_ctx = get_underlying_context(underlying)
        tide_bullish   = underlying_ctx.get("tide_bullish", False)
        vix            = get_vix()
        qqq_ctx        = get_qqq_context()

        if _phase4_memory:
            hour = datetime.now(tz=CENTRAL).hour
            skip, wr, has_data = _phase4_memory.should_skip_entry(
                self.symbol, is_bear, self.mode, sym_ctx.get("rsi", 50),
                spy_ctx, qqq_ctx, hour
            )
            if skip:
                log(self.symbol, f"🚫 WIN-RATE GATE: {sym} historical WR={wr:.0%}")
                return False

        size_mult = 1.0 if signal_boost == 2 else 0.8 if signal_boost == 1 else 0.6
        if reversal_quality == 3:
            size_mult = min(1.25, size_mult + 0.25)
        elif reversal_quality == 1:
            size_mult *= 0.5
        if vix >= VIX_CAUTION:
            size_mult *= 0.5
            log(self.symbol, f"⚠ VIX={vix:.1f} — reducing size 50%")

        trade_size = round(base_size * size_mult, 2)
        if trade_size < 1.00:
            return False

        # V2.2: Cross-service capital coordination -- Berserker (main.py)
        # trades against this SAME Alpaca account from a separate process.
        # Clamp our intended spend against what's actually available once
        # Berserker's outstanding reservations (if any) are accounted for.
        # Fails open: if the coordinator can't reach the DB, available falls
        # back to bp (raw buying power) unchanged.
        if _capital_coordinator:
            available = _capital_coordinator.get_available(bp)
            if trade_size > available:
                if available < 1.00:
                    log(self.symbol, f"💰 CAPITAL COORD: ${available:.2f} available "
                        f"(Berserker holding the rest) — skipping {sym}")
                    return False
                log(self.symbol, f"💰 CAPITAL COORD: trimmed ${trade_size:.2f} -> "
                    f"${available:.2f} (Berserker reservation active)")
                trade_size = round(available, 2)

        price = prices[-1] if prices else get_current_price(sym)
        if not price or price <= 0:
            return False

        boost_label = " 🔥COMBO" if signal_boost == 2 else " ✨sig" if signal_boost == 1 else ""
        tide_label  = " 🌊TIDE"  if tide_bullish else ""
        log(self.symbol,
            f"📊 BUY signal | score={entry_score} | mode={self.mode} | "
            f"RSI={sym_ctx['rsi']:.1f} | size_mult={size_mult:.0%} | "
            f"analyst={analyst_score}{boost_label}{tide_label}")

        # V2.2: Reserve against the shared account immediately before
        # submitting, release immediately after -- closes the race window
        # where Berserker (separate process, same Alpaca account) could read
        # stale buying_power between now and Alpaca settling this order.
        _res_id = _capital_coordinator.reserve(trade_size, symbol=sym) if _capital_coordinator else None
        try:
            success = place_order(sym, "BUY", trade_size)
        finally:
            if _capital_coordinator:
                _capital_coordinator.release(_res_id)
        if success:
            import secrets
            self.in_position           = True
            self.active_sym            = sym
            self.entry_price           = price
            self.peak_price            = price
            self.entry_time            = time.time()
            self.mfe                   = 0.0
            self.mae                   = 0.0
            self.trade_id              = secrets.token_hex(8)
            self._entry_spy_ctx        = spy_ctx
            self._entry_qqq_ctx        = qqq_ctx
            self._entry_sym_ctx        = sym_ctx
            self._entry_rsi            = sym_ctx.get("rsi", 50)
            self._entry_analyst_score  = analyst_score
            self._entry_signal_boost   = signal_boost
            self._entry_score          = entry_score
            self._entry_rev_quality    = reversal_quality
            self._entry_tide           = tide_bullish
            self._entry_vix            = vix
            self._bear_ext_trailing    = False
            self._bear_ext_peak        = price
            self._late_ratchet_active  = False

            if _phase4_memory:
                _phase4_memory.record_entry(
                    self.trade_id, self.symbol, self.bear_pair,
                    is_bear, self.mode, price,
                    self._entry_rsi, spy_ctx, qqq_ctx, sym_ctx,
                    analyst_score, signal_boost,
                    entry_score, reversal_quality, tide_bullish, vix
                )

            log(self.symbol,
                f"⚡ BUY: {sym} | ${trade_size:.2f} notional @ ~${round(price,2)} | "
                f"mode={self.mode}{boost_label}")
            alert(
                f"⚡ PHASE4 BUY [{self.mode}]: {sym} | ${trade_size:.2f} @ ~${round(price,2)}"
                f"\nscore={entry_score} | boost={signal_boost} | vix={vix:.1f}{boost_label}"
            )
            return True
        return False

    def try_sell(self, reason: str, pnl_pct: float) -> bool:
        success = place_sell_all(self.active_sym)
        if success:
            emoji    = "✅" if pnl_pct > 0 else "🛑"
            pnl_s    = f"+{round(pnl_pct*100,3)}%" if pnl_pct > 0 else f"{round(pnl_pct*100,3)}%"
            hold_min = int((time.time() - self.entry_time) / 60) if self.entry_time > 0 else 0
            log(self.symbol,
                f"{emoji} SELL [{reason}]: {self.active_sym} | P&L: {pnl_s} | "
                f"MFE: {round(self.mfe*100,2):+.2f}% | MAE: {round(self.mae*100,2):+.2f}% | "
                f"held: {hold_min}m")
            alert(f"{emoji} PHASE4 [{reason}]: {self.active_sym} | {pnl_s} | {hold_min}m")

            if _phase4_memory and self.trade_id:
                _phase4_memory.record_exit(
                    self.trade_id, pnl_pct > 0, pnl_pct,
                    reason, hold_min, self.mfe, self.mae
                )

            self.in_position          = False
            self.peak_price           = 0.0
            self.entry_price          = 0.0
            self.entry_time           = 0.0
            self.trade_id             = ""
            self.mfe                  = 0.0
            self.mae                  = 0.0
            self._bear_ext_trailing   = False
            self._bear_ext_peak       = 0.0
            self._late_ratchet_active = False

            if pnl_pct > 0:
                self.daily_wins += 1
                self.set_cooldown(WIN_COOLDOWN_SECS)
            else:
                self.daily_losses += 1
                self.set_cooldown(LOSS_COOLDOWN_SECS)
            self.daily_pnl += pnl_pct * 100
            return True
        return False

    def recover_position(self):
        positions = get_all_positions()
        for sym in [self.symbol, self.bear_pair]:
            if sym in positions:
                import secrets
                pos              = positions[sym]
                cost             = float(pos.avg_entry_price or 0)
                self.in_position = True
                self.active_sym  = sym
                self.entry_price = cost
                self.peak_price  = max(cost, get_current_price(sym) or cost)
                self.trade_id    = secrets.token_hex(8)
                self.entry_time  = time.time()
                self.mfe         = 0.0
                self.mae         = 0.0
                spy_ctx          = get_spy_context()
                sym_ctx          = self.get_signal_suite(self.prices, self.volumes)
                self.mode        = self.select_mode(spy_ctx, sym_ctx)
                self._bear_ext_trailing   = False
                self._bear_ext_peak       = self.peak_price
                self._late_ratchet_active = False
                log(self.symbol,
                    f"🔄 Recovered: {sym} | entry=${cost:.3f} | mode={self.mode}")
                return

    def run_loop(self):
        log(self.symbol,
            f"🚀 Bot online | bear={self.bear_pair} | underlying={self.underlying} | "
            f"budget={int(self.budget_pct*100)}% | min_score={self.cfg['min_score']}")

        # Warmup — keep retrying until we have bars
        for attempt in range(30):
            self.refresh_prices()
            if len(self.prices) >= WARMUP_BARS:
                log(self.symbol, f"✅ Warmed up | {len(self.prices)} bars")
                break
            log(self.symbol, f"⏳ Warming up: {len(self.prices)}/{WARMUP_BARS} bars")
            time.sleep(12)
        else:
            log(self.symbol, f"⚠ Warmup timeout — continuing with {len(self.prices)} bars")

        self.recover_position()

        while True:
            try:
                if not is_market_hours():
                    if self.in_position:
                        log(self.symbol, "📌 Market closed — holding overnight")
                    time.sleep(60)
                    continue

                self.refresh_prices()
                spy_ctx        = get_spy_context()
                sym_ctx        = self.get_signal_suite(self.prices, self.volumes)
                underlying_ctx = get_underlying_context(self.underlying)
                prices         = self.prices

                if not prices:
                    time.sleep(LOOP_INTERVAL)
                    continue

                if self.in_position:
                    active_prices  = self.prices  if self.active_sym == self.symbol else self.bear_prices
                    active_volumes = self.volumes  if self.active_sym == self.symbol else self.bear_volumes
                    active_ctx     = self.get_signal_suite(active_prices, active_volumes)
                    if not active_prices:
                        time.sleep(LOOP_INTERVAL)
                        continue

                    price      = active_prices[-1]
                    profit_pct = (price - self.entry_price) / self.entry_price if self.entry_price > 0 else 0
                    drawdown   = (self.peak_price - price) / self.peak_price if self.peak_price > 0 else 0

                    self.peak_price = max(self.peak_price, price)
                    self.mfe        = max(self.mfe, profit_pct)
                    self.mae        = min(self.mae, profit_pct)

                    sl, early_r, late_r, trail_n, trail_t = self.get_exit_params()

                    log(self.symbol,
                        f"📊 {self.active_sym} | P&L={round(profit_pct*100,2):+.2f}% | "
                        f"peak=${self.peak_price:.3f} | dd={round(drawdown*100,2):.2f}% | "
                        f"mode={self.mode} | rsi={active_ctx.get('rsi',50):.0f}")

                    ext_cfg = BEAR_EXTENDED_TP.get(self.active_sym) if self.active_sym == self.bear_pair else None
                    if ext_cfg:
                        self._bear_ext_peak = max(self._bear_ext_peak, price)
                        if not self._bear_ext_trailing and profit_pct >= ext_cfg["trail_activate"]:
                            self._bear_ext_trailing = True
                            log(self.symbol, f"🎯 {self.active_sym} EXTENDED TP activated at +{profit_pct*100:.1f}%")
                        if self._bear_ext_trailing:
                            ext_dd = (self._bear_ext_peak - price) / self._bear_ext_peak if self._bear_ext_peak > 0 else 0
                            if ext_dd >= ext_cfg["trail_stop"]:
                                self.try_sell("ext-trail", profit_pct)
                                time.sleep(LOOP_INTERVAL)
                                continue
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)
                    else:
                        # V2.1 FIX (Jun 30 2026): ratchet now checked BEFORE
                        # rsi-overbought. See module docstring for the full
                        # rationale and backtest evidence -- in short, the old
                        # order let rsi-overbought cut nearly every winner the
                        # instant profit ticked positive (mean-reversion
                        # entries naturally push RSI toward overbought as the
                        # thesis plays out), so winners almost never reached
                        # early_ratchet. 93% of wins were rsi-overbought exits
                        # averaging ~+0.24%, against stop-losses averaging
                        # ~-1.7% -- a ~7:1 mismatch that made a 73% WR system
                        # net PnL-negative. Stop-loss priority is unchanged.
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)
                        elif profit_pct >= early_r:
                            rsi_now  = active_ctx.get("rsi", 50)
                            obv_flat = not active_ctx.get("obv_rising") and not active_ctx.get("obv_falling")
                            if (profit_pct >= late_r or rsi_now >= 65 or
                                    (obv_flat and profit_pct >= early_r * 1.5)):
                                self._late_ratchet_active = True
                            trail  = trail_t if self._late_ratchet_active else trail_n
                            if drawdown >= trail:
                                reason = "trail-tight" if self._late_ratchet_active else "trail"
                                self.try_sell(reason, profit_pct)
                        elif active_ctx.get("rsi", 50) >= RSI_OVERBOUGHT_EXIT and profit_pct > 0:
                            log(self.symbol, f"🔄 RSI REVERSAL EXIT: RSI={active_ctx['rsi']:.0f}")
                            self.try_sell("rsi-overbought", profit_pct)
                        elif (self.mode == "EXTENDED" and self.active_sym == self.symbol and
                              profit_pct > -0.005 and not active_ctx.get("higher_lows", True)):
                            log(self.symbol, "📉 EXTENDED: trend break")
                            self.try_sell("trend-break", profit_pct)
                        elif self.entry_time > 0:
                            held_min = (time.time() - self.entry_time) / 60
                            if held_min >= DWELL_MINUTES and abs(profit_pct) < DWELL_FLAT_THRESHOLD:
                                log(self.symbol, f"⏱ DWELL EXIT: {held_min:.0f}m | flat at {profit_pct*100:+.3f}%")
                                self.try_sell("dwell", profit_pct)

                elif not self.is_on_cooldown():
                    rev_ok, rev_quality = self.check_reversal()
                    if rev_ok:
                        if not self.bear_prices:
                            log(self.symbol, "⚠ Reversal but bear_prices empty — skip")
                        else:
                            bear_ctx = self.get_signal_suite(self.bear_prices, self.bear_volumes)
                            self.try_buy(self.bear_pair, self.bear_prices, self.bear_volumes,
                                         spy_ctx, bear_ctx, reversal_quality=rev_quality)
                    else:
                        should_enter, score, reason = self.should_enter_bull(
                            spy_ctx, sym_ctx, underlying_ctx)
                        if should_enter:
                            self.try_buy(self.symbol, prices, self.volumes, spy_ctx, sym_ctx)
                        elif score > 0:
                            log(self.symbol,
                                f"⏳ score={score} (need {self.cfg['min_score']}) | {reason}")

            except Exception as e:
                log(self.symbol, f"🔴 Loop error: {e}")
                log(self.symbol, traceback.format_exc())

            time.sleep(LOOP_INTERVAL)


# ── Phase4 Service ────────────────────────────────────────────────────────────
def run():
    global _phase4_memory, _capital_coordinator
    print("[PHASE4] NEXUS PHASE 4 V2.4 STARTING — Alpaca Edition", flush=True)
    print("[PHASE4] Broker: Alpaca | Fractional shares | Real-time IEX feed", flush=True)
    print(f"[PHASE4] Bots: NUGT(30%) | SOXL(25%) | LABU(25%) | TQQQ(20%)", flush=True)
    print(f"[PHASE4] Bear pairs: DUST | SOXS | LABD" + (" | SQQQ" if SQQQ_ENABLED else " | SQQQ(DISABLED)"), flush=True)
    print(f"[PHASE4] V2.4: Capital coordination | Exit priority fix | ADX regime filter (live) | Vol confirmation (live) | Underlying exit | Daily loss limit enforced ({MAX_DAILY_LOSS_PCT}%, per-bot)", flush=True)

    # Auth check
    print(f"[PHASE4] Auth check: API key={'SET (' + ALPACA_API_KEY[:6] + ')' if ALPACA_API_KEY else 'MISSING'}", flush=True)
    print(f"[PHASE4] Auth check: Secret={'SET' if ALPACA_API_SECRET else 'MISSING'}", flush=True)
    print(f"[PHASE4] Alpaca clients ready (paper={PAPER_MODE})", flush=True)

    if DATABASE_URL and _db_available:
        _phase4_memory = Phase4Memory(DATABASE_URL)
        _phase4_memory.init_tables()
        _phase4_memory.start_scheduler()
        print("[PHASE4] Pattern memory: DB connected", flush=True)
    else:
        _phase4_memory = Phase4Memory("")
        print("[PHASE4] Pattern memory: disabled (no DATABASE_URL)", flush=True)

    # V2.2: Capital coordinator -- Phase4 trades against the SAME live Alpaca
    # account as Berserker (main.py, separate Railway service/process) with
    # zero prior coordination. See capital_coordinator.py for full rationale.
    _capital_coordinator = CapitalCoordinator(DATABASE_URL, service_name="phase4")
    _capital_coordinator.init_table()

    # Verify Alpaca account
    if _trade_client:
        try:
            acct = _trade_client.get_account()
            print(f"[PHASE4] Alpaca account: buying_power=${float(acct.buying_power):.2f} | paper={PAPER_MODE}", flush=True)
        except Exception as e:
            print(f"[PHASE4] ⚠ Account check failed: {e}", flush=True)
    else:
        print("[PHASE4] ⚠ No Alpaca trade client — orders disabled", flush=True)

    print(f"[PHASE4] Analyst bridge: {ANALYST_URL if ANALYST_URL else 'disabled'}", flush=True)
    print(f"[PHASE4] VIX caution={VIX_CAUTION} / pause={VIX_PAUSE}", flush=True)

    ctx_thread = threading.Thread(target=refresh_context_data, daemon=True)
    ctx_thread.start()
    print("[PHASE4] Context refresh thread started — waiting 15s for data warmup...", flush=True)
    time.sleep(15)

    spy_status  = "✅" if len(_spy_prices) > 0 else "⚠ EMPTY"
    soxl_status = "✅" if "SMH" in _underlying_prices else "⚠ EMPTY"
    print(f"[PHASE4] Data check: SPY={spy_status} | SOXL={soxl_status}", flush=True)

    bots    = []
    threads = []
    for symbol, config in BOT_CONFIGS.items():
        bot = SymbolBot(symbol, config)
        bots.append(bot)
        t = threading.Thread(target=bot.run_loop, daemon=True, name=f"bot_{symbol}")
        threads.append(t)
        t.start()
        print(f"[PHASE4] ✅ {symbol} bot started (underlying={config['underlying']}, min_score={config['min_score']})", flush=True)
        time.sleep(2)

    from phase4_server import start_server
    start_server(bots)

    vix_now = get_vix()
    alert(
        f"⚡ PHASE4 V2.4 ONLINE — Alpaca Edition\n"
        f"SOXL(SMH) TQQQ(QQQ) NUGT(GDX) LABU(XBI)\n"
        f"VIX: {vix_now:.1f} | SQQQ: {'ON' if SQQQ_ENABLED else 'OFF'}\n"
        f"Analyst: {'✅' if ANALYST_URL else '⚠ disabled'}\n"
        f"V2.4: Daily loss limit enforced ({MAX_DAILY_LOSS_PCT}%/bot)\n"
        f"V2.3: ADX regime filter + vol confirmation now live\n"
        f"V2.2: Capital coordination (shared Alpaca account w/ Berserker)\n"
        f"V2.1: Exit priority fix — ratchet before rsi-overbought"
    )
    print("[PHASE4] All bots running.", flush=True)

    last_day = datetime.now(tz=CENTRAL).date()
    while True:
        today = datetime.now(tz=CENTRAL).date()
        if today != last_day:
            for bot in bots:
                bot.daily_wins   = 0
                bot.daily_losses = 0
                bot.daily_pnl    = 0.0
            last_day = today
            print("[PHASE4] 🌅 Daily reset", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    run()
