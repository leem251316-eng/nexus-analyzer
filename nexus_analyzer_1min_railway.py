"""
nexus_analyzer.py — NEXUS Berserker Backtester V2.0
====================================================
Railway cron worker (genuine-reverence project).
Scheduled: every Sunday 11pm UTC.

Replays Berserker's EXACT V10.9 signal logic against 2 years of Alpaca
1-minute bar data and writes fingerprints directly to the shared Railway
PostgreSQL database (berserker_trade_fingerprints table).

BerserkerMemory in main.py reads those fingerprints every day to build
the win-rate gate that blocks historically losing setups before entry.

V2.0 changes vs V1.x:
  ✅ Signal engine rebuilt to match Berserker V10.9 exactly:
       - Wilder EWM RSI (alpha=1/period) -- same as main.py V10.8+
       - MACD (12/26/9 EMA) -- matches main.py compute_macd()
       - MA20 above-price check
       - V10.9 confluence gate: needs 1 of 4 confirmation signals
         (momentum acceleration, EMA9>EMA21, near lower BB, bouncing)
       - RSI_BUY_TRIGGER=62, TRUMP sector gets 72 gate when WEAK
       - Per-symbol avoid_hours / avoid_days from BERSERKER_RECIPES
       - 15-bar sector health (TRUMP_THEME majority down = WEAK)
  ✅ Exit logic mirrors manage_exits() V10.9 exactly:
       - Hard take-profit at +1.5% (TAKE_PROFIT_PCT)
       - Trailing stop 1.5% (TRAILING_STOP)
       - Ratchet: trail tightens to 0.5% after +1.5% profit
       - Hard stop-loss at -4.0% (STOP_LOSS_PCT)
       - MIN_HOLD_MINUTES=20 suppresses trail exit before 20m
       - EOD close at 14:58 CST (market close)
  ✅ SPY context simulated from SPY bar data in the same replay window
       - spy_bullish: SPY price > SPY MA20 in price history
       - spy_momentum: % SPY moved in last 30 bars
       - qqq_rsi: QQQ Wilder RSI over last 20 bars
  ✅ Sector health computed per-bar from TRUMP_THEME price histories
  ✅ PDT slots set to 2 (conservative -- live bot often has 1-2 used)
  ✅ Fingerprints written in exact schema BerserkerMemory expects
  ✅ Clears old backtest fingerprints before each run (source='backtest')
     so stale data doesn't pollute the live win-rate gate
  ✅ Telegram alert on start and finish (with race-condition sleep fix)
  ✅ BERSERKER_STOCKS updated: removed CCJ, COIN, AMZN, GOOGL; added SPCX
  ✅ Runs 2-year window (730 days) for maximum bucket coverage

Usage (Railway cron):
    python nexus_analyzer.py

Environment variables required:
    ALPACA_API_KEY, ALPACA_SECRET_KEY
    DATABASE_URL
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID (optional -- alerts)
"""

import os
import sys
import time
import secrets
import logging
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ANALYZER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("nexus.analyzer")

# ── Environment ───────────────────────────────────────────────────────────────
ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

CENTRAL = ZoneInfo("America/Chicago")

# ── Berserker symbol universe (matches main.py V10.9) ─────────────────────────
# V2.0: Removed CCJ (40% WR), COIN (40% WR), AMZN (44.9% WR), GOOGL (44% WR)
# V2.0: Added SPCX (SpaceX, IPO'd 2026-06-12)
TRUMP_THEME = ["CLSK", "MARA", "PLTR", "GEO", "CXW", "NUE", "MSTR"]
TECH_GROWTH = ["NVDA", "AMD", "TSLA", "AAPL", "MSFT", "META", "SMCI", "SPCX"]
SYMBOLS     = TRUMP_THEME + TECH_GROWTH

# SPY + QQQ needed for context simulation
CONTEXT_SYMBOLS = ["SPY", "QQQ"]

# ── Per-symbol hour/day gates (matches BERSERKER_RECIPES in main.py V10.9) ───
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

# ── Signal constants (exact match with main.py V10.9) ─────────────────────────
RSI_PERIOD         = 9
RSI_BUY_TRIGGER    = 62
MACD_FAST          = 12
MACD_SLOW          = 26
MACD_SIGNAL_P      = 9
WARMUP_BARS        = 50        # minimum bars before any signal fires
TAKE_PROFIT_PCT    = 0.015     # +1.5% hard take-profit
TRAILING_STOP      = 0.015     # 1.5% trailing stop
RATCHET_PROFIT     = 0.015     # profit level where trail tightens
RATCHET_TRAIL_TIGHT = 0.005   # tight trail after ratchet fires
STOP_LOSS_PCT      = 0.04      # -4.0% hard stop
MIN_HOLD_MINUTES   = 20        # suppress trail exit before this

# Simulated PDT slots (conservative -- live bot often has 1-2 used)
SIM_PDT_SLOTS_USED = 2

# How many days of history to replay
BACKTEST_DAYS = 730  # 2 years

# ── Helpers ───────────────────────────────────────────────────────────────────
def send_alert(msg: str):
    """Send Telegram alert. Silent fail if not configured."""
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


def is_market_hours(dt: datetime) -> bool:
    local = dt.astimezone(CENTRAL)
    return local.weekday() < 5 and 8 <= local.hour < 15


def is_eod(dt: datetime) -> bool:
    """True if at or past the EOD auto-close time (14:58 CST)."""
    local = dt.astimezone(CENTRAL)
    return local.weekday() < 5 and (local.hour > 14 or
           (local.hour == 14 and local.minute >= 58))


# ── Signal engine (exact match with main.py V10.9 get_signals) ────────────────
def compute_wilder_rsi(prices: list, period: int = 9) -> float:
    """Wilder EWM RSI -- matches main.py V10.8+ and phase4.py V1.7+."""
    if len(prices) < period + 1:
        return 50.0
    s        = pd.Series(prices, dtype=float)
    delta    = s.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("nan"))
    rsi      = (100 - (100 / (1 + rs))).iloc[-1]
    if pd.isna(rsi) or not (0 < rsi < 100):
        return 50.0
    return float(round(rsi, 2))


def compute_macd(prices: list) -> tuple:
    """MACD (12/26/9) -- matches main.py compute_macd()."""
    if len(prices) < MACD_SLOW + MACD_SIGNAL_P:
        return 0.0, 0.0
    s           = pd.Series(prices, dtype=float)
    ema_fast    = s.ewm(span=MACD_FAST,     adjust=False).mean()
    ema_slow    = s.ewm(span=MACD_SLOW,     adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL_P, adjust=False).mean()
    return float(macd_line.iloc[-1]), float(signal_line.iloc[-1])


def get_signals(symbol: str, prices: list, sector_health: str,
                hour: int, dow: int) -> dict:
    """
    Exact replica of main.py V10.9 get_signals().
    Returns dict with 'buy', 'rsi', 'macd_bull', 'confluence'.
    """
    if len(prices) < max(RSI_PERIOD + 1, 26, MACD_SLOW + MACD_SIGNAL_P):
        return {"buy": False, "rsi": 50.0, "macd_bull": False, "confluence": 0}

    rsi          = compute_wilder_rsi(prices, RSI_PERIOD)
    macd_val, macd_sig = compute_macd(prices)
    macd_bullish = macd_val > macd_sig
    ma20         = float(sum(prices[-20:]) / 20)
    price        = prices[-1]

    # V10.9: TECH symbols not penalized by Trump-sector weakness
    is_trump    = symbol in TRUMP_THEME
    sector_weak = sector_health == "WEAK"
    required_rsi = 72 if (is_trump and sector_weak) else RSI_BUY_TRIGGER

    recipe       = BERSERKER_RECIPES.get(symbol, {})
    hour_blocked = hour    in recipe.get("avoid_hours", [])
    day_blocked  = dow     in recipe.get("avoid_days",  [])

    base_ok = (rsi > required_rsi and macd_bullish and price > ma20
               and not hour_blocked and not day_blocked)

    if not base_ok:
        return {"buy": False, "rsi": rsi, "macd_bull": macd_bullish, "confluence": 0}

    # V10.9: Confluence gate -- need at least 1 of 4 signals
    confluence = 0

    # 1. Momentum acceleration (price momentum increasing)
    if len(prices) >= 11:
        mom_recent = prices[-1] - prices[-6]
        mom_prior  = prices[-6] - prices[-11]
        if mom_recent > 0 and mom_recent > mom_prior:
            confluence += 1

    # 2. EMA9 above EMA21
    if len(prices) >= 21:
        s     = pd.Series(prices, dtype=float)
        ema9  = float(s.ewm(span=9,  adjust=False).mean().iloc[-1])
        ema21 = float(s.ewm(span=21, adjust=False).mean().iloc[-1])
        if ema9 > ema21:
            confluence += 1

    # 3. Near lower Bollinger Band (lower 35% of band)
    if len(prices) >= 20:
        s      = pd.Series(prices, dtype=float)
        mid    = float(s.rolling(20).mean().iloc[-1])
        std    = float(s.rolling(20).std().iloc[-1])
        lower  = mid - 2.0 * std
        upper  = mid + 2.0 * std
        band_w = upper - lower
        pct_b  = (price - lower) / band_w if band_w > 0 else 0.5
        if pct_b < 0.35:
            confluence += 1

    # 4. Bouncing (recovering from recent low, last 3 bars up)
    if len(prices) >= 4 and prices[-1] > prices[-4]:
        confluence += 1

    return {
        "buy":        confluence >= 1,
        "rsi":        rsi,
        "macd_bull":  macd_bullish,
        "above_ma20": price > ma20,
        "confluence": confluence,
    }


# ── SPY/QQQ context computation ───────────────────────────────────────────────
def get_spy_context(spy_prices: list, qqq_prices: list) -> dict:
    """
    Simulate the SPY/QQQ context that main.py _get_spy_context_for_fingerprint()
    computes in real time. Used to populate spy_bullish, spy_momentum, qqq_rsi
    in the fingerprint so BerserkerMemory bucket keys are meaningful.
    """
    if len(spy_prices) < 20:
        return {"bullish": True, "momentum": 0.0, "rsi": 50.0, "qqq_rsi": 50.0}

    spy_ma20   = float(sum(spy_prices[-20:]) / 20)
    spy_bull   = spy_prices[-1] > spy_ma20
    spy_mom    = (spy_prices[-1] - spy_prices[-6]) / spy_prices[-6] * 100 \
                 if len(spy_prices) >= 6 and spy_prices[-6] > 0 else 0.0
    spy_rsi    = compute_wilder_rsi(spy_prices[-20:], 7)
    qqq_rsi    = compute_wilder_rsi(qqq_prices[-20:], 7) if len(qqq_prices) >= 8 else 50.0

    return {
        "bullish":  spy_bull,
        "momentum": round(spy_mom, 3),
        "rsi":      spy_rsi,
        "qqq_rsi":  qqq_rsi,
    }


# ── Sector health computation ─────────────────────────────────────────────────
def compute_sector_health(price_histories: dict) -> str:
    """
    Mirrors main.py V10.9 update_sector_health().
    15-bar lookback on TRUMP_THEME symbols (was 5 in V10.8, fixed in V10.9).
    """
    down_count = sum(
        1 for sym in TRUMP_THEME
        if (sym in price_histories
            and len(price_histories[sym]) >= 15
            and price_histories[sym][-1] < price_histories[sym][-15])
    )
    return "WEAK" if down_count > len(TRUMP_THEME) / 2 else "STRONG"


# ── Bucket key (exact match with BerserkerMemory._bucket_key) ─────────────────
def bucket_key(symbol: str, rsi: float, spy_bullish: bool,
               sector_health: str, hour: int, pdt_used: int) -> str:
    sector = "TRUMP" if symbol in TRUMP_THEME else "TECH"
    rsi_b  = "rsi_hi" if rsi > 70 else "rsi_mid" if rsi > 60 else "rsi_low"
    spy_b  = "spy_bull" if spy_bullish else "spy_bear"
    sec_b  = sector_health or "STRONG"
    hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
    pdt_b  = "pdt_ok" if pdt_used < 2 else "pdt_tight"
    return f"{symbol}|{rsi_b}|{spy_b}|{sec_b}|{sector}|{hr_b}|{pdt_b}"


# ── Data fetcher ──────────────────────────────────────────────────────────────
def fetch_bars(client: StockHistoricalDataClient,
               symbols: list, start: datetime, end: datetime) -> dict:
    """Fetch 1-minute bars for all symbols. Returns {symbol: DataFrame}."""
    result = {}
    for i, symbol in enumerate(symbols):
        log.info(f"  Fetching {symbol} ({i+1}/{len(symbols)})...")
        try:
            bars = client.get_stock_bars(StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Minute,
                start=start,
                end=end,
                feed=DataFeed.IEX,
            ))
            df = bars.df
            if hasattr(df.index, "levels"):
                df = df.xs(symbol, level=0)
            if not df.empty:
                result[symbol] = df
                log.info(f"    {symbol}: {len(df):,} bars")
            else:
                log.warning(f"    {symbol}: no data returned")
        except Exception as e:
            log.warning(f"    {symbol}: fetch error -- {e}")
        time.sleep(0.3)  # be gentle with the API

    fetched = len(result)
    missed  = len(symbols) - fetched
    log.info(f"Data fetch complete: {fetched}/{len(symbols)} symbols"
             + (f" | {missed} had no data" if missed else ""))
    return result


# ── Core replay engine ────────────────────────────────────────────────────────
def replay(all_bars: dict, spy_df, qqq_df) -> list:
    """
    Replay Berserker V10.9 signal logic across all bar data.
    Returns list of fingerprint dicts ready for DB insertion.
    """
    # Build unified sorted timestamp index from all symbol bars
    all_timestamps = set()
    for df in all_bars.values():
        all_timestamps.update(df.index.tolist())
    if spy_df is not None:
        all_timestamps.update(spy_df.index.tolist())
    if qqq_df is not None:
        all_timestamps.update(qqq_df.index.tolist())
    all_timestamps = sorted(all_timestamps)

    log.info(f"Replaying {len(all_timestamps):,} timestamps across "
             f"{len(all_bars)} symbols + SPY/QQQ context...")

    # Price history buffers
    price_hist:  dict = {s: [] for s in all_bars}
    spy_prices:  list = []
    qqq_prices:  list = []

    open_trades: dict = {}   # symbol -> trade dict
    fingerprints: list = []

    total_bars = len(all_timestamps)
    wins = losses = 0

    for bar_idx, ts_val in enumerate(all_timestamps):
        if bar_idx % 50000 == 0:
            log.info(f"  Progress: {bar_idx:,}/{total_bars:,} bars | "
                     f"{wins}W {losses}L | {len(open_trades)} open")

        # Convert timestamp
        dt_utc = ts_val if hasattr(ts_val, "tzinfo") else ts_val.to_pydatetime()
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        local = dt_utc.astimezone(CENTRAL)

        # ── Update SPY/QQQ context buffers ────────────────────────────────
        if spy_df is not None and ts_val in spy_df.index:
            spy_prices.append(float(spy_df.loc[ts_val, "close"]))
            if len(spy_prices) > 100:
                spy_prices.pop(0)

        if qqq_df is not None and ts_val in qqq_df.index:
            qqq_prices.append(float(qqq_df.loc[ts_val, "close"]))
            if len(qqq_prices) > 100:
                qqq_prices.pop(0)

        # ── Update symbol price buffers ───────────────────────────────────
        for symbol, df in all_bars.items():
            if ts_val not in df.index:
                continue

            price = float(df.loc[ts_val, "close"])
            price_hist[symbol].append(price)
            if len(price_hist[symbol]) > 100:
                price_hist[symbol].pop(0)

        # ── Manage open trades (exits) ────────────────────────────────────
        # Process exits before entries at each bar
        for symbol in list(open_trades.keys()):
            if ts_val not in all_bars.get(symbol, pd.DataFrame()).index:
                continue

            trade  = open_trades[symbol]
            price  = price_hist[symbol][-1]
            entry  = trade["entry_price"]

            profit_pct = (price - entry) / entry
            peak       = trade["peak_price"]
            if price > peak:
                trade["peak_price"] = price
                peak = price

            trade["mfe"] = max(trade.get("mfe", 0.0), profit_pct)
            trade["mae"] = min(trade.get("mae", 0.0), profit_pct)

            held_secs = (dt_utc - trade["entry_dt"]).total_seconds()
            held_min  = held_secs / 60.0

            trailing = RATCHET_TRAIL_TIGHT if profit_pct >= RATCHET_PROFIT else TRAILING_STOP

            closed     = False
            exit_reason = ""

            # Hard take-profit at +1.5%
            if profit_pct >= TAKE_PROFIT_PCT:
                closed      = True
                exit_reason = "take-profit"

            # Hard stop loss at -4%
            elif profit_pct <= -STOP_LOSS_PCT:
                closed      = True
                exit_reason = "stop-loss"

            # Trailing stop (suppressed before MIN_HOLD_MINUTES)
            elif held_min >= MIN_HOLD_MINUTES:
                drawdown = (peak - price) / peak if peak > 0 else 0
                if drawdown >= trailing:
                    closed      = True
                    exit_reason = "trail"

            # EOD auto-close
            elif is_eod(dt_utc) and not closed:
                closed      = True
                exit_reason = "eod"

            if closed:
                won = profit_pct > 0
                if won:
                    wins += 1
                else:
                    losses += 1

                fp = dict(trade)
                fp["exit_ts"]      = int(dt_utc.timestamp())
                fp["won"]          = won
                fp["pnl_pct"]      = round(profit_pct * 100, 3)
                fp["exit_reason"]  = exit_reason
                fp["hold_time_min"] = int(held_min)
                fp["mfe"]          = round(trade["mfe"] * 100, 3)
                fp["mae"]          = round(trade["mae"] * 100, 3)
                fingerprints.append(fp)
                del open_trades[symbol]

        # ── Look for new entries ──────────────────────────────────────────
        if not is_market_hours(dt_utc):
            continue

        hour = local.hour
        dow  = local.weekday()

        # Compute sector health from current price histories
        sector_health = compute_sector_health(price_hist)

        # SPY context for fingerprint
        spy_ctx = get_spy_context(spy_prices, qqq_prices)

        for symbol in SYMBOLS:
            if symbol in open_trades:
                continue
            if symbol not in all_bars:
                continue
            if ts_val not in all_bars[symbol].index:
                continue

            prices = price_hist[symbol]
            if len(prices) < WARMUP_BARS:
                continue

            sigs = get_signals(symbol, prices, sector_health, hour, dow)
            if not sigs.get("buy"):
                continue

            # Entry confirmed -- create fingerprint stub
            trade_id = secrets.token_hex(8)
            open_trades[symbol] = {
                "trade_id":      trade_id,
                "symbol":        symbol,
                "sector":        "TRUMP" if symbol in TRUMP_THEME else "TECH",
                "entry_ts":      int(dt_utc.timestamp()),
                "entry_dt":      dt_utc,
                "entry_price":   prices[-1],
                "peak_price":    prices[-1],
                "symbol_rsi":    round(sigs["rsi"], 2),
                "macd_bullish":  bool(sigs["macd_bull"]),
                "above_ma20":    bool(sigs.get("above_ma20", True)),
                "spy_rsi":       spy_ctx["rsi"],
                "spy_momentum":  spy_ctx["momentum"],
                "spy_bullish":   spy_ctx["bullish"],
                "qqq_rsi":       spy_ctx["qqq_rsi"],
                "sector_health": sector_health,
                "hour_cdt":      hour,
                "day_of_week":   dow,
                "pdt_slots_used": SIM_PDT_SLOTS_USED,
                "mfe":           0.0,
                "mae":           0.0,
                # exit fields filled in on close
                "exit_ts":       None,
                "won":           None,
                "pnl_pct":       None,
                "exit_reason":   None,
                "hold_time_min": None,
            }

    # ── Close any still-open trades at end of data ────────────────────────
    for symbol, trade in open_trades.items():
        prices = price_hist.get(symbol, [])
        if prices:
            profit_pct = (prices[-1] - trade["entry_price"]) / trade["entry_price"]
            held_min   = (all_timestamps[-1].to_pydatetime().replace(tzinfo=timezone.utc)
                          - trade["entry_dt"]).total_seconds() / 60 \
                         if hasattr(all_timestamps[-1], "to_pydatetime") \
                         else 0
            fp = dict(trade)
            fp["exit_ts"]       = int(time.time())
            fp["won"]           = profit_pct > 0
            fp["pnl_pct"]       = round(profit_pct * 100, 3)
            fp["exit_reason"]   = "timeout"
            fp["hold_time_min"] = int(held_min)
            fp["mfe"]           = round(max(trade.get("mfe", 0.0), profit_pct) * 100, 3)
            fp["mae"]           = round(min(trade.get("mae", 0.0), profit_pct) * 100, 3)
            fingerprints.append(fp)
            if profit_pct > 0:
                wins += 1
            else:
                losses += 1

    total = wins + losses
    wr    = round(wins / total * 100, 1) if total > 0 else 0
    log.info(f"Replay complete: {wins}W {losses}L ({wr}% WR) | "
             f"{len(fingerprints)} fingerprints generated")
    return fingerprints


# ── Database writer ───────────────────────────────────────────────────────────
def write_fingerprints(fingerprints: list, db_url: str) -> int:
    """
    Write fingerprints to berserker_trade_fingerprints.
    Clears old backtest rows first (source='backtest' tagged via trade_id prefix).
    Returns count of rows written.
    """
    if not fingerprints:
        log.warning("No fingerprints to write.")
        return 0

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    written = 0

    try:
        with conn.cursor() as cur:
            # Ensure table + indexes exist (safe to run on existing schema)
            cur.execute("""
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
                CREATE INDEX IF NOT EXISTS idx_btf_symbol
                    ON berserker_trade_fingerprints(symbol);
                CREATE INDEX IF NOT EXISTS idx_btf_won
                    ON berserker_trade_fingerprints(won);
            """)

            # Clear previous backtest rows -- trade_ids from this script
            # are plain hex (no prefix); live trades also use token_hex.
            # We tag backtest rows by exit_reason patterns and a large
            # entry_ts range check to avoid wiping live trades.
            # Safest approach: delete rows where exit_reason IN backtest
            # reasons AND entry_ts < (now - 2 weeks) -- live trades are
            # recent; backtest trades are from 2yr ago.
            cutoff_ts = int(time.time()) - 14 * 86400  # 2 weeks ago
            cur.execute("""
                DELETE FROM berserker_trade_fingerprints
                WHERE entry_ts < %s
                  AND exit_reason IN ('take-profit','stop-loss','trail','eod','timeout')
            """, (cutoff_ts,))
            deleted = cur.rowcount
            log.info(f"Cleared {deleted} old backtest fingerprints (entry_ts < 2 weeks ago)")

            # Insert new fingerprints
            for fp in fingerprints:
                if fp.get("won") is None:
                    continue  # skip incomplete
                cur.execute("""
                    INSERT INTO berserker_trade_fingerprints
                    (trade_id, symbol, sector, entry_ts, exit_ts, entry_price,
                     symbol_rsi, macd_bullish, above_ma20,
                     spy_rsi, spy_momentum, spy_bullish, qqq_rsi,
                     sector_health, hour_cdt, day_of_week, pdt_slots_used,
                     won, pnl_pct, exit_reason, hold_time_min, mfe, mae)
                    VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,
                            %s,%s,%s,%s, %s,%s,%s,%s,%s,%s)
                    ON CONFLICT (trade_id) DO UPDATE
                    SET won          = EXCLUDED.won,
                        pnl_pct      = EXCLUDED.pnl_pct,
                        exit_reason  = EXCLUDED.exit_reason,
                        hold_time_min= EXCLUDED.hold_time_min,
                        exit_ts      = EXCLUDED.exit_ts,
                        mfe          = EXCLUDED.mfe,
                        mae          = EXCLUDED.mae
                """, (
                    fp["trade_id"], fp["symbol"], fp["sector"],
                    fp["entry_ts"], fp.get("exit_ts"), fp["entry_price"],
                    float(fp["symbol_rsi"]),
                    bool(fp["macd_bullish"]),
                    bool(fp["above_ma20"]),
                    float(fp["spy_rsi"]) if fp.get("spy_rsi") else None,
                    float(fp["spy_momentum"]) if fp.get("spy_momentum") is not None else None,
                    bool(fp["spy_bullish"]) if fp.get("spy_bullish") is not None else None,
                    float(fp["qqq_rsi"]) if fp.get("qqq_rsi") else None,
                    fp["sector_health"],
                    int(fp["hour_cdt"]),
                    int(fp["day_of_week"]),
                    int(fp["pdt_slots_used"]),
                    bool(fp["won"]),
                    float(fp["pnl_pct"]),
                    fp["exit_reason"],
                    int(fp["hold_time_min"]) if fp.get("hold_time_min") is not None else None,
                    float(fp["mfe"]),
                    float(fp["mae"]),
                ))
                written += 1

        conn.commit()
        log.info(f"DB write complete: {written} fingerprints written")

    except Exception as e:
        conn.rollback()
        log.error(f"DB write error: {e}")
        raise
    finally:
        conn.close()

    return written


# ── Pattern stats analysis ────────────────────────────────────────────────────
def run_analysis(db_url: str) -> tuple:
    """
    Rebuild berserker_pattern_stats from all completed fingerprints.
    Mirrors BerserkerMemory.run_analysis() in main.py.
    Returns (bucket_count, total_trades, overall_wr).
    """
    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, symbol_rsi, spy_bullish, sector_health,
                       hour_cdt, pdt_slots_used, won, pnl_pct
                FROM berserker_trade_fingerprints
                WHERE won IS NOT NULL
            """)
            rows = cur.fetchall()

        if not rows:
            log.warning("No completed fingerprints found for analysis.")
            return 0, 0, 0.0

        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)

        for row in rows:
            key = bucket_key(
                row["symbol"],
                float(row["symbol_rsi"]) if row["symbol_rsi"] is not None else 50,
                bool(row["spy_bullish"]) if row["spy_bullish"] is not None else True,
                row["sector_health"] or "STRONG",
                int(row["hour_cdt"]) if row["hour_cdt"] is not None else 12,
                int(row["pdt_slots_used"]) if row["pdt_slots_used"] is not None else 2,
            )
            buckets[key].append(bool(row["won"]))
            if row["pnl_pct"] is not None:
                pnl_bkts[key].append(float(row["pnl_pct"]))

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS berserker_pattern_stats (
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
                if len(outcomes) < 3:   # PM_MIN_BUCKET_TRADES
                    continue
                wr      = sum(outcomes) / len(outcomes)
                avg_pnl = (sum(pnl_bkts[key]) / len(pnl_bkts[key])
                           if pnl_bkts[key] else None)
                cur.execute("""
                    INSERT INTO berserker_pattern_stats
                    (bucket_key, win_rate, sample_count, avg_pnl)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (bucket_key) DO UPDATE
                    SET win_rate     = EXCLUDED.win_rate,
                        sample_count = EXCLUDED.sample_count,
                        avg_pnl      = EXCLUDED.avg_pnl,
                        last_updated = NOW()
                """, (key, wr, len(outcomes), avg_pnl))
                written_buckets += 1

        conn.commit()

        total = len(rows)
        wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
        log.info(f"Analysis: {written_buckets} buckets | {total} trades | {wr:.1%} WR")
        return written_buckets, total, wr

    except Exception as e:
        conn.rollback()
        log.error(f"Analysis error: {e}")
        raise
    finally:
        conn.close()


# ── Summary printer ───────────────────────────────────────────────────────────
def print_summary(fingerprints: list):
    if not fingerprints:
        return

    completed = [f for f in fingerprints if f.get("won") is not None]
    if not completed:
        return

    total  = len(completed)
    wins   = sum(1 for f in completed if f["won"])
    losses = total - wins
    wr     = round(wins / total * 100, 1) if total > 0 else 0

    avg_mfe = round(sum(f.get("mfe", 0) for f in completed) / total, 2) if total else 0
    avg_mae = round(sum(f.get("mae", 0) for f in completed) / total, 2) if total else 0

    # Per-symbol breakdown
    sym_stats = defaultdict(lambda: {"w": 0, "l": 0, "pnl": []})
    for f in completed:
        s = f["symbol"]
        if f["won"]:
            sym_stats[s]["w"] += 1
        else:
            sym_stats[s]["l"] += 1
        if f.get("pnl_pct") is not None:
            sym_stats[s]["pnl"].append(f["pnl_pct"])

    # Exit reason breakdown
    exit_counts = defaultdict(int)
    exit_wins   = defaultdict(int)
    for f in completed:
        r = f.get("exit_reason", "unknown")
        exit_counts[r] += 1
        if f["won"]:
            exit_wins[r] += 1

    # Hour breakdown
    hr_stats = defaultdict(lambda: {"w": 0, "l": 0})
    for f in completed:
        h = f.get("hour_cdt", 0)
        if f["won"]:
            hr_stats[h]["w"] += 1
        else:
            hr_stats[h]["l"] += 1

    print(f"""
╔══════════════════════════════════════════════════════════╗
║         NEXUS BERSERKER BACKTEST SUMMARY V2.0            ║
╚══════════════════════════════════════════════════════════╝

Period:     Last {BACKTEST_DAYS} days (2yr)
Trades:     {total} total | {wins}W {losses}L | {wr}% win rate
Avg MFE:    +{avg_mfe}%   Avg MAE: {avg_mae}%

EXIT REASONS:""")
    for reason, count in sorted(exit_counts.items(), key=lambda x: -x[1]):
        r_wr = round(exit_wins[reason] / count * 100) if count > 0 else 0
        print(f"  {reason:<18} {count:>5} trades | {r_wr}% WR")

    print(f"\nHOUR BREAKDOWN (CST):")
    for hr in sorted(hr_stats.keys()):
        s  = hr_stats[hr]
        t  = s["w"] + s["l"]
        r  = round(s["w"] / t * 100) if t > 0 else 0
        bar = "█" * (r // 5)
        print(f"  {hr:02d}h  {r:>3}% WR  {bar}  ({t} trades)")

    print(f"\nSYMBOL BREAKDOWN:")
    for sym in sorted(sym_stats.keys(),
                      key=lambda s: sym_stats[s]["w"] + sym_stats[s]["l"],
                      reverse=True):
        s  = sym_stats[sym]
        t  = s["w"] + s["l"]
        r  = round(s["w"] / t * 100) if t > 0 else 0
        ap = round(sum(s["pnl"]) / len(s["pnl"]), 2) if s["pnl"] else 0
        tag = " [TRUMP]" if sym in TRUMP_THEME else " [TECH]"
        print(f"  {sym:<6} {r:>3}% WR | {t:>4} trades | avg {ap:+.2f}%{tag}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("NEXUS BERSERKER BACKTESTER V2.0 STARTING")
    log.info("=" * 60)
    log.info(f"Symbols: {len(SYMBOLS)} -- {', '.join(SYMBOLS)}")
    log.info(f"Period:  {BACKTEST_DAYS} days")
    log.info(f"DB:      {'connected' if DATABASE_URL else 'NOT SET'}")

    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("ALPACA_API_KEY or ALPACA_SECRET_KEY not set.")
        sys.exit(1)
    if not DATABASE_URL:
        log.error("DATABASE_URL not set.")
        sys.exit(1)

    send_alert(
        f"🔬 NEXUS ANALYZER V2.0 STARTING\n"
        f"Berserker backtest: {len(SYMBOLS)} symbols | {BACKTEST_DAYS} days\n"
        f"Signal engine: V10.9 exact replica\n"
        f"ETA: ~10-20 min"
    )

    start_time = time.time()

    # ── Date range ────────────────────────────────────────────────────────
    end_dt   = datetime.now(tz=timezone.utc)
    start_dt = end_dt - timedelta(days=BACKTEST_DAYS)
    log.info(f"Date range: {start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}")

    # ── Fetch data ────────────────────────────────────────────────────────
    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)

    log.info("Fetching BERSERKER symbol data...")
    symbol_bars = fetch_bars(client, SYMBOLS, start_dt, end_dt)

    log.info("Fetching SPY + QQQ context data...")
    context_bars = fetch_bars(client, CONTEXT_SYMBOLS, start_dt, end_dt)
    spy_df = context_bars.get("SPY")
    qqq_df = context_bars.get("QQQ")

    if not symbol_bars:
        log.error("No symbol data fetched. Aborting.")
        send_alert("❌ NEXUS ANALYZER: No data fetched -- check Alpaca API keys")
        sys.exit(1)

    log.info(f"Data fetched in {round(time.time()-start_time, 1)}s")

    # ── Replay ────────────────────────────────────────────────────────────
    replay_start = time.time()
    fingerprints = replay(symbol_bars, spy_df, qqq_df)
    replay_time  = round(time.time() - replay_start, 1)
    log.info(f"Replay completed in {replay_time}s")

    if not fingerprints:
        log.error("No fingerprints generated. Aborting DB write.")
        send_alert("❌ NEXUS ANALYZER: 0 fingerprints generated -- check signal thresholds")
        sys.exit(1)

    # ── Write to DB ───────────────────────────────────────────────────────
    log.info("Writing fingerprints to database...")
    written = write_fingerprints(fingerprints, DATABASE_URL)

    # ── Run analysis to rebuild pattern stats ─────────────────────────────
    log.info("Running pattern analysis...")
    buckets, total_trades, overall_wr = run_analysis(DATABASE_URL)

    # ── Summary ───────────────────────────────────────────────────────────
    print_summary(fingerprints)

    elapsed = round(time.time() - start_time, 1)
    completed = [f for f in fingerprints if f.get("won") is not None]
    total  = len(completed)
    wins   = sum(1 for f in completed if f["won"])
    wr_pct = round(wins / total * 100, 1) if total else 0

    # Per-symbol WR for alert
    sym_lines = []
    sym_stats = defaultdict(lambda: {"w": 0, "l": 0})
    for f in completed:
        if f["won"]: sym_stats[f["symbol"]]["w"] += 1
        else:        sym_stats[f["symbol"]]["l"] += 1
    for sym in sorted(SYMBOLS):
        if sym not in sym_stats:
            continue
        s = sym_stats[sym]
        t = s["w"] + s["l"]
        r = round(s["w"] / t * 100) if t > 0 else 0
        sym_lines.append(f"  {sym}: {r}% ({t} trades)")

    # Sleep before final alert to avoid Telegram race condition
    time.sleep(3)
    send_alert(
        f"✅ NEXUS ANALYZER V2.0 COMPLETE\n"
        f"──────────────────\n"
        f"Fingerprints: {written:,}\n"
        f"Pattern buckets: {buckets}\n"
        f"Overall WR: {wr_pct}%\n"
        f"Win-rate gate: active\n"
        f"──────────────────\n"
        + "\n".join(sym_lines[:10]) + "\n"
        f"──────────────────\n"
        f"Elapsed: {elapsed}s"
    )

    log.info(f"DONE. {written} fingerprints | {buckets} buckets | "
             f"{wr_pct}% WR | {elapsed}s total")


if __name__ == "__main__":
    main()
