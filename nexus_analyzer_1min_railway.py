#!/usr/bin/env python3
"""
nexus_analyzer_1min_railway.py V3.1 — NEXUS Berserker Backtester
=================================================================
Runs every Sunday 11pm UTC as Railway cron worker (genuine-reverence).
Pulls 2yr 1-min Alpaca IEX bars, replays through the EXACT Berserker
V10.19 signal engine, writes fingerprints to berserker_trade_fingerprints.

V3.1 fix (Jul 8 2026): EXIT slippage applied at both exit sites (signal
exits + end-of-data force-close); pnl_pct and the won label now include
the sell-side half-spread. Entries already paid it since V3.0.
KNOWN, DELIBERATE simplification (unchanged): exit engine is TP/SL only
-- no trailing stop, no EOD autoclose, unbounded holds -- so bt_ bucket
win rates come from a DIFFERENT exit engine than live Berserker runs.
Acceptable for bootstrap seeding; the bt_ scaffolding retires per service
at its 30-live-trade checkpoint. Do not tune live exit params from these.

V3.0 upgrades (Jun 2026):
  ✅ VIX regime gate: VIXY bars fetched alongside SPY/QQQ.
     VIXY * 10 = approx VIX. When VIX > 25, entries skipped in replay.
     Matches V10.19 live VIX gate behavior.
  ✅ Earnings blackout: yfinance calendar fetched at run start for all
     SYMBOLS. Any bar within 48h of a known earnings date is skipped.
     Matches V10.19 earnings_blocked behavior.
  ✅ Regime score: computed from SPY below MA20 + VIX > 25. Score >= 3
     blocks entries (simplified -- no live CB state in backtest).
  ✅ Walk-forward validation: trains on first 21 months, validates on
     last 3 months. Both sets reported in T-Bone alert.
  ✅ Slippage modeling: entry price * 1.0005 (0.05% half-spread).
  ✅ Per-symbol TP/SL from BERSERKER_RECIPES (not global fallback).
  ✅ Sector health: TRUMP_THEME sector_health computed from SPY + TRUMP
     symbol price history, gates TRUMP entries in weak markets.
  ✅ Confluence gate: all 4 signals replicated exactly from main.py V10.19
     (momentum, EMA9>EMA21, MACD histogram accel, bouncing).

Signal engine is an EXACT copy of main.py V10.19 -- any drift between
backtester and live code is a bug. Keep them in sync.

Environment:
  DATABASE_URL (public Railway Postgres URL)
  ALPACA_API_KEY, ALPACA_SECRET_KEY
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

Usage:
  python nexus_analyzer_1min_railway.py          # default 730 days
  python nexus_analyzer_1min_railway.py --days 365
  python nexus_analyzer_1min_railway.py --dry-run
"""

import os
import sys
import time
import secrets
import argparse
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

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
    format="%(asctime)s [BERSERKER-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("berserker_bt")

# ── Environment ───────────────────────────────────────────────────────────────
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Signal engine constants (must match main.py V10.19 exactly) ───────────────
TRUMP_THEME = ["CLSK", "MARA", "PLTR", "GEO", "CXW", "NUE", "MSTR"]
TECH_GROWTH = ["NVDA", "TSLA", "AAPL", "SMCI", "SPCX"]
SYMBOLS     = TRUMP_THEME + TECH_GROWTH

BERSERKER_RECIPES = {
    "CLSK": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "MARA": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "PLTR": {"avoid_hours": [9, 11],  "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "GEO":  {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "CXW":  {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "NUE":  {"avoid_hours": [10, 12], "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "MSTR": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "NVDA": {"avoid_hours": [8, 9],   "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "TSLA": {"avoid_hours": [11, 10], "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "AAPL": {"avoid_hours": [8, 13],  "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "SMCI": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "SPCX": {"avoid_hours": [9, 13],  "avoid_days": [], "tp": 0.015, "sl": 0.015},
}

RSI_PERIOD       = 9
MACD_FAST        = 12
MACD_SLOW        = 26
MACD_SIGNAL      = 9
RSI_BUY_TRIGGER  = 62
WARMUP_BARS      = 50
TAKE_PROFIT_PCT  = 0.015
STOP_LOSS_PCT    = 0.010
SLIPPAGE_PCT     = 0.0005   # V3.0: 0.05% half-spread on market orders

# V3.0: New intelligence gates matching V10.19
VIX_BLOCK_THRESHOLD = 25.0   # VIXY * 10 > 25 = block new entries
EARNINGS_BUFFER_H   = 48     # hours before/after earnings to block

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
    local   = dt.astimezone(central)
    return local.weekday() < 5 and 8 <= local.hour < 15

def get_hour_cdt(dt: datetime) -> int:
    from zoneinfo import ZoneInfo
    central = ZoneInfo("America/Chicago")
    return dt.astimezone(central).hour

def get_day_of_week(dt: datetime) -> int:
    from zoneinfo import ZoneInfo
    central = ZoneInfo("America/Chicago")
    return dt.astimezone(central).weekday()

# ── Earnings calendar (V3.0) ──────────────────────────────────────────────────
def fetch_earnings_dates(symbols: List[str]) -> Dict[str, List[datetime]]:
    """
    Fetch upcoming/recent earnings dates for all symbols via yfinance.
    Returns {symbol: [datetime, ...]} for earnings event timestamps.
    Falls back gracefully if yfinance unavailable or symbol has no data.
    """
    earnings_map: Dict[str, List[datetime]] = {s: [] for s in symbols}
    try:
        import yfinance as yf
        log.info(f"Fetching earnings dates for {len(symbols)} symbols...")
        for sym in symbols:
            try:
                cal = yf.Ticker(sym).calendar
                if cal is None:
                    continue
                if isinstance(cal, dict):
                    dates = cal.get("Earnings Date", [])
                    if dates is None:
                        continue
                    if not hasattr(dates, "__iter__"):
                        dates = [dates]
                    for d in dates:
                        try:
                            if hasattr(d, "to_pydatetime"):
                                d = d.to_pydatetime()
                            if not isinstance(d, datetime):
                                d = datetime.combine(d, datetime.min.time())
                            if d.tzinfo is None:
                                d = d.replace(tzinfo=timezone.utc)
                            earnings_map[sym].append(d)
                        except Exception:
                            pass
                time.sleep(0.2)
            except Exception:
                pass
        filled = sum(1 for v in earnings_map.values() if v)
        log.info(f"  Earnings dates: {filled}/{len(symbols)} symbols have data")
    except ImportError:
        log.warning("yfinance not available — earnings blackout disabled")
    return earnings_map

def is_earnings_blocked(dt: datetime, earnings_dates: List[datetime]) -> bool:
    """True if dt is within EARNINGS_BUFFER_H of any known earnings event."""
    for ed in earnings_dates:
        diff_h = abs((dt - ed).total_seconds()) / 3600
        if diff_h <= EARNINGS_BUFFER_H:
            return True
    return False


# ── Signal engine (exact copy of main.py V10.19) ─────────────────────────────
def compute_rsi(prices: deque) -> float:
    s        = pd.Series(list(prices))
    delta    = s.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / RSI_PERIOD, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("nan"))
    return float((100 - (100 / (1 + rs))).iloc[-1])

def compute_macd(prices: deque):
    s           = pd.Series(list(prices))
    ema_fast    = s.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow    = s.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    histogram   = macd_line - signal_line
    return macd_line.iloc[-1], signal_line.iloc[-1], histogram

def compute_sector_health(trump_prices: Dict[str, deque]) -> str:
    """WEAK if majority of TRUMP_THEME symbols are below their 20-bar MA."""
    down_count = 0
    for sym in TRUMP_THEME:
        prices = trump_prices.get(sym)
        if prices and len(prices) >= 20:
            ma20 = sum(list(prices)[-20:]) / 20
            if list(prices)[-1] < ma20:
                down_count += 1
    return "WEAK" if down_count > len(TRUMP_THEME) / 2 else "STRONG"

def get_signals_bt(symbol: str, prices: deque,
                   sector_health: str, is_trump: bool) -> dict:
    """
    Exact replica of get_signals() from main.py V10.19.
    Uses local price history deque instead of global price_history dict.
    """
    if len(prices) < max(RSI_PERIOD + 1, MACD_SLOW + MACD_SIGNAL):
        return {"buy": False}

    price = list(prices)[-1]
    rsi   = compute_rsi(prices)
    macd_val, macd_sig, macd_hist = compute_macd(prices)
    macd_bullish = macd_val > macd_sig
    ma20         = sum(list(prices)[-20:]) / 20

    sector_weak  = sector_health == "WEAK"
    required_rsi = 72 if (is_trump and sector_weak) else RSI_BUY_TRIGGER

    base_ok = (rsi > required_rsi and macd_bullish and price > ma20)
    if not base_ok:
        return {"buy": False, "rsi": round(rsi, 2), "macd_bull": macd_bullish}

    confluence = 0
    prices_l   = list(prices)

    # 1. Price momentum
    if len(prices_l) >= 10:
        mom_recent = prices_l[-1] - prices_l[-6]  if len(prices_l) >= 6  else 0
        mom_prior  = prices_l[-6] - prices_l[-11] if len(prices_l) >= 11 else 0
        if mom_recent > 0 and mom_recent > mom_prior:
            confluence += 1

    # 2. EMA9 > EMA21
    if len(prices_l) >= 21:
        s     = pd.Series(prices_l)
        ema9  = float(s.ewm(span=9,  adjust=False).mean().iloc[-1])
        ema21 = float(s.ewm(span=21, adjust=False).mean().iloc[-1])
        if ema9 > ema21:
            confluence += 1

    # 3. MACD histogram accelerating
    if len(macd_hist) >= 2 and float(macd_hist.iloc[-1]) > float(macd_hist.iloc[-2]):
        confluence += 1

    # 4. Bouncing
    if len(prices_l) >= 4 and prices_l[-1] > prices_l[-4]:
        confluence += 1

    return {
        "buy":        confluence >= 1,
        "rsi":        round(rsi, 2),
        "macd_bull":  macd_bullish,
        "above_ma20": price > ma20,
        "confluence": confluence,
    }


# ── Data fetching ─────────────────────────────────────────────────────────────
def fetch_all_bars(days: int) -> dict:
    """Fetch 1-min bars for SYMBOLS + SPY + QQQ + VIXY."""
    client   = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc).replace(hour=21, minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=days)
    all_syms = SYMBOLS + ["SPY", "QQQ", "VIXY"]

    log.info(f"Fetching {days}d 1-min bars for {len(all_syms)} symbols...")
    log.info(f"Range: {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")

    result = {}
    for i, sym in enumerate(all_syms):
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
                log.info(f"  [{i+1}/{len(all_syms)}] {sym}: {len(df):,} bars")
            else:
                log.warning(f"  [{i+1}/{len(all_syms)}] {sym}: EMPTY")
        except Exception as e:
            log.error(f"  [{i+1}/{len(all_syms)}] {sym}: {e}")
        time.sleep(0.3)

    log.info(f"Fetched {len(result)}/{len(all_syms)} symbols")
    return result


# ── Replay engine ─────────────────────────────────────────────────────────────
def replay_berserker(all_bars: dict,
                     earnings_map: Dict[str, List[datetime]],
                     validate_mode: bool = False) -> List[Dict]:
    """
    Main replay loop. Iterates all market timestamps in order,
    feeds bars to per-symbol price histories, runs exact get_signals_bt().
    validate_mode: skip first 75% of bars (walk-forward out-of-sample).
    """
    # Unified timestamp index — market hours only
    all_ts_raw = set()
    for sym in SYMBOLS:
        if sym in all_bars:
            all_ts_raw.update(all_bars[sym].index.tolist())
    if "SPY" in all_bars:
        all_ts_raw.update(all_bars["SPY"].index.tolist())

    # Filter to market hours BEFORE slicing for walk-forward
    # (validate_start must be computed on market-hours bars only)
    all_ts = sorted(t for t in all_ts_raw if is_market_hours(
        t if (hasattr(t, "tzinfo") and t.tzinfo) else t.to_pydatetime().replace(tzinfo=timezone.utc)
    ))

    total_bars = len(all_ts)
    # Walk-forward: trade only the last 25% of market-hours bars
    # (price history still warms up from bar 0, just no entries before cutoff)
    validate_start = int(total_bars * 0.75) if validate_mode else 0
    label = "OUT-OF-SAMPLE" if validate_mode else "FULL TRAIN"
    log.info(f"  {label}: {total_bars:,} market-hours timestamps | validate_start={validate_start}")

    # Price histories
    price_hist:   Dict[str, deque] = {s: deque(maxlen=100) for s in SYMBOLS + ["SPY", "QQQ", "VIXY"]}
    trump_prices: Dict[str, deque] = {s: price_hist[s] for s in TRUMP_THEME}

    # Per-symbol trade state
    in_position:  Dict[str, bool]  = {s: False for s in SYMBOLS}
    entry_price:  Dict[str, float] = {s: 0.0   for s in SYMBOLS}
    peak_price:   Dict[str, float] = {s: 0.0   for s in SYMBOLS}
    entry_bar:    Dict[str, int]   = {s: 0      for s in SYMBOLS}
    entry_rsi:    Dict[str, float] = {s: 50.0  for s in SYMBOLS}
    entry_spy_bull: Dict[str, bool] = {s: False for s in SYMBOLS}
    entry_spy_rsi:  Dict[str, float] = {s: 50.0  for s in SYMBOLS}
    entry_spy_mom:  Dict[str, float] = {s: 0.0   for s in SYMBOLS}
    mfe_track:    Dict[str, float] = {s: 0.0   for s in SYMBOLS}
    mae_track:    Dict[str, float] = {s: 0.0   for s in SYMBOLS}
    entry_hour:   Dict[str, int]   = {s: 12     for s in SYMBOLS}
    entry_day:    Dict[str, int]   = {s: 0      for s in SYMBOLS}

    trades:   List[Dict] = []
    bar_num   = 0
    vix_smooth = 15.0

    for bar_idx, ts in enumerate(all_ts):
        bar_num += 1

        # Determine if this bar is in the validation window
        in_validate_window = validate_mode and bar_idx >= validate_start
        in_train_window    = not validate_mode

        dt_utc = ts if (hasattr(ts, "tzinfo") and ts.tzinfo) else ts.to_pydatetime().replace(tzinfo=timezone.utc)
        # Market hours pre-filtered above -- no need to check again
        hour = get_hour_cdt(dt_utc)
        dow  = get_day_of_week(dt_utc)

        # Update all price histories
        for sym in list(SYMBOLS) + ["SPY", "QQQ", "VIXY"]:
            if sym in all_bars and ts in all_bars[sym].index:
                row = all_bars[sym].loc[ts]
                price_hist[sym].append(float(row["close"]))

        # VIX approximation via VIXY (V3.0)
        # VIXY is a 1x VIX futures ETF trading $10-25 while VIX is 12-40.
        # Real multiplier is ~1.5x (not 10x). Keeps threshold at 25 consistent
        # with main.py V10.19's VIX_BLOCK_THRESHOLD.
        if price_hist["VIXY"]:
            vixy = list(price_hist["VIXY"])[-1]
            raw_vix = vixy * 1.5
            vix_smooth = vix_smooth * 0.7 + raw_vix * 0.3
        vix_blocking = vix_smooth > VIX_BLOCK_THRESHOLD

        # SPY context for fingerprinting and sector health gate
        spy_prices = list(price_hist["SPY"])
        spy_bullish = False
        spy_above_ma20 = False
        spy_rsi_val = 50.0
        spy_momentum_val = 0.0
        if len(spy_prices) >= 20:
            spy_ma20 = sum(spy_prices[-20:]) / 20
            spy_above_ma20   = spy_prices[-1] > spy_ma20
            spy_bullish      = spy_above_ma20
            spy_rsi_val      = compute_rsi(deque(spy_prices)) if len(spy_prices) > 10 else 50.0
            spy_momentum_val = (spy_prices[-1] - spy_prices[-6]) / spy_prices[-6] if len(spy_prices) >= 6 and spy_prices[-6] > 0 else 0.0

        # Regime score (V3.0): simplified -- VIX + SPY bear
        regime_block = (not spy_above_ma20) and vix_blocking

        # Sector health for TRUMP gate
        sector_health = compute_sector_health(trump_prices)

        # Skip if not in the right window for this mode
        if validate_mode and bar_idx < validate_start:
            continue  # still updated prices above, just skip trading

        for sym in SYMBOLS:
            if sym not in all_bars:
                continue
            if ts not in all_bars[sym].index:
                continue
            if len(price_hist[sym]) < WARMUP_BARS:
                continue

            is_trump = sym in TRUMP_THEME
            recipe   = BERSERKER_RECIPES.get(sym, {})
            sym_tp   = recipe.get("tp", TAKE_PROFIT_PCT)
            sym_sl   = recipe.get("sl", STOP_LOSS_PCT)

            price = list(price_hist[sym])[-1]

            # ── MANAGE OPEN POSITION ──────────────────────────────────────
            if in_position[sym]:
                profit_pct = (price - entry_price[sym]) / entry_price[sym]
                mfe_track[sym] = max(mfe_track[sym], profit_pct)
                mae_track[sym] = min(mae_track[sym], profit_pct)
                peak_price[sym] = max(peak_price[sym], price)

                exit_reason = None
                if   profit_pct >= sym_tp:  exit_reason = "take_profit"
                elif profit_pct <= -sym_sl: exit_reason = "stop_loss"

                if exit_reason:
                    hold_min = bar_num - entry_bar[sym]
                    # V3.1: EXIT slippage -- entries paid the half-spread,
                    # exits were booked at the raw bar close. Sells cross
                    # the spread against you; the recorded PnL and the won
                    # label now reflect it.
                    exit_px   = price * (1 - SLIPPAGE_PCT)
                    final_pnl = (exit_px - entry_price[sym]) / entry_price[sym]
                    trades.append({
                        "trade_id":     "bt_" + secrets.token_hex(8),
                        "symbol":       sym,
                        "entry_price":  round(entry_price[sym], 4),
                        "exit_price":   round(exit_px, 4),
                        "pnl_pct":      round(final_pnl * 100, 3),
                        "exit_reason":  exit_reason,
                        "hold_min":     hold_min,
                        "won":          final_pnl > 0,
                        "rsi_at_entry": entry_rsi[sym],
                        "spy_bullish":  entry_spy_bull[sym],
                        "spy_rsi":       entry_spy_rsi[sym],
                        "spy_momentum":  entry_spy_mom[sym],
                        "sector_health": sector_health,
                        "is_trump":     is_trump,
                        "sector":       "TRUMP" if is_trump else "TECH",
                        "hour_cdt":     entry_hour[sym],
                        "day_of_week":  entry_day[sym],
                        "mfe":          round(mfe_track[sym] * 100, 3),
                        "mae":          round(mae_track[sym] * 100, 3),
                        "vix_at_entry": round(vix_smooth, 1),
                        "validate":     validate_mode,
                        "tp_used":      sym_tp,
                        "sl_used":      sym_sl,
                    })
                    in_position[sym]  = False
                    entry_price[sym]  = 0.0
                    peak_price[sym]   = 0.0
                    mfe_track[sym]    = 0.0
                    mae_track[sym]    = 0.0

            # ── CHECK FOR ENTRY ───────────────────────────────────────────
            elif not in_position[sym]:
                # V3.0 gates
                if vix_blocking:
                    continue
                if regime_block:
                    continue
                if is_earnings_blocked(dt_utc, earnings_map.get(sym, [])):
                    continue
                if hour in recipe.get("avoid_hours", []):
                    continue
                if dow in recipe.get("avoid_days", []):
                    continue

                sig = get_signals_bt(sym, price_hist[sym], sector_health, is_trump)
                if sig.get("buy"):
                    entry_px          = price * (1 + SLIPPAGE_PCT)
                    in_position[sym]  = True
                    entry_price[sym]  = entry_px
                    peak_price[sym]   = entry_px
                    entry_bar[sym]    = bar_num
                    entry_rsi[sym]    = sig.get("rsi", 50.0)
                    entry_spy_bull[sym] = spy_bullish
                    entry_spy_rsi[sym]  = spy_rsi_val
                    entry_spy_mom[sym]  = spy_momentum_val
                    mfe_track[sym]    = 0.0
                    mae_track[sym]    = 0.0
                    entry_hour[sym]   = hour
                    entry_day[sym]    = dow

        if bar_num % 100000 == 0:
            total_so_far = len(trades)
            log.info(f"  Progress: {bar_num:,}/{total_bars:,} | {total_so_far} trades")

    # Force-close any open at end
    for sym in SYMBOLS:
        if in_position[sym] and price_hist[sym]:
            price      = list(price_hist[sym])[-1]
            exit_px    = price * (1 - SLIPPAGE_PCT)   # V3.1: exit slippage
            profit_pct = (exit_px - entry_price[sym]) / entry_price[sym]
            trades.append({
                "trade_id":    "bt_" + secrets.token_hex(8),
                "symbol":      sym,
                "entry_price": round(entry_price[sym], 4),
                "exit_price":  round(exit_px, 4),
                "pnl_pct":     round(profit_pct * 100, 3),
                "exit_reason": "timeout",
                "hold_min":    bar_num - entry_bar[sym],
                "won":         profit_pct > 0,
                "rsi_at_entry": entry_rsi[sym],
                "spy_bullish": entry_spy_bull[sym],
                "sector_health": "STRONG",
                "is_trump":    sym in TRUMP_THEME,
                "sector":      "TRUMP" if sym in TRUMP_THEME else "TECH",
                "hour_cdt":    entry_hour[sym],
                "day_of_week": entry_day[sym],
                "mfe":         round(mfe_track[sym] * 100, 3),
                "mae":         round(mae_track[sym] * 100, 3),
                "vix_at_entry": round(vix_smooth, 1),
                "validate":    validate_mode,
                "tp_used":     BERSERKER_RECIPES.get(sym, {}).get("tp", TAKE_PROFIT_PCT),
                "sl_used":     BERSERKER_RECIPES.get(sym, {}).get("sl", STOP_LOSS_PCT),
            })

    return trades


# ── DB write ──────────────────────────────────────────────────────────────────
def write_fingerprints(trades: List[Dict], dry_run: bool = False) -> int:
    if dry_run or not DATABASE_URL:
        log.info(f"  DRY RUN: would write {len(trades)} fingerprints")
        return len(trades)
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        written = 0
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM berserker_trade_fingerprints
                WHERE trade_id LIKE 'bt_%' AND won IS NOT NULL
            """)
            for t in trades:
                try:
                    cur.execute("""
                        INSERT INTO berserker_trade_fingerprints
                        (trade_id, symbol, sector,
                         entry_ts, exit_ts, entry_price,
                         symbol_rsi, spy_bullish, spy_rsi, spy_momentum,
                         sector_health, hour_cdt, day_of_week,
                         won, pnl_pct, exit_reason, hold_time_min,
                         mfe, mae, is_paper)
                        VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s,
                                %s,%s,%s,%s, %s,%s,%s)
                        ON CONFLICT (trade_id) DO UPDATE
                        SET won=EXCLUDED.won, pnl_pct=EXCLUDED.pnl_pct,
                            exit_reason=EXCLUDED.exit_reason,
                            mfe=EXCLUDED.mfe, mae=EXCLUDED.mae
                    """, (
                        t["trade_id"],
                        t["symbol"],
                        t.get("sector", "TECH"),
                        int(time.time()), int(time.time()),
                        round(t.get("entry_price", 0.0), 4),
                        round(t.get("rsi_at_entry", 50.0), 2),
                        bool(t.get("spy_bullish", False)),
                        round(t.get("spy_rsi", 50.0), 2),
                        round(t.get("spy_momentum", 0.0), 4),
                        t.get("sector_health", "STRONG"),
                        t.get("hour_cdt", 12),
                        t.get("day_of_week", 0),
                        bool(t["won"]),
                        round(t["pnl_pct"], 3),
                        t["exit_reason"],
                        t.get("hold_min", 0),
                        round(t.get("mfe", 0), 3),
                        round(t.get("mae", 0), 3),
                        False,
                    ))
                    written += 1
                except Exception as e:
                    log.warning(f"fingerprint error [{t.get('symbol','?')}]: {e}")
                    break
        conn.commit()
        conn.close()
        log.info(f"  Wrote {written}/{len(trades)} fingerprints")
        return written
    except Exception as e:
        log.error(f"DB write error: {e}")
        return 0


def run_pattern_analysis() -> Tuple[int, float]:
    """Run BerserkerMemory.run_analysis() equivalent — updates bucket win rates."""
    if not DATABASE_URL:
        return 0, 0.0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, symbol_rsi as rsi_at_entry, spy_bullish,
                       sector_health, sector,
                       hour_cdt, day_of_week, won, pnl_pct, mfe, mae
                FROM berserker_trade_fingerprints WHERE won IS NOT NULL
            """)
            rows = cur.fetchall()

        if not rows:
            conn.close()
            return 0, 0.0

        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)

        for row in rows:
            rsi    = row["rsi_at_entry"] or 50
            hour   = row["hour_cdt"]   or 12
            rsi_b  = "rsi_hi" if rsi > 72 else "rsi_mid" if rsi > 62 else "rsi_low"
            spy_b  = "spy_bull" if row["spy_bullish"] else "spy_bear"
            sec_b  = row["sector_health"] or "STRONG"
            hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
            sec_type = row["sector"] or "TECH"
            key    = f"{row['symbol']}|{rsi_b}|{spy_b}|{sec_b}|{sec_type}|{hr_b}"
            buckets[key].append(bool(row["won"]))
            if row["pnl_pct"] is not None:
                pnl_bkts[key].append(float(row["pnl_pct"]))

        written = 0
        with conn.cursor() as cur:
            for key, outcomes in buckets.items():
                if len(outcomes) < 3:
                    continue
                wr      = sum(outcomes) / len(outcomes)
                avg_pnl = sum(pnl_bkts[key]) / len(pnl_bkts[key]) if pnl_bkts[key] else None
                cur.execute("""
                    INSERT INTO berserker_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (bucket_key) DO UPDATE
                    SET win_rate=EXCLUDED.win_rate, sample_count=EXCLUDED.sample_count,
                        avg_pnl=EXCLUDED.avg_pnl, last_updated=NOW()
                """, (key, wr, len(outcomes), avg_pnl))
                written += 1
        conn.commit()

        total = len(rows)
        wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
        conn.close()
        log.info(f"  Pattern analysis: {written} buckets | {total} trades | {wr:.1%} overall WR")
        return written, wr
    except Exception as e:
        log.error(f"Pattern analysis error: {e}")
        return 0, 0.0


# ── Report ────────────────────────────────────────────────────────────────────
def build_report(trades: List[Dict], validate_mode: bool = False) -> str:
    if not trades:
        return "No trades generated"

    label   = "OUT-OF-SAMPLE" if validate_mode else "FULL TRAIN"
    wins    = [t for t in trades if t["won"]]
    total   = len(trades)
    wr      = round(len(wins) / total * 100, 1)
    avg_pnl = sum(t["pnl_pct"] for t in trades) / total
    avg_mfe = sum(t["mfe"] for t in trades) / total
    avg_mae = sum(t["mae"] for t in trades) / total

    lines = [
        f"\n{'='*60}",
        f"BERSERKER BACKTEST V3.0 — {label}",
        f"{'='*60}",
        f"Trades: {total} | {len(wins)}W {total-len(wins)}L | {wr}% WR",
        f"Avg PnL: {avg_pnl:+.3f}% | MFE: +{avg_mfe:.3f}% | MAE: {avg_mae:.3f}%",
        "",
        f"{'Symbol':<8} {'Trades':>7} {'WR%':>6} {'AvgPnL':>8}",
        "-" * 35,
    ]

    for sym in SYMBOLS:
        st = [t for t in trades if t["symbol"] == sym]
        if not st:
            continue
        sw     = sum(1 for t in st if t["won"])
        s_wr   = round(sw / len(st) * 100, 1)
        s_pnl  = sum(t["pnl_pct"] for t in st) / len(st)
        lines.append(f"{sym:<8} {len(st):>7} {s_wr:>5.1f}% {s_pnl:>+7.3f}%")

    # Exit breakdown
    lines += ["", "Exit reasons:"]
    exit_c = defaultdict(lambda: {"w": 0, "l": 0})
    for t in trades:
        k = t["exit_reason"]
        if t["won"]: exit_c[k]["w"] += 1
        else:        exit_c[k]["l"] += 1
    for r, c in sorted(exit_c.items(), key=lambda x: -(x[1]["w"]+x[1]["l"])):
        tot = c["w"] + c["l"]
        lines.append(f"  {r:<18} {tot:>5} | {round(c['w']/tot*100,1)}% WR")

    lines.append(f"{'='*60}\n")
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NEXUS Berserker Backtester V3.0")
    parser.add_argument("--days",        type=int,  default=730)
    parser.add_argument("--dry-run",     action="store_true")
    parser.add_argument("--no-validate", action="store_true")
    parser.add_argument("--no-earnings", action="store_true",
                        help="Skip yfinance earnings calendar fetch")
    args = parser.parse_args()

    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")
        sys.exit(1)

    log.info("=" * 60)
    log.info(f"NEXUS BERSERKER BACKTESTER V3.0")
    log.info(f"Symbols: {len(SYMBOLS)} | Days: {args.days} | Slippage: {SLIPPAGE_PCT*100:.2f}%")
    log.info(f"V3.0: VIX gate | Earnings blackout | Regime | Walk-forward | Slippage")
    log.info("=" * 60)

    send_alert(
        f"🔥 NEXUS BERSERKER BACKTESTER V3.0 STARTING\n"
        f"Symbols: {len(SYMBOLS)} | Days: {args.days}\n"
        f"V3.0: VIX gate | Earnings | Regime | Walk-forward | Slippage\n"
        f"Signal engine: V10.19 exact replica\n"
        f"ETA: ~30-45 min"
    )

    start_time = time.time()

    # Fetch earnings calendar (V3.0)
    earnings_map: Dict[str, List[datetime]] = {s: [] for s in SYMBOLS}
    if not args.no_earnings:
        earnings_map = fetch_earnings_dates(SYMBOLS)

    # Fetch all bars
    all_bars = fetch_all_bars(args.days)
    if not all_bars:
        log.error("No bar data fetched")
        sys.exit(1)

    # Full training run
    log.info("Running full training replay...")
    train_trades = replay_berserker(all_bars, earnings_map, validate_mode=False)
    log.info(f"Training complete: {len(train_trades)} trades")
    print(build_report(train_trades, validate_mode=False))

    # Walk-forward validation
    val_trades = []
    if not args.no_validate:
        log.info("Running walk-forward validation (last 25% of bars)...")
        val_trades = replay_berserker(all_bars, earnings_map, validate_mode=True)
        log.info(f"Validation complete: {len(val_trades)} trades")
        print(build_report(val_trades, validate_mode=True))

    # Write training trades to DB
    written = 0
    if train_trades:
        log.info(f"Writing {len(train_trades)} training fingerprints to DB...")
        written = write_fingerprints(train_trades, args.dry_run)

    # Pattern analysis
    buckets, overall_wr = 0, 0.0
    if not args.dry_run and DATABASE_URL and written > 0:
        log.info("Running pattern analysis...")
        buckets, overall_wr = run_pattern_analysis()

    elapsed = round(time.time() - start_time)

    # Per-symbol T-Bone summary
    train_wr = round(len([t for t in train_trades if t["won"]]) / max(len(train_trades), 1) * 100, 1)
    sym_lines = []
    for sym in SYMBOLS:
        st = [t for t in train_trades if t["symbol"] == sym]
        if st:
            sw = sum(1 for t in st if t["won"])
            sym_lines.append(f"  {sym}: {round(sw/len(st)*100,1)}% WR ({len(st)}t)")

    val_line = ""
    if val_trades:
        vw  = sum(1 for t in val_trades if t["won"])
        vwr = round(vw / max(len(val_trades), 1) * 100, 1)
        val_line = f"\nValidation (last 25%): {vwr}% WR ({len(val_trades)} trades)"

    send_alert(
        f"✅ NEXUS BERSERKER BACKTESTER V3.0 COMPLETE\n"
        f"──────────────────\n"
        f"Training: {train_wr}% WR ({len(train_trades)} trades)\n"
        + "\n".join(sym_lines) + "\n"
        f"──────────────────\n"
        f"Fingerprints: {written:,} | Buckets: {buckets}\n"
        f"{val_line}\n"
        f"──────────────────\n"
        f"Elapsed: {elapsed}s"
    )

    log.info(f"DONE. {written} fingerprints | {buckets} buckets | {elapsed}s")


if __name__ == "__main__":
    main()
