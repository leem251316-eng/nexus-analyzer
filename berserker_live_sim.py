#!/usr/bin/env python3
"""
berserker_live_sim.py — Berserker V4 replay engine (live-path replica).

This module is pure: no database, no Alpaca, no Telegram. The Sunday runner
in nexus_analyzer_1min_railway.py feeds it bars and writes fingerprints.

Live path this replicates
-------------------------
Trading-bot (leem251316-eng/Trading-bot, main.py V10.68) is a private repo
and was not readable from the analyzer environment. The exit state machine
follows the control replica in trail_prereg_sim.py, which was written
against main.py V10.67 (TP / SL / base trail / ratchet / 20-minute arm /
EOD 14:58 CT carry) and checked against live fills. Entry clocks follow
the V10.61 opening wall and the V10.68 band_0905 prereg. Signal math for
RSI, MACD, the 20-bar short MA, sector health, and the first four
confluence legs is the V3.3 copy of get_signals() from main.py V10.19.
Where V10.68 is stricter than that copy, the constants below say so.

Exit (evaluated on each RTH minute bar, after the entry minute):
  * Hard take-profit at the recipe TP (usually 1.5% above the fill).
  * Hard stop at the recipe SL (1.0%, SPCX 1.5%). A gap through the stop
    fills at the open, not at the stop. That is the overnight gap.
  * Base trail: 1.5% off the prior peak, only after 20 minutes held.
  * Ratchet: 0.5% trail when *current* profit is already >= +1.5%.
    Take-profit is checked first and uses the same +1.5% level, so the
    ratchet almost never fires. The width selector is still live.
  * EOD at 14:58 CT (close-2). Flat or red, or green but not higher than
    ~3 minutes ago, is sold. Green and still rising carries overnight.
    The next RTH open is a real fill, so the gap is in the P&L.

Within a bar the order matches the V10.67 replica: open gap, then the low
against the stop and the trail (prior peak), then the high against TP.
A low that hits the trail before the high tags TP is a trail, not a winner.

Entry gates (all must pass; scan is SYMBOLS order, one fill per minute):
  RSI(9) > 62, or > 72 for a TRUMP_THEME name when sector health is WEAK;
  MACD line above signal; price above the 20-bar short MA; at least one
  of five confluence legs; recipe avoid-hours/days; 08:30–09:00 CT opening
  wall; 09:05–09:30 CT band_0905; no buys at or after 14:55 CT (close-5);
  SPY 5-bar momentum > 0; VIX proxy > 25 blocks; regime score >= 3 blocks;
  48h earnings blackout; 30-minute per-symbol cooldown; at most 3 open
  positions.

Costs are a per-symbol half-spread on the entry and on the exit, not a
flat 5 bps. Quote samples win, then the Roll estimator on closes, then
FALLBACK_HALF_SPREAD (wider for SPCX, GEO, CXW, CLSK, MARA, MSTR, SMCI).

Walk-forward: one continuous book. An entry before the 75% timestamp is
in-sample. An entry at or after it is the holdout. Holdout rows are not
written (see rows_for_db). hold_time_min is clock minutes between the
entry and exit timestamps, including the overnight gap.

Fingerprints use trade_id prefix bt_v4_. Live BerserkerMemory.run_analysis
includes trade_id LIKE 'bt_%', so these rows still feed that gate. The
v4 tag is what a later Trading-bot change can require in order to ignore
pre-v4 rows. This repo does not modify Trading-bot.
"""

from __future__ import annotations

import math
import secrets
from collections import deque
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

CT = ZoneInfo("America/Chicago")

ENGINE_VERSION = "v4"
TRADE_ID_PREFIX = "bt_v4_"

# Live BerserkerMemory.run_analysis keeps trade_id LIKE 'bt_%'.
# bt_v4_ matches that predicate on purpose.
LIVE_BT_GATE_LIKE = "bt_%"

RSI_PERIOD = 9
RSI_BUY_TRIGGER = 62.0
RSI_BUY_TRUMP_WEAK = 72.0
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
SHORT_MA_BARS = 20          # "price above the short MA"
CONFLUENCE_MIN = 1
WARMUP_BARS = 50

TAKE_PROFIT_PCT = 0.015
STOP_LOSS_PCT = 0.010
SL_BY_SYMBOL = {"SPCX": 0.015}
TRAILING_STOP = 0.015       # 1.5% off the peak
RATCHET_PROFIT = 0.015      # arm the tight trail only once profit is here
RATCHET_TRAIL_TIGHT = 0.005
MIN_HOLD_MINUTES = 20.0
COOLDOWN_MINUTES = 30.0
MAX_POSITIONS = 3

VIX_BLOCK_THRESHOLD = 25.0
EARNINGS_BUFFER_H = 48.0
REGIME_BLOCK_SCORE = 3
HOLDOUT_FRACTION = 0.75     # first 75% in-sample, last 25% true holdout

# Clock minutes, America/Chicago. RTH is 08:30–15:00 CT (09:30–16:00 ET).
RTH_OPEN = 8 * 60 + 30
OPENING_WALL_END = 9 * 60          # [08:30, 09:00)
BAND_0905_LO = 9 * 60 + 5          # [09:05, 09:30)
BAND_0905_HI = 9 * 60 + 30
ENTRY_CUTOFF = 14 * 60 + 55        # buys stop at close-5
EOD_MINUTE = 14 * 60 + 58          # flatten / carry decision at close-2
RTH_CLOSE = 15 * 60

THIN_SYMBOLS = frozenset({"SPCX", "GEO", "CXW", "CLSK", "MARA", "MSTR", "SMCI"})

# One-side half-spread used when neither a quote sample nor the Roll
# estimator is available. Wider names are the thin set above.
# 0.0020 = 20 bps half-spread = 40 bps round trip.
FALLBACK_HALF_SPREAD = {
    "AAPL": 0.0002,
    "NVDA": 0.0003,
    "TSLA": 0.0004,
    "PLTR": 0.0005,
    "NUE":  0.0008,
    "MSTR": 0.0010,
    "MARA": 0.0012,
    "CLSK": 0.0012,
    "SMCI": 0.0012,
    "GEO":  0.0015,
    "CXW":  0.0015,
    "SPCX": 0.0020,
}
FALLBACK_HALF_SPREAD_DEFAULT = 0.0010

# Gates the Sunday report scores on the holdout book. P&L is in percent
# (0.15 means +0.15%). Drawdown is percentage points of the cumulative
# sum of per-trade net P&L, ordered by exit time. Each trade is one unit
# of notional, not a compounded full-account bet.
GATE_AVG_PNL = 0.15
GATE_PROFIT_FACTOR = 1.15
GATE_MAX_DRAWDOWN = 8.0
GATE_MAX_SYMBOL_SHARE = 0.25

CONFLUENCE_LEGS = (
    "momentum",
    "ema9_above_ema21",
    "macd_hist_accel",
    "bouncing",
    "above_session_vwap",
)


def trade_id() -> str:
    return TRADE_ID_PREFIX + secrets.token_hex(8)


def to_utc(ts) -> datetime:
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def to_ct(ts) -> datetime:
    return to_utc(ts).astimezone(CT)


def minute_of_day(ts) -> int:
    ct = to_ct(ts)
    return ct.hour * 60 + ct.minute


def in_rth(ts) -> bool:
    ct = to_ct(ts)
    if ct.weekday() >= 5:
        return False
    minute = ct.hour * 60 + ct.minute
    return RTH_OPEN <= minute < RTH_CLOSE


def clock_entry_block(minute: int) -> Optional[str]:
    """Why this clock minute cannot open a new long, or None if it can."""
    if minute < RTH_OPEN or minute >= RTH_CLOSE:
        return "outside_rth"
    if RTH_OPEN <= minute < OPENING_WALL_END:
        return "opening_wall"
    if BAND_0905_LO <= minute < BAND_0905_HI:
        return "band_0905"
    if minute >= ENTRY_CUTOFF:
        return "close_minus_5"
    return None


def required_rsi(is_trump: bool, sector_health: str) -> float:
    if is_trump and sector_health == "WEAK":
        return RSI_BUY_TRUMP_WEAK
    return RSI_BUY_TRIGGER


def sl_pct_for(symbol: str, recipes: Optional[dict] = None) -> float:
    if recipes and symbol in recipes and "sl" in recipes[symbol]:
        return float(recipes[symbol]["sl"])
    return SL_BY_SYMBOL.get(symbol, STOP_LOSS_PCT)


def tp_pct_for(symbol: str, recipes: Optional[dict] = None) -> float:
    if recipes and symbol in recipes and "tp" in recipes[symbol]:
        return float(recipes[symbol]["tp"])
    return TAKE_PROFIT_PCT


def trail_width(current_profit: float) -> float:
    """Live ratchet: tight 0.5% trail only while current profit >= +1.5%.

    Take-profit sits on the same +1.5% level and is checked first, so this
    tight width is usually preempted. Arming uses current profit, not the
    peak (main.py V10.67).
    """
    if current_profit >= RATCHET_PROFIT:
        return RATCHET_TRAIL_TIGHT
    return TRAILING_STOP


def regime_score(
    *,
    vix: float,
    spy_above_short_ma: bool,
    spy_momentum: float,
    sector_health: str,
    qqq_above_short_ma: bool,
    spy_rsi: float,
) -> int:
    """Bar-visible regime score. Entries block at REGIME_BLOCK_SCORE (3).

    Live also adds circuit-breaker and consecutive-stop state. Those are
    account facts, not in the bars, so this score under-blocks versus a
    session that has already tripped the breaker. Weights:
      +2  VIX proxy above 25
      +1  SPY under its short MA
      +1  SPY 5-bar momentum <= 0
      +1  sector health WEAK
      +1  QQQ under its short MA
      +1  SPY RSI(9) under 40
    The hard VIX>25 entry block is separate and still applies at +2.
    """
    score = 0
    if vix > VIX_BLOCK_THRESHOLD:
        score += 2
    if not spy_above_short_ma:
        score += 1
    if spy_momentum <= 0:
        score += 1
    if sector_health == "WEAK":
        score += 1
    if not qqq_above_short_ma:
        score += 1
    if spy_rsi < 40:
        score += 1
    return score


def is_earnings_blocked(dt: datetime, earnings_dates: Iterable[datetime],
                        buffer_h: float = EARNINGS_BUFFER_H) -> bool:
    """True when dt is within buffer_h of any earnings timestamp."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    window = buffer_h * 3600.0
    for ed in earnings_dates:
        if ed is None:
            continue
        if hasattr(ed, "to_pydatetime"):
            ed = ed.to_pydatetime()
        if not isinstance(ed, datetime):
            ed = datetime.combine(ed, datetime.min.time())
        if ed.tzinfo is None:
            ed = ed.replace(tzinfo=timezone.utc)
        if abs((dt - ed).total_seconds()) <= window:
            return True
    return False


def compute_rsi(prices: Sequence[float], period: int = RSI_PERIOD) -> float:
    s = pd.Series(list(prices))
    delta = s.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return float((100 - (100 / (1 + rs))).iloc[-1])


def compute_macd(prices: Sequence[float]):
    s = pd.Series(list(prices))
    ema_fast = s.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = s.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line.iloc[-1], signal_line.iloc[-1], histogram


def compute_sector_health(trump_prices: Dict[str, Sequence[float]],
                          trump_theme: Sequence[str]) -> str:
    """WEAK when a majority of the TRUMP basket is under its 20-bar average."""
    down = 0
    seen = 0
    for sym in trump_theme:
        prices = trump_prices.get(sym)
        if prices and len(prices) >= SHORT_MA_BARS:
            seen += 1
            window = list(prices)[-SHORT_MA_BARS:]
            ma = sum(window) / SHORT_MA_BARS
            if window[-1] < ma:
                down += 1
    if seen == 0:
        return "STRONG"
    return "WEAK" if down > len(list(trump_theme)) / 2 else "STRONG"


def confluence_count(prices: Sequence[float], macd_hist, session_vwap: Optional[float]) -> int:
    """How many of the five confluence legs are true. Entry needs >= 1."""
    prices_l = list(prices)
    legs = 0
    if len(prices_l) >= 6:
        mom_recent = prices_l[-1] - prices_l[-6]
        mom_prior = prices_l[-6] - prices_l[-11] if len(prices_l) >= 11 else 0.0
        if mom_recent > 0 and mom_recent > mom_prior:
            legs += 1
    if len(prices_l) >= 21:
        s = pd.Series(prices_l)
        ema9 = float(s.ewm(span=9, adjust=False).mean().iloc[-1])
        ema21 = float(s.ewm(span=21, adjust=False).mean().iloc[-1])
        if ema9 > ema21:
            legs += 1
    if macd_hist is not None and len(macd_hist) >= 2:
        if float(macd_hist.iloc[-1]) > float(macd_hist.iloc[-2]):
            legs += 1
    if len(prices_l) >= 4 and prices_l[-1] > prices_l[-4]:
        legs += 1
    if session_vwap is not None and session_vwap > 0 and prices_l[-1] > session_vwap:
        legs += 1
    return legs


def get_signals_bt(prices: Sequence[float], sector_health: str, is_trump: bool,
                   session_vwap: Optional[float] = None) -> dict:
    """Base gates plus 1-of-5 confluence. buy is False until warmup is done."""
    if len(prices) < max(RSI_PERIOD + 1, MACD_SLOW + MACD_SIGNAL):
        return {"buy": False}
    price = list(prices)[-1]
    rsi = compute_rsi(prices)
    macd_val, macd_sig, macd_hist = compute_macd(prices)
    macd_bullish = bool(macd_val > macd_sig)
    ma = sum(list(prices)[-SHORT_MA_BARS:]) / SHORT_MA_BARS
    above_ma = price > ma
    rsi_need = required_rsi(is_trump, sector_health)
    if not (rsi > rsi_need and macd_bullish and above_ma):
        return {
            "buy": False,
            "rsi": round(rsi, 2) if rsi == rsi else 50.0,
            "macd_bull": macd_bullish,
            "above_ma": above_ma,
            "confluence": 0,
        }
    legs = confluence_count(prices, macd_hist, session_vwap)
    return {
        "buy": legs >= CONFLUENCE_MIN,
        "rsi": round(rsi, 2),
        "macd_bull": macd_bullish,
        "above_ma": above_ma,
        "confluence": legs,
    }


def vix_from_spy(closes: Sequence[float], prev_smooth: float) -> float:
    """Annualized 30-bar SPY realized vol, smoothed 0.7/0.3 like V3.2."""
    if len(closes) < 31:
        return prev_smooth
    window = list(closes)[-31:]
    rets = [math.log(window[j] / window[j - 1])
            for j in range(1, len(window)) if window[j - 1] > 0]
    if len(rets) < 2:
        return prev_smooth
    mu = sum(rets) / len(rets)
    var = sum((r - mu) ** 2 for r in rets) / (len(rets) - 1)
    raw = (var ** 0.5) * ((390 * 252) ** 0.5) * 100
    return prev_smooth * 0.7 + raw * 0.3


def spy_context(closes: Sequence[float]) -> dict:
    if len(closes) < SHORT_MA_BARS:
        return {
            "above_ma": False,
            "bullish": False,
            "rsi": 50.0,
            "momentum": 0.0,
            "ready": False,
        }
    prices = list(closes)
    ma = sum(prices[-SHORT_MA_BARS:]) / SHORT_MA_BARS
    above = prices[-1] > ma
    rsi = compute_rsi(prices) if len(prices) > RSI_PERIOD else 50.0
    if rsi != rsi:  # NaN
        rsi = 50.0
    mom = 0.0
    if len(prices) >= 6 and prices[-6] > 0:
        mom = (prices[-1] - prices[-6]) / prices[-6]
    return {
        "above_ma": above,
        "bullish": above and mom > 0,
        "rsi": float(rsi),
        "momentum": float(mom),
        "ready": True,
    }


def roll_half_spread(closes: Sequence[float]) -> Optional[float]:
    """Roll (1984) half-spread from close-to-close log returns.

    Returns None when the serial covariance is not negative (the estimator
    is undefined) or the result is outside 0.5 bps .. 2%.
    """
    if len(closes) < 40:
        return None
    rets = []
    for i in range(1, len(closes)):
        prev, cur = closes[i - 1], closes[i]
        if prev > 0 and cur > 0:
            rets.append(math.log(cur / prev))
    if len(rets) < 40:
        return None
    x = rets[1:]
    y = rets[:-1]
    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y)) / (n - 1)
    if cov >= 0:
        return None
    full = 2.0 * math.sqrt(-cov)
    half = full / 2.0
    if half < 0.00005 or half > 0.02:
        return None
    return half


def resolve_costs(
    symbols: Sequence[str],
    closes_by_symbol: Dict[str, Sequence[float]],
    quote_half: Optional[Dict[str, float]] = None,
) -> Dict[str, Tuple[float, str]]:
    """Per-symbol (half_spread, source). quote > Roll-on-bars > table."""
    quote_half = quote_half or {}
    out: Dict[str, Tuple[float, str]] = {}
    for sym in symbols:
        quoted = quote_half.get(sym)
        if quoted is not None and quoted > 0:
            out[sym] = (float(quoted), "quote")
            continue
        rolled = roll_half_spread(list(closes_by_symbol.get(sym) or []))
        if rolled is not None:
            out[sym] = (rolled, "bar_roll")
            continue
        out[sym] = (FALLBACK_HALF_SPREAD.get(sym, FALLBACK_HALF_SPREAD_DEFAULT), "table")
    return out


def net_pnl_pct(entry_fill: float, exit_raw: float, half_spread: float) -> float:
    """Percent P&L after the exit half-spread. entry_fill already includes entry cost."""
    if entry_fill <= 0:
        return 0.0
    exit_fill = exit_raw * (1.0 - half_spread)
    return (exit_fill - entry_fill) / entry_fill * 100.0


def new_position(entry_ts: datetime, entry_raw: float, half_spread: float,
                 tp: float, sl: float) -> dict:
    fill = entry_raw * (1.0 + half_spread)
    return {
        "entry_ts": to_utc(entry_ts),
        "entry_raw": float(entry_raw),
        "entry_fill": float(fill),
        "half_spread": float(half_spread),
        "tp": float(tp),
        "sl": float(sl),
        "peak": float(entry_raw),
        "closes": [float(entry_raw)],
        "mfe": 0.0,
        "mae": 0.0,
        "skip_date": None,
        "carried": False,
    }


def _finish(state: dict, reason: str, exit_raw: float, ts: datetime) -> dict:
    held = (to_utc(ts) - state["entry_ts"]).total_seconds() / 60.0
    return {
        "reason": reason,
        "exit_raw": float(exit_raw),
        "ts": to_utc(ts),
        "hold_min": held,
    }


def step_position(state: dict, ts: datetime, open_: float, high: float,
                  low: float, close: float) -> Optional[dict]:
    """Advance one open position by one RTH bar.

    Returns an exit dict, or None if the position stays open (including
    an overnight carry). Mutates peak, excursion, and the session-close
    buffer. The entry bar itself must not be passed in.
    """
    ct = to_ct(ts)
    if state["skip_date"] is not None and ct.date() == state["skip_date"]:
        return None

    entry_fill = state["entry_fill"]
    entry_raw = state["entry_raw"]
    tp_px = entry_fill * (1.0 + state["tp"])
    sl_px = entry_fill * (1.0 - state["sl"])
    peak = state["peak"]
    held = (to_utc(ts) - state["entry_ts"]).total_seconds() / 60.0

    state["mfe"] = max(state["mfe"], (high - entry_fill) / entry_fill)
    state["mae"] = min(state["mae"], (low - entry_fill) / entry_fill)

    def trail_level(ref_price: float, peak_px: float) -> float:
        profit = ref_price / entry_fill - 1.0
        width = trail_width(profit)
        return peak_px * (1.0 - width)

    # 1. Gap through at the open. A stop (or trail) gap fills at the open.
    if open_ >= tp_px:
        return _finish(state, "take-profit", open_, ts)
    if open_ <= sl_px:
        return _finish(state, "stop-loss", open_, ts)
    if held >= MIN_HOLD_MINUTES:
        trail_px = trail_level(open_, peak)
        if trail_px > sl_px and open_ <= trail_px:
            return _finish(state, "trailing-stop", open_, ts)

    # 2. Low versus the hard stop, then the trail off the PRIOR peak.
    if low <= sl_px:
        return _finish(state, "stop-loss", sl_px, ts)
    if held >= MIN_HOLD_MINUTES:
        trail_px = trail_level(low, peak)
        if trail_px > sl_px and low <= trail_px:
            return _finish(state, "trailing-stop", trail_px, ts)

    # 3. High updates the peak and can take profit.
    state["peak"] = max(peak, high)
    if high >= tp_px:
        return _finish(state, "take-profit", tp_px, ts)

    state["closes"].append(float(close))

    # 4. Close-2. Carry only when the position is green on the fill and
    #    the close is higher than the close ~3 minutes ago.
    if ct.hour * 60 + ct.minute >= EOD_MINUTE:
        profit = close / entry_fill - 1.0
        closes = state["closes"]
        rising = len(closes) >= 4 and close > closes[-4]
        if profit > 0 and rising:
            state["skip_date"] = ct.date()
            state["carried"] = True
            return None
        return _finish(state, "eod-autoclose", close, ts)
    return None


def holdout_cutoff_epoch(sorted_epochs: Sequence[int]) -> int:
    """First holdout timestamp. Entries at or after this epoch are OOS."""
    if not sorted_epochs:
        return 0
    idx = int(len(sorted_epochs) * HOLDOUT_FRACTION)
    idx = min(idx, len(sorted_epochs) - 1)
    return int(sorted_epochs[idx])


def rows_for_db(trades: Sequence[dict]) -> List[dict]:
    """In-sample rows only. Holdout trades stay in the report and off the gate."""
    return [t for t in trades if not t.get("holdout")]


def _bars_from_obj(obj) -> List[tuple]:
    """Normalize a DataFrame or a list of tuples to (utc_dt, o, h, l, c, v)."""
    if obj is None:
        return []
    if isinstance(obj, list):
        out = []
        for row in obj:
            ts, o, h, l, c = row[:5]
            v = float(row[5]) if len(row) > 5 and row[5] is not None else 0.0
            out.append((to_utc(ts), float(o), float(h), float(l), float(c), v))
        return out
    # DataFrame path (Alpaca minute bars).
    df = obj
    if hasattr(df.index, "levels"):
        # caller should already have sliced; leave a single symbol frame
        pass
    idx = df.index
    opens = df["open"].to_numpy()
    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()
    closes = df["close"].to_numpy()
    vols = df["volume"].to_numpy() if "volume" in df.columns else [0.0] * len(df)
    out = []
    for i in range(len(df)):
        out.append((
            to_utc(idx[i]),
            float(opens[i]),
            float(highs[i]),
            float(lows[i]),
            float(closes[i]),
            float(vols[i]) if vols[i] == vols[i] else 0.0,
        ))
    return out


def _index_rth(rows: List[tuple]) -> Dict[int, tuple]:
    """Map epoch-second -> bar, RTH only. Later duplicate minute wins."""
    out = {}
    for ts, o, h, l, c, v in rows:
        if not in_rth(ts):
            continue
        if not (c > 0 and h > 0 and l > 0 and o > 0):
            continue
        out[int(ts.timestamp())] = (ts, o, h, l, c, v)
    return out


def replay(
    all_bars: dict,
    earnings_map: Optional[Dict[str, List[datetime]]] = None,
    *,
    symbols: Sequence[str],
    recipes: Optional[dict] = None,
    trump_theme: Sequence[str],
    costs: Optional[Dict[str, Tuple[float, str]]] = None,
    quote_half: Optional[Dict[str, float]] = None,
    signal_fn: Optional[Callable] = None,
    validate_mode: bool = False,
) -> List[dict]:
    """Continuous replay. validate_mode=True returns only holdout entries.

    all_bars maps symbol -> DataFrame or list of (ts, o, h, l, c[, v]).
    Include SPY and QQQ. Scan order is `symbols` (live universe order).
    signal_fn(symbol, prices, sector_health, is_trump, session_vwap) may
    replace get_signals_bt in tests.
    """
    earnings_map = earnings_map or {}
    recipes = recipes or {}
    indexed: Dict[str, Dict[int, tuple]] = {}
    for sym, obj in all_bars.items():
        indexed[sym] = _index_rth(_bars_from_obj(obj))

    epochs = set()
    for sym in list(symbols) + ["SPY", "QQQ"]:
        epochs.update(indexed.get(sym, {}))
    timeline = sorted(epochs)
    cutoff = holdout_cutoff_epoch(timeline)

    if costs is None:
        closes = {}
        for sym in symbols:
            closes[sym] = [bar[4] for _, bar in sorted(indexed.get(sym, {}).items())]
        costs = resolve_costs(symbols, closes, quote_half=quote_half)

    price_hist: Dict[str, deque] = {
        s: deque(maxlen=120) for s in list(symbols) + ["SPY", "QQQ"]
    }
    vwap_state: Dict[str, tuple] = {s: (None, 0.0, 0.0) for s in symbols}
    in_pos = {s: False for s in symbols}
    pos: Dict[str, dict] = {}
    last_exit: Dict[str, Optional[datetime]] = {s: None for s in symbols}
    entry_meta: Dict[str, dict] = {}
    vix_smooth = 15.0
    trades: List[dict] = []

    def session_vwap(sym: str, ts: datetime, high: float, low: float,
                     close: float, volume: float) -> Optional[float]:
        day, cum_pv, cum_v = vwap_state[sym]
        ct_day = to_ct(ts).date()
        if day != ct_day:
            cum_pv, cum_v = 0.0, 0.0
            day = ct_day
        if volume > 0:
            typical = (high + low + close) / 3.0
            cum_pv += typical * volume
            cum_v += volume
        vwap_state[sym] = (day, cum_pv, cum_v)
        if cum_v <= 0:
            return None
        return cum_pv / cum_v

    def close_trade(sym: str, event: dict, sector_health: str):
        st = pos[sym]
        meta = entry_meta[sym]
        half, source = costs.get(sym, (FALLBACK_HALF_SPREAD_DEFAULT, "table"))
        pnl = net_pnl_pct(st["entry_fill"], event["exit_raw"], half)
        exit_fill = event["exit_raw"] * (1.0 - half)
        entry_ct = to_ct(st["entry_ts"])
        exit_ct = to_ct(event["ts"])
        overnight = exit_ct.date() > entry_ct.date()
        holdout = int(st["entry_ts"].timestamp()) >= cutoff
        trades.append({
            "trade_id": trade_id(),
            "symbol": sym,
            "entry_price": round(st["entry_fill"], 4),
            "exit_price": round(exit_fill, 4),
            "pnl_pct": round(pnl, 6),
            "exit_reason": event["reason"],
            "hold_min": int(round(event["hold_min"])),
            "hold_min_exact": event["hold_min"],
            "won": pnl > 0,
            "rsi_at_entry": meta.get("rsi", 50.0),
            "spy_bullish": meta.get("spy_bullish", False),
            "spy_rsi": meta.get("spy_rsi", 50.0),
            "spy_momentum": meta.get("spy_momentum", 0.0),
            "sector_health": meta.get("sector_health", sector_health),
            "is_trump": sym in trump_theme,
            "sector": "TRUMP" if sym in trump_theme else "TECH",
            "hour_cdt": entry_ct.hour,
            "day_of_week": entry_ct.weekday(),
            "mfe": round(st["mfe"] * 100.0, 3),
            "mae": round(st["mae"] * 100.0, 3),
            "vix_at_entry": meta.get("vix", vix_smooth),
            "validate": holdout,
            "holdout": holdout,
            "tp_used": st["tp"],
            "sl_used": st["sl"],
            "entry_ts_ep": int(st["entry_ts"].timestamp()),
            "exit_ts_ep": int(event["ts"].timestamp()),
            "overnight": overnight,
            "thin": sym in THIN_SYMBOLS,
            "half_spread": half,
            "cost_source": source,
            "engine": ENGINE_VERSION,
            "carried": bool(st["carried"] or overnight),
        })
        in_pos[sym] = False
        pos.pop(sym, None)
        entry_meta.pop(sym, None)
        last_exit[sym] = event["ts"]

    for epoch in timeline:
        # Ingest every symbol that printed, then manage exits, then one entry.
        printed: Dict[str, tuple] = {}
        for sym in list(symbols) + ["SPY", "QQQ"]:
            bar = indexed.get(sym, {}).get(epoch)
            if bar is None or sym not in price_hist:
                continue
            ts, o, h, l, c, v = bar
            price_hist[sym].append(c)
            printed[sym] = bar
            if sym in vwap_state:
                session_vwap(sym, ts, h, l, c, v)

        if not printed:
            continue
        ts0 = datetime.fromtimestamp(epoch, timezone.utc)
        ct = to_ct(ts0)
        minute = ct.hour * 60 + ct.minute

        vix_smooth = vix_from_spy(price_hist["SPY"], vix_smooth)
        spy = spy_context(price_hist["SPY"])
        qqq = spy_context(price_hist["QQQ"])
        sector = compute_sector_health(
            {s: price_hist[s] for s in trump_theme}, trump_theme
        )
        score = regime_score(
            vix=vix_smooth,
            spy_above_short_ma=spy["above_ma"],
            spy_momentum=spy["momentum"],
            sector_health=sector,
            qqq_above_short_ma=qqq["above_ma"],
            spy_rsi=spy["rsi"],
        )

        for sym in symbols:
            if not in_pos[sym]:
                continue
            bar = printed.get(sym)
            if bar is None and minute >= EOD_MINUTE:
                last_px = price_hist[sym][-1] if price_hist[sym] else None
                if last_px is None:
                    continue
                bar = (ts0, last_px, last_px, last_px, last_px, 0.0)
            if bar is None:
                continue
            ts, o, h, l, c, _v = bar
            event = step_position(pos[sym], ts, o, h, l, c)
            if event:
                close_trade(sym, event, sector)

        # One new entry per minute, live scan order, after exits free a slot.
        if sum(1 for s in symbols if in_pos[s]) >= MAX_POSITIONS:
            continue
        block = clock_entry_block(minute)
        if block:
            continue
        if vix_smooth > VIX_BLOCK_THRESHOLD:
            continue
        if not spy["ready"] or spy["momentum"] <= 0:
            continue
        if score >= REGIME_BLOCK_SCORE:
            continue

        for sym in symbols:
            if in_pos[sym]:
                continue
            if sum(1 for s in symbols if in_pos[s]) >= MAX_POSITIONS:
                break
            bar = printed.get(sym)
            if bar is None:
                continue
            if len(price_hist[sym]) < WARMUP_BARS:
                continue
            recipe = recipes.get(sym, {})
            if ct.hour in recipe.get("avoid_hours", []):
                continue
            if ct.weekday() in recipe.get("avoid_days", []):
                continue
            if last_exit[sym] is not None:
                gap_min = (ts0 - last_exit[sym]).total_seconds() / 60.0
                if gap_min < COOLDOWN_MINUTES:
                    continue
            if is_earnings_blocked(ts0, earnings_map.get(sym, [])):
                continue
            ts, o, h, l, c, v = bar
            _day, cum_pv, cum_v = vwap_state[sym]
            vwap = (cum_pv / cum_v) if cum_v > 0 else None
            is_trump = sym in trump_theme
            if signal_fn is not None:
                sig = signal_fn(sym, price_hist[sym], sector, is_trump, vwap)
            else:
                sig = get_signals_bt(price_hist[sym], sector, is_trump, vwap)
            if not sig.get("buy"):
                continue
            half, _source = costs.get(sym, (FALLBACK_HALF_SPREAD_DEFAULT, "table"))
            opened = new_position(
                ts, c, half, tp_pct_for(sym, recipes), sl_pct_for(sym, recipes)
            )
            in_pos[sym] = True
            pos[sym] = opened
            entry_meta[sym] = {
                "rsi": sig.get("rsi", 50.0),
                "spy_bullish": spy["bullish"],
                "spy_rsi": spy["rsi"],
                "spy_momentum": spy["momentum"],
                "sector_health": sector,
                "vix": round(vix_smooth, 1),
            }
            break  # one new entry per scan step

    # Data ended. Flatten anything still open at the last print. This is
    # not a live EOD; the reason is distinct so the mix shows it.
    for sym in symbols:
        if not in_pos[sym] or not price_hist[sym]:
            continue
        last_epoch = max(indexed.get(sym, {}), default=None)
        if last_epoch is None:
            continue
        ts, _o, _h, _l, c, _v = indexed[sym][last_epoch]
        event = _finish(pos[sym], "end-of-data", c, ts)
        # If the last bar was already consumed as a carry, still flatten
        # so a holdout window cannot hide an open winner.
        if event["ts"] <= pos[sym]["entry_ts"]:
            continue
        close_trade(sym, event, "STRONG")

    if validate_mode:
        return [t for t in trades if t["holdout"]]
    return trades


def _profit_factor(pnls: Sequence[float]) -> float:
    wins = sum(p for p in pnls if p > 0)
    losses = sum(p for p in pnls if p < 0)
    if losses < 0:
        return wins / abs(losses)
    if wins > 0:
        return float("inf")
    return 0.0


def _max_drawdown(pnls_exit_order: Sequence[float]) -> float:
    """Peak-to-trough of the cumulative sum, in percentage points."""
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for p in pnls_exit_order:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > worst:
            worst = dd
    return worst


def summarize(trades: Sequence[dict]) -> dict:
    """Metrics the Sunday report and the PASS/FAIL gates both use."""
    rows = list(trades)
    pnls = [float(t["pnl_pct"]) for t in rows]
    n = len(rows)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    by_exit: Dict[str, int] = {}
    by_symbol: Dict[str, float] = {}
    by_symbol_n: Dict[str, int] = {}
    liquid = []
    thin = []
    overnight = []
    same_day = []
    for t in rows:
        by_exit[t["exit_reason"]] = by_exit.get(t["exit_reason"], 0) + 1
        by_symbol[t["symbol"]] = by_symbol.get(t["symbol"], 0.0) + float(t["pnl_pct"])
        by_symbol_n[t["symbol"]] = by_symbol_n.get(t["symbol"], 0) + 1
        (thin if t.get("thin") else liquid).append(float(t["pnl_pct"]))
        (overnight if t.get("overnight") else same_day).append(float(t["pnl_pct"]))
    ordered = sorted(rows, key=lambda t: (t.get("exit_ts_ep") or 0, t["symbol"]))
    total = sum(pnls)
    if total > 0 and by_symbol:
        max_share = max(by_symbol.values()) / total
        max_sym = max(by_symbol, key=lambda s: by_symbol[s])
    else:
        max_share = None
        max_sym = None
    return {
        "n": n,
        "wins": len(wins),
        "losses": n - len(wins),
        "avg_pnl": (total / n) if n else 0.0,
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
        "profit_factor": _profit_factor(pnls) if n else 0.0,
        "max_drawdown": _max_drawdown([float(t["pnl_pct"]) for t in ordered]) if n else 0.0,
        "total_pnl": total,
        "max_symbol_share": max_share,
        "max_symbol": max_sym,
        "by_exit": by_exit,
        "by_symbol": by_symbol,
        "by_symbol_n": by_symbol_n,
        "liquid_avg": (sum(liquid) / len(liquid)) if liquid else None,
        "liquid_n": len(liquid),
        "thin_avg": (sum(thin) / len(thin)) if thin else None,
        "thin_n": len(thin),
        "overnight_avg": (sum(overnight) / len(overnight)) if overnight else None,
        "overnight_n": len(overnight),
        "sameday_avg": (sum(same_day) / len(same_day)) if same_day else None,
        "sameday_n": len(same_day),
    }


def evaluate_gates(summary: dict) -> dict:
    """PASS only when every holdout gate clears. Empty book is FAIL."""
    n = summary["n"]
    avg_ok = n > 0 and summary["avg_pnl"] >= GATE_AVG_PNL
    pf = summary["profit_factor"]
    pf_ok = n > 0 and pf >= GATE_PROFIT_FACTOR
    dd_ok = n > 0 and summary["max_drawdown"] <= GATE_MAX_DRAWDOWN
    share = summary["max_symbol_share"]
    share_ok = n > 0 and share is not None and share <= GATE_MAX_SYMBOL_SHARE
    return {
        "avg_pnl": avg_ok,
        "profit_factor": pf_ok,
        "max_drawdown": dd_ok,
        "symbol_share": share_ok,
        "pass": bool(avg_ok and pf_ok and dd_ok and share_ok),
    }


def _fmt_pf(pf: float) -> str:
    if pf == float("inf"):
        return "inf"
    return f"{pf:.2f}"


def _fmt_avg(v: Optional[float], n: int) -> str:
    if v is None or n == 0:
        return f"n=0"
    return f"n={n}  avg {v:+.3f}%"


def format_report(trades: Sequence[dict], *, label: str, verdict: bool) -> str:
    s = summarize(trades)
    g = evaluate_gates(s)
    lines = [
        "",
        "=" * 64,
        f"BERSERKER BACKTEST V4 — {label}",
        "=" * 64,
        f"Trades: {s['n']} | {s['wins']}W {s['losses']}L",
        f"Avg P&L: {s['avg_pnl']:+.3f}%   avg win {s['avg_win']:+.3f}%   "
        f"avg loss {s['avg_loss']:+.3f}%",
        f"Profit factor: {_fmt_pf(s['profit_factor'])}   "
        f"max drawdown: {s['max_drawdown']:.2f} pp "
        f"(cumsum of per-trade net P&L)",
        "",
        f"{'Symbol':<8} {'N':>5} {'TotalPnL':>10} {'Avg':>8} {'Share':>8}",
        "-" * 44,
    ]
    total = s["total_pnl"]
    for sym in sorted(s["by_symbol"], key=lambda k: -s["by_symbol"][k]):
        n = s["by_symbol_n"][sym]
        tot = s["by_symbol"][sym]
        share = (tot / total * 100.0) if total > 0 else float("nan")
        share_s = f"{share:5.1f}%" if share == share else "  n/a"
        lines.append(
            f"{sym:<8} {n:5d} {tot:+10.2f}% {tot / n:+7.3f}% {share_s:>8}"
        )
    lines += ["", "Exit reasons:"]
    for reason, n in sorted(s["by_exit"].items(), key=lambda kv: -kv[1]):
        lines.append(f"  {reason:<18} {n:5d}")
    lines += [
        "",
        f"Liquid:    {_fmt_avg(s['liquid_avg'], s['liquid_n'])}",
        f"Thin:      {_fmt_avg(s['thin_avg'], s['thin_n'])}  "
        f"({', '.join(sorted(THIN_SYMBOLS))})",
        f"Same-day:  {_fmt_avg(s['sameday_avg'], s['sameday_n'])}",
        f"Overnight: {_fmt_avg(s['overnight_avg'], s['overnight_n'])}",
        "",
    ]
    if verdict:
        def mark(ok: bool) -> str:
            return "PASS" if ok else "FAIL"
        share = s["max_symbol_share"]
        share_s = "n/a" if share is None else f"{share * 100:.1f}%"
        lines += [
            "GATES",
            f"  avg P&L/trade    {s['avg_pnl']:+.3f}%    >= +{GATE_AVG_PNL:.2f}%    {mark(g['avg_pnl'])}",
            f"  profit factor    {_fmt_pf(s['profit_factor']):>8}    >= {GATE_PROFIT_FACTOR:.2f}      {mark(g['profit_factor'])}",
            f"  max drawdown     {s['max_drawdown']:8.2f} pp <= {GATE_MAX_DRAWDOWN:.0f}        {mark(g['max_drawdown'])}",
            f"  max symbol share {share_s:>8}    <= {GATE_MAX_SYMBOL_SHARE * 100:.0f}%       {mark(g['symbol_share'])}",
            "",
            f"VERDICT: {'PASS' if g['pass'] else 'FAIL'}",
        ]
    lines.append("=" * 64)
    return "\n".join(lines)


def split_holdout(trades: Sequence[dict]) -> Tuple[List[dict], List[dict]]:
    ins, oos = [], []
    for t in trades:
        (oos if t.get("holdout") else ins).append(t)
    return ins, oos
