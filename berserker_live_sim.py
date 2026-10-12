#!/usr/bin/env python3
"""
berserker_live_sim.py — Berserker V4 replay engine (Trading-bot main.py V10.68).

Pure module: no database, no Alpaca, no Telegram. nexus_analyzer_1min_railway.py
feeds it one-minute bars. Checked against main.py V10.68 (get_signals,
manage_exits, is_position_still_running, compute_regime_score,
get_spy_regime_and_momentum, get_dynamic_tp, get_sorted_symbols).

What the bars can match
-----------------------
RSI(9) Wilder, MACD 12/26/9, price above the 20-sample average, confluence
legs 1-4, recipe avoid-hours/days, the 08:30-09:00 and 09:05-09:30 entry
walls (per symbol, so a later name can still fill), buys stopped at 14:55,
SPY regime (last vs 20-sample average; BEAR caps the book at 1), the SPY
momentum gate (not falling faster than -0.3% over the lookback), VIX
block at >= 25 and the extreme cap at >= 30, regime shutdown only at
score >= 4, 48h earnings blackout, 30-minute cooldown, one new entry per
step, EOD at 14:58 (green AND last > the print 5 samples back, else flat;
earnings names always flat), TP / SL / 20-minute trail / 0.5% ratchet.

``eod_policy="carry_all"`` is a research replay. Entries are unchanged.
At 14:58 every non-earnings position is carried, green or red. Earnings
still flatten. A carried name keeps its slot, so the next day's entries
can differ from the live policy. The baseline book is still ``"live"``.

What the bars cannot match, and how this replay treats it
---------------------------------------------------------
Live samples last-trade prints about every 30s and keeps 50 of them.
This replay only has 1-minute closes, kept to the same 50-sample deque.
Indicator math matches; the clock each sample covers does not.

The fifth confluence leg is nexus_client.analyst_scores() >= 1. That HTTP
service has no historical tape. A missing score does not block (the live
try/except). This replay passes score 0 on every bar, so the leg never
fires. Legs 1-4 still qualify a buy. Session VWAP is not a leg.

Regime points for the Berserker circuit breaker, the Scanner circuit
breaker, and crypto fear-and-greed are account and service state. They
default off, so a bar-only score is 0-2 (SPY below its average, VIX > 25)
and never reaches the shutdown threshold of 4. Score 3 would only scale
notional; this book is one unit per trade, so percent P&L does not change.

Win Follower HOT/bench order and /focus (10-minute cooldown) need the live
fingerprint table. With those off, live sorts by recently_traded. This
replay does that: a name not in the last 10 exits keeps SYMBOLS order and
scans first.

Dynamic TP (2.0% / 2.5% when a bucket has >= 30 winner MFEs) reads the same
fingerprint table this job would write. In-sample entries use the recipe
TP. At the holdout boundary the replay freezes winner-MFE stats from
in-sample trades already closed and applies get_dynamic_tp only to holdout
entries. Live's online mix of every bt_ row is not copied.

VIX is still the SPY realized-vol proxy (live uses yfinance ^VIX). The
NYSE holiday and early-close calendar is not in the bars (weekday
08:30-15:00, fixed 14:55 / 14:58). strategy_recipes.json can override
TP/SL at boot; hours and days stay the hardcoded recipes, which match.
The win-rate skip inside execute_trade is not applied: it reads production
buckets, and applying it here would be circular.

Exit path inside a minute bar: gap at the open, then the low against the
stop and the trail off the prior peak, then the high against TP. Live
checks one last price at a time and updates the peak before the trail, so
a new high cannot trail on the same print. The prior-peak-then-low order
is the one-minute analogue.
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
SHORT_MA_BARS = 20          # price > sum(last 20) / 20
SECTOR_LOOKBACK = 15        # live: last print < the print 15 samples ago
CONFLUENCE_MIN = 1
# Live price_history maxlen is 50 prints. get_signals itself fires once
# len >= max(RSI_PERIOD+1, 26, MACD_SLOW+MACD_SIGNAL) == 35. WARMUP_BARS
# is that gate (tests patch it). HIST_MAXLEN is the deque.
HIST_MAXLEN = 50
SIGNAL_MIN_BARS = max(RSI_PERIOD + 1, 26, MACD_SLOW + MACD_SIGNAL)
WARMUP_BARS = SIGNAL_MIN_BARS

TAKE_PROFIT_PCT = 0.015
STOP_LOSS_PCT = 0.010
SL_BY_SYMBOL = {"SPCX": 0.015}
TRAILING_STOP = 0.015       # 1.5% off the peak
RATCHET_PROFIT = 0.015      # arm the tight trail only once profit is here
RATCHET_TRAIL_TIGHT = 0.005
MIN_HOLD_MINUTES = 20.0
COOLDOWN_MINUTES = 30.0
MAX_POSITIONS = 3

VIX_BLOCK_THRESHOLD = 25.0     # block new entries when VIX >= 25
VIX_EXTREME_THRESHOLD = 30.0   # also cap the book at 1 (redundant with the block)
SPY_MOMENTUM_GATE = -0.3       # percent; block only if SPY is falling faster
SPY_BEAR_MAX_POS = 1
SPY_MOM_LOOKBACK = 60          # live asks for 60 prints; the deque holds 50
EARNINGS_BUFFER_H = 48.0
REGIME_BLOCK_SCORE = 4         # shutdown. Score 3 does not block Berserker.
HOLDOUT_FRACTION = 0.75        # first 75% in-sample, last 25% true holdout
DYN_TP_MIN_WINNERS = 30
DYN_TP_HI = 0.025
DYN_TP_MID = 0.020
MAX_RECENT = 10                # recently_traded rotation depth

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
# |net P&L| below this fraction of gross absolute P&L is not a
# concentration ratio. The share gate skips instead of failing.
SHARE_MATERIAL_FRACTION = 0.01

CONFLUENCE_LEGS = (
    "momentum",
    "ema9_above_ema21",
    "macd_hist_accel",
    "bouncing",
    "analyst_score",
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
    berserker_cb: bool = False,
    scanner_cb: bool = False,
    fear_greed: Optional[float] = None,
) -> int:
    """V10.68 compute_regime_score. One point each, shutdown at 4.

    +1 Berserker circuit breaker, +1 Scanner circuit breaker,
    +1 crypto fear-and-greed < 25, +1 SPY below its 20-sample average,
    +1 VIX > 25 (strict). Score 3 scales size and still allows Berserker.
    The three service flags default off because a bar replay does not
    have them. SPY-bear plus VIX>25 is therefore 2, under the block.
    """
    score = 0
    if berserker_cb:
        score += 1
    if scanner_cb:
        score += 1
    if fear_greed is not None and float(fear_greed) < 25:
        score += 1
    if not spy_above_short_ma:
        score += 1
    if vix > VIX_BLOCK_THRESHOLD:
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
    """WEAK when a majority of TRUMP_THEME printed lower than 15 samples ago.

    Names with a short tape do not count as down. The denominator is the
    full theme, matching update_sector_health. On these bars, 15 samples
    is 15 minutes; live's 15 prints are about 7.5 minutes.
    """
    theme = list(trump_theme)
    down = 0
    for sym in theme:
        prices = list(trump_prices.get(sym) or [])
        if len(prices) >= SECTOR_LOOKBACK and prices[-1] < prices[-SECTOR_LOOKBACK]:
            down += 1
    if not theme:
        return "STRONG"
    return "WEAK" if down > len(theme) / 2 else "STRONG"


def confluence_count(prices: Sequence[float], macd_hist,
                     analyst_score: int = 0) -> int:
    """How many of the five V10.68 confluence legs are true. Entry needs >= 1.

    Leg 5 is the analyst-service score. Pass 0 when the service has no
    reading for this bar (the live failure path: agreement can add a leg,
    absence never blocks).
    """
    prices_l = list(prices)[-HIST_MAXLEN:]
    legs = 0
    if len(prices_l) >= 10:
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
    try:
        if int(analyst_score) >= 1:
            legs += 1
    except (TypeError, ValueError):
        pass
    return legs


def get_signals_bt(prices: Sequence[float], sector_health: str, is_trump: bool,
                   analyst_score: int = 0) -> dict:
    """Base gates plus 1-of-5 confluence. The fifth leg is the analyst score.

    The price window is the last HIST_MAXLEN samples, matching the live
    deque. analyst_score defaults to 0 (service absent).
    """
    prices_l = list(prices)[-HIST_MAXLEN:]
    if len(prices_l) < SIGNAL_MIN_BARS:
        return {"buy": False}
    price = prices_l[-1]
    rsi = compute_rsi(prices_l)
    macd_val, macd_sig, macd_hist = compute_macd(prices_l)
    macd_bullish = bool(macd_val > macd_sig)
    ma = sum(prices_l[-SHORT_MA_BARS:]) / SHORT_MA_BARS
    above_ma = price > ma
    rsi_need = required_rsi(is_trump, sector_health)
    rsi_out = round(rsi, 2) if rsi == rsi else 50.0
    if not (rsi == rsi and rsi > rsi_need and macd_bullish and above_ma):
        return {
            "buy": False,
            "rsi": rsi_out,
            "macd_bull": macd_bullish,
            "above_ma": above_ma,
            "confluence": 0,
            "analyst_score": int(analyst_score or 0),
        }
    legs = confluence_count(prices_l, macd_hist, analyst_score)
    return {
        "buy": legs >= CONFLUENCE_MIN,
        "rsi": round(rsi, 2),
        "macd_bull": macd_bullish,
        "above_ma": above_ma,
        "confluence": legs,
        "analyst_score": int(analyst_score or 0),
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


def _fingerprint_rsi(prices: Sequence[float]) -> float:
    """Live _get_spy_context_for_fingerprint RSI: Wilder, period min(7, n), last 20."""
    window = list(prices)[-20:]
    if len(window) < 8:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(window)):
        d = window[i] - window[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    period = min(7, len(gains))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    return round(100.0 - (100.0 / (1.0 + ag / al)), 2)


def spy_context(closes: Sequence[float]) -> dict:
    """SPY regime gate plus the separate fingerprint fields.

    Regime and the momentum gate follow get_spy_regime_and_momentum.
    With fewer than 20 samples live returns BULL and momentum_ok.
    The lookback is min(60, n-1) on a deque of 50, so it never actually
    sees 60 samples. These bars are 1 minute, not 30s prints.

    bullish / rsi / momentum on the fingerprint are _get_spy_context_for_fingerprint:
    last > average of up to 20, a 7-period Wilder on those 20, and the
    5-sample percent return. They are not the entry gate.
    """
    prices = list(closes)[-HIST_MAXLEN:]
    fp_rsi = _fingerprint_rsi(prices)
    fp_mom = 0.0
    if len(prices) >= 6 and prices[-6] > 0:
        fp_mom = (prices[-1] - prices[-6]) / prices[-6] * 100.0
    fp_bull = False
    if len(prices) >= 8:
        window = prices[-20:]
        ma = sum(window) / len(window)
        fp_bull = prices[-1] > ma
    if len(prices) < SHORT_MA_BARS:
        return {
            "regime": "BULL",
            "above_ma": True,
            "momentum_ok": True,
            "momentum_pct": 0.0,
            "bullish": fp_bull,
            "rsi": fp_rsi,
            "momentum": float(fp_mom),
            "ready": False,
        }
    lookback = min(SPY_MOM_LOOKBACK, len(prices) - 1)
    base = prices[-lookback]
    mom_pct = (prices[-1] - base) / base * 100.0 if base > 0 else 0.0
    ma20 = sum(prices[-SHORT_MA_BARS:]) / SHORT_MA_BARS
    above = prices[-1] > ma20
    return {
        "regime": "BULL" if above else "BEAR",
        "above_ma": above,
        "momentum_ok": mom_pct >= SPY_MOMENTUM_GATE,
        "momentum_pct": float(mom_pct),
        "bullish": fp_bull,
        "rsi": fp_rsi,
        "momentum": float(fp_mom),
        "ready": True,
    }


def bucket_key(symbol: str, rsi: float, spy_bullish: bool,
               sector_health: str, is_trump: bool, hour: int) -> str:
    """BerserkerMemory._bucket_key. RSI splits at 72 and 62."""
    sector = "TRUMP" if is_trump else "TECH"
    if rsi > 72:
        rsi_b = "rsi_hi"
    elif rsi > 62:
        rsi_b = "rsi_mid"
    else:
        rsi_b = "rsi_low"
    spy_b = "spy_bull" if spy_bullish else "spy_bear"
    sec_b = sector_health or "STRONG"
    if hour < 10:
        hr_b = "hr_open"
    elif hour < 13:
        hr_b = "hr_mid"
    else:
        hr_b = "hr_late"
    return f"{symbol}|{rsi_b}|{spy_b}|{sec_b}|{sector}|{hr_b}"


def dynamic_tp(recipe_tp: float, winner_mfes: Sequence[float]) -> float:
    """get_dynamic_tp. winner_mfes are percent points (1.5 means +1.5%).

    >= 30 winners, avg MFE >= 3 and at least 15% of them reached 3 → 2.5%.
    Else avg MFE >= 2 and at least 25% reached 2 → 2.0%. Never below recipe.
    """
    n = len(winner_mfes)
    if n < DYN_TP_MIN_WINNERS:
        return float(recipe_tp)
    avg = sum(winner_mfes) / n
    pct_3 = sum(1 for m in winner_mfes if m >= 3.0) / n
    pct_2 = sum(1 for m in winner_mfes if m >= 2.0) / n
    if avg >= 3.0 and pct_3 >= 0.15:
        dyn = DYN_TP_HI
    elif avg >= 2.0 and pct_2 >= 0.25:
        dyn = DYN_TP_MID
    else:
        return float(recipe_tp)
    return max(dyn, float(recipe_tp))


def rotation_priority(symbol: str, recently_traded: Sequence[str]) -> int:
    """get_symbol_priority: index in recently_traded + 1, else 0."""
    try:
        return list(recently_traded).index(symbol) + 1
    except ValueError:
        return 0


def scan_order(symbols: Sequence[str], recently_traded: Sequence[str],
               held: Iterable[str], cooling: Iterable[str]) -> List[str]:
    """get_sorted_symbols with Win Follower and /focus off.

    Held, cooling, and (live-only) benched names are dropped. Everyone else
    sorts by rotation priority. Priority 0 keeps SYMBOLS order.
    """
    skip = set(held) | set(cooling)
    available = [s for s in symbols if s not in skip]
    return sorted(available, key=lambda s: rotation_priority(s, recently_traded))


def mark_recently_traded(recently: List[str], symbol: str) -> None:
    if symbol in recently:
        recently.remove(symbol)
    recently.insert(0, symbol)
    del recently[MAX_RECENT:]


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
                  low: float, close: float, *,
                  earnings_blocked: bool = False,
                  tape: Optional[Sequence[float]] = None,
                  eod_policy: str = "live") -> Optional[dict]:
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

    # 4. Close-2. Live carries only when broker PnL is green AND the symbol
    #    tape's last print is above the print 5 samples back (index -6),
    #    which needs at least 6 prints. A short tape does not carry.
    #    Earnings-blackout names are sold even when that test would carry.
    #    `tape` is the symbol's running closes (live price_history). When
    #    the caller does not pass one, the closes since entry are the tape.
    if ct.hour * 60 + ct.minute >= EOD_MINUTE:
        if earnings_blocked:
            return _finish(state, "eod-earnings-close", close, ts)
        # Research book: carry every non-earnings position through the
        # close. The live book still requires a green tape.
        if eod_policy == "carry_all":
            state["skip_date"] = ct.date()
            state["carried"] = True
            return None
        if eod_policy != "live":
            raise ValueError(f"unknown eod_policy {eod_policy!r}")
        profit = close / entry_fill - 1.0
        series = list(tape) if tape is not None else state["closes"]
        rising = len(series) >= 6 and float(series[-1]) > float(series[-6])
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
    eod_policy: str = "live",
) -> List[dict]:
    """Continuous replay. validate_mode=True returns only holdout entries.

    all_bars maps symbol -> DataFrame or list of (ts, o, h, l, c[, v]).
    Include SPY (QQQ is ingested when present and is not a gate).
    signal_fn(symbol, prices, sector_health, is_trump, analyst_score) may
    replace get_signals_bt in tests. analyst_score is 0: the analyst
    service is not on the tape.
    """
    if eod_policy not in ("live", "carry_all"):
        raise ValueError(f"unknown eod_policy {eod_policy!r}")
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
        s: deque(maxlen=HIST_MAXLEN) for s in list(symbols) + ["SPY", "QQQ"]
    }
    in_pos = {s: False for s in symbols}
    pos: Dict[str, dict] = {}
    last_exit: Dict[str, Optional[datetime]] = {s: None for s in symbols}
    entry_meta: Dict[str, dict] = {}
    recently: List[str] = []
    vix_smooth = 15.0
    trades: List[dict] = []
    # Frozen at the first holdout timestamp, from in-sample winners already
    # closed. Holdout entries read it; in-sample entries stay on recipe TP.
    frozen_mfes: Optional[Dict[str, List[float]]] = None

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
            "recipe_tp": meta.get("recipe_tp", st["tp"]),
            "bucket_key": meta.get("bucket_key"),
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
        mark_recently_traded(recently, sym)

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

        if not printed:
            continue
        ts0 = datetime.fromtimestamp(epoch, timezone.utc)
        ct = to_ct(ts0)
        minute = ct.hour * 60 + ct.minute

        vix_smooth = vix_from_spy(price_hist["SPY"], vix_smooth)
        spy = spy_context(price_hist["SPY"])
        sector = compute_sector_health(
            {s: price_hist[s] for s in trump_theme}, trump_theme
        )
        score = regime_score(
            vix=vix_smooth,
            spy_above_short_ma=spy["above_ma"],
        )
        # BEAR cuts the book to 1. VIX >= 30 does too, and VIX >= 25
        # already refuses every new entry, so the extreme cap never
        # admits a trade the block would have refused.
        max_pos = SPY_BEAR_MAX_POS if spy["regime"] == "BEAR" else MAX_POSITIONS
        if vix_smooth >= VIX_EXTREME_THRESHOLD:
            max_pos = min(max_pos, 1)

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
            event = step_position(
                pos[sym], ts, o, h, l, c,
                earnings_blocked=is_earnings_blocked(ts, earnings_map.get(sym, [])),
                tape=list(price_hist[sym]),
                eod_policy=eod_policy,
            )
            if event:
                close_trade(sym, event, sector)

        if frozen_mfes is None and epoch >= cutoff:
            frozen_mfes = {}
            for closed in trades:
                if closed.get("holdout") or not closed.get("won"):
                    continue
                key = closed.get("bucket_key")
                if not key:
                    continue
                frozen_mfes.setdefault(key, []).append(float(closed["mfe"]))

        # One new entry per step, after exits free a slot.
        open_n = sum(1 for s in symbols if in_pos[s])
        if open_n >= max_pos:
            continue
        if minute < RTH_OPEN or minute >= RTH_CLOSE or minute >= ENTRY_CUTOFF:
            continue
        if vix_smooth >= VIX_BLOCK_THRESHOLD:
            continue
        if not spy["momentum_ok"]:
            continue
        if score >= REGIME_BLOCK_SCORE:
            continue

        cooling = []
        for sym in symbols:
            if last_exit[sym] is None:
                continue
            gap_min = (ts0 - last_exit[sym]).total_seconds() / 60.0
            if gap_min < COOLDOWN_MINUTES:
                cooling.append(sym)
        held = [s for s in symbols if in_pos[s]]
        for sym in scan_order(symbols, recently, held, cooling):
            if sum(1 for s in symbols if in_pos[s]) >= max_pos:
                break
            # Opening wall and band_0905 skip this name and keep scanning.
            # Both windows are clock-wide, so in practice nobody fills.
            if RTH_OPEN <= minute < OPENING_WALL_END:
                continue
            if BAND_0905_LO <= minute < BAND_0905_HI:
                continue
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
            if is_earnings_blocked(ts0, earnings_map.get(sym, [])):
                continue
            ts, o, h, l, c, v = bar
            is_trump = sym in trump_theme
            analyst_score = 0
            if signal_fn is not None:
                sig = signal_fn(sym, price_hist[sym], sector, is_trump, analyst_score)
            else:
                sig = get_signals_bt(price_hist[sym], sector, is_trump, analyst_score)
            if not sig.get("buy"):
                continue
            half, _source = costs.get(sym, (FALLBACK_HALF_SPREAD_DEFAULT, "table"))
            recipe_tp = tp_pct_for(sym, recipes)
            rsi_now = float(sig.get("rsi", 50.0))
            key = bucket_key(sym, rsi_now, bool(spy["bullish"]), sector, is_trump, ct.hour)
            if epoch >= cutoff and frozen_mfes is not None:
                tp = dynamic_tp(recipe_tp, frozen_mfes.get(key, []))
            else:
                tp = recipe_tp
            opened = new_position(ts, c, half, tp, sl_pct_for(sym, recipes))
            in_pos[sym] = True
            pos[sym] = opened
            entry_meta[sym] = {
                "rsi": rsi_now,
                "spy_bullish": spy["bullish"],
                "spy_rsi": spy["rsi"],
                "spy_momentum": spy["momentum"],
                "sector_health": sector,
                "vix": round(vix_smooth, 1),
                "bucket_key": key,
                "recipe_tp": recipe_tp,
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


def symbol_pnl_shares(by_symbol: Dict[str, float]) -> dict:
    """Each symbol's share of net P&L.

    share = symbol_pnl / total_pnl, including when the total is negative.
    A large loser then has a large positive share of the net loss, and the
    max of those shares is the dominant contributor.

    When there is no book, the gross is zero, or |net| is under 1% of the
    gross absolute P&L, the ratio is not a concentration measure. scored
    is False and the share gate must not veto.
    """
    total = float(sum(by_symbol.values()))
    gross = float(sum(abs(v) for v in by_symbol.values()))
    material = (
        bool(by_symbol)
        and gross > 0.0
        and abs(total) >= SHARE_MATERIAL_FRACTION * gross
    )
    if not material:
        return {
            "scored": False,
            "total": total,
            "gross": gross,
            "max_share": None,
            "max_symbol": None,
            "shares": {},
        }
    shares = {sym: pnl / total for sym, pnl in by_symbol.items()}
    max_symbol = max(shares, key=lambda s: shares[s])
    return {
        "scored": True,
        "total": total,
        "gross": gross,
        "max_share": shares[max_symbol],
        "max_symbol": max_symbol,
        "shares": shares,
    }


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
    share_info = symbol_pnl_shares(by_symbol)
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
        "max_symbol_share": share_info["max_share"],
        "max_symbol": share_info["max_symbol"],
        "symbol_share_scored": share_info["scored"],
        "symbol_shares": share_info["shares"],
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
    if "symbol_share_scored" in summary:
        scored = bool(summary["symbol_share_scored"])
    else:
        scored = share is not None
    if n <= 0:
        share_ok = False
    elif not scored:
        share_ok = True
    else:
        share_ok = share is not None and share <= GATE_MAX_SYMBOL_SHARE
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
    shares = s["symbol_shares"]
    for sym in sorted(s["by_symbol"], key=lambda k: -s["by_symbol"][k]):
        n = s["by_symbol_n"][sym]
        tot = s["by_symbol"][sym]
        if sym in shares:
            share_s = f"{shares[sym] * 100:.1f}%"
        else:
            share_s = "skip"
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
        if s["n"] == 0:
            share_s, share_mark = "n/a", "FAIL"
        elif not s["symbol_share_scored"]:
            share_s, share_mark = "SKIP", "SKIP"
        else:
            share_s = f"{s['max_symbol_share'] * 100:.1f}%"
            share_mark = mark(g["symbol_share"])
        lines += [
            "GATES",
            f"  avg P&L/trade    {s['avg_pnl']:+.3f}%    >= +{GATE_AVG_PNL:.2f}%    {mark(g['avg_pnl'])}",
            f"  profit factor    {_fmt_pf(s['profit_factor']):>8}    >= {GATE_PROFIT_FACTOR:.2f}      {mark(g['profit_factor'])}",
            f"  max drawdown     {s['max_drawdown']:8.2f} pp <= {GATE_MAX_DRAWDOWN:.0f}        {mark(g['max_drawdown'])}",
            f"  max symbol share {share_s:>8}    <= {GATE_MAX_SYMBOL_SHARE * 100:.0f}%       {share_mark}",
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


def select_holdout_above_median(trades: Sequence[dict]):
    """Drop the worse half of symbols by in-sample total P&L.

    Rank is in-sample sum of pnl_pct, worst first. Ties break on the
    symbol name so the cut is stable. ``len(ranked) // 2`` names are
    dropped, so an odd count keeps the median (11 names drop 5 and keep
    6). A symbol with no in-sample trades is not kept, including one that
    only appears in the holdout.

    Returns ``(holdout_rows, dropped, kept, is_totals)``. dropped and kept
    are ``(symbol, in_sample_pnl)`` pairs. holdout_rows are the holdout
    trades of the kept symbols, in their original order. Score those rows
    with the same gates; do not score the in-sample book again.
    """
    ins, oos = split_holdout(trades)
    totals: Dict[str, float] = {}
    for t in ins:
        sym = t["symbol"]
        totals[sym] = totals.get(sym, 0.0) + float(t["pnl_pct"])
    ranked = sorted(totals, key=lambda s: (totals[s], s))
    n_drop = len(ranked) // 2
    dropped = [(s, totals[s]) for s in ranked[:n_drop]]
    kept = [(s, totals[s]) for s in ranked[n_drop:]]
    kept_set = {s for s, _pnl in kept}
    holdout_rows = [t for t in oos if t["symbol"] in kept_set]
    return holdout_rows, dropped, kept, totals


def format_drop_bottom(trades: Sequence[dict], *, label: Optional[str] = None) -> str:
    """Holdout report after dropping the worse in-sample half. Not a DB write."""
    holdout_rows, dropped, kept, _totals = select_holdout_above_median(trades)
    lines = [
        "",
        "DROP-BOTTOM (research, not written)",
        "Ranked by in-sample total P&L. The worse half is dropped.",
        "Names with no in-sample trades are not kept. Gates score holdout only.",
        "Dropped:",
    ]
    if not dropped:
        lines.append("  (none)")
    for sym, pnl in dropped:
        lines.append(f"  {sym:<8} in-sample {pnl:+.2f}%")
    lines.append("Kept:")
    if not kept:
        lines.append("  (none)")
    for sym, pnl in kept:
        lines.append(f"  {sym:<8} in-sample {pnl:+.2f}%")
    lines.append(format_report(
        holdout_rows,
        label=label or "RESEARCH DROP-BOTTOM HOLDOUT (not written)",
        verdict=True,
    ))
    return "\n".join(lines)
