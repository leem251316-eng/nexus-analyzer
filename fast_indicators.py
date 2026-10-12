"""Last-value indicators that match pandas ewm/rolling on a short window.

Phase4 and the crypto replay recompute RSI, EMA, MACD, ADX, and
Bollinger on a deque of about 120 closes, once per bar, by building a
Series. That construction is the stage timeout: the Oct 11 dry run
killed Phase4 during the holdout walk and crypto during LTC. These
functions walk the same window in plain floats.

They are not a full-history Wilder. Each call reseeds on prices[0],
which is what ``Series.ewm`` does on the slice the signal functions pass
in. Do not replace them with an incremental indicator over the whole tape.

Two pandas seeds show up here:

* RSI gains/losses. ``Series.diff`` is NaN at index 0, and
  ``where(delta > 0, 0.0)`` turns that NaN into 0 because ``NaN > 0`` is
  false. The average therefore starts at 0.
* ADX true range and directional movement. ``diff().clip`` and
  ``diff().abs`` keep the leading NaN. ``ewm(adjust=False)`` drops that
  point and seeds on the first finite value.
"""

from __future__ import annotations

import math
from typing import Optional, Sequence


def _finite(x: float) -> bool:
    return x == x and not math.isinf(x)


def wilder_rsi(prices: Sequence[float], period: int = 14) -> Optional[float]:
    """Last Wilder RSI, or None when the window is shorter than period+1.

    A one-sided tape (no losses, or no gains) returns NaN, matching
    ``avg_loss.replace(0, nan)`` / the reciprocal. Callers that treat NaN
    as "no reading" must use an explicit NaN check: ``nan or 50`` is nan.
    """
    n = len(prices)
    if period < 1 or n < period + 1:
        return None
    alpha = 1.0 / period
    one = 1.0 - alpha
    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(1, n):
        delta = float(prices[i]) - float(prices[i - 1])
        gain = delta if delta > 0.0 else 0.0
        loss = -delta if delta < 0.0 else 0.0
        avg_gain = one * avg_gain + alpha * gain
        avg_loss = one * avg_loss + alpha * loss
    if avg_loss == 0.0:
        return float("nan")
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def rsi_series(prices: Sequence[float], period: int) -> list:
    """Full RSI series. Index 0 is NaN (the zero-seeded empty diff)."""
    n = len(prices)
    out = [float("nan")] * n
    if period < 1 or n == 0:
        return out
    alpha = 1.0 / period
    one = 1.0 - alpha
    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(1, n):
        delta = float(prices[i]) - float(prices[i - 1])
        gain = delta if delta > 0.0 else 0.0
        loss = -delta if delta < 0.0 else 0.0
        avg_gain = one * avg_gain + alpha * gain
        avg_loss = one * avg_loss + alpha * loss
        if avg_loss == 0.0:
            continue
        rs = avg_gain / avg_loss
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def ema_last(prices: Sequence[float], span: int) -> float:
    """Last ``ewm(span=span, adjust=False)``. Seeds on prices[0]."""
    if span < 1 or not prices:
        return float("nan")
    alpha = 2.0 / (span + 1.0)
    one = 1.0 - alpha
    y = float(prices[0])
    for x in prices[1:]:
        y = one * y + alpha * float(x)
    return y


def macd_last(prices: Sequence[float], fast: int = 12, slow: int = 26, signal: int = 9):
    """Last MACD line and signal line. Both seed the way pandas ewm does."""
    if not prices:
        return float("nan"), float("nan")
    a_fast = 2.0 / (fast + 1.0)
    a_slow = 2.0 / (slow + 1.0)
    a_sig = 2.0 / (signal + 1.0)
    e_fast = e_slow = float(prices[0])
    macd = 0.0
    sig = 0.0
    for x in prices[1:]:
        px = float(x)
        e_fast = (1.0 - a_fast) * e_fast + a_fast * px
        e_slow = (1.0 - a_slow) * e_slow + a_slow * px
        macd = e_fast - e_slow
        sig = (1.0 - a_sig) * sig + a_sig * macd
    return macd, sig


def bollinger_last(prices: Sequence[float], period: int = 20):
    """Last rolling mean and sample std (ddof=1) of the tail window."""
    if period < 2 or len(prices) < period:
        return float("nan"), float("nan")
    window = [float(p) for p in prices[-period:]]
    middle = sum(window) / period
    var = sum((p - middle) ** 2 for p in window) / (period - 1)
    return middle, math.sqrt(var)


def stochrsi_last(prices: Sequence[float], rsi_period: int = 14, stoch_period: int = 14) -> float:
    """Last stochastic RSI K.

    ``rolling`` requires a full window of finite RSI values. A zero
    denominator becomes 50, matching ``.where(denom != 0, 50)``.
    """
    rsi = rsi_series(prices, rsi_period)
    if len(rsi) < stoch_period:
        return float("nan")
    window = rsi[-stoch_period:]
    if any(not _finite(x) for x in window):
        return float("nan")
    lo = min(window)
    hi = max(window)
    denom = hi - lo
    if denom == 0.0:
        return 50.0
    return (rsi[-1] - lo) / denom * 100.0


def adx_last(prices: Sequence[float], period: int = 14) -> float:
    """Last ADX from close-only true range, matching phase4's pandas path.

    True range here is ``diff().abs()`` (close to close), not the classic
    high/low/close TR. Directional movement is the clipped close diff.
    A NaN DX point reseeds the ADX average on the next finite DX, which is
    how ``ewm(adjust=False)`` treats the leading NaN and a later 0/0.
    """
    n = len(prices)
    if period < 1 or n < 2:
        return float("nan")
    alpha = 1.0 / period
    one = 1.0 - alpha
    trs = []
    plus = []
    minus = []
    for i in range(1, n):
        delta = float(prices[i]) - float(prices[i - 1])
        trs.append(abs(delta))
        plus.append(delta if delta > 0.0 else 0.0)
        minus.append(-delta if delta < 0.0 else 0.0)

    def _ewm(vals: list) -> list:
        y = vals[0]
        out = [y]
        for x in vals[1:]:
            y = one * y + alpha * x
            out.append(y)
        return out

    atr = _ewm(trs)
    pdm = _ewm(plus)
    ndm = _ewm(minus)
    dx = []
    for a, p, m in zip(atr, pdm, ndm):
        if a == 0.0:
            dx.append(float("nan"))
            continue
        pdi = 100.0 * p / a
        ndi = 100.0 * m / a
        denom = pdi + ndi
        if denom == 0.0:
            dx.append(float("nan"))
        else:
            dx.append(100.0 * abs(pdi - ndi) / denom)

    y = float("nan")
    for x in dx:
        if not _finite(x):
            y = float("nan")
            continue
        if not _finite(y):
            y = x
        else:
            y = one * y + alpha * x
    return y
