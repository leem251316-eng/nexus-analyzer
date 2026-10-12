"""Pandas-equivalence tests for the pure-Python indicator path.

Phase4 and crypto cannot be imported in this environment (no alpaca
package). The wrappers are one call into fast_indicators, and these
tests compare that module to the pandas formulas those wrappers used
to inline. Run from the repo root:

    python3 -m unittest tests.test_fast_indicators tests.test_crypto_symbols tests.test_berserker_exit
"""

import math
import os
import sys
import types
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fast_indicators as fi


def _prices(n=121, seed=0):
    rng = np.random.default_rng(seed)
    return list(100.0 + np.cumsum(rng.normal(0.0, 0.4, n)))


def pandas_rsi(prices, period):
    s = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def pandas_adx(prices, period=14):
    s = pd.Series(prices, dtype=float)
    pos_dm = s.diff().clip(lower=0)
    neg_dm = (-s.diff()).clip(lower=0)
    tr = s.diff().abs()
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    pdm = pos_dm.ewm(alpha=1.0 / period, adjust=False).mean()
    ndm = neg_dm.ewm(alpha=1.0 / period, adjust=False).mean()
    pdi = 100 * pdm / atr.replace(0, float("nan"))
    ndi = 100 * ndm / atr.replace(0, float("nan"))
    dx = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, float("nan"))
    adx = dx.ewm(alpha=1.0 / period, adjust=False).mean()
    return float(adx.iloc[-1])


def pandas_stoch_k(prices, rsi_period=14, stoch_period=14):
    s = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / rsi_period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    lo = rsi.rolling(stoch_period).min()
    hi = rsi.rolling(stoch_period).max()
    denom = hi - lo
    k = ((rsi - lo) / denom * 100).where(denom != 0, 50)
    return float(k.iloc[-1])


def _close(a, b, tol=1e-9):
    if (a != a) and (b != b):
        return True
    return abs(a - b) <= tol


class FastIndicatorTests(unittest.TestCase):
    def test_rsi_matches_pandas_on_the_crypto_windows(self):
        prices = _prices()
        for n, period in ((20, 7), (60, 14), (100, 14), (121, 14), (25, 7)):
            got = fi.wilder_rsi(prices[:n], period)
            ref = pandas_rsi(prices[:n], period)
            self.assertTrue(_close(got, ref), (n, period, got, ref))

    def test_rsi_matches_on_one_sided_tapes(self):
        up = [100.0 + i * 0.1 for i in range(40)]
        down = [100.0 - i * 0.1 for i in range(40)]
        self.assertTrue(_close(fi.wilder_rsi(up, 14), pandas_rsi(up, 14)))
        self.assertTrue(math.isnan(fi.wilder_rsi(up, 14)))
        self.assertTrue(_close(fi.wilder_rsi(down, 14), pandas_rsi(down, 14)))
        self.assertEqual(fi.wilder_rsi(up[:10], 14), None)

    def test_ema_and_macd_match(self):
        prices = _prices()
        s = pd.Series(prices, dtype=float)
        for span in (9, 12, 20, 21, 26):
            ref = float(s.ewm(span=span, adjust=False).mean().iloc[-1])
            self.assertTrue(_close(fi.ema_last(prices, span), ref), span)
        ema12 = s.ewm(span=12, adjust=False).mean()
        ema26 = s.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        got_m, got_s = fi.macd_last(prices)
        self.assertTrue(_close(got_m, float(macd.iloc[-1])))
        self.assertTrue(_close(got_s, float(signal.iloc[-1])))

    def test_bollinger_matches_sample_std(self):
        prices = _prices()
        s = pd.Series(prices, dtype=float)
        mid = float(s.rolling(20).mean().iloc[-1])
        std = float(s.rolling(20).std().iloc[-1])
        got_mid, got_std = fi.bollinger_last(prices, 20)
        self.assertTrue(_close(got_mid, mid, tol=1e-8))
        self.assertTrue(_close(got_std, std, tol=1e-8))

    def test_stochrsi_and_adx_match(self):
        prices = _prices()
        for n in (40, 80, 121):
            self.assertTrue(_close(
                fi.stochrsi_last(prices[:n]), pandas_stoch_k(prices[:n])
            ), n)
            self.assertTrue(_close(
                fi.adx_last(prices[:n]), pandas_adx(prices[:n])
            ), n)
        flat = [50.0] * 20 + prices[:30]
        self.assertTrue(_close(fi.adx_last(flat), pandas_adx(flat)))
        up = [100.0 + i * 0.1 for i in range(40)]
        self.assertTrue(_close(fi.stochrsi_last(up), pandas_stoch_k(up)))

    def test_phase4_rounding_rules(self):
        prices = _prices(80, seed=1)
        raw = fi.wilder_rsi(prices, 7)
        rounded = round(raw, 2)
        self.assertTrue(0 < rounded < 100)
        # Monotonic down is RSI 0, which phase4 turns into "no reading".
        down = [100.0 - i for i in range(30)]
        self.assertEqual(fi.wilder_rsi(down, 7), 0.0)


def _install_import_stubs():
    """alpaca and psycopg2 are not installed in this environment.

    When they are installed, leave them alone so this test still imports
    the real clients.
    """
    try:
        import alpaca.data.historical  # noqa: F401
        import psycopg2  # noqa: F401
        return
    except ImportError:
        pass

    def stub(name, **attrs):
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        for key, value in attrs.items():
            setattr(mod, key, value)
        return mod

    stub("alpaca")
    stub("alpaca.data")
    stub("alpaca.data.historical", StockHistoricalDataClient=object, CryptoHistoricalDataClient=object)
    stub("alpaca.data.requests", StockBarsRequest=object, CryptoBarsRequest=object)
    stub("alpaca.data.timeframe", TimeFrame=object, TimeFrameUnit=object)
    stub("alpaca.data.enums", DataFeed=object, Adjustment=object)
    stub("psycopg2", connect=lambda *a, **k: None)
    stub("psycopg2.extras", RealDictCursor=object)


class WrapperMatchTests(unittest.TestCase):
    """The backtester wrappers must stay on the pandas-matching functions."""

    @classmethod
    def setUpClass(cls):
        _install_import_stubs()
        import phase4_backtester
        import crypto_backtester
        cls.p4 = phase4_backtester
        cls.cb = crypto_backtester

    def test_phase4_and_crypto_wrappers_match_pandas_rounding(self):
        prices = _prices(120, seed=2)
        for period in (7, 14, 21):
            raw = pandas_rsi(prices, period)
            self.assertEqual(self.p4.compute_rsi(prices, period), round(raw, 2))
        got = self.cb.calc_rsi(prices, 14)
        self.assertTrue(_close(got, pandas_rsi(prices, 14)))
        self.assertEqual(self.p4.compute_adx(prices), round(pandas_adx(prices), 2))

    def test_bar_lookup_is_last_close_and_volume_for_the_timestamp(self):
        idx = pd.to_datetime(["2024-01-02 15:00:00+00:00", "2024-01-02 15:01:00+00:00"])
        df = pd.DataFrame({"close": [10.0, 11.5], "volume": [100.0, 250.0]}, index=idx)
        lookup = self.p4._bar_lookup(df)
        self.assertEqual(lookup[idx[0]], (10.0, 100.0))
        self.assertEqual(lookup[idx[1]], (11.5, 250.0))
        self.assertIsNone(lookup.get(idx[0] + pd.Timedelta(minutes=5)))


if __name__ == "__main__":
    unittest.main()
