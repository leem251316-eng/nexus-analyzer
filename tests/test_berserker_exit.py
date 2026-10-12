"""Exit-engine tests for the V4 Berserker replica.

No network, no database. Run from the repo root:

    python -m unittest tests.test_berserker_exit
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import berserker_live_sim as sim

CT = ZoneInfo("America/Chicago")


def at(hour, minute, day=8):
    """Thursday 2026-10-08 (or a later October day) at hour:minute CT."""
    return datetime(2026, 10, day, hour, minute, tzinfo=CT)


def step_many(state, start, prices):
    """Step one bar per minute. prices is (o, h, l, c) or a float close."""
    last = None
    for i, px in enumerate(prices):
        ts = start + timedelta(minutes=i)
        if isinstance(px, tuple):
            o, h, l, c = px
        else:
            o = h = l = c = px
        last = sim.step_position(state, ts, o, h, l, c)
        if last:
            return last
    return last


class TrailWidthTests(unittest.TestCase):
    def test_base_width_below_ratchet(self):
        self.assertEqual(sim.trail_width(0.014), sim.TRAILING_STOP)
        self.assertEqual(sim.trail_width(0.0), 0.015)

    def test_tight_width_at_ratchet_profit(self):
        self.assertEqual(sim.trail_width(0.015), sim.RATCHET_TRAIL_TIGHT)
        self.assertEqual(sim.trail_width(0.02), 0.005)


class ExitEngineTests(unittest.TestCase):
    def test_take_profit_hits_before_the_ratchet(self):
        state = sim.new_position(at(10, 0), 100.0, 0.0, tp=0.015, sl=0.010)
        # Open already on the +1.5% level. TP is checked first, so this is
        # not a 0.5% ratchet trail.
        event = sim.step_position(state, at(10, 21), 101.5, 101.6, 101.4, 101.5)
        self.assertIsNotNone(event)
        self.assertEqual(event["reason"], "take-profit")
        self.assertAlmostEqual(event["exit_raw"], 101.5)

    def test_stop_loss_and_spcx_wider_stop(self):
        narrow = sim.new_position(at(10, 0), 100.0, 0.0, tp=0.015, sl=0.010)
        hit = sim.step_position(narrow, at(10, 5), 100.0, 100.1, 98.7, 99.0)
        self.assertEqual(hit["reason"], "stop-loss")
        self.assertAlmostEqual(hit["exit_raw"], 99.0)  # the level, not the wick

        wide = sim.new_position(at(10, 0), 100.0, 0.0, tp=0.015, sl=0.015)
        miss = sim.step_position(wide, at(10, 5), 100.0, 100.1, 98.7, 99.0)
        self.assertIsNone(miss)
        self.assertEqual(sim.sl_pct_for("SPCX"), 0.015)
        self.assertEqual(sim.sl_pct_for("NVDA"), 0.010)
        self.assertEqual(sim.sl_pct_for("NVDA", {"NVDA": {"sl": 0.010}}), 0.010)

    def test_trail_waits_for_twenty_minutes(self):
        state = sim.new_position(at(10, 0), 100.0, 0.0, tp=0.015, sl=0.010)
        # Peak to 101 inside the hold window. A 1.5% giveback is still
        # above the hard stop (99.0) and must not trail yet.
        quiet = sim.step_position(state, at(10, 5), 100.2, 101.0, 100.2, 100.8)
        self.assertIsNone(quiet)
        self.assertAlmostEqual(state["peak"], 101.0)
        early = sim.step_position(state, at(10, 19), 100.4, 100.6, 99.40, 99.80)
        self.assertIsNone(early)

        armed = sim.new_position(at(10, 0), 100.0, 0.0, tp=0.015, sl=0.010)
        sim.step_position(armed, at(10, 5), 100.2, 101.0, 100.2, 100.8)
        event = sim.step_position(armed, at(10, 20), 100.4, 100.6, 99.40, 99.80)
        self.assertEqual(event["reason"], "trailing-stop")
        self.assertAlmostEqual(event["exit_raw"], 101.0 * (1.0 - 0.015))
        self.assertAlmostEqual(event["hold_min"], 20.0)

    def test_low_through_trail_beats_a_same_bar_high_at_tp(self):
        state = sim.new_position(at(10, 0), 100.0, 0.0, tp=0.015, sl=0.010)
        sim.step_position(state, at(10, 5), 100.2, 101.0, 100.2, 100.8)
        # Prior peak 101 → trail 99.485. High would tag TP at 101.5, but
        # the low is tested first.
        event = sim.step_position(
            state, at(10, 25), 100.4, 101.5, 99.40, 100.0
        )
        self.assertEqual(event["reason"], "trailing-stop")
        self.assertLess(event["exit_raw"], 100.0)

    def test_hold_min_is_clock_time_not_bar_count(self):
        state = sim.new_position(at(10, 0), 100.0, 0.0, tp=0.015, sl=0.010)
        # One bar, 47 minutes later. A bar-index hold would be 1.
        event = sim.step_position(state, at(10, 47), 100.0, 100.0, 98.0, 98.5)
        self.assertEqual(event["reason"], "stop-loss")
        self.assertAlmostEqual(event["hold_min"], 47.0)

    def test_eod_carries_green_and_rising_and_flattens_otherwise(self):
        # Enter at 14:50. Steps from 14:51 through 14:58. Strictly rising
        # and green → carry, no exit.
        carry = sim.new_position(at(14, 50), 100.0, 0.0, tp=0.015, sl=0.010)
        event = None
        for i, px in enumerate([100.2, 100.3, 100.4, 100.5, 100.6, 100.7, 100.8, 100.9]):
            event = sim.step_position(
                carry, at(14, 51) + timedelta(minutes=i), px, px, px, px
            )
        self.assertIsNone(event)
        self.assertTrue(carry["carried"])

        # Same shape but the last close gives back the prior three minutes.
        # Still green versus the fill, so the old "any winner carries" rule
        # would keep it. Live sells it.
        flat = sim.new_position(at(14, 50), 100.0, 0.0, tp=0.015, sl=0.010)
        path = [100.4, 100.5, 100.6, 100.7, 100.8, 100.9, 100.85, 100.3]
        event = None
        for i, px in enumerate(path):
            event = sim.step_position(
                flat, at(14, 51) + timedelta(minutes=i), px, px + 0.01, px - 0.01, px
            )
        self.assertIsNotNone(event)
        self.assertEqual(event["reason"], "eod-autoclose")
        self.assertAlmostEqual(event["exit_raw"], 100.3)

        # Flat price is not green.
        red = sim.new_position(at(14, 50), 100.0, 0.0, tp=0.015, sl=0.010)
        event = sim.step_position(red, at(14, 58), 100.0, 100.0, 100.0, 100.0)
        self.assertEqual(event["reason"], "eod-autoclose")

        # Fewer than 6 closes on the tape does not carry, even if green
        # and rising. Live returns False when price_history is shorter
        # than 6 prints.
        young = sim.new_position(at(14, 55), 100.0, 0.0, tp=0.015, sl=0.010)
        event = None
        for i, px in enumerate([100.2, 100.4, 100.6]):
            event = sim.step_position(
                young, at(14, 56) + timedelta(minutes=i), px, px, px, px
            )
        self.assertEqual(event["reason"], "eod-autoclose")
        self.assertFalse(young["carried"])

        # The live test uses the symbol tape, not "closes since entry".
        # A one-bar position still carries when that tape is long and rising.
        taped = sim.new_position(at(14, 57), 100.0, 0.0, tp=0.015, sl=0.010)
        tape = [100.0, 100.1, 100.2, 100.3, 100.4, 100.6]
        event = sim.step_position(
            taped, at(14, 58), 100.6, 100.6, 100.6, 100.6, tape=tape
        )
        self.assertIsNone(event)
        self.assertTrue(taped["carried"])

        # Earnings blackout flattens a winner the carry test would keep.
        blocked = sim.new_position(at(14, 50), 100.0, 0.0, tp=0.015, sl=0.010)
        event = None
        for i, px in enumerate([100.2, 100.3, 100.4, 100.5, 100.6, 100.7, 100.8, 100.9]):
            event = sim.step_position(
                blocked, at(14, 51) + timedelta(minutes=i), px, px, px, px,
                earnings_blocked=(i == 7),
            )
        self.assertEqual(event["reason"], "eod-earnings-close")

    def test_overnight_gap_fills_at_the_open(self):
        # Friday 14:58 carry, Monday 08:30 opens through the stop.
        state = sim.new_position(at(14, 50), 100.0, 0.0, tp=0.015, sl=0.010)
        event = None
        for i, px in enumerate([100.2, 100.3, 100.4, 100.5, 100.6, 100.7, 100.8, 100.9]):
            event = sim.step_position(
                state, at(14, 51) + timedelta(minutes=i), px, px, px, px
            )
        self.assertIsNone(event)
        monday = datetime(2026, 10, 12, 8, 30, tzinfo=CT)
        gap = sim.step_position(state, monday, 90.0, 91.0, 89.5, 90.5)
        self.assertEqual(gap["reason"], "stop-loss")
        self.assertAlmostEqual(gap["exit_raw"], 90.0)  # the open, not 99.0
        held = (monday - at(14, 50)).total_seconds() / 60.0
        self.assertAlmostEqual(gap["hold_min"], held)
        self.assertGreater(gap["hold_min"], 60 * 24)

        # Gap up through TP fills at the open, not capped at +1.5%.
        up = sim.new_position(at(14, 50), 100.0, 0.0, tp=0.015, sl=0.010)
        for i, px in enumerate([100.2, 100.3, 100.4, 100.5, 100.6, 100.7, 100.8, 100.9]):
            sim.step_position(up, at(14, 51) + timedelta(minutes=i), px, px, px, px)
        gap_up = sim.step_position(up, monday, 110.0, 111.0, 109.0, 110.5)
        self.assertEqual(gap_up["reason"], "take-profit")
        self.assertAlmostEqual(gap_up["exit_raw"], 110.0)

    def test_costs_come_out_of_a_flat_round_trip(self):
        state = sim.new_position(at(14, 50), 100.0, 0.002, tp=0.015, sl=0.010)
        event = sim.step_position(state, at(14, 58), 100.0, 100.0, 100.0, 100.0)
        self.assertEqual(event["reason"], "eod-autoclose")
        pnl = sim.net_pnl_pct(state["entry_fill"], event["exit_raw"], 0.002)
        self.assertLess(pnl, 0.0)
        # Entry haircut and exit haircut, not a flat 5 bps each side.
        self.assertAlmostEqual(state["entry_fill"], 100.0 * 1.002)


class ClockAndGateTests(unittest.TestCase):
    def test_opening_wall_band_and_close_minus_five(self):
        cases = [
            (8 * 60 + 29, "outside_rth"),
            (8 * 60 + 30, "opening_wall"),
            (8 * 60 + 59, "opening_wall"),
            (9 * 60 + 0, None),
            (9 * 60 + 4, None),
            (9 * 60 + 5, "band_0905"),
            (9 * 60 + 29, "band_0905"),
            (9 * 60 + 30, None),
            (14 * 60 + 54, None),
            (14 * 60 + 55, "close_minus_5"),
            (14 * 60 + 58, "close_minus_5"),
        ]
        for minute, expected in cases:
            self.assertEqual(sim.clock_entry_block(minute), expected, minute)

    def test_trump_rsi_threshold_and_regime_score(self):
        self.assertEqual(sim.required_rsi(False, "WEAK"), 62.0)
        self.assertEqual(sim.required_rsi(True, "STRONG"), 62.0)
        self.assertEqual(sim.required_rsi(True, "WEAK"), 72.0)
        healthy = sim.regime_score(vix=18, spy_above_short_ma=True)
        self.assertEqual(healthy, 0)
        self.assertLess(healthy, sim.REGIME_BLOCK_SCORE)
        # Bar-only inputs top out at 2 (SPY bear + VIX > 25). That is
        # under the shutdown threshold of 4, so the score does not block.
        bars_only = sim.regime_score(vix=26, spy_above_short_ma=False)
        self.assertEqual(bars_only, 2)
        self.assertLess(bars_only, sim.REGIME_BLOCK_SCORE)
        # Score 3 is defensive size, not a Berserker entry block.
        defensive = sim.regime_score(
            vix=18, spy_above_short_ma=False, berserker_cb=True, fear_greed=10,
        )
        self.assertEqual(defensive, 3)
        self.assertLess(defensive, sim.REGIME_BLOCK_SCORE)
        shutdown = sim.regime_score(
            vix=26, spy_above_short_ma=False, berserker_cb=True, scanner_cb=True,
        )
        self.assertEqual(shutdown, 4)
        self.assertGreaterEqual(shutdown, sim.REGIME_BLOCK_SCORE)
        # VIX == 25 blocks entries in get_vix_status, but the regime point
        # is strict greater-than.
        self.assertEqual(sim.regime_score(vix=25, spy_above_short_ma=True), 0)
        self.assertEqual(sim.regime_score(vix=25.01, spy_above_short_ma=True), 1)

    def test_earnings_blackout_uses_each_historical_print(self):
        old = datetime(2024, 11, 1, 21, 0, tzinfo=ZoneInfo("UTC"))
        recent = datetime(2026, 10, 8, 21, 0, tzinfo=ZoneInfo("UTC"))
        dates = [old, recent]
        inside = datetime(2026, 10, 8, 15, 0, tzinfo=CT)
        outside = datetime(2026, 10, 6, 15, 0, tzinfo=CT)
        self.assertTrue(sim.is_earnings_blocked(inside, dates))
        self.assertFalse(sim.is_earnings_blocked(outside, dates))
        # The 2024 print still blocks its own window. A single "next
        # earnings" snapshot would have dropped it.
        near_old = datetime(2024, 11, 1, 16, 0, tzinfo=CT)
        self.assertTrue(sim.is_earnings_blocked(near_old, dates))

    def test_fallback_table_is_wider_for_the_thin_names(self):
        costs = sim.resolve_costs(
            ["AAPL", "NVDA", "SPCX", "GEO", "CXW", "CLSK", "MARA", "MSTR", "SMCI"],
            {},
        )
        liquid = costs["AAPL"][0]
        for sym in ("SPCX", "GEO", "CXW", "CLSK", "MARA", "MSTR", "SMCI"):
            self.assertGreater(costs[sym][0], liquid, sym)
            self.assertEqual(costs[sym][1], "table")
        self.assertIn("SPCX", sim.THIN_SYMBOLS)
        self.assertNotIn("AAPL", sim.THIN_SYMBOLS)

    def test_quote_override_beats_the_table(self):
        costs = sim.resolve_costs(
            ["SPCX"], {}, quote_half={"SPCX": 0.0004}
        )
        self.assertEqual(costs["SPCX"], (0.0004, "quote"))

    def test_roll_estimator_sees_a_bounce_and_rejects_a_drift(self):
        bounce = [100.0 if i % 2 == 0 else 100.2 for i in range(80)]
        half = sim.roll_half_spread(bounce)
        self.assertIsNotNone(half)
        drift = [100.0 + i * 0.05 for i in range(80)]
        self.assertIsNone(sim.roll_half_spread(drift))


class ReportGateTests(unittest.TestCase):
    def _trade(self, sym, pnl, exit_ts, reason="take-profit", overnight=False, thin=False):
        return {
            "symbol": sym,
            "pnl_pct": pnl,
            "exit_reason": reason,
            "exit_ts_ep": exit_ts,
            "overnight": overnight,
            "thin": thin,
            "holdout": False,
            "trade_id": "bt_v4_abc",
        }

    def test_pass_when_every_gate_clears(self):
        trades = []
        # Four symbols, equal positive P&L, no drawdown. Share is exactly 25%.
        for i, sym in enumerate(["AAPL", "NVDA", "TSLA", "PLTR"]):
            trades.append(self._trade(sym, 0.20, 1000 + i))
            trades.append(self._trade(sym, 0.20, 2000 + i))
        summary = sim.summarize(trades)
        gates = sim.evaluate_gates(summary)
        self.assertTrue(gates["pass"], gates)
        self.assertGreaterEqual(summary["profit_factor"], 1.15)
        self.assertLessEqual(summary["max_drawdown"], 8.0)
        text = sim.format_report(trades, label="HOLDOUT", verdict=True)
        self.assertIn("VERDICT: PASS", text)
        self.assertIn("Profit factor:", text)

    def test_fail_on_avg_drawdown_factor_and_concentration(self):
        thin = self._trade("AAPL", 0.05, 1)
        self.assertFalse(sim.evaluate_gates(sim.summarize([thin]))["avg_pnl"])

        # Cumsum +1, +1, -10 → 10 pp drawdown.
        dd = [
            self._trade("AAPL", 1.0, 1),
            self._trade("NVDA", 1.0, 2),
            self._trade("TSLA", -10.0, 3, reason="stop-loss"),
        ]
        self.assertGreater(sim.summarize(dd)["max_drawdown"], 8.0)
        self.assertFalse(sim.evaluate_gates(sim.summarize(dd))["max_drawdown"])

        pf = [
            self._trade("AAPL", 1.0, 1),
            self._trade("NVDA", -1.0, 2, reason="stop-loss"),
            self._trade("TSLA", -1.0, 3, reason="stop-loss"),
            self._trade("PLTR", -1.0, 4, reason="stop-loss"),
        ]
        self.assertLess(sim.summarize(pf)["profit_factor"], 1.15)

        concentrated = [
            self._trade("SMCI", 10.0, 1, thin=True),
            self._trade("AAPL", 1.0, 2),
        ]
        share = sim.summarize(concentrated)["max_symbol_share"]
        self.assertGreater(share, 0.25)
        text = sim.format_report(concentrated, label="HOLDOUT", verdict=True)
        self.assertIn("VERDICT: FAIL", text)
        self.assertIn("Thin:", text)
        self.assertIn("Overnight:", text)

    def test_exit_mix_and_overnight_split(self):
        trades = [
            self._trade("AAPL", 1.0, 1, reason="take-profit"),
            self._trade("AAPL", -0.5, 2, reason="eod-autoclose"),
            self._trade("NVDA", 0.4, 3, reason="trailing-stop", overnight=True),
        ]
        summary = sim.summarize(trades)
        self.assertEqual(summary["by_exit"]["take-profit"], 1)
        self.assertEqual(summary["by_exit"]["trailing-stop"], 1)
        self.assertEqual(summary["overnight_n"], 1)
        self.assertEqual(summary["sameday_n"], 2)
        self.assertAlmostEqual(summary["avg_win"], (1.0 + 0.4) / 2)

    def test_v4_ids_match_the_live_bt_gate_and_holdout_is_not_written(self):
        tid = sim.trade_id()
        self.assertTrue(tid.startswith("bt_v4_"))
        self.assertTrue(tid.startswith(sim.LIVE_BT_GATE_LIKE.replace("%", "")))
        legacy = "bt_" + "ab" * 8
        self.assertFalse(legacy.startswith("bt_v4_"))
        rows = [
            {"holdout": False, "trade_id": "bt_v4_in"},
            {"holdout": True, "trade_id": "bt_v4_out"},
        ]
        kept = sim.rows_for_db(rows)
        self.assertEqual([r["trade_id"] for r in kept], ["bt_v4_in"])

    def test_holdout_cutoff_is_the_last_quarter(self):
        epochs = list(range(100))
        # index 75 of 100. Entries at or after it are the holdout.
        self.assertEqual(sim.holdout_cutoff_epoch(epochs), 75)


def _day(start_px, drift, day=8, volume=1000.0):
    start = at(8, 30, day=day)
    rows = []
    px = start_px
    for i in range(390):
        px = px + drift
        ts = start + timedelta(minutes=i)
        rows.append((ts, px, px, px, px, volume))
    return rows


class ReplayBookTests(unittest.TestCase):
    def test_one_entry_per_bar_scan_order_max_three_and_walls(self):
        symbols = ["CLSK", "MARA", "PLTR", "NVDA"]
        bars = {sym: _day(10.0, 0.0) for sym in symbols}
        bars["SPY"] = _day(500.0, 0.02)
        recipes = {
            "PLTR": {"avoid_hours": [9, 11], "avoid_days": [], "tp": 0.015, "sl": 0.010},
        }
        costs = {sym: (0.0, "table") for sym in symbols}

        def always_buy(sym, prices, sector, is_trump, vwap):
            return {"buy": True, "rsi": 70.0}

        trades = sim.replay(
            bars, {},
            symbols=symbols,
            recipes=recipes,
            trump_theme=["CLSK", "MARA", "PLTR"],
            costs=costs,
            signal_fn=always_buy,
        )
        ordered = sorted(trades, key=lambda t: t["entry_ts_ep"])
        self.assertEqual([t["symbol"] for t in ordered], ["CLSK", "MARA", "NVDA"])
        # Signal gate is 35 one-minute samples: 08:30 + 34 min = 09:04,
        # which is after the opening wall and before band_0905. PLTR is
        # skipped for avoid-hour 9, so the next fills are 09:30 and 09:31
        # once the band ends. One fill per minute.
        minutes = []
        for t in ordered:
            ct = datetime.fromtimestamp(t["entry_ts_ep"], CT)
            minutes.append(ct.hour * 60 + ct.minute)
        self.assertEqual(minutes, [9 * 60 + 4, 9 * 60 + 30, 9 * 60 + 31])
        epochs = [t["entry_ts_ep"] for t in ordered]
        self.assertEqual(len(epochs), len(set(epochs)))
        for t in ordered:
            self.assertEqual(t["exit_reason"], "eod-autoclose")
            self.assertFalse(t["overnight"])
            self.assertTrue(t["trade_id"].startswith("bt_v4_"))
            # 09:04 → 14:58 is 354 clock minutes, not a cross-symbol bar index.
            if t["symbol"] == "CLSK":
                self.assertEqual(t["hold_min"], 354)

    def test_cooldown_and_holdout_tag_from_one_book(self):
        # 40 minutes from 10:00. Warmup is patched down so the short book
        # can trade. Cutoff index is int(40 * 0.75) = 30.
        start = at(10, 0)
        def series(fill_at=None):
            rows = []
            for i in range(40):
                px = 100.0
                ts = start + timedelta(minutes=i)
                rows.append((ts, px, px, px, px, 1000.0))
            return rows

        symbols = ["AAA", "BBB"]
        bars = {s: series() for s in symbols}
        spy = []
        px = 500.0
        for i in range(40):
            px += 0.05
            ts = start + timedelta(minutes=i)
            spy.append((ts, px, px, px, px, 1000.0))
        bars["SPY"] = spy
        costs = {s: (0.0, "table") for s in symbols}

        def picky(sym, prices, sector, is_trump, vwap):
            n = len(prices)
            if sym == "AAA" and n >= 21:
                return {"buy": True, "rsi": 70.0}
            if sym == "BBB" and n >= 31:
                return {"buy": True, "rsi": 70.0}
            return {"buy": False}

        original = sim.WARMUP_BARS
        try:
            sim.WARMUP_BARS = 20
            trades = sim.replay(
                bars, {},
                symbols=symbols,
                recipes={},
                trump_theme=[],
                costs=costs,
                signal_fn=picky,
            )
        finally:
            sim.WARMUP_BARS = original

        by_sym = {t["symbol"]: t for t in trades}
        self.assertIn("AAA", by_sym)
        self.assertIn("BBB", by_sym)
        self.assertFalse(by_sym["AAA"]["holdout"])
        self.assertTrue(by_sym["BBB"]["holdout"])
        written = sim.rows_for_db(trades)
        self.assertEqual([t["symbol"] for t in written], ["AAA"])

    def test_validate_mode_returns_only_the_holdout_slice(self):
        start = at(10, 0)
        symbols = ["AAA", "BBB"]
        bars = {}
        for sym in symbols:
            rows = []
            for i in range(40):
                ts = start + timedelta(minutes=i)
                rows.append((ts, 100.0, 100.0, 100.0, 100.0, 1000.0))
            bars[sym] = rows
        spy = []
        px = 500.0
        for i in range(40):
            px += 0.05
            ts = start + timedelta(minutes=i)
            spy.append((ts, px, px, px, px, 1000.0))
        bars["SPY"] = spy

        def picky(sym, prices, sector, is_trump, vwap):
            n = len(prices)
            if sym == "AAA" and n >= 21:
                return {"buy": True, "rsi": 70.0}
            if sym == "BBB" and n >= 31:
                return {"buy": True, "rsi": 70.0}
            return {"buy": False}

        original = sim.WARMUP_BARS
        try:
            sim.WARMUP_BARS = 20
            oos = sim.replay(
                bars, {},
                symbols=symbols,
                recipes={},
                trump_theme=[],
                costs={s: (0.0, "table") for s in symbols},
                signal_fn=picky,
                validate_mode=True,
            )
        finally:
            sim.WARMUP_BARS = original
        self.assertEqual([t["symbol"] for t in oos], ["BBB"])
        self.assertTrue(all(t["holdout"] for t in oos))

    def test_thirty_minute_cooldown_blocks_the_stopped_symbol(self):
        start = at(10, 0)
        rows_a = []
        rows_b = []
        for i in range(40):
            ts = start + timedelta(minutes=i)
            # Warmup is 20 closes, so the first entry is index 19 (10:19).
            # Index 21 (10:21) opens through the stop.
            if i == 21:
                rows_a.append((ts, 90.0, 90.0, 90.0, 90.0, 1000.0))
            else:
                rows_a.append((ts, 100.0, 100.0, 100.0, 100.0, 1000.0))
            rows_b.append((ts, 50.0, 50.0, 50.0, 50.0, 1000.0))
        spy = []
        px = 500.0
        for i in range(40):
            px += 0.05
            ts = start + timedelta(minutes=i)
            spy.append((ts, px, px, px, px, 1000.0))
        bars = {"AAA": rows_a, "BBB": rows_b, "SPY": spy}

        def always(sym, prices, sector, is_trump, vwap):
            return {"buy": True, "rsi": 80.0}

        original = sim.WARMUP_BARS
        try:
            sim.WARMUP_BARS = 20
            trades = sim.replay(
                bars, {},
                symbols=["AAA", "BBB"],
                recipes={},
                trump_theme=[],
                costs={"AAA": (0.0, "table"), "BBB": (0.0, "table")},
                signal_fn=always,
            )
        finally:
            sim.WARMUP_BARS = original

        aaa = [t for t in trades if t["symbol"] == "AAA"]
        bbb = [t for t in trades if t["symbol"] == "BBB"]
        self.assertEqual(len(aaa), 1, "cooldown must block a same-session re-entry")
        self.assertEqual(aaa[0]["exit_reason"], "stop-loss")
        # BBB takes the next scan after AAA is in, or the freed slot.
        # AAA must not re-enter inside 30 minutes of its stop.
        aaa_sorted = sorted(aaa, key=lambda t: t["entry_ts_ep"])
        if len(aaa_sorted) >= 2:
            gap = (aaa_sorted[1]["entry_ts_ep"] - aaa_sorted[0]["exit_ts_ep"]) / 60.0
            self.assertGreaterEqual(gap, 30.0)
        self.assertTrue(bbb, "the other symbol should still be allowed to enter")
        # The stop fill is the open of the gap bar (90), not the 1% level.
        self.assertAlmostEqual(aaa_sorted[0]["pnl_pct"], -10.0, places=4)


class ConfluenceTests(unittest.TestCase):
    def test_each_leg_can_be_the_one_that_qualifies(self):
        # Momentum: last 5-sample change positive and larger than the prior one.
        up = list(range(100, 112))
        quiet_hist = _hist(0.0, 0.0)
        self.assertGreaterEqual(sim.confluence_count(up, quiet_hist, analyst_score=0), 1)

        # Bouncing: close above the close three samples ago.
        bounce = [10, 10, 10, 9, 9, 9, 9, 10]
        self.assertGreaterEqual(
            sim.confluence_count(bounce, quiet_hist, analyst_score=0), 1
        )

        # Fifth leg is the analyst score, not session VWAP. A flat tape
        # has zero price legs. Score 0 (service absent) adds nothing.
        # Score >= 1 adds the leg and does not require the other four.
        flat = [5.0] * 30
        self.assertEqual(sim.confluence_count(flat, quiet_hist, analyst_score=0), 0)
        self.assertEqual(sim.confluence_count(flat, quiet_hist, analyst_score=1), 1)

    def test_strong_uptrend_passes_the_base_gates(self):
        # Chop, then a push. A perfectly smooth rise leaves Wilder RSI
        # undefined (zero average loss), matching live get_signals.
        prices = [100 + (i % 5) * 0.05 for i in range(40)]
        px = prices[-1]
        for _ in range(40):
            px += 0.8
            prices.append(px)
        sig = sim.get_signals_bt(prices, "STRONG", is_trump=False, analyst_score=0)
        self.assertTrue(sig["buy"], sig)
        self.assertGreater(sig["rsi"], 62)
        self.assertGreaterEqual(sig["confluence"], 1)
        self.assertEqual(sig["analyst_score"], 0)
        weak = sim.get_signals_bt(prices, "WEAK", is_trump=True, analyst_score=0)
        self.assertTrue(weak["buy"])


class LiveGateTests(unittest.TestCase):
    def test_sector_health_is_fifteen_samples_not_the_short_ma(self):
        # Older drop still inside a 20-sample average, but the last print
        # is not below the print 15 samples ago. That name is not "down".
        stale = [200.0] * 5 + [10.0] * 15
        fresh = [10.0] * 14 + [9.0]
        flat = [10.0] * 20
        theme = ["STALE", "FRESH", "FLAT", "ALSO"]
        prices = {"STALE": stale, "FRESH": fresh, "FLAT": flat, "ALSO": flat}
        self.assertEqual(sim.compute_sector_health(prices, theme), "STRONG")
        prices["B"] = fresh
        theme3 = ["FRESH", "B", "C"]
        prices["C"] = fresh
        self.assertEqual(sim.compute_sector_health(prices, theme3), "WEAK")

    def test_spy_momentum_gate_allows_a_small_dip(self):
        soft = [100.0] * 50
        soft[-1] = 99.8  # -0.2% over the deque lookback, inside -0.3
        self.assertTrue(sim.spy_context(soft)["momentum_ok"])
        hard = [100.0] * 50
        hard[-1] = 99.5
        self.assertFalse(sim.spy_context(hard)["momentum_ok"])
        # Under 20 samples live assumes BULL and does not block.
        early = sim.spy_context([100.0] * 10)
        self.assertTrue(early["momentum_ok"])
        self.assertEqual(early["regime"], "BULL")

    def test_dynamic_tp_thresholds_and_the_recipe_floor(self):
        self.assertAlmostEqual(sim.dynamic_tp(0.015, [3.2] * 30), 0.025)
        self.assertAlmostEqual(sim.dynamic_tp(0.015, [2.2] * 30), 0.020)
        self.assertAlmostEqual(sim.dynamic_tp(0.015, [3.2] * 29), 0.015)
        self.assertAlmostEqual(sim.dynamic_tp(0.030, [4.0] * 40), 0.030)
        key = sim.bucket_key("NVDA", 73, True, "STRONG", False, 9)
        self.assertEqual(key, "NVDA|rsi_hi|spy_bull|STRONG|TECH|hr_open")

    def test_scan_order_is_the_no_focus_rotation(self):
        # Never-traded names keep SYMBOLS order and scan before a name
        # that just exited. Cooling names are left out of the list.
        symbols = ["CLSK", "MARA", "NVDA"]
        self.assertEqual(
            sim.scan_order(symbols, ["NVDA"], held=[], cooling=[]),
            ["CLSK", "MARA", "NVDA"],
        )
        self.assertEqual(
            sim.scan_order(symbols, ["NVDA", "CLSK"], held=[], cooling=["MARA"]),
            ["NVDA", "CLSK"],
        )


def _hist(prev, last):
    import pandas as pd
    return pd.Series([prev, last])


class WriteSwitchTests(unittest.TestCase):
    def test_database_writes_default_off(self):
        import backtest_safety as gate
        old = os.environ.pop("BACKTEST_WRITE_DB", None)
        try:
            self.assertFalse(gate.writes_enabled())
            self.assertTrue(gate.dry_run(False))
            self.assertIn("DRY RUN", gate.banner(False))
            self.assertEqual(gate.banner(True), "")
            for raw in ("", "0", "false", "no", "off"):
                os.environ["BACKTEST_WRITE_DB"] = raw
                self.assertFalse(gate.writes_enabled(), raw)
            for raw in ("1", "true", "YES", "on"):
                os.environ["BACKTEST_WRITE_DB"] = raw
                self.assertTrue(gate.writes_enabled(), raw)
                self.assertFalse(gate.dry_run(False))
            # An explicit --dry-run still wins when the variable is on.
            self.assertTrue(gate.dry_run(True))
        finally:
            if old is None:
                os.environ.pop("BACKTEST_WRITE_DB", None)
            else:
                os.environ["BACKTEST_WRITE_DB"] = old


class SymbolShareTests(unittest.TestCase):
    def _trade(self, sym, pnl, exit_ts, reason="take-profit", overnight=False, thin=False):
        return {
            "symbol": sym,
            "pnl_pct": pnl,
            "exit_reason": reason,
            "exit_ts_ep": exit_ts,
            "overnight": overnight,
            "thin": thin,
            "holdout": False,
            "trade_id": "bt_v4_abc",
        }

    def test_negative_total_reports_each_symbols_share_of_the_loss(self):
        trades = [
            self._trade("SMCI", -35.89, 1, reason="stop-loss", thin=True),
            self._trade("AAPL", -1.0, 2, reason="stop-loss"),
        ]
        summary = sim.summarize(trades)
        self.assertTrue(summary["symbol_share_scored"])
        self.assertEqual(summary["max_symbol"], "SMCI")
        self.assertAlmostEqual(summary["symbol_shares"]["SMCI"], -35.89 / -36.89)
        self.assertAlmostEqual(summary["symbol_shares"]["AAPL"], -1.0 / -36.89)
        self.assertGreater(summary["max_symbol_share"], 0.25)
        gates = sim.evaluate_gates(summary)
        self.assertFalse(gates["symbol_share"])
        text = sim.format_report(trades, label="HOLDOUT", verdict=True)
        self.assertIn("97.3%", text)
        self.assertIn("VERDICT: FAIL", text)
        self.assertNotIn("n/a", text)

    def test_winner_inside_a_losing_book_prints_a_negative_share(self):
        trades = [
            self._trade("SMCI", -30.0, 1, reason="stop-loss", thin=True),
            self._trade("AAPL", 5.0, 2),
        ]
        summary = sim.summarize(trades)
        self.assertAlmostEqual(summary["symbol_shares"]["SMCI"], 1.2)
        self.assertAlmostEqual(summary["symbol_shares"]["AAPL"], -0.2)
        text = sim.format_report(trades, label="HOLDOUT", verdict=True)
        self.assertIn("120.0%", text)
        self.assertIn("-20.0%", text)
        self.assertNotIn("n/a", text)

    def test_near_zero_net_skips_the_share_gate(self):
        trades = [
            self._trade("AAPL", 1.0, 1),
            self._trade("NVDA", -1.0, 2, reason="stop-loss"),
        ]
        summary = sim.summarize(trades)
        self.assertFalse(summary["symbol_share_scored"])
        self.assertIsNone(summary["max_symbol_share"])
        gates = sim.evaluate_gates(summary)
        self.assertTrue(gates["symbol_share"])
        self.assertFalse(gates["pass"])
        text = sim.format_report(trades, label="HOLDOUT", verdict=True)
        self.assertIn("SKIP", text)
        self.assertNotIn("n/a", text)

    def test_empty_book_still_fails(self):
        gates = sim.evaluate_gates(sim.summarize([]))
        self.assertFalse(gates["symbol_share"])
        self.assertFalse(gates["pass"])


class CarryAllAndDropBottomTests(unittest.TestCase):
    def test_carry_all_holds_a_red_close_and_earnings_still_flatten(self):
        # 99.5 is red versus the 100 fill and still above the 1% stop.
        red = sim.new_position(at(14, 57), 100.0, 0.0, tp=0.015, sl=0.010)
        live = sim.step_position(red, at(14, 58), 99.5, 99.5, 99.5, 99.5)
        self.assertEqual(live["reason"], "eod-autoclose")

        carried = sim.new_position(at(14, 57), 100.0, 0.0, tp=0.015, sl=0.010)
        event = sim.step_position(
            carried, at(14, 58), 99.5, 99.5, 99.5, 99.5, eod_policy="carry_all"
        )
        self.assertIsNone(event)
        self.assertTrue(carried["carried"])
        self.assertEqual(carried["skip_date"], at(14, 58).date())

        blocked = sim.new_position(at(14, 57), 100.0, 0.0, tp=0.015, sl=0.010)
        flat = sim.step_position(
            blocked, at(14, 58), 101.0, 101.0, 101.0, 101.0,
            earnings_blocked=True, eod_policy="carry_all",
        )
        self.assertEqual(flat["reason"], "eod-earnings-close")

    def test_carry_all_replay_is_overnight_and_live_is_same_day(self):
        symbols = ["NVDA"]
        bars = {
            "NVDA": _day(10.0, 0.0, day=8) + _day(10.0, 0.0, day=9),
            "SPY": _day(500.0, 0.02, day=8) + _day(500.0, 0.02, day=9),
        }
        costs = {"NVDA": (0.0, "table")}

        def always_buy(sym, prices, sector, is_trump, analyst):
            return {"buy": True, "rsi": 70.0}

        common = dict(
            symbols=symbols,
            recipes={},
            trump_theme=[],
            costs=costs,
            signal_fn=always_buy,
        )
        live = sim.replay(bars, {}, eod_policy="live", **common)
        carry = sim.replay(bars, {}, eod_policy="carry_all", **common)
        self.assertTrue(live)
        self.assertTrue(any(t["exit_reason"] == "eod-autoclose" and not t["overnight"] for t in live))
        self.assertTrue(any(t["overnight"] for t in carry))
        self.assertTrue(all(t["exit_reason"] != "eod-autoclose" or t["overnight"] for t in carry))
        with self.assertRaises(ValueError):
            sim.replay(bars, {}, eod_policy="nope", **common)

    def test_drop_bottom_keeps_the_upper_half_and_scores_holdout_only(self):
        def row(sym, pnl, holdout, exit_ts):
            return {
                "symbol": sym,
                "pnl_pct": pnl,
                "exit_reason": "take-profit" if pnl > 0 else "stop-loss",
                "exit_ts_ep": exit_ts,
                "overnight": False,
                "thin": False,
                "holdout": holdout,
                "trade_id": "bt_v4_abc",
            }

        trades = []
        # Worst first after the sort: DDD -20, CCC -5, then BBB +1, AAA +10.
        for i, (sym, pnl) in enumerate([("DDD", -20.0), ("CCC", -5.0), ("BBB", 1.0), ("AAA", 10.0)]):
            trades.append(row(sym, pnl, False, i + 1))
        for i, sym in enumerate(["AAA", "BBB", "CCC", "DDD", "SPCX"]):
            trades.append(row(sym, 0.40, True, 100 + i))

        kept_oos, dropped, kept, totals = sim.select_holdout_above_median(trades)
        self.assertEqual([s for s, _ in dropped], ["DDD", "CCC"])
        self.assertEqual([s for s, _ in kept], ["BBB", "AAA"])
        self.assertEqual(sorted(t["symbol"] for t in kept_oos), ["AAA", "BBB"])
        self.assertNotIn("SPCX", totals)
        text = sim.format_drop_bottom(trades)
        self.assertIn("DDD", text)
        self.assertIn("RESEARCH DROP-BOTTOM HOLDOUT", text)
        self.assertIn("VERDICT:", text)
        # Holdout of the kept names is two +0.40 trades. The dropped
        # names' holdout rows are not in the scored book.
        summary = sim.summarize(kept_oos)
        self.assertEqual(summary["n"], 2)
        self.assertAlmostEqual(summary["avg_pnl"], 0.40)

    def test_eleven_names_drop_five_and_keep_the_median(self):
        trades = []
        for i in range(11):
            trades.append({
                "symbol": f"S{i:02d}",
                "pnl_pct": -float(i),
                "exit_reason": "stop-loss",
                "exit_ts_ep": i,
                "overnight": False,
                "thin": False,
                "holdout": False,
                "trade_id": "bt_v4_abc",
            })
            trades.append({
                "symbol": f"S{i:02d}",
                "pnl_pct": 0.2,
                "exit_reason": "take-profit",
                "exit_ts_ep": 100 + i,
                "overnight": False,
                "thin": False,
                "holdout": True,
                "trade_id": "bt_v4_oos",
            })
        kept_oos, dropped, kept, _totals = sim.select_holdout_above_median(trades)
        self.assertEqual(len(dropped), 5)
        self.assertEqual(len(kept), 6)
        self.assertEqual(len(kept_oos), 6)
        # S00 pnl 0 is the best. S10 pnl -10 is the worst. Median S05 is kept.
        self.assertIn("S05", [s for s, _ in kept])
        self.assertIn("S10", [s for s, _ in dropped])
        self.assertNotIn("S05", [s for s, _ in dropped])

    def test_tied_in_sample_pnl_breaks_on_symbol_name(self):
        trades = [
            {"symbol": "BBB", "pnl_pct": 1.0, "exit_reason": "take-profit",
             "exit_ts_ep": 1, "holdout": False, "overnight": False, "thin": False},
            {"symbol": "AAA", "pnl_pct": 1.0, "exit_reason": "take-profit",
             "exit_ts_ep": 2, "holdout": False, "overnight": False, "thin": False},
            {"symbol": "AAA", "pnl_pct": 0.2, "exit_reason": "take-profit",
             "exit_ts_ep": 3, "holdout": True, "overnight": False, "thin": False},
            {"symbol": "BBB", "pnl_pct": 0.2, "exit_reason": "take-profit",
             "exit_ts_ep": 4, "holdout": True, "overnight": False, "thin": False},
        ]
        _oos, dropped, kept, _totals = sim.select_holdout_above_median(trades)
        self.assertEqual(dropped, [("AAA", 1.0)])
        self.assertEqual(kept, [("BBB", 1.0)])


if __name__ == "__main__":
    unittest.main()
