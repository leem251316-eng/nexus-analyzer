"""orb_verdict.py pure-function tests. No network, no database.

    python -m unittest tests.test_orb_verdict
"""

import os
import re
import sys
import unittest
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import orb_verdict as ov

CT = ZoneInfo("America/Chicago")
SRC = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "orb_verdict.py")).read()


def ep(d, h, m):
    return int(datetime(d.year, d.month, d.day, h, m, tzinfo=CT).timestamp())


D1, D2, D3 = date(2026, 10, 21), date(2026, 10, 22), date(2026, 10, 23)


class ReadOnlyTests(unittest.TestCase):
    def test_no_write_sql(self):
        for kw in ("INSERT", "UPDATE ", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE"):
            self.assertNotIn(kw, SRC.upper().replace("UPDATED", ""), kw)

    def test_readonly_session(self):
        self.assertIn("set_session(readonly=True", SRC)

    def test_same_engine_as_band(self):
        import band_verdict
        self.assertIs(ov.simulate_bracket, band_verdict.simulate_bracket)
        self.assertEqual(ov.FEE_PCT, 0.05)


class FirstSignalTests(unittest.TestCase):
    def test_first_row_per_session_symbol(self):
        rows = [
            (ep(D1, 9, 1), "MARA", 20.0, ov.BLOCK),
            (ep(D1, 9, 40), "MARA", 20.9, ov.PASS),      # later: ignored
            (ep(D1, 9, 2), "PLTR", 0.0, ov.PASS),        # bad price: skipped
            (ep(D1, 9, 3), "PLTR", 30.0, ov.PASS),
            (ep(D1, 9, 4), "SMCI", 40.0, ov.NODATA),     # first is nodata: excluded
            (ep(D1, 9, 50), "SMCI", 41.0, ov.PASS),
            (ep(D2, 9, 1), "MARA", 21.0, ov.PASS),
            (ep(D1, 9, 6), "AAPL", 10.0, "band_0905"),   # other reason: ignored
        ]
        firsts, nodata, per = ov.first_signals(rows, [D1, D2])
        got = [(f[1], f[3], f[4]) for f in firsts]
        self.assertEqual(got, [("MARA", ov.BLOCK, D1), ("PLTR", ov.PASS, D1),
                               ("MARA", ov.PASS, D2)])
        self.assertEqual(nodata, {D1: 1})
        self.assertEqual(per[D1], 6)

    def test_outside_window_and_excluded(self):
        rows = [(ep(D1, 9, 1), "MARA", 20.0, ov.BLOCK),
                (ep(D3, 9, 1), "MARA", 20.0, ov.BLOCK)]
        firsts, _n, _p = ov.first_signals(rows, [D1, D2], excluded={D1})
        self.assertEqual(firsts, [])


class WindowTests(unittest.TestCase):
    def test_states(self):
        self.assertEqual(ov.window_status(19, 19, 99, 99)[0], "INTERIM")
        self.assertEqual(ov.window_status(20, 20, 40, 15)[0], "READY")
        self.assertEqual(ov.window_status(20, 20, 39, 15)[0], "EXTEND")
        self.assertEqual(ov.window_status(25, 25, 40, 14)[0], "EXTEND")
        self.assertEqual(ov.window_status(20, 14, 40, 15)[0], "EXTEND")
        self.assertEqual(ov.window_status(30, 30, 40, 14)[0], "PARK")
        self.assertEqual(ov.window_status(30, 30, 40, 15)[0], "READY")

    def test_sessions_after_deploy(self):
        dep, today = date(2026, 10, 20), date(2026, 10, 27)
        self.assertEqual(ov.trading_sessions_after(dep, today),
                         [date(2026, 10, 21), date(2026, 10, 22), date(2026, 10, 23),
                          date(2026, 10, 26)])
        cal = [dep, date(2026, 10, 21), date(2026, 10, 23), today]
        self.assertEqual(ov.trading_sessions_after(dep, today, cal),
                         [date(2026, 10, 21), date(2026, 10, 23)])


class GateTests(unittest.TestCase):
    def test_gate_a_diff_and_prob(self):
        ok, diff = ov.gate_a([0.3, 0.3], [0.3, 0.3, -0.2, -0.2], 0.9)
        self.assertAlmostEqual(diff, 0.25)
        self.assertTrue(ok)
        self.assertFalse(ov.gate_a([0.3, 0.3], [0.3, 0.3, -0.2, -0.2], 0.79)[0])
        self.assertFalse(ov.gate_a([0.1], [0.1, 0.05], 0.99)[0])     # diff 0.025
        self.assertEqual(ov.gate_a([], [0.1], 1.0), (False, None))

    def test_gate_a_is_relative_not_absolute(self):
        # Book negative everywhere; the filter still passes if it helps enough.
        ok, diff = ov.gate_a([-0.2] * 10, [-0.2] * 10 + [-0.6] * 10, 0.95)
        self.assertTrue(ok)
        self.assertAlmostEqual(diff, 0.2)

    def test_gate_b(self):
        self.assertTrue(ov.gate_b([0.10], [-0.10]))    # net 0.05 / -0.15
        self.assertFalse(ov.gate_b([0.10], [0.0]))     # blocked net -0.05 > -0.10
        self.assertFalse(ov.gate_b([-0.30], [-0.40]))  # gap only 0.10
        self.assertFalse(ov.gate_b([], [-1.0]))

    def test_gate_c(self):
        ok, m, n = ov.gate_c([(0.0, 0.05), (-1.0, -0.95)])
        self.assertTrue(ok)
        self.assertAlmostEqual(m, 0.0)
        self.assertEqual(n, 2)
        self.assertFalse(ov.gate_c([(-1.0, 0.0)])[0])
        self.assertEqual(ov.gate_c([]), (False, None, 0))

    def test_ridge(self):
        ok, rows, neg = ov.ridge({"A": 3, "B": 3, "C": 2, "D": 4},
                                 {"A": [-1, -1, 0.5], "B": [1, 1, -0.1],
                                  "C": [-1, -1], "D": [-0.2]})
        self.assertEqual([r[0] for r in rows], ["A", "B", "D"])
        self.assertEqual(neg, 2)
        self.assertTrue(ok)                      # 2/3 >= 60%
        ok2, _r, _n = ov.ridge({"A": 3, "B": 3}, {"A": [-1] * 3, "B": [1] * 3})
        self.assertFalse(ok2)                    # 1/2
        self.assertFalse(ov.ridge({"A": 2}, {"A": [-1, -1]})[0])   # none qualify
        self.assertFalse(ov.ridge({"A": 3}, {})[0])                # no sims != negative

    def test_decide_order(self):
        self.assertEqual(ov.decide("INTERIM", True, True, True, True), "INTERIM")
        self.assertEqual(ov.decide("EXTEND", True, True, True, True), "EXTEND")
        self.assertEqual(ov.decide("PARK", True, True, True, True), "PARK")
        self.assertEqual(ov.decide("READY", False, False, False, False), "PARK")
        self.assertEqual(ov.decide("READY", True, False, True, True), "REVERT")
        self.assertEqual(ov.decide("READY", True, True, False, True), "REVERT")
        self.assertEqual(ov.decide("READY", True, True, True, False), "PARK")
        self.assertEqual(ov.decide("READY", True, True, True, True), "KEEP")


class BootstrapTests(unittest.TestCase):
    def test_deterministic_and_directional(self):
        good = []
        for i in range(20):
            d = date(2026, 11, 1) + timedelta(days=i)
            good += [(d, ov.PASS, 0.5), (d, ov.BLOCK, -0.5), (d, ov.BLOCK, -0.4)]
        p1 = ov.bootstrap_prob(good, reps=300)
        self.assertEqual(p1, ov.bootstrap_prob(good, reps=300))
        self.assertGreater(p1, 0.99)
        bad = [(d, ov.PASS if r == ov.BLOCK else ov.BLOCK, p) for d, r, p in good]
        self.assertLess(ov.bootstrap_prob(bad, reps=300), 0.01)
        self.assertEqual(ov.bootstrap_prob([]), 0.0)


class MatchTests(unittest.TestCase):
    def test_earliest_same_session_after_pass(self):
        f1 = (ep(D1, 9, 31), "MARA", 20.0, ov.PASS, D1)
        f2 = (ep(D2, 9, 31), "MARA", 20.0, ov.PASS, D2)
        live = [(ep(D1, 9, 30), "MARA", 19.9, 0.1),   # before pass
                (ep(D1, 9, 33), "MARA", 20.1, -1.0),
                (ep(D1, 10, 0), "MARA", 20.5, 1.5),
                (ep(D1, 9, 40), "PLTR", 30.0, 0.2)]
        m = ov.match_live([f1, f2], live)
        self.assertEqual(len(m), 1)
        self.assertEqual(m[0][1][0], ep(D1, 9, 33))


class KillSwitchTests(unittest.TestCase):
    def test_all_quiet(self):
        sess = [date(2026, 11, 2) + timedelta(days=i) for i in range(6)]
        firsts = []
        for d in sess:
            firsts += [(0, "A", 1, ov.PASS, d), (0, "B", 1, ov.BLOCK, d)]
        ks = ov.kill_switches(sess, {d: 3 for d in sess}, firsts, {}, [0.1] * 9)
        self.assertEqual([f for _n, f, _d in ks], [False, False, False, False])

    def test_each_fires(self):
        sess = [date(2026, 11, 2) + timedelta(days=i) for i in range(10)]
        per = {d: 1 for d in sess[3:]}                 # 3 empty sessions
        firsts = [(0, "B", 1, ov.BLOCK, d) for d in sess]   # 0% pass rate
        nodata = {d: 5 for d in sess}                  # 5/6 nodata
        ks = ov.kill_switches(sess, per, firsts, nodata, [-1.5] * 8)
        self.assertEqual([f for _n, f, _d in ks], [True, True, True, True])

    def test_ks2_needs_ten_sessions_and_eight_trades(self):
        sess = [date(2026, 11, 2) + timedelta(days=i) for i in range(9)]
        ks = ov.kill_switches(sess, {d: 1 for d in sess}, [], {}, [-2.0] * 8)
        self.assertFalse(ks[1][1])


if __name__ == "__main__":
    unittest.main()
