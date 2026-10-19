"""flat_verdict.py pure-function tests. No network, no database."""

import os
import re
import sys
import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import flat_verdict as fv

CT = ZoneInfo("America/Chicago")
SRC = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "flat_verdict.py")).read()
D0, D1, D2 = date(2026, 10, 19), date(2026, 10, 20), date(2026, 10, 21)


def ep(d, h, m):
    return int(datetime(d.year, d.month, d.day, h, m, tzinfo=CT).timestamp())


class ReadOnly(unittest.TestCase):
    def test_no_write_sql(self):
        for kw in ("INSERT", "UPDATE ", "DELETE", "CREATE", "DROP", "ALTER"):
            self.assertIsNone(re.search(r"\b" + kw, SRC.split('"""', 2)[2]), kw)
        self.assertIn("readonly=True", SRC)


class Pure(unittest.TestCase):
    def test_dedup(self):
        rows = [(D0, "A", 10, 11, fv.ELIGIBLE, True),     # deploy day excluded
                (D1, "A", 10, 11, fv.ELIGIBLE, True),
                (D1, "A", 10, 12, fv.ELIGIBLE, True),     # dup
                (D1, "B", 10, 11, "flat_close", True),
                (D1, "C", 10, 11, fv.ELIGIBLE, False),
                (D1, "D", 0, 11, fv.ELIGIBLE, True)]
        self.assertEqual(fv.dedup(rows, D0), [(D1, "A", 10.0, 11.0)])

    def test_eod(self):
        bars = [(ep(D2, 8, 30), 101, 101.2, 100.8, 101), (ep(D2, 14, 57), 101, 101, 101, 101.5),
                (ep(D2, 14, 58), 90, 90, 90, 90)]
        d, r = fv.simulate_carry(100, 101, bars, 0.01)
        self.assertEqual(r, "eod")
        self.assertAlmostEqual(d, (101.5 / 101 - 1) * 100)

    def test_gap_sl(self):
        d, r = fv.simulate_carry(100, 101, [(ep(D2, 8, 30), 95, 96, 94, 95)], 0.01)
        self.assertEqual(r, "gap_sl")
        self.assertAlmostEqual(d, (95 / 101 - 1) * 100)

    def test_gap_tp(self):
        d, r = fv.simulate_carry(100, 101, [(ep(D2, 8, 30), 103, 103, 103, 103)], 0.01)
        self.assertEqual(r, "gap_tp")

    def test_tp_then_sl_and_trail(self):
        bars = [(ep(D2, 8, 30), 101, 101.6, 101, 101.5)]
        self.assertEqual(fv.simulate_carry(100, 101, bars, 0.01)[1], "tp")
        bars = [(ep(D2, 8, 30), 101, 101.4, 101, 101.3), (ep(D2, 8, 31), 101, 101, 99.7, 99.8)]
        d, r = fv.simulate_carry(100, 101, bars, 0.01)
        self.assertEqual(r, "trail")
        self.assertAlmostEqual(d, (101.4 * 0.985 / 101 - 1) * 100)
        bars = [(ep(D2, 8, 30), 100, 100.2, 98.9, 99)]
        self.assertEqual(fv.simulate_carry(100, 100.2, bars, 0.01)[1], "sl")

    def test_none(self):
        self.assertIsNone(fv.simulate_carry(100, 101, [], 0.01))

    def test_next_session(self):
        bars = [(ep(D1, 14, 0), 1, 1, 1, 1), (ep(D2, 9, 0), 2, 2, 2, 2)]
        self.assertEqual(fv.next_session_bars(bars, D1), [bars[1]])
        self.assertEqual(fv.next_session_bars(bars, D2), [])

    def test_decide(self):
        self.assertEqual(fv.decide(19, 1.0, 0.0), "INTERIM")
        self.assertEqual(fv.decide(20, 0.30, 0.0), "REVERT")
        self.assertEqual(fv.decide(25, 0.10, 0.0), "INTERIM")
        self.assertEqual(fv.decide(30, -0.1, 0.85), "KEEP")
        self.assertEqual(fv.decide(30, -0.1, 0.5), "PARK")
        self.assertEqual(fv.decide(30, 0.1, 0.9), "PARK")

    def test_bootstrap_deterministic(self):
        by = {D1: [-0.5, -0.2], D2: [0.1]}
        a = fv.bootstrap_prob_le0(by)
        self.assertEqual(a, fv.bootstrap_prob_le0(by))
        self.assertGreater(a, 0.5)
        self.assertEqual(fv.bootstrap_prob_le0({}), 0.0)


if __name__ == "__main__":
    unittest.main()
