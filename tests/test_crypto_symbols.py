"""Symbol-candidate tests. No network. Alpaca is not installed here."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import crypto_symbols as cs


class CryptoSymbolTests(unittest.TestCase):
    def test_pol_tries_the_rebrand_then_the_legacy_name(self):
        self.assertEqual(cs.alpaca_symbol_candidates("POL-USDC"), ["POL/USD", "MATIC/USD"])
        self.assertEqual(cs.alpaca_symbol_candidates("BTC-USDC"), ["BTC/USD"])

    def test_sui_is_tried_once_and_the_skip_line_says_why(self):
        self.assertEqual(cs.alpaca_symbol_candidates("SUI-USDC"), ["SUI/USD"])
        msg = cs.empty_pair_message("SUI-USDC", ["SUI/USD"], 365)
        self.assertIn("SKIP", msg)
        self.assertIn("SUI/USD", msg)
        self.assertIn("March 2026", msg)
        self.assertIn("not on Alpaca", msg)

    def test_pol_skip_names_both_symbols_and_the_window(self):
        msg = cs.empty_pair_message("POL-USDC", ["POL/USD", "MATIC/USD"], 365)
        self.assertIn("SKIP", msg)
        self.assertIn("POL/USD", msg)
        self.assertIn("MATIC/USD", msg)
        self.assertIn("365", msg)
        self.assertIn("September 2024", msg)


if __name__ == "__main__":
    unittest.main()
