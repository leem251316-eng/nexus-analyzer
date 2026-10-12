"""Alpaca crypto symbols for the backtester universe.

The live book uses Coinbase-style ``*-USDC`` names. Alpaca's historical
crypto API uses ``BASE/USD``. As of the March 2026 support table
(https://alpaca.markets/support/what-cryptocurrencies-does-alpaca-currently-support):

* POL/USD is listed. MATIC is not. Polygon rebranded MATIC to POL in
  September 2024, so a 365-day window ending in October 2026 is empty
  when the fetcher asks only for MATIC/USD. That is why POL-USDC came
  back EMPTY on the Oct 11 dry run.
* SUI is not listed. SUI/USD is empty for the same reason, and there is
  no alternate symbol to try.
"""

from __future__ import annotations

from typing import List

# March 2026 Alpaca support table. SUI is absent; MATIC is absent; POL is present.
SUI_NOT_LISTED = (
    "SUI/USD is not on Alpaca's March 2026 crypto support list, "
    "so SUI-USDC has no historical bars"
)


def alpaca_symbol_candidates(pair: str) -> List[str]:
    """Symbols to request, in order, for one live pair name."""
    if pair == "POL-USDC":
        return ["POL/USD", "MATIC/USD"]
    if pair == "SUI-USDC":
        return ["SUI/USD"]
    if pair.endswith("-USDC"):
        return [pair.replace("-USDC", "/USD")]
    return [pair]


def empty_pair_message(pair: str, tried: List[str], days: int) -> str:
    """One log line when every candidate came back empty."""
    joined = " then ".join(tried) if tried else "(none)"
    if pair == "SUI-USDC":
        return f"  {pair}: SKIP — tried {joined}. {SUI_NOT_LISTED}."
    if pair == "POL-USDC":
        return (
            f"  {pair}: SKIP — tried {joined} over the last {days} days "
            f"and both were empty. POL/USD is the post-September 2024 "
            f"Polygon name; MATIC/USD is the legacy name."
        )
    return f"  {pair}: SKIP — no bars from {joined} over the last {days} days."
