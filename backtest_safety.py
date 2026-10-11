"""Write switch for the nexus-backtester suite.

The Railway worker starts `python run_all_backtests.py` on every deploy
(Procfile). That process used to delete closed bt_% fingerprint rows and
insert a new book. Those rows match the live bot's
`trade_id LIKE 'bt_%'` win-rate query, so a deploy would move live gating.

Writes are off unless BACKTEST_WRITE_DB is one of: 1, true, yes, on.
Anything else, including an unset variable, is a dry run. A dry run still
replays, prints, and sends Telegram. It does not DELETE or INSERT, and it
does not rebuild pattern-stats tables.

`--dry-run` on a child script forces the dry run even when the variable
is on.
"""

import os

_ON = frozenset({"1", "true", "yes", "on"})


def writes_enabled() -> bool:
    return os.environ.get("BACKTEST_WRITE_DB", "").strip().lower() in _ON


def dry_run(cli_flag: bool = False) -> bool:
    """True when this process must not touch fingerprint tables."""
    return bool(cli_flag) or not writes_enabled()


def banner(writing: bool) -> str:
    """First line of a Telegram summary. Empty when writes are on."""
    if writing:
        return ""
    return "DRY RUN — no fingerprint deletes or inserts\n"
