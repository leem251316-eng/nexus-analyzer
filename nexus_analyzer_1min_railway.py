#!/usr/bin/env python3
"""
nexus_analyzer_1min_railway.py V4.0 — NEXUS Berserker Backtester
=================================================================
V4.0 (Oct 2026): the replay follows the live Berserker path.

V3.3's exit engine was take-profit / stop-loss only, with no trail, no
EOD flatten, and no overnight carry, so its win rates do not describe
live Berserker (Trading-bot main.py V10.68). V4 delegates the book to
berserker_live_sim.py:

  * Hard TP (recipe, usually 1.5%) and hard SL (1.0%, SPCX 1.5%).
  * 1.5% trail off the peak, armed after 20 minutes.
  * 0.5% ratchet once current profit is +1.5% (preempted by TP).
  * EOD at 14:58 CT. Green and last close above the close 5 samples
    back carries; a short tape or an earnings blackout flattens.
    The next RTH open fills the gap.
  * Entry gates follow main.py V10.68. The fifth confluence leg is the
    analyst-service score and is modeled as 0 (service absent, does not
    block). Regime shutdown is score >= 4; bar-only inputs cannot reach
    it. SPY momentum blocks only a drop faster than -0.3%. See
    berserker_live_sim.py for the matched / approximated list.
  * Per-symbol half-spread (quote sample, else Roll on closes, else
    the documented table). Not a flat 5 bps.
  * Historical earnings dates (yfinance get_earnings_dates). The
    calendar snapshot is only a fallback when history is empty.
  * hold_time_min is clock minutes from entry_ts to exit_ts.
  * One continuous walk-forward. Entries before the 75% timestamp are
    in-sample and are the only rows written. Entries at or after it
    are the holdout: reported, scored PASS/FAIL, not written.

trade_id prefix is bt_v4_. Live BerserkerMemory.run_analysis includes
trade_id LIKE 'bt_%' (see reclassify_bt_rows.py: PatternMemory ingests
bt_ rows through that gate). bt_v4_ still matches, so a production write
feeds the live win-rate gate. The v4 tag is how Trading-bot can later
keep this engine (LIKE 'bt_v4_%') and ignore pre-v4 rows. This repo
does not change Trading-bot. When BACKTEST_WRITE_DB is on, the writer
still deletes every closed bt_% row and inserts the in-sample book.
The default is off, so a deploy does not do that.

Gates on the holdout (net of costs):
  avg P&L/trade >= +0.15%, profit factor >= 1.15,
  max drawdown <= 8 pp of cumulative per-trade P&L,
  no symbol > 25% of total P&L.

Usage:
  python nexus_analyzer_1min_railway.py                 # dry run (default)
  python nexus_analyzer_1min_railway.py --dry-run       # same, explicit
  BACKTEST_WRITE_DB=1 python nexus_analyzer_1min_railway.py
                                                        # delete/insert bt_ rows
  python nexus_analyzer_1min_railway.py --days 30 --no-quotes

Writes stay off unless BACKTEST_WRITE_DB is 1, true, yes, or on.
--dry-run forces them off even then. A dry run still prints and telegrams
and never connects to delete or insert.
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed, Adjustment

import backtest_safety
import berserker_live_sim as sim

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BERSERKER-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("berserker_bt")

DATABASE_URL     = os.environ.get("DATABASE_URL", "")
ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

TRUMP_THEME = ["CLSK", "MARA", "PLTR", "GEO", "CXW", "NUE", "MSTR"]
TECH_GROWTH = ["NVDA", "TSLA", "AAPL", "SMCI", "SPCX"]
# Live scan order. One new entry per minute walks this list and stops.
SYMBOLS     = TRUMP_THEME + TECH_GROWTH

BERSERKER_RECIPES = {
    "CLSK": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "MARA": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "PLTR": {"avoid_hours": [9, 11],  "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "GEO":  {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "CXW":  {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "NUE":  {"avoid_hours": [10, 12], "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "MSTR": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "NVDA": {"avoid_hours": [8, 9],   "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "TSLA": {"avoid_hours": [11, 10], "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "AAPL": {"avoid_hours": [8, 13],  "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "SMCI": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},
    "SPCX": {"avoid_hours": [9, 13],  "avoid_days": [], "tp": 0.015, "sl": 0.015},
}

# Closed bt_% rows are replaced on each production run (the V3 contract).
# New ids are bt_v4_, which still match LIKE 'bt_%'.
BT_DELETE_SQL = """
    DELETE FROM berserker_trade_fingerprints
    WHERE trade_id LIKE 'bt_%' AND won IS NOT NULL
"""

GATE_NOTE = (
    "Live BerserkerMemory.run_analysis includes trade_id LIKE 'bt_%'. "
    "bt_v4_ matches, so these in-sample rows feed that win-rate gate. "
    "A later Trading-bot change can require LIKE 'bt_v4_%' and ignore "
    "pre-v4 rows. Holdout trades are not written."
)


def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[ALERT] {msg}", flush=True)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=8,
        )
    except Exception:
        pass


def _coerce_ts(d) -> Optional[datetime]:
    if d is None:
        return None
    try:
        if hasattr(d, "to_pydatetime"):
            d = d.to_pydatetime()
        if not isinstance(d, datetime):
            d = datetime.combine(d, datetime.min.time())
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_earnings_dates(symbols: List[str]) -> Dict[str, List[datetime]]:
    """Historical earnings timestamps per symbol.

    yfinance get_earnings_dates returns the past prints (and the next
    estimate), which is what a 2-year replay can black out. ticker.calendar
    is only the current snapshot and is used when the history pull is empty.
    """
    earnings_map: Dict[str, List[datetime]] = {s: [] for s in symbols}
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance not available — earnings blackout disabled")
        return earnings_map

    log.info(f"Fetching historical earnings dates for {len(symbols)} symbols...")
    for sym in symbols:
        dates: List[datetime] = []
        try:
            ticker = yf.Ticker(sym)
            frame = None
            getter = getattr(ticker, "get_earnings_dates", None)
            if callable(getter):
                try:
                    frame = getter(limit=80)
                except Exception as exc:
                    log.warning(f"  {sym}: get_earnings_dates failed ({exc})")
                    frame = None
            if frame is None or (hasattr(frame, "empty") and frame.empty):
                frame = getattr(ticker, "earnings_dates", None)
            if frame is not None and hasattr(frame, "index") and len(frame.index):
                for raw in frame.index:
                    ts = _coerce_ts(raw)
                    if ts is not None:
                        dates.append(ts)
            if not dates:
                log.warning(
                    f"  {sym}: no historical earnings; falling back to the "
                    f"current calendar snapshot"
                )
                cal = getattr(ticker, "calendar", None)
                raw_dates = []
                if isinstance(cal, dict):
                    raw_dates = cal.get("Earnings Date") or []
                elif hasattr(cal, "loc") and cal is not None:
                    try:
                        raw_dates = cal.loc["Earnings Date"].tolist()
                    except Exception:
                        raw_dates = []
                if not hasattr(raw_dates, "__iter__") or isinstance(raw_dates, str):
                    raw_dates = [raw_dates]
                for raw in raw_dates:
                    ts = _coerce_ts(raw)
                    if ts is not None:
                        dates.append(ts)
        except Exception as exc:
            log.warning(f"  {sym}: earnings fetch failed ({exc})")
        # Dedupe, keep chronological.
        uniq = sorted({int(d.timestamp()): d for d in dates}.values(),
                      key=lambda d: d.timestamp())
        earnings_map[sym] = uniq
        if uniq:
            log.info(
                f"  {sym}: {len(uniq)} earnings "
                f"{uniq[0].date().isoformat()} -> {uniq[-1].date().isoformat()}"
            )
        else:
            log.info(f"  {sym}: no earnings dates (blackout off for this name)")
        time.sleep(0.2)
    return earnings_map


def sample_quote_half_spreads(symbols: List[str]) -> Dict[str, float]:
    """Median IEX half-spread from a recent quote window. {} on any failure.

    Half-spread = (ask-bid) / (ask+bid), which is (ask-bid)/mid/2.
    One call for the whole basket. A symbol with fewer than 20 valid
    quotes is left unset so the Roll estimator or the table can fill it.
    """
    if not ALPACA_API_KEY or not ALPACA_SECRET or not symbols:
        return {}
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=4)
    url = "https://data.alpaca.markets/v2/stocks/quotes"
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    params = {
        "symbols": ",".join(symbols),
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": 10000,
        "feed": "iex",
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json().get("quotes") or {}
    except Exception as exc:
        log.warning(f"Quote sample failed ({exc}); using bars or the table")
        return {}

    out: Dict[str, float] = {}
    for sym in symbols:
        halves = []
        for q in payload.get(sym) or []:
            ask = q.get("ap")
            bid = q.get("bp")
            if not ask or not bid or ask <= bid:
                continue
            half = (ask - bid) / (ask + bid)
            if 0.00005 <= half <= 0.02:
                halves.append(half)
        if len(halves) < 20:
            continue
        halves.sort()
        out[sym] = float(halves[len(halves) // 2])
    log.info(f"Quote half-spreads for {len(out)}/{len(symbols)} symbols")
    return out


def fetch_all_bars(days: int) -> dict:
    """Fetch 1-min bars for SYMBOLS + SPY + QQQ. Adjusted IEX."""
    client   = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc).replace(hour=21, minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=days)
    all_syms = list(SYMBOLS) + ["SPY", "QQQ"]

    log.info(f"Fetching {days}d 1-min bars for {len(all_syms)} symbols...")
    log.info(f"Range: {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")

    result = {}
    for i, sym in enumerate(all_syms):
        try:
            bars = client.get_stock_bars(StockBarsRequest(
                symbol_or_symbols=sym,
                timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                start=start_dt,
                end=end_dt,
                feed=DataFeed.IEX,
                adjustment=Adjustment.ALL,
            ))
            df = bars.df
            if hasattr(df.index, "levels"):
                df = df.xs(sym, level=0)
            if not df.empty:
                result[sym] = df
                log.info(f"  [{i+1}/{len(all_syms)}] {sym}: {len(df):,} bars")
            else:
                log.warning(f"  [{i+1}/{len(all_syms)}] {sym}: EMPTY")
        except Exception as e:
            log.error(f"  [{i+1}/{len(all_syms)}] {sym}: {e}")
        time.sleep(0.3)

    log.info(f"Fetched {len(result)}/{len(all_syms)} symbols")
    return result


def _closes_for_costs(all_bars: dict) -> Dict[str, list]:
    out = {}
    for sym in SYMBOLS:
        df = all_bars.get(sym)
        if df is None or not hasattr(df, "columns") or "close" not in df.columns:
            continue
        out[sym] = [float(x) for x in df["close"].tolist() if x == x and x > 0]
    return out


def replay_berserker(all_bars: dict,
                     earnings_map: Dict[str, List[datetime]],
                     validate_mode: bool = False,
                     half_spreads: Optional[Dict[str, float]] = None) -> list:
    """Census-compatible wrapper. Reads SYMBOLS / recipes at call time.

    validate_mode=False returns the full continuous book (each trade tagged
    holdout). validate_mode=True returns only entries at or after the 75%
    timestamp of that same book — not a second simulation that restarts flat.
    """
    costs = sim.resolve_costs(
        list(SYMBOLS), _closes_for_costs(all_bars), quote_half=half_spreads
    )
    return sim.replay(
        all_bars,
        earnings_map,
        symbols=list(SYMBOLS),
        recipes=BERSERKER_RECIPES,
        trump_theme=list(TRUMP_THEME),
        costs=costs,
        validate_mode=validate_mode,
    )


def write_fingerprints(trades: List[dict], dry_run: bool = False) -> int:
    """Replace closed bt_% rows with the in-sample V4 book.

    Holdout trades are dropped here even if the caller passed the full
    book. A dry run (the default) and a missing DATABASE_URL never connect.
    """
    rows = sim.rows_for_db(trades)
    skipped = len(trades) - len(rows)
    if skipped:
        log.info(f"  Holdout rows kept out of the DB: {skipped}")
    if dry_run or not backtest_safety.writes_enabled() or not DATABASE_URL:
        log.info(
            f"  DRY RUN: not deleting or inserting. "
            f"In-sample bt_v4_ rows left untouched: {len(rows)}"
        )
        log.info(f"  {GATE_NOTE}")
        return 0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        written = 0
        with conn.cursor() as cur:
            cur.execute(BT_DELETE_SQL)
            deleted = cur.rowcount
            log.info(f"  Deleted {deleted} prior closed bt_% rows")
            for t in rows:
                try:
                    cur.execute("""
                        INSERT INTO berserker_trade_fingerprints
                        (trade_id, symbol, sector,
                         entry_ts, exit_ts, entry_price,
                         symbol_rsi, spy_bullish, spy_rsi, spy_momentum,
                         sector_health, hour_cdt, day_of_week,
                         won, pnl_pct, exit_reason, hold_time_min,
                         mfe, mae, is_paper)
                        VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s,
                                %s,%s,%s,%s, %s,%s,%s)
                        ON CONFLICT (trade_id) DO UPDATE
                        SET won=EXCLUDED.won, pnl_pct=EXCLUDED.pnl_pct,
                            exit_reason=EXCLUDED.exit_reason,
                            mfe=EXCLUDED.mfe, mae=EXCLUDED.mae
                    """, (
                        t["trade_id"],
                        t["symbol"],
                        t.get("sector", "TECH"),
                        t.get("entry_ts_ep") or int(time.time()),
                        t.get("exit_ts_ep") or int(time.time()),
                        round(t.get("entry_price", 0.0), 4),
                        round(t.get("rsi_at_entry", 50.0), 2),
                        bool(t.get("spy_bullish", False)),
                        round(t.get("spy_rsi", 50.0), 2),
                        round(t.get("spy_momentum", 0.0), 4),
                        t.get("sector_health", "STRONG"),
                        t.get("hour_cdt", 12),
                        t.get("day_of_week", 0),
                        bool(t["won"]),
                        round(t["pnl_pct"], 3),
                        t["exit_reason"],
                        t.get("hold_min", 0),
                        round(t.get("mfe", 0), 3),
                        round(t.get("mae", 0), 3),
                        False,
                    ))
                    written += 1
                except Exception as e:
                    log.warning(f"fingerprint error [{t.get('symbol','?')}]: {e}")
                    break
        conn.commit()
        conn.close()
        log.info(f"  Wrote {written}/{len(rows)} bt_v4_ fingerprints")
        log.info(f"  {GATE_NOTE}")
        return written
    except Exception as e:
        log.error(f"DB write error: {e}")
        return 0


def run_pattern_analysis() -> Tuple[int, float]:
    """Rebuild berserker_pattern_stats from every closed fingerprint.

    This is the local equivalent of BerserkerMemory.run_analysis: it does
    not filter trade_id. After a production write the table holds live rows
    plus bt_v4_ in-sample rows (pre-v4 bt_ rows were deleted). Holdout
    trades were never inserted, so they do not move the buckets.
    """
    if not backtest_safety.writes_enabled():
        log.info("  DRY RUN: skipping pattern analysis")
        return 0, 0.0
    if not DATABASE_URL:
        return 0, 0.0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, symbol_rsi as rsi_at_entry, spy_bullish,
                       sector_health, sector,
                       hour_cdt, day_of_week, won, pnl_pct, mfe, mae
                FROM berserker_trade_fingerprints WHERE won IS NOT NULL
            """)
            rows = cur.fetchall()

        if not rows:
            conn.close()
            return 0, 0.0

        from collections import defaultdict
        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)

        for row in rows:
            rsi    = row["rsi_at_entry"] or 50
            hour   = row["hour_cdt"]   or 12
            rsi_b  = "rsi_hi" if rsi > 72 else "rsi_mid" if rsi > 62 else "rsi_low"
            spy_b  = "spy_bull" if row["spy_bullish"] else "spy_bear"
            sec_b  = row["sector_health"] or "STRONG"
            hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
            sec_type = row["sector"] or "TECH"
            key    = f"{row['symbol']}|{rsi_b}|{spy_b}|{sec_b}|{sec_type}|{hr_b}"
            buckets[key].append(bool(row["won"]))
            if row["pnl_pct"] is not None:
                pnl_bkts[key].append(float(row["pnl_pct"]))

        written = 0
        with conn.cursor() as cur:
            for key, outcomes in buckets.items():
                if len(outcomes) < 3:
                    continue
                wr      = sum(outcomes) / len(outcomes)
                avg_pnl = sum(pnl_bkts[key]) / len(pnl_bkts[key]) if pnl_bkts[key] else None
                cur.execute("""
                    INSERT INTO berserker_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (bucket_key) DO UPDATE
                    SET win_rate=EXCLUDED.win_rate, sample_count=EXCLUDED.sample_count,
                        avg_pnl=EXCLUDED.avg_pnl, last_updated=NOW()
                """, (key, wr, len(outcomes), avg_pnl))
                written += 1
        conn.commit()

        total = len(rows)
        wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
        conn.close()
        log.info(f"  Pattern analysis: {written} buckets | {total} trades | {wr:.1%} overall WR")
        return written, wr
    except Exception as e:
        log.error(f"Pattern analysis error: {e}")
        return 0, 0.0


def _log_costs(costs: dict):
    log.info("Per-symbol half-spread (entry and exit):")
    for sym in SYMBOLS:
        half, source = costs.get(sym, (None, "missing"))
        if half is None:
            log.info(f"  {sym:<6} missing")
            continue
        thin = "thin" if sym in sim.THIN_SYMBOLS else "liquid"
        log.info(f"  {sym:<6} {half * 10000:6.1f} bps half  {source:<8} {thin}")


def main():
    parser = argparse.ArgumentParser(description="NEXUS Berserker Backtester V4")
    parser.add_argument("--days", type=int, default=730)
    parser.add_argument("--dry-run", action="store_true",
                        help="Replay and report. Do not connect to the database.")
    parser.add_argument("--no-validate", action="store_true",
                        help="Skip the holdout PASS/FAIL section.")
    parser.add_argument("--no-earnings", action="store_true",
                        help="Skip the historical earnings fetch.")
    parser.add_argument("--no-quotes", action="store_true",
                        help="Skip the IEX quote sample; use Roll or the table.")
    args = parser.parse_args()
    dry_run = backtest_safety.dry_run(args.dry_run)

    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")
        sys.exit(1)

    log.info("=" * 60)
    log.info("NEXUS BERSERKER BACKTESTER V4.0")
    log.info(f"Symbols: {len(SYMBOLS)} | Days: {args.days}")
    log.info("Exits: TP / SL / 1.5% trail after 20m / ratchet / EOD 14:58 CT")
    log.info(GATE_NOTE)
    log.info("DB writes: %s", "ON" if not dry_run else "OFF (DRY RUN)")
    log.info("=" * 60)

    send_alert(
        f"{backtest_safety.banner(not dry_run)}"
        f"🔥 NEXUS BERSERKER BACKTESTER V4.0 STARTING\n"
        f"Symbols: {len(SYMBOLS)} | Days: {args.days}\n"
        f"Live-path exits (TP/SL/trail/EOD carry) | per-symbol costs\n"
        f"Holdout is the last 25% of the book and is not written\n"
        f"Research (not written): carry-all overnight, drop-bottom half\n"
        f"ETA: ~30-45 min"
    )

    start_time = time.time()

    earnings_map: Dict[str, List[datetime]] = {s: [] for s in SYMBOLS}
    if not args.no_earnings:
        earnings_map = fetch_earnings_dates(SYMBOLS)

    all_bars = fetch_all_bars(args.days)
    if not all_bars:
        log.error("No bar data fetched")
        sys.exit(1)

    quotes: Dict[str, float] = {}
    if not args.no_quotes:
        quotes = sample_quote_half_spreads(list(SYMBOLS))
    costs = sim.resolve_costs(list(SYMBOLS), _closes_for_costs(all_bars), quote_half=quotes)
    _log_costs(costs)

    log.info("Running continuous replay (in-sample + holdout from one book)...")
    trades = sim.replay(
        all_bars,
        earnings_map,
        symbols=list(SYMBOLS),
        recipes=BERSERKER_RECIPES,
        trump_theme=list(TRUMP_THEME),
        costs=costs,
        validate_mode=False,
    )
    in_sample, holdout = sim.split_holdout(trades)
    log.info(f"Replay complete: {len(in_sample)} in-sample, {len(holdout)} holdout")
    ins_label = (
        "IN-SAMPLE (first 75%, DRY RUN — not written)" if dry_run
        else "IN-SAMPLE (first 75%, written)"
    )
    print(sim.format_report(in_sample, label=ins_label, verdict=False))
    gates = None
    if not args.no_validate:
        print(sim.format_report(
            holdout,
            label="OUT-OF-SAMPLE HOLDOUT (last 25%, not written)",
            verdict=True,
        ))
        gates = sim.evaluate_gates(sim.summarize(holdout))

    # Research only. Same bars, costs, and earnings as the baseline.
    # Neither book is passed to write_fingerprints.
    log.info(
        "Research carry-all: same entry gates; at 14:58 every non-earnings "
        "position is carried. A carried name still occupies a slot, so "
        "later entries can differ from the baseline. Not written."
    )
    carry_trades = sim.replay(
        all_bars,
        earnings_map,
        symbols=list(SYMBOLS),
        recipes=BERSERKER_RECIPES,
        trump_theme=list(TRUMP_THEME),
        costs=costs,
        validate_mode=False,
        eod_policy="carry_all",
    )
    carry_in, carry_out = sim.split_holdout(carry_trades)
    print(sim.format_report(
        carry_in,
        label="RESEARCH CARRY-ALL IN-SAMPLE (not written)",
        verdict=False,
    ))
    print(sim.format_report(
        carry_out,
        label="RESEARCH CARRY-ALL HOLDOUT (not written)",
        verdict=True,
    ))
    log.info(
        "Research drop-bottom: drop the worse half of symbols by in-sample "
        "total P&L. Symbols with no in-sample trades are not kept. "
        "Judged on holdout only. Not written."
    )
    print(sim.format_drop_bottom(trades))
    _drop_rows, dropped_syms, kept_syms, _is_totals = sim.select_holdout_above_median(trades)

    written = 0
    if trades:
        log.info(
            f"Fingerprint step: {len(in_sample)} in-sample rows "
            f"({'DRY RUN' if dry_run else 'WRITE'})"
        )
        written = write_fingerprints(trades, dry_run)

    buckets, _overall_wr = 0, 0.0
    if not dry_run and DATABASE_URL and written > 0:
        log.info("Running pattern analysis...")
        buckets, _overall_wr = run_pattern_analysis()

    elapsed = round(time.time() - start_time)
    ins = sim.summarize(in_sample)
    oos = sim.summarize(holdout)
    verdict = "SUPPRESSED" if gates is None else ("PASS" if gates["pass"] else "FAIL")
    carry_oos = sim.summarize(carry_out)
    carry_g = sim.evaluate_gates(carry_oos)
    drop_oos = sim.summarize(_drop_rows)
    drop_g = sim.evaluate_gates(drop_oos)
    carry_verdict = "PASS" if carry_g["pass"] else "FAIL"
    drop_verdict = "PASS" if drop_g["pass"] else "FAIL"

    send_alert(
        f"{backtest_safety.banner(not dry_run)}"
        f"✅ NEXUS BERSERKER BACKTESTER V4.0 COMPLETE\n"
        f"──────────────────\n"
        f"In-sample: {ins['n']} trades  avg {ins['avg_pnl']:+.3f}%  "
        f"PF {sim._fmt_pf(ins['profit_factor'])}\n"
        f"Holdout:   {oos['n']} trades  avg {oos['avg_pnl']:+.3f}%  "
        f"PF {sim._fmt_pf(oos['profit_factor'])}  "
        f"DD {oos['max_drawdown']:.2f}pp\n"
        f"HOLDOUT VERDICT: {verdict}\n"
        f"Carry-all holdout: {carry_oos['n']} avg {carry_oos['avg_pnl']:+.3f}%  "
        f"{carry_verdict} (not written)\n"
        f"Drop-bottom holdout: kept {len(kept_syms)} dropped {len(dropped_syms)}  "
        f"{drop_oos['n']} avg {drop_oos['avg_pnl']:+.3f}%  "
        f"{drop_verdict} (not written)\n"
        f"──────────────────\n"
        f"Fingerprints written (bt_v4_, baseline in-sample only): {written:,}\n"
        f"Pattern buckets: {buckets}\n"
        f"bt_v4_ matches live LIKE 'bt_%' and feeds the win-rate gate.\n"
        f"Elapsed: {elapsed}s"
    )
    log.info(
        f"DONE. wrote {written} | holdout {verdict} | "
        f"carry-all {carry_verdict} | drop-bottom {drop_verdict} | {elapsed}s"
    )


if __name__ == "__main__":
    main()
