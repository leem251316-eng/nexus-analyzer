#!/usr/bin/env python3
"""
scanner_backtester.py V1.2 — NEXUS Scanner Backtester
=======================================================
Pulls 2yr 1-min Alpaca IEX bars for Scanner's 44-symbol universe, replays
through the EXACT Scanner V2.4 signal engine (scan_for_entry, manage_scanner_
exits, bucket key), writes fingerprints to scanner_trade_fingerprints,
triggers pattern analysis.

V1.2 fix (Jul 8 2026): Slippage sign on exits. Both exit sites used
`(1 - SLIP if winning else 1 + SLIP)` -- but a market sell crosses the
spread against you whether the trade won or lost. Adding slippage to the
exit price on losers made every losing trade look ~0.05% better than
reality, biasing WR/EV stats and the seeded fingerprints optimistic on
exactly the trades that hurt.

V1.1 fix (Jun 30 2026): First real run (15,362 trades, full 2yr) failed the
DB write almost entirely -- only 5/15,362 rows landed. Root cause: vol_ratio
was computed as latest_vol / avg_vol where avg_vol came from a pandas
Series.mean(), which returns numpy.float64. Dividing produces numpy.float64
even though the inputs upstream had been through Python's float(). psycopg2
can't adapt numpy scalars -- it serialized them using numpy's own repr style
("np.float64(5.0)") instead of a plain numeric literal, and Postgres tried
to parse "np" as a schema name ("schema np does not exist"). Worse: the
original write loop wrapped the ENTIRE per-trade insert loop in one
try/except, so the single numpy-typed row anywhere in the batch rolled back
the whole transaction -- 15,357 good trades silently vanished along with the
one bad row. Fixed two ways: (1) vol_ratio cast to float() at its source
(avg_vol and latest_vol both explicitly float()'d before division), and (2)
defensive float()/int()/bool()/str() coercion on every field immediately
before the SQL call as a second line of defense, with each row now
committing independently so one bad row can never take down the batch again.

Why this exists (Jun 30 2026): Scanner went live on day one with almost no
backtest evidence -- SCANNER_VOL_MULT's comments reference per-symbol EV
figures from a prior "nexus_analyzer.py 2yr backtest" for only 6 of its 44
symbols (TNA/TZA/MSTU/MSTZ/NVDL/NVDS), and that backtester no longer exists
in the codebase. The other ~38 symbols have never been backtested at all --
they've been running on live pattern-memory learning since boot, with
PM_MIN_TRADES=15 minimum completed trades before the win-rate gate even
starts evaluating anything. This script gives Scanner the same evidence base
Berserker and Phase4 already have.

Signal engine is an EXACT copy of scanner.py V2.4:
  - Entry: scan_for_entry() -- volume ratio (vs 10-bar avg) >= per-symbol
    SCANNER_VOL_MULT, AND price move over 10 bars >= PRICE_MOVE_MIN (1.5%).
  - Exit: manage_scanner_exits() -- stop-loss (-1.5%), two-phase trailing
    stop (1.5% trail below RATCHET_THRESHOLD=3% profit, 0.4% tight trail
    above it), max-hold time exit (60min, only fires if NOT meaningfully
    profitable -- a real runner is never forced out on time alone).
  - Bucket key: ScannerMemory._bucket_key() -- symbol|vol_ratio band|
    price_move band|SPY trend|hour-of-day.
Any drift between this backtester and scanner.py is a bug -- keep them
in sync, the same discipline already applied to the Berserker and Phase4
backtesters.

Walk-forward validation: trains on first 75% of bars, validates on the
remaining 25% out-of-sample, both reported separately. Same convention as
nexus_analyzer_1min_railway.py and phase4_backtester.py.

Environment:
  DATABASE_URL (public Railway Postgres URL)
  ALPACA_API_KEY, ALPACA_SECRET_KEY
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

Usage:
  python scanner_backtester.py          # default 730 days
  python scanner_backtester.py --days 365
  python scanner_backtester.py --dry-run
"""

import os
import sys
import time
import secrets
import argparse
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCANNER-BT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scanner_bt")

# ── Environment ───────────────────────────────────────────────────────────────
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Signal engine constants (must match scanner.py V2.4 exactly) ──────────────
SCANNER_UNIVERSE = [
    "SPY", "QQQ", "IWM", "DIA", "VXX", "UVXY", "SVXY",
    "XLF", "XLE", "XLK", "XLV", "XLI", "ARKK",
    "GLD", "SLV", "USO", "UNG",
    "UPRO", "TMF", "TNA", "TZA", "NAIL", "WANT",
    "MSTU", "MSTZ", "NVDL", "NVDS",
    "NFLX", "BABA", "UBER", "SNAP", "RIVN",
    "HOOD", "SOFI", "UPST", "RBLX",
    "IONQ", "RGTI", "QUBT", "JOBY", "ACHR",
    "AI", "BBAI", "SOUN",
]

SCANNER_VOL_MULT = {
    "TNA":  3.0,
    "TZA":  2.5,
    "MSTU": 1.5,
    "MSTZ": 1.5,
    "NVDL": 2.5,
    "NVDS": 1.5,
}
DEFAULT_VOL_MULT = 1.5   # VOLUME_SPIKE_MULT fallback in scanner.py

TRAILING_STOP_PHASE1 = 0.015
TRAILING_STOP_PHASE2 = 0.004
RATCHET_THRESHOLD    = 0.03
STOP_LOSS_PCT        = 0.015
MAX_HOLD_MINUTES     = 60
PRICE_MOVE_MIN       = 0.015
SLIPPAGE_PCT         = 0.0005   # matches Berserker/Phase4 backtester convention

WARMUP_BARS = 11   # scan_for_entry needs 11 bars (10-bar lookback + current)

# ── Helpers ───────────────────────────────────────────────────────────────────
def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.info(f"[ALERT] {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=8
        )
    except Exception:
        pass

def is_market_hours(dt: datetime) -> bool:
    central_offset = timedelta(hours=-5)   # approx CDT, fine for hour-bucket purposes
    local = dt + central_offset
    return local.weekday() < 5 and 8 <= local.hour < 15

def get_hour_cdt(dt: datetime) -> int:
    return (dt + timedelta(hours=-5)).hour

def get_day_of_week(dt: datetime) -> int:
    return (dt + timedelta(hours=-5)).weekday()

# ── Data fetch ───────────────────────────────────────────────────────────────
def fetch_all_bars(days: int) -> dict:
    """Fetch 1-min bars for SCANNER_UNIVERSE + SPY (SPY already in universe,
    fetched once, reused for both its own signal and the SPY-trend context
    other symbols' bucket keys need)."""
    client   = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET)
    end_dt   = datetime.now(timezone.utc).replace(hour=21, minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=days)

    log.info(f"Fetching {days}d 1-min bars for {len(SCANNER_UNIVERSE)} symbols...")
    log.info(f"Range: {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")

    result = {}
    for i, sym in enumerate(SCANNER_UNIVERSE):
        try:
            bars = client.get_stock_bars(StockBarsRequest(
                symbol_or_symbols=sym,
                timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                start=start_dt,
                end=end_dt,
                feed=DataFeed.IEX,
            ))
            df = bars.df
            if hasattr(df.index, "levels"):
                df = df.xs(sym, level=0)
            if not df.empty:
                result[sym] = df
                log.info(f"  [{i+1}/{len(SCANNER_UNIVERSE)}] {sym}: {len(df):,} bars")
            else:
                log.warning(f"  [{i+1}/{len(SCANNER_UNIVERSE)}] {sym}: EMPTY")
        except Exception as e:
            log.error(f"  [{i+1}/{len(SCANNER_UNIVERSE)}] {sym}: {e}")
        time.sleep(0.3)

    log.info(f"Fetched {len(result)}/{len(SCANNER_UNIVERSE)} symbols")
    return result


# ── Bucket key (exact copy of ScannerMemory._bucket_key in scanner.py) ────────
def bucket_key(symbol: str, vol_ratio: float, price_move_pct: float,
               spy_bullish: bool, hour: int) -> str:
    vol_b  = ("vol_lt2x" if vol_ratio < 2.0 else
              "vol_2_3x" if vol_ratio < 3.0 else "vol_gt3x")
    move_b = ("move_lt2pct" if price_move_pct < 2.0 else
              "move_2_4pct" if price_move_pct < 4.0 else "move_gt4pct")
    spy_b  = "spy_bull" if spy_bullish else "spy_bear"
    hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
    return f"{symbol}|{vol_b}|{move_b}|{spy_b}|{hr_b}"


# ── Replay engine ─────────────────────────────────────────────────────────────
def replay_scanner(all_bars: dict, validate_mode: bool = False,
                    validate_start: Optional[int] = None) -> List[Dict]:
    """
    Replays Scanner's exact entry/exit logic bar-by-bar.

    Each symbol is independent (Scanner has no cross-symbol position limit
    or correlation block, unlike crypto's RiskManager), so this iterates per
    symbol rather than per-timestamp -- simpler and matches Scanner's actual
    architecture, where each symbol's scan is independent of every other's.

    validate_mode + validate_start: if set, only timestamps from
    validate_start onward generate trades (walk-forward out-of-sample test).
    Bars before validate_start still feed the rolling lookback windows so
    indicators are warmed up correctly at the validation boundary -- same
    convention as nexus_analyzer_1min_railway.py.
    """
    if "SPY" not in all_bars:
        log.error("SPY bars missing -- cannot compute spy_bullish context")
        return []
    spy_df = all_bars["SPY"]

    trades: List[Dict] = []
    mode_label = "OUT-OF-SAMPLE" if validate_mode else "FULL TRAIN"

    for sym in SCANNER_UNIVERSE:
        if sym not in all_bars:
            continue
        df = all_bars[sym]
        if len(df) < WARMUP_BARS + 1:
            continue

        vol_mult = SCANNER_VOL_MULT.get(sym, DEFAULT_VOL_MULT)

        in_position  = False
        entry_price  = 0.0
        entry_ts     = None
        peak_price   = 0.0
        trough_price = 0.0
        entry_signal = {}   # holds vol_ratio/price_move/spy_bullish captured at entry

        n = len(df)
        start_idx = WARMUP_BARS
        if validate_mode and validate_start is not None:
            start_idx = max(start_idx, validate_start)

        for i in range(start_idx, n):
            ts = df.index[i]
            if not is_market_hours(ts):
                continue
            price = float(df["close"].iloc[i])

            if in_position:
                profit_pct = (price - entry_price) / entry_price
                if price > peak_price:
                    peak_price = price
                if price < trough_price:
                    trough_price = price
                drawdown = (peak_price - price) / peak_price if peak_price > 0 else 0
                trailing = TRAILING_STOP_PHASE2 if profit_pct >= RATCHET_THRESHOLD else TRAILING_STOP_PHASE1

                exit_reason = None
                if profit_pct <= -STOP_LOSS_PCT:
                    exit_reason = "stop-loss"
                elif drawdown >= trailing:
                    exit_reason = "trail-tight" if profit_pct >= RATCHET_THRESHOLD else "trail"
                else:
                    held_min = (ts - entry_ts).total_seconds() / 60
                    if held_min >= MAX_HOLD_MINUTES and profit_pct <= 0.005:
                        exit_reason = "max-hold"

                if exit_reason:
                    # V1.2: slippage sign fix -- a market SELL crosses the
                    # spread AGAINST you regardless of PnL sign. The old
                    # conditional ADDED slippage to the exit price on losers,
                    # flattering every losing trade in the results.
                    exit_price = price * (1 - SLIPPAGE_PCT)
                    final_pnl  = (exit_price - entry_price) / entry_price
                    mfe = (peak_price - entry_price) / entry_price
                    mae = (trough_price - entry_price) / entry_price
                    trades.append({
                        "symbol":         sym,
                        "entry_ts":       entry_ts,
                        "exit_ts":        ts,
                        "entry_price":    entry_price,
                        "vol_ratio":      entry_signal["vol_ratio"],
                        "price_move_pct": entry_signal["price_move_pct"],
                        "spy_bullish":    entry_signal["spy_bullish"],
                        "hour_cdt":       get_hour_cdt(entry_ts),
                        "day_of_week":    get_day_of_week(entry_ts),
                        "won":            final_pnl > 0,
                        "pnl_pct":        round(final_pnl * 100, 4),
                        "exit_reason":    exit_reason,
                        "hold_time_min":  round((ts - entry_ts).total_seconds() / 60),
                        "mfe":            round(mfe * 100, 4),
                        "mae":            round(mae * 100, 4),
                    })
                    in_position = False
                    entry_signal = {}
                continue

            # Not in position -- check entry (scan_for_entry logic exactly)
            if i < 10:
                continue
            window     = df["close"].iloc[i-10:i+1]
            vol_window = df["volume"].iloc[i-10:i+1]
            avg_vol    = float(vol_window.iloc[:-1].mean())
            latest_vol = float(vol_window.iloc[-1])
            vol_ratio  = (latest_vol / avg_vol) if avg_vol > 0 else 0.0
            price_10m  = float(window.iloc[0])
            price_now  = float(window.iloc[-1])
            price_move = (price_now - price_10m) / price_10m if price_10m > 0 else 0

            if vol_ratio >= vol_mult and price_move >= PRICE_MOVE_MIN:
                spy_idx = spy_df.index.searchsorted(ts)
                spy_bullish = True
                if 11 <= spy_idx < len(spy_df):
                    spy_now  = float(spy_df["close"].iloc[spy_idx])
                    spy_then = float(spy_df["close"].iloc[spy_idx - 11])
                    spy_bullish = spy_now > spy_then

                entry_price  = price_now * (1 + SLIPPAGE_PCT)
                entry_ts     = ts
                peak_price   = entry_price
                trough_price = entry_price
                in_position  = True
                entry_signal = {
                    "vol_ratio":      round(vol_ratio, 3),
                    "price_move_pct": round(price_move * 100, 3),
                    "spy_bullish":    spy_bullish,
                }

        # V1.0 fix: a position still open when this symbol's bars run out
        # (e.g. a slow drifter that's mildly profitable but never pulls back
        # far enough to trail and never qualifies for max-hold because it's
        # "too profitable" by that rule's own definition -- a real edge case
        # in scanner.py's exit logic, not a backtester artifact) was being
        # silently dropped from `trades` entirely rather than recorded. That
        # both undercounts total trades AND biases win rate upward, since a
        # position still open precisely because it hasn't hit a stop-loss is
        # more likely to be a winner than the dropped-trade population at
        # large. Force-close at the last available bar instead, tagged with
        # its own exit_reason so it's visible in the report rather than
        # silently vanishing.
        if in_position:
            last_price = float(df["close"].iloc[-1])
            exit_price = last_price * (1 - SLIPPAGE_PCT)   # V1.2: sells always cross the spread against you
            final_pnl  = (exit_price - entry_price) / entry_price
            mfe = (peak_price - entry_price) / entry_price
            mae = (trough_price - entry_price) / entry_price
            trades.append({
                "symbol":         sym,
                "entry_ts":       entry_ts,
                "exit_ts":        df.index[-1],
                "entry_price":    entry_price,
                "vol_ratio":      entry_signal["vol_ratio"],
                "price_move_pct": entry_signal["price_move_pct"],
                "spy_bullish":    entry_signal["spy_bullish"],
                "hour_cdt":       get_hour_cdt(entry_ts),
                "day_of_week":    get_day_of_week(entry_ts),
                "won":            final_pnl > 0,
                "pnl_pct":        round(final_pnl * 100, 4),
                "exit_reason":    "data-end-forced-close",
                "hold_time_min":  round((df.index[-1] - entry_ts).total_seconds() / 60),
                "mfe":            round(mfe * 100, 4),
                "mae":            round(mae * 100, 4),
            })

    log.info(f"  {mode_label}: replayed {len(SCANNER_UNIVERSE)} symbols, {len(trades)} trades generated")
    return trades


# ── DB write ───────────────────────────────────────────────────────────────────
def write_fingerprints(trades: List[Dict], dry_run: bool = False) -> int:
    if dry_run:
        log.info(f"[DRY RUN] Would write {len(trades)} fingerprints")
        return len(trades)
    if not DATABASE_URL:
        log.warning("No DATABASE_URL -- skipping DB write")
        return 0

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Only clear prior backtest-seeded rows (bt_ prefix), never touch
            # live fingerprints -- same safety convention as Phase4's V1.1
            # fix (a blanket DELETE WHERE exit_ts IS NULL would wipe a live
            # open position's fingerprint if the backtest runs mid-trade).
            cur.execute("DELETE FROM scanner_trade_fingerprints WHERE trade_id LIKE 'bt_%'")
            conn.commit()   # commit the delete on its own -- isolates it from
                             # the per-row insert loop below, so a later insert
                             # failure can never roll back the delete too.

            written = 0
            failed  = 0
            for t in trades:
                trade_id = f"bt_{secrets.token_hex(8)}"
                try:
                    cur.execute("""
                        INSERT INTO scanner_trade_fingerprints
                        (trade_id, symbol, entry_ts, exit_ts, entry_price,
                         vol_ratio, price_move_pct, spy_bullish, hour_cdt, day_of_week,
                         won, pnl_pct, exit_reason, hold_time_min, mfe, mae)
                        VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        trade_id, str(t["symbol"]),
                        int(t["entry_ts"].timestamp()), int(t["exit_ts"].timestamp()),
                        # V1.1 fix (Jun 30 2026): explicit float()/int()/bool() on
                        # every numeric field immediately before the query --
                        # pandas/numpy operations upstream (e.g. Series.mean(),
                        # division involving a numpy-derived value) can produce
                        # numpy.float64/int64 even when the inputs were cast with
                        # Python's float() earlier, because mixed numpy/Python
                        # arithmetic upcasts to numpy's type. round() on a numpy
                        # scalar also returns a numpy scalar, not a plain float --
                        # it does NOT coerce back to Python's float like round()
                        # on a plain float does. psycopg2 can't adapt numpy types
                        # (it tried to interpret "np" from "np.float64(...)" as a
                        # schema name -- "schema np does not exist"), so every
                        # value crossing into the SQL boundary is explicitly
                        # coerced here as a second, defensive line on top of the
                        # float() casts already present earlier in the pipeline.
                        float(t["entry_price"]), float(t["vol_ratio"]), float(t["price_move_pct"]),
                        bool(t["spy_bullish"]), int(t["hour_cdt"]), int(t["day_of_week"]),
                        bool(t["won"]), float(t["pnl_pct"]), str(t["exit_reason"]), int(t["hold_time_min"]),
                        float(t["mfe"]), float(t["mae"]),
                    ))
                    written += 1
                except Exception as row_err:
                    # V1.1 fix: isolate per-row failures so one bad trade can't
                    # silently roll back the entire batch -- the original bug
                    # caused 15,357 of 15,362 trades to vanish because a SINGLE
                    # numpy-typed field anywhere in the list aborted the whole
                    # transaction. Each row now commits independently; a failure
                    # here logs and skips, but every other row still lands.
                    conn.rollback()
                    failed += 1
                    if failed <= 5:   # cap log spam if something is systemically wrong
                        log.error(f"  row insert error [{t.get('symbol','?')}]: {row_err}")
                    continue
                conn.commit()

        if failed > 0:
            log.warning(f"  {failed}/{len(trades)} rows failed to write (see errors above)")
        log.info(f"  Wrote {written}/{len(trades)} fingerprints")
        return written
    except Exception as e:
        conn.rollback()
        log.error(f"write_fingerprints error: {e}")
        return 0
    finally:
        conn.close()


def run_pattern_analysis() -> Tuple[int, float]:
    """Mirrors ScannerMemory.run_analysis() in scanner.py -- reads all
    completed fingerprints (backtest + any live), rebuilds scanner_pattern_
    stats bucket-by-bucket. Returns (bucket_count, overall_win_rate)."""
    if not DATABASE_URL:
        return 0, 0.0
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, vol_ratio, price_move_pct, spy_bullish,
                       hour_cdt, won, pnl_pct
                FROM scanner_trade_fingerprints WHERE won IS NOT NULL
            """)
            rows = cur.fetchall()

        buckets  = defaultdict(list)
        pnl_bkts = defaultdict(list)
        for row in rows:
            key = bucket_key(
                row["symbol"], row["vol_ratio"] or 0, row["price_move_pct"] or 0,
                row["spy_bullish"], row["hour_cdt"] if row["hour_cdt"] is not None else 12,
            )
            buckets[key].append(bool(row["won"]))
            if row["pnl_pct"] is not None:
                pnl_bkts[key].append(float(row["pnl_pct"]))

        written = 0
        with conn.cursor() as cur:
            for key, outcomes in buckets.items():
                if len(outcomes) < 3:   # PM_MIN_BUCKET_TRADES in scanner.py
                    continue
                wr      = sum(outcomes) / len(outcomes)
                avg_pnl = sum(pnl_bkts[key]) / len(pnl_bkts[key]) if pnl_bkts[key] else None
                cur.execute("""
                    INSERT INTO scanner_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (bucket_key) DO UPDATE
                    SET win_rate=EXCLUDED.win_rate, sample_count=EXCLUDED.sample_count,
                        avg_pnl=EXCLUDED.avg_pnl, last_updated=NOW()
                """, (key, wr, len(outcomes), avg_pnl))
                written += 1
        conn.commit()

        total = len(rows)
        wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
        log.info(f"  Pattern analysis: {written} buckets | {total} trades | {wr:.1%} overall WR")
        return written, wr
    except Exception as e:
        conn.rollback()
        log.error(f"run_pattern_analysis error: {e}")
        return 0, 0.0
    finally:
        conn.close()


# ── Report ─────────────────────────────────────────────────────────────────────
def build_report(trades: List[Dict], validate_mode: bool = False) -> str:
    label = "OUT-OF-SAMPLE" if validate_mode else "FULL BACKTEST"
    if not trades:
        return f"SCANNER BACKTEST — {label}\nNo trades generated"

    wins   = sum(1 for t in trades if t["won"])
    losses = len(trades) - wins
    wr     = wins / len(trades) * 100
    avg_pnl = sum(t["pnl_pct"] for t in trades) / len(trades)
    avg_mfe = sum(t["mfe"] for t in trades) / len(trades)
    avg_mae = sum(t["mae"] for t in trades) / len(trades)

    by_symbol = defaultdict(list)
    for t in trades:
        by_symbol[t["symbol"]].append(t)

    lines = [
        f"=" * 60,
        f"SCANNER BACKTEST — {label}",
        f"=" * 60,
        f"Trades:   {len(trades)} | {wins}W | {losses}L | {wr:.1f}% WR",
        f"Avg PnL:  {avg_pnl:+.3f}% | Avg MFE: {avg_mfe:+.3f}% | Avg MAE: {avg_mae:+.3f}%",
        f"",
        f"{'Symbol':<8}{'Trades':>8}{'WR':>8}{'AvgPnL':>10}",
        f"-" * 40,
    ]
    for sym in sorted(by_symbol.keys(), key=lambda s: -len(by_symbol[s])):
        sym_trades = by_symbol[sym]
        s_wins = sum(1 for t in sym_trades if t["won"])
        s_wr   = s_wins / len(sym_trades) * 100
        s_pnl  = sum(t["pnl_pct"] for t in sym_trades) / len(sym_trades)
        lines.append(f"{sym:<8}{len(sym_trades):>8}{s_wr:>7.1f}%{s_pnl:>9.3f}%")

    exit_reasons = defaultdict(lambda: {"n": 0, "wins": 0})
    for t in trades:
        exit_reasons[t["exit_reason"]]["n"]    += 1
        exit_reasons[t["exit_reason"]]["wins"] += 1 if t["won"] else 0
    lines.append(f"")
    lines.append(f"Exit reasons:")
    for reason, s in sorted(exit_reasons.items(), key=lambda x: -x[1]["n"]):
        r_wr = s["wins"] / s["n"] * 100 if s["n"] > 0 else 0
        lines.append(f"  {reason:<20} {s['n']:>5} trades | {r_wr:>5.1f}% WR")
    lines.append(f"=" * 60)
    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=730)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("NEXUS SCANNER BACKTESTER V1.1")
    log.info(f"Symbols: {len(SCANNER_UNIVERSE)} | Days: {args.days} | Slippage: {SLIPPAGE_PCT*100}%")
    log.info("Signal engine: scanner.py V2.4 exact replica")
    log.info("=" * 60)

    if not ALPACA_API_KEY or not ALPACA_SECRET:
        log.error("ALPACA_API_KEY / ALPACA_SECRET_KEY not set")
        sys.exit(1)

    t0 = time.time()
    send_alert(
        f"🔍 SCANNER BACKTESTER V1.1 STARTING\n"
        f"Symbols: {len(SCANNER_UNIVERSE)} | Days: {args.days}\n"
        f"Signal engine: scanner.py V2.4 exact replica\n"
        f"ETA: ~15-25 min"
    )

    all_bars = fetch_all_bars(args.days)
    if "SPY" not in all_bars:
        log.error("SPY fetch failed -- cannot proceed")
        send_alert("❌ SCANNER BACKTESTER FAILED — SPY bars unavailable")
        sys.exit(1)

    # Full training replay
    log.info("Running full training replay...")
    train_trades = replay_scanner(all_bars, validate_mode=False)
    train_report = build_report(train_trades, validate_mode=False)
    log.info("\n" + train_report)

    # Walk-forward validation -- last 25% of bars, out-of-sample
    log.info("Running walk-forward validation (last 25% of data)...")
    spy_n = len(all_bars["SPY"])
    validate_start = int(spy_n * 0.75)
    val_trades = replay_scanner(all_bars, validate_mode=True, validate_start=validate_start)
    val_report = build_report(val_trades, validate_mode=True)
    log.info("\n" + val_report)

    # Write fingerprints (training set is the larger, more representative
    # sample for seeding pattern memory -- same convention as Berserker/
    # Phase4 backtesters, which write training trades, not validation trades)
    log.info(f"Writing {len(train_trades)} training fingerprints to DB...")
    write_fingerprints(train_trades, dry_run=args.dry_run)

    if not args.dry_run:
        log.info("Running pattern analysis...")
        buckets, wr = run_pattern_analysis()
    else:
        buckets, wr = 0, 0.0

    elapsed = round(time.time() - t0)
    log.info(f"DONE. {len(train_trades)} fingerprints | {buckets} buckets | {elapsed}s")

    train_wr = (sum(1 for t in train_trades if t["won"]) / len(train_trades) * 100) if train_trades else 0
    val_wr   = (sum(1 for t in val_trades if t["won"]) / len(val_trades) * 100) if val_trades else 0
    send_alert(
        f"✅ SCANNER BACKTESTER V1.1 COMPLETE\n"
        f"──────────────────\n"
        f"Training: {len(train_trades)} trades | {train_wr:.1f}% WR\n"
        f"OOS:      {len(val_trades)} trades | {val_wr:.1f}% WR\n"
        f"Buckets:  {buckets} | Overall WR: {wr:.1%}\n"
        f"──────────────────\n"
        f"Elapsed: {elapsed}s ({round(elapsed/60)}min)"
    )


if __name__ == "__main__":
    main()
