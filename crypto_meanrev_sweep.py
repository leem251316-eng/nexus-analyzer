#!/usr/bin/env python3
"""
crypto_meanrev_sweep.py V1.0 -- SOL-USDC Mean-Reversion Parameter Sweep
========================================================================
Inverted version of crypto_sol_sweep.py: THIS time mean-reversion's own
parameters (rsi_entry_max, stop/tp) get swept, and momentum stays fixed
at its current (V5.2 pullback thesis) baseline, unswept.

Why the inversion: three independent tests tonight (8-pair full run, the
16-combo momentum sweep, the full-year pullback-thesis rerun) all agreed
momentum doesn't have an edge with this signal set. Mean-reversion is the
one thesis that's shown real signal (roughly breakeven full-year, genuinely
positive on several pairs) -- and its thresholds were set before any of
tonight's infrastructure (real F&G, walk-forward pattern memory, score
visibility) existed. Never gotten the same rigorous look. This gives it one.

Same architecture as the momentum sweep on purpose: cache the expensive
signals once (Phase 1), replay every combo cheaply against that cache
(Phase 2), split train/validation, rank by validation not training.
Same honest ceiling too: one 75/25 split of one coin's one year, not true
multi-window walk-forward.

Reuses crypto_backtester.py's signal functions directly -- must sit in
the same directory to import correctly.

Usage:
  python crypto_meanrev_sweep.py              # 365 days, full grid
  python crypto_meanrev_sweep.py --days 180   # shorter window
"""

import os
import sys
import time
import argparse
import logging
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass

import pandas as pd

# Reuses crypto_backtester.py directly -- must be in the same directory.
try:
    from crypto_backtester import (
        RECIPES, WARMUP_BARS, SLIPPAGE_PCT, PARTIAL_TP_MULT,
        BTC_VOL_THRESHOLD, BTC_IS_ETH, FNG_GREED_BLOCK, FNG_FEAR_LOOSE,
        FNG_RSI_BONUS, MOM_PULLBACK_RSI_MIN, MOM_PULLBACK_RSI_MAX,
        ALPACA_API_KEY,
        calc_multi_tf_rsi, calc_obv_momentum, calc_trend_structure, calc_vwap,
        calc_rsi, detect_trend_regime_bt, compute_bucket_key,
        lookup_bucket_hist_score, compute_btc_realized_vol, get_utc_hour,
        _score_sentiment_bt, fetch_historical_fg, fetch_all_crypto_bars,
        send_alert,
    )
except ImportError as e:
    print(f"FATAL: could not import from crypto_backtester.py -- this file "
          f"must sit in the same directory. ({e})")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MEANREV-SWEEP] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("meanrev_sweep")

PAIR = "SOL-USDC"

# ── The grid: mean-reversion's own parameters, this time ──────────────────────
# SOL's baseline rsi_entry_max is 40 (RECIPES). Sweeping a delta around it
# rather than absolute values keeps this readable and keeps the grid
# centered on what's already there, not an arbitrary range.
MEANREV_RSI_DELTA_GRID = [-10, -5, 0, +5, +10]   # -> 30, 35, 40, 45, 50
RISK_MULT_GRID = [
    (1.00, 1.00),   # baseline: SOL's current stop/tp, unchanged
    (0.80, 0.80),   # tighter both ways
    (1.20, 1.20),   # looser both ways -- give winners more room
    (1.00, 1.40),   # same stop, meaningfully further target
]
# 5 x 4 = 20 combos


@dataclass
class BarSignals:
    """Everything expensive, computed once per bar, reused by every combo."""
    price: float
    hour: int
    is_weekend: bool
    regime: str
    rsi_5m: Optional[float]
    rsi_1m: Optional[float]
    rsi_vals: List[float]
    higher_lows: bool
    uptrend: bool
    vwap_above: Optional[bool]
    obv_momentum: Optional[float]
    fg: int
    fg_momentum: float
    btc_rsi: Optional[float]
    alt_vol_blocked: bool


def build_signal_cache(pair: str, df: pd.DataFrame, btc_df: pd.DataFrame,
                        fg_by_date: Dict[str, int]) -> List[Optional[BarSignals]]:
    """Phase 1: the expensive pass, unchanged from crypto_sol_sweep.py --
    this part is strategy-agnostic, no reason for it to differ."""
    closes  = df["close"].tolist()
    volumes = df["volume"].tolist() if "volume" in df.columns else [0.0] * len(closes)
    times   = df.index.tolist()
    total_bars = len(closes)
    is_btc_eth = pair in BTC_IS_ETH

    cache: List[Optional[BarSignals]] = [None] * total_bars

    log.info(f"Building signal cache for {total_bars:,} bars (one-time expensive pass)...")
    t0 = time.time()

    for i in range(WARMUP_BARS, total_bars):
        closes_window  = closes[max(0, i-120):i+1]
        volumes_window = volumes[max(0, i-120):i+1]
        regime_window  = closes[max(0, i-1050):i+1]
        btc_window     = []
        if btc_df is not None and not btc_df.empty and i < len(btc_df):
            btc_window = btc_df["close"].tolist()[max(0, i-120):i+1]

        price = closes[i]
        hour  = get_utc_hour(times[i])

        try:
            date_str = times[i].strftime("%Y-%m-%d") if hasattr(times[i], "strftime") else None
        except Exception:
            date_str = None
        fg_today = fg_by_date.get(date_str, 50) if date_str else 50
        fg_momentum = 0.0
        if date_str and fg_by_date:
            try:
                d_3ago = (times[i] - pd.Timedelta(days=3)).strftime("%Y-%m-%d")
                fg_3ago = fg_by_date.get(d_3ago)
                if fg_3ago is not None:
                    fg_momentum = float(fg_today - fg_3ago)
            except Exception:
                fg_momentum = 0.0

        alt_vol_blocked = False
        if not is_btc_eth and btc_window:
            btc_vol = compute_btc_realized_vol(btc_window[-120:])
            alt_vol_blocked = btc_vol > BTC_VOL_THRESHOLD

        regime = detect_trend_regime_bt(regime_window)
        rsi_dict = calc_multi_tf_rsi(closes_window)
        rsi_vals = [v for v in rsi_dict.values() if v is not None]
        trend = calc_trend_structure(closes_window)
        vwap = calc_vwap(closes_window, volumes_window)
        vwap_above = (closes_window[-1] > vwap) if vwap else None
        obv_mom = calc_obv_momentum(closes_window, volumes_window)
        is_weekend = times[i].weekday() >= 5 if hasattr(times[i], "weekday") else False
        btc_rsi = calc_rsi(btc_window, 14) if len(btc_window) >= 15 else None

        cache[i] = BarSignals(
            price=price, hour=hour, is_weekend=is_weekend, regime=regime,
            rsi_5m=rsi_dict.get("5m"), rsi_1m=rsi_dict.get("1m"), rsi_vals=rsi_vals,
            higher_lows=trend.get("higher_lows", False),
            uptrend=trend.get("uptrend", False),
            vwap_above=vwap_above, obv_momentum=obv_mom,
            fg=fg_today, fg_momentum=fg_momentum, btc_rsi=btc_rsi,
            alt_vol_blocked=alt_vol_blocked,
        )

        if i % 20000 == 0 and i > 0:
            elapsed = time.time() - t0
            pct = (i - WARMUP_BARS) / max(total_bars - WARMUP_BARS, 1) * 100
            log.info(f"  cache: {i:,}/{total_bars:,} ({pct:.0f}%) -- {elapsed:.0f}s elapsed")

    log.info(f"Signal cache built in {time.time()-t0:.0f}s")
    return cache


def run_combo(pair: str, cache: List[Optional[BarSignals]],
              rsi_delta: int, meanrev_stop_mult: float, meanrev_tp_mult: float,
              start_idx: int) -> List[Dict]:
    """
    Phase 2: cheap replay for ONE combo. Mean-reversion's rsi_entry_max and
    risk profile are THIS combo's swept values. Momentum stays completely
    fixed at its current pullback-thesis baseline (1.0/1.0 risk, its own
    already-set PULLBACK_RSI_MIN/MAX) -- unswept, same role meanrev played
    in the momentum sweep.
    """
    recipe        = RECIPES.get(pair, {})
    base_max_rsi  = recipe.get("rsi_entry_max", 40)
    swept_max_rsi = base_max_rsi + rsi_delta
    base_stop_pct = recipe.get("stop_pct", 0.015)
    base_tp_pct   = recipe.get("tp_pct", 0.025)

    trades: List[Dict] = []
    in_pos = False
    entry_price = 0.0
    partial_done = False
    mfe = mae = 0.0
    entry_bar = 0
    strategy_at_entry = ""
    bucket_key_at_entry = ""
    active_stop_pct = base_stop_pct
    active_tp_pct   = base_tp_pct
    bucket_stats: Dict[str, list] = {}

    total_bars = len(cache)

    for i in range(WARMUP_BARS, total_bars):
        sig = cache[i]
        if sig is None:
            continue
        price = sig.price

        if sig.alt_vol_blocked and not in_pos:
            continue

        if in_pos:
            profit_pct = (price - entry_price) / entry_price
            mfe = max(mfe, profit_pct)
            mae = min(mae, profit_pct)
            exit_reason = None

            if profit_pct <= -active_stop_pct:
                exit_reason = "STOP_LOSS"
            elif not partial_done and profit_pct >= active_tp_pct * PARTIAL_TP_MULT:
                partial_done = True
                trades.append(_trade_dict(pair, entry_price, price, profit_pct,
                                           "PARTIAL_TP", i - entry_bar, mfe, mae, True,
                                           strategy_at_entry, sig.hour, entry_bar >= start_idx))
            elif profit_pct >= active_tp_pct:
                exit_reason = "TAKE_PROFIT"
            elif (i - entry_bar) >= 288 and abs(profit_pct) < 0.002:
                exit_reason = "TIME_FAILSAFE"

            if exit_reason:
                won = profit_pct > 0
                trades.append(_trade_dict(pair, entry_price, price, profit_pct,
                                           exit_reason, i - entry_bar, mfe, mae, won,
                                           strategy_at_entry, sig.hour, entry_bar >= start_idx))
                if bucket_key_at_entry:
                    bucket_stats.setdefault(bucket_key_at_entry, []).append(won)
                in_pos = False
                partial_done = False
                mfe = mae = 0.0

        else:
            bucket_key = compute_bucket_key(sig.rsi_5m, sig.fg, sig.vwap_above,
                                             sig.uptrend, sig.higher_lows, sig.is_weekend)
            hist_score = lookup_bucket_hist_score(bucket_stats, bucket_key)

            if sig.regime == "TRENDING":
                mode, strategy = _score_momentum_baseline(sig, hist_score)
            else:
                mode, strategy = _score_meanrev(sig, swept_max_rsi, hist_score)

            if mode in ("FULL", "CAUTIOUS"):
                in_pos = True
                entry_price = price * (1 + SLIPPAGE_PCT)
                partial_done = False
                mfe = mae = 0.0
                entry_bar = i
                strategy_at_entry = strategy
                bucket_key_at_entry = bucket_key
                if strategy == "MEAN_REVERSION":
                    active_stop_pct = base_stop_pct * meanrev_stop_mult
                    active_tp_pct   = base_tp_pct * meanrev_tp_mult
                else:
                    active_stop_pct = base_stop_pct
                    active_tp_pct   = base_tp_pct

    if in_pos and total_bars:
        last_sig = cache[total_bars - 1]
        price = last_sig.price if last_sig else entry_price
        profit_pct = (price - entry_price) / entry_price
        trades.append(_trade_dict(pair, entry_price, price, profit_pct, "TIMEOUT",
                                   total_bars - entry_bar, mfe, mae, profit_pct > 0,
                                   strategy_at_entry, 0, entry_bar >= start_idx))

    return trades


def _trade_dict(pair, entry_price, price, profit_pct, exit_reason, hold_bars,
                 mfe, mae, won, strategy, hour, validate) -> Dict:
    return {
        "pair": pair, "entry_price": round(entry_price, 6),
        "exit_price": round(price * (1 - SLIPPAGE_PCT), 6),
        "pnl_pct": round(profit_pct * 100, 3), "exit_reason": exit_reason,
        "hold_bars": hold_bars, "mfe": round(mfe * 100, 3), "mae": round(mae * 100, 3),
        "won": won, "strategy": strategy, "hour_utc": hour, "validate": validate,
    }


def _score_meanrev(sig: BarSignals, max_rsi: int, hist_score: int) -> Tuple[str, str]:
    """Mirrors crypto_backtester.py's CURRENT _compute_confidence_meanrev_bt
    exactly, with max_rsi as the swept parameter instead of a fixed constant."""
    if sig.fg > FNG_GREED_BLOCK:
        return "BLOCK", "MEAN_REVERSION"
    gate = max_rsi + (FNG_RSI_BONUS if sig.fg < FNG_FEAR_LOOSE else 0)
    if sig.rsi_5m is not None and sig.rsi_5m > gate:
        return "BLOCK", "MEAN_REVERSION"

    score = 0
    oc = sum(1 for r in sig.rsi_vals if r < 40)
    if   oc >= 5: score += 20
    elif oc == 4: score += 15
    elif oc == 3: score += 10
    elif oc == 2: score += 5
    if sig.higher_lows: score += 5
    if sig.vwap_above is False: score += 5
    if sig.rsi_5m and sig.rsi_1m and sig.rsi_5m < 35 and sig.rsi_1m > sig.rsi_5m:
        score += 5
    tech_score = min(score, 40)

    macro_score = 0
    if sig.btc_rsi is not None:
        if   sig.btc_rsi < 25: macro_score += 8
        elif sig.btc_rsi < 35: macro_score += 5
        elif sig.btc_rsi < 40: macro_score += 2
        elif sig.btc_rsi > 72: macro_score -= 5
        elif sig.btc_rsi > 65: macro_score -= 2
    sent_score = _score_sentiment_bt(sig.fg, sig.fg_momentum)
    vol_score = 0
    if sig.obv_momentum is not None:
        if   sig.obv_momentum >  5.0: vol_score += 5
        elif sig.obv_momentum >  1.0: vol_score += 2
        elif sig.obv_momentum < -5.0: vol_score -= 4
        elif sig.obv_momentum < -1.0: vol_score -= 2

    total = max(0, min(100, tech_score + macro_score + vol_score + hist_score + sent_score))
    mode = "FULL" if total >= 75 else "CAUTIOUS" if total >= 55 else "SKIP" if total >= 40 else "BLOCK"
    return mode, "MEAN_REVERSION"


def _score_momentum_baseline(sig: BarSignals, hist_score: int) -> Tuple[str, str]:
    """Momentum FIXED at its current V5.2 pullback-thesis baseline -- not
    swept this round. Mirrors crypto_backtester.py's CURRENT
    _compute_confidence_momentum_bt exactly (the pullback version, not the
    old retired chase-strength version)."""
    if sig.fg > FNG_GREED_BLOCK:
        return "BLOCK", "MOMENTUM"
    if sig.rsi_5m is None or not (MOM_PULLBACK_RSI_MIN <= sig.rsi_5m <= MOM_PULLBACK_RSI_MAX):
        return "BLOCK", "MOMENTUM"
    if sig.rsi_1m is None or sig.rsi_1m >= sig.rsi_5m:
        return "BLOCK", "MOMENTUM"

    score = 0
    sc = sum(1 for r in sig.rsi_vals if MOM_PULLBACK_RSI_MIN <= r <= MOM_PULLBACK_RSI_MAX)
    if   sc >= 5: score += 20
    elif sc == 4: score += 15
    elif sc == 3: score += 10
    elif sc == 2: score += 5
    if sig.higher_lows: score += 10
    if sig.uptrend: score += 5
    if sig.vwap_above is True: score += 5
    tech_score = min(score, 40)

    macro_score = 0
    if sig.btc_rsi is not None:
        if   sig.btc_rsi < 25: macro_score += 8
        elif sig.btc_rsi < 35: macro_score += 5
        elif sig.btc_rsi < 40: macro_score += 2
        elif sig.btc_rsi > 72: macro_score -= 5
        elif sig.btc_rsi > 65: macro_score -= 2
    sent_score = _score_sentiment_bt(sig.fg, sig.fg_momentum)
    vol_score = 0
    if sig.obv_momentum is not None:
        if   -1.0 <= sig.obv_momentum <= 1.0: vol_score += 5
        elif sig.obv_momentum < -5.0:         vol_score -= 4

    total = max(0, min(100, tech_score + macro_score + vol_score + hist_score + sent_score))
    mode = "FULL" if total >= 75 else "CAUTIOUS" if total >= 55 else "SKIP" if total >= 40 else "BLOCK"
    return mode, "MOMENTUM"


def summarize(trades: List[Dict]) -> Dict[str, Any]:
    non_partial = [t for t in trades if t["exit_reason"] != "PARTIAL_TP"]
    if not non_partial:
        return {"n": 0, "wr": 0.0, "avg_pnl": 0.0}
    wins = sum(1 for t in non_partial if t["won"])
    return {
        "n": len(non_partial),
        "wr": round(wins / len(non_partial) * 100, 1),
        "avg_pnl": round(sum(t["pnl_pct"] for t in non_partial) / len(non_partial), 3),
    }


def main():
    parser = argparse.ArgumentParser(description="SOL-USDC mean-reversion sweep")
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    if not ALPACA_API_KEY:
        log.error("Missing ALPACA_API_KEY")
        sys.exit(1)

    n_combos = len(MEANREV_RSI_DELTA_GRID) * len(RISK_MULT_GRID)
    log.info("=" * 60)
    log.info(f"SOL-USDC MEAN-REVERSION SWEEP V1.0")
    log.info(f"Grid: {len(MEANREV_RSI_DELTA_GRID)} RSI deltas x {len(RISK_MULT_GRID)} risk profiles = "
             f"{n_combos} combos | Days: {args.days}")
    log.info("Momentum FIXED at current pullback-thesis baseline -- not swept")
    log.info("=" * 60)

    send_alert(
        f"🎯 SOL MEAN-REVERSION SWEEP STARTING\n"
        f"{n_combos} combos | {args.days} days\n"
        f"Momentum unswept (fixed baseline)\n"
        f"ETA: building cache first, then fast per-combo replay"
    )

    t0 = time.time()
    fg_by_date = fetch_historical_fg(args.days)
    all_bars = fetch_all_crypto_bars([PAIR, "BTC-USDC"], args.days)
    if PAIR not in all_bars:
        log.error(f"No data for {PAIR}")
        sys.exit(1)

    df = all_bars[PAIR]
    btc_df = all_bars.get("BTC-USDC")
    total_bars = len(df)
    start_idx = int(total_bars * 0.75)
    base_max_rsi = RECIPES.get(PAIR, {}).get("rsi_entry_max", 40)

    cache = build_signal_cache(PAIR, df, btc_df, fg_by_date)

    results = []
    for rsi_delta in MEANREV_RSI_DELTA_GRID:
        for stop_mult, tp_mult in RISK_MULT_GRID:
            trades = run_combo(PAIR, cache, rsi_delta, stop_mult, tp_mult, start_idx)
            train_t = [t for t in trades if not t["validate"]]
            val_t   = [t for t in trades if t["validate"]]

            mr_train  = summarize([t for t in train_t if t["strategy"] == "MEAN_REVERSION"])
            mr_val    = summarize([t for t in val_t if t["strategy"] == "MEAN_REVERSION"])
            mom_train = summarize([t for t in train_t if t["strategy"] == "MOMENTUM"])
            mom_val   = summarize([t for t in val_t if t["strategy"] == "MOMENTUM"])

            results.append({
                "rsi_delta": rsi_delta, "max_rsi": base_max_rsi + rsi_delta,
                "stop_mult": stop_mult, "tp_mult": tp_mult,
                "mr_train": mr_train, "mr_val": mr_val,
                "mom_train": mom_train, "mom_val": mom_val,
            })
            log.info(f"  RSI<={base_max_rsi + rsi_delta} stop={stop_mult} tp={tp_mult} | "
                     f"MR train n={mr_train['n']} wr={mr_train['wr']}% avg={mr_train['avg_pnl']:+.3f}% "
                     f"| MR val n={mr_val['n']} wr={mr_val['wr']}% avg={mr_val['avg_pnl']:+.3f}%")

    # Rank by VALIDATION avg P&L (mean-reversion), not training
    ranked = sorted(results, key=lambda r: r["mr_val"]["avg_pnl"], reverse=True)

    print(f"\n{'='*100}")
    print(f"SOL-USDC MEAN-REVERSION SWEEP COMPLETE -- ranked by VALIDATION avg P&L")
    print(f"{'='*100}")
    print(f"{'RSI<=':<7}{'stop':<7}{'tp':<7}{'TRAIN n/wr/avg':<26}{'VAL n/wr/avg':<26}{'flag'}")
    for r in ranked:
        mt, mv = r["mr_train"], r["mr_val"]
        gap_flag = "<- train>>val, suspect" if (mt["avg_pnl"] > 0 and mv["avg_pnl"] < mt["avg_pnl"] - 0.3) else ""
        print(f"{r['max_rsi']:<7}{r['stop_mult']:<7}{r['tp_mult']:<7}"
              f"{mt['n']}/{mt['wr']}%/{mt['avg_pnl']:+.3f}%{'':<10}"
              f"{mv['n']}/{mv['wr']}%/{mv['avg_pnl']:+.3f}%{'':<10}{gap_flag}")
    print(f"{'-'*100}")
    print(f"Momentum baseline (unswept, identical every row): "
          f"train {ranked[0]['mom_train']} | val {ranked[0]['mom_val']}")
    print(f"{'='*100}")
    print(f"Elapsed: {time.time()-t0:.0f}s")

    best = ranked[0]
    send_alert(
        f"✅ SOL MEAN-REVERSION SWEEP COMPLETE\n"
        f"──────────────────\n"
        f"Best (by validation): RSI<={best['max_rsi']} stop={best['stop_mult']} tp={best['tp_mult']}\n"
        f"MR train: n={best['mr_train']['n']} {best['mr_train']['wr']}% {best['mr_train']['avg_pnl']:+.3f}%\n"
        f"MR val:   n={best['mr_val']['n']} {best['mr_val']['wr']}% {best['mr_val']['avg_pnl']:+.3f}%\n"
        f"──────────────────\n"
        f"Full ranked table in Railway logs\n"
        f"Elapsed: {round(time.time()-t0)}s"
    )
    log.info(f"DONE in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
