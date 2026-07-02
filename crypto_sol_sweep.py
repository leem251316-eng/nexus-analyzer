#!/usr/bin/env python3
"""
crypto_sol_sweep.py V1.0 -- SOL-USDC Momentum Parameter Sweep
================================================================
Single-coin, multi-combo sweep for SOL-USDC's momentum path specifically.
Mean-reversion stays exactly as-is (SOL's existing RECIPES entry,
untouched) -- only momentum's RSI band and risk multipliers are swept.
Deliberately not sweeping mean-reversion too: more free parameters
moving at once makes it easier to fit noise instead of signal. If this
shows something real, mean-reversion-for-SOL-specifically is a good
NEXT round, not this one.

Design: the expensive part of a backtest is computing RSI/trend/OBV/VWAP
from raw prices (pandas-heavy). That doesn't change between combos --
only the thresholds applied to it do. So: compute and cache the expensive
signals ONCE per bar (Phase 1), then replay all 16 combos cheaply against
that cache (Phase 2, arithmetic only, no pandas). Naive approach (rerun
the full simulation per combo) would be ~16x a normal backtest's runtime;
this should land closer to ~1.5-2x.

Reuses crypto_backtester.py's signal functions directly (imported, not
duplicated) so there's one source of truth for what "RSI," "trend,"
"F&G-adjusted gate," etc. mean -- this file MUST sit in the same
directory as crypto_backtester.py to import correctly.

Validation: each combo's trades get split by entry_bar >= 75% mark into
train/validation, exactly like crypto_backtester.py's "validate" flag.
Ranked by VALIDATION performance, not training -- a combo that looks
great in training and falls apart in validation gets exposed, not hidden.
Honest ceiling: this is still one 75/25 split of one coin's one year,
not true multi-window walk-forward. Better than not checking. Not bulletproof.

Usage:
  python crypto_sol_sweep.py              # 365 days, full 16-combo grid
  python crypto_sol_sweep.py --days 180   # shorter window
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
        FNG_RSI_BONUS, MOM_RSI_MIN, ALPACA_API_KEY,
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
    format="%(asctime)s [SOL-SWEEP] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sol_sweep")

PAIR = "SOL-USDC"

# ── The 16-combo grid ─────────────────────────────────────────────────────────
MOM_RSI_MAX_GRID = [60, 65, 70, 75]
RISK_MULT_GRID = [
    (1.00, 1.00),  # baseline: no change from mean-reversion's own stop/tp
    (0.85, 0.75),
    (0.70, 0.60),  # last night's single guess
    (0.55, 0.50),  # more aggressive tightening
]


@dataclass
class BarSignals:
    """Everything expensive, computed once per bar, reused by all 16 combos."""
    price: float
    hour: int
    is_weekend: bool
    regime: str            # "TRENDING" or "CHOPPY" -- doesn't depend on swept params
    rsi_5m: Optional[float]
    rsi_1m: Optional[float]
    rsi_vals: List[float]  # all 5 timeframe proxies, for the tech-score TF count
    higher_lows: bool
    uptrend: bool
    vwap_above: Optional[bool]
    obv_momentum: Optional[float]
    fg: int
    fg_momentum: float
    btc_rsi: Optional[float]
    alt_vol_blocked: bool  # True = BTC vol regime gate would block a NEW entry this bar


def build_signal_cache(pair: str, df: pd.DataFrame, btc_df: pd.DataFrame,
                        fg_by_date: Dict[str, int]) -> List[Optional[BarSignals]]:
    """
    Phase 1: the expensive pass. One entry per bar index (None for bars
    before WARMUP_BARS, since nothing can trade there anyway). This is
    the ONLY place calc_multi_tf_rsi / calc_trend_structure / calc_vwap /
    calc_obv_momentum get called -- everything downstream reuses these.
    """
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
              mom_rsi_max: int, mom_stop_mult: float, mom_tp_mult: float,
              start_idx: int) -> List[Dict]:
    """
    Phase 2: cheap replay for ONE combo against the pre-built cache.
    No pandas calls in here -- pure arithmetic against cached signals.
    Mean-reversion logic/thresholds are SOL's existing RECIPES entry,
    completely unswept. Only momentum's band + risk multipliers vary.
    """
    recipe   = RECIPES.get(pair, {})
    max_rsi  = recipe.get("rsi_entry_max", 40)
    stop_pct = recipe.get("stop_pct", 0.015)
    tp_pct   = recipe.get("tp_pct", 0.025)

    trades: List[Dict] = []
    in_pos = False
    entry_price = 0.0
    partial_done = False
    mfe = mae = 0.0
    entry_bar = 0
    strategy_at_entry = ""
    bucket_key_at_entry = ""
    active_stop_pct = stop_pct
    active_tp_pct   = tp_pct
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
                mode, strategy = _score_momentum(sig, mom_rsi_max, hist_score)
            else:
                mode, strategy = _score_meanrev(sig, max_rsi, hist_score)

            if mode in ("FULL", "CAUTIOUS"):
                in_pos = True
                entry_price = price * (1 + SLIPPAGE_PCT)
                partial_done = False
                mfe = mae = 0.0
                entry_bar = i
                strategy_at_entry = strategy
                bucket_key_at_entry = bucket_key
                if strategy == "MOMENTUM":
                    active_stop_pct = stop_pct * mom_stop_mult
                    active_tp_pct   = tp_pct * mom_tp_mult
                else:
                    active_stop_pct = stop_pct
                    active_tp_pct   = tp_pct

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
    """Mirrors crypto_backtester.py's _compute_confidence_meanrev_bt exactly,
    just reading from cached BarSignals instead of recomputing from raw prices."""
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


def _score_momentum(sig: BarSignals, mom_rsi_max: int, hist_score: int) -> Tuple[str, str]:
    """Mirrors crypto_backtester.py's _compute_confidence_momentum_bt, with
    mom_rsi_max as the swept parameter instead of the fixed constant."""
    if sig.fg > FNG_GREED_BLOCK:
        return "BLOCK", "MOMENTUM"
    if sig.rsi_5m is None or not (MOM_RSI_MIN <= sig.rsi_5m <= mom_rsi_max):
        return "BLOCK", "MOMENTUM"

    score = 0
    sc = sum(1 for r in sig.rsi_vals if MOM_RSI_MIN <= r <= mom_rsi_max)
    if   sc >= 5: score += 20
    elif sc == 4: score += 15
    elif sc == 3: score += 10
    elif sc == 2: score += 5
    if sig.higher_lows: score += 5
    if sig.vwap_above is True: score += 5
    if (sig.rsi_5m is not None and sig.rsi_1m is not None
            and MOM_RSI_MIN <= sig.rsi_5m <= mom_rsi_max and sig.rsi_1m >= sig.rsi_5m):
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
        elif sig.obv_momentum < -1.0: vol_score -= 4

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
    parser = argparse.ArgumentParser(description="SOL-USDC momentum sweep")
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    if not ALPACA_API_KEY:
        log.error("Missing ALPACA_API_KEY")
        sys.exit(1)

    n_combos = len(MOM_RSI_MAX_GRID) * len(RISK_MULT_GRID)
    log.info("=" * 60)
    log.info(f"SOL-USDC MOMENTUM SWEEP V1.0")
    log.info(f"Grid: {len(MOM_RSI_MAX_GRID)} RSI bands x {len(RISK_MULT_GRID)} risk profiles = "
             f"{n_combos} combos | Days: {args.days}")
    log.info("Mean-reversion NOT swept -- SOL's existing RECIPES entry, unchanged")
    log.info("=" * 60)

    send_alert(
        f"🎯 SOL SWEEP STARTING\n"
        f"{n_combos} combos | {args.days} days\n"
        f"Mean-reversion unswept (baseline)\n"
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

    cache = build_signal_cache(PAIR, df, btc_df, fg_by_date)

    results = []
    for mom_rsi_max in MOM_RSI_MAX_GRID:
        for stop_mult, tp_mult in RISK_MULT_GRID:
            trades = run_combo(PAIR, cache, mom_rsi_max, stop_mult, tp_mult, start_idx)
            train_t = [t for t in trades if not t["validate"]]
            val_t   = [t for t in trades if t["validate"]]

            mom_train = summarize([t for t in train_t if t["strategy"] == "MOMENTUM"])
            mom_val   = summarize([t for t in val_t if t["strategy"] == "MOMENTUM"])
            mr_train  = summarize([t for t in train_t if t["strategy"] == "MEAN_REVERSION"])
            mr_val    = summarize([t for t in val_t if t["strategy"] == "MEAN_REVERSION"])

            results.append({
                "mom_rsi_max": mom_rsi_max, "stop_mult": stop_mult, "tp_mult": tp_mult,
                "mom_train": mom_train, "mom_val": mom_val,
                "mr_train": mr_train, "mr_val": mr_val,
            })
            log.info(f"  RSI<={mom_rsi_max} stop={stop_mult} tp={tp_mult} | "
                     f"MOM train n={mom_train['n']} wr={mom_train['wr']}% avg={mom_train['avg_pnl']:+.3f}% "
                     f"| MOM val n={mom_val['n']} wr={mom_val['wr']}% avg={mom_val['avg_pnl']:+.3f}%")

    # Rank by VALIDATION avg P&L (momentum), not training -- the whole point
    ranked = sorted(results, key=lambda r: r["mom_val"]["avg_pnl"], reverse=True)

    print(f"\n{'='*100}")
    print(f"SOL-USDC MOMENTUM SWEEP COMPLETE -- ranked by VALIDATION avg P&L")
    print(f"{'='*100}")
    print(f"{'RSI<=':<7}{'stop':<7}{'tp':<7}{'TRAIN n/wr/avg':<26}{'VAL n/wr/avg':<26}{'flag'}")
    for r in ranked:
        mt, mv = r["mom_train"], r["mom_val"]
        gap_flag = "<- train>>val, suspect" if (mt["avg_pnl"] > 0 and mv["avg_pnl"] < mt["avg_pnl"] - 0.3) else ""
        print(f"{r['mom_rsi_max']:<7}{r['stop_mult']:<7}{r['tp_mult']:<7}"
              f"{mt['n']}/{mt['wr']}%/{mt['avg_pnl']:+.3f}%{'':<10}"
              f"{mv['n']}/{mv['wr']}%/{mv['avg_pnl']:+.3f}%{'':<10}{gap_flag}")
    print(f"{'-'*100}")
    print(f"Mean-reversion baseline (unswept, identical every row): "
          f"train {ranked[0]['mr_train']} | val {ranked[0]['mr_val']}")
    print(f"{'='*100}")
    print(f"Elapsed: {time.time()-t0:.0f}s")

    best = ranked[0]
    send_alert(
        f"✅ SOL SWEEP COMPLETE\n"
        f"──────────────────\n"
        f"Best (by validation): RSI<={best['mom_rsi_max']} stop={best['stop_mult']} tp={best['tp_mult']}\n"
        f"MOM train: n={best['mom_train']['n']} {best['mom_train']['wr']}% {best['mom_train']['avg_pnl']:+.3f}%\n"
        f"MOM val:   n={best['mom_val']['n']} {best['mom_val']['wr']}% {best['mom_val']['avg_pnl']:+.3f}%\n"
        f"──────────────────\n"
        f"Full ranked table in Railway logs\n"
        f"Elapsed: {round(time.time()-t0)}s"
    )
    log.info(f"DONE in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
