"""
NEXUS MARKET ANALYZER — 1-MIN RAILWAY ONE-SHOT
Runs once, saves output to /app/output volume, then exits.
Deploy as a Railway service with ALPACA_API_KEY + ALPACA_SECRET_KEY env vars.
Output: /app/output/nexus_analysis_report_1min.txt
        /app/output/nexus_recipes_updated_1min.json
"""

import os
import json
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore")

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests   import StockBarsRequest
    from alpaca.data.timeframe  import TimeFrame, TimeFrameUnit
    from alpaca.data.enums      import DataFeed, Adjustment
except ImportError:
    print("Install alpaca-py: pip install alpaca-py")
    exit(1)

API_KEY    = os.environ.get("ALPACA_API_KEY",    "")
SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
CENTRAL    = ZoneInfo("America/Chicago")

if not API_KEY or not SECRET_KEY:
    print("ERROR: ALPACA_API_KEY and ALPACA_SECRET_KEY env vars required")
    exit(1)

LOOKBACK_YEARS = 1
BAR_SIZE       = TimeFrame(1, TimeFrameUnit.Minute)

OUTPUT_DIR  = "/app/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
out_file    = os.path.join(OUTPUT_DIR, "nexus_analysis_report_1min.txt")
recipe_file = os.path.join(OUTPUT_DIR, "nexus_recipes_updated_1min.json")

SCALPER_SYMBOLS = [
    "SOXL", "SOXS", "TQQQ", "SQQQ", "SPXL", "SPXU",
    "LABU", "LABD", "NUGT", "DUST", "ERX",  "ERY",
]

BERSERKER_SYMBOLS = [
    "CLSK", "MARA", "PLTR", "GEO", "CXW",  "CCJ",  "NUE",  "MSTR", "COIN",
    "NVDA", "AMD",  "TSLA", "AAPL","MSFT", "AMZN", "META", "GOOGL","SMCI",
]

SCANNER_PRIORITY = [
    "TNA", "TZA", "MSTU", "MSTZ", "NVDL", "NVDS",
    "SOXL","SOXS","TQQQ","LABD","LABU","FNGU","FAS","FAZ","ERX","DUST","SDOW","UDOW","SPXL","SPXU",
]

PAIRS = {
    "SOXS": "SOXL", "SQQQ": "TQQQ", "SPXU": "SPXL",
    "LABD": "LABU", "DUST": "NUGT",  "ERY":  "ERX",
    "FAZ":  "FAS",  "SDOW": "UDOW",  "FNGD": "FNGU",
    "TZA":  "TNA",  "MSTZ": "MSTU",  "NVDS": "NVDL",
}

ALL_SYMBOLS = list(set(SCALPER_SYMBOLS + BERSERKER_SYMBOLS + SCANNER_PRIORITY))

def pct(n, d):
    return round(n / d * 100, 1) if d > 0 else 0

def calc_rsi(prices, period=7):
    if len(prices) < period + 1:
        return np.nan
    s     = pd.Series(prices)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs    = gain / loss
    return (100 - 100 / (1 + rs)).iloc[-1]

def calc_macd(prices, fast=12, slow=26, signal=9):
    s     = pd.Series(prices)
    ema_f = s.ewm(span=fast,   adjust=False).mean()
    ema_s = s.ewm(span=slow,   adjust=False).mean()
    macd  = ema_f - ema_s
    sig   = macd.ewm(span=signal, adjust=False).mean()
    return macd.iloc[-1], sig.iloc[-1]

def hr_cst(ts):
    if hasattr(ts, 'tz_convert'):
        return ts.tz_convert(CENTRAL).hour
    return ts.hour

def day_cst(ts):
    if hasattr(ts, 'tz_convert'):
        return ts.tz_convert(CENTRAL).weekday()
    return ts.weekday()

def print_and_log(msg, f):
    print(msg, flush=True)
    f.write(msg + "\n")

def fetch_bars(symbols, client, years=1, log_f=None):
    end   = datetime.now()
    start = end - timedelta(days=years * 365)

    def log(msg):
        print(msg, flush=True)
        if log_f:
            log_f.write(msg + "\n")

    log(f"  Fetching {len(symbols)} symbols | {start.date()} -> {end.date()}")
    data       = {}
    batch_size = 5  # smaller batches for 1-min data
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        try:
            req  = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=BAR_SIZE,
                start=start, end=end,
                feed=DataFeed.IEX,
                adjustment=Adjustment.SPLIT
            )
            bars = client.get_stock_bars(req).df
            if not bars.empty:
                for sym in batch:
                    if sym in bars.index.get_level_values(0):
                        df = bars.loc[sym].copy()
                        df.index = pd.to_datetime(df.index, utc=True)
                        data[sym] = df
                        log(f"    OK {sym}: {len(df):,} bars")
                    else:
                        log(f"    SKIP {sym}: no data")
            else:
                log(f"    WARN batch {batch}: empty response")
        except Exception as e:
            log(f"    ERR batch {batch}: {e}")
    return data

def simulate_dip_trades(df, rsi_threshold=40, stop_pct=0.02,
                         trail_pct=0.004, ratchet_pct=0.0075):
    prices = df["close"].values
    times  = df.index
    trades = []
    in_pos = False
    entry_price = peak_price = 0.0
    entry_idx = entry_hour = entry_day = 0

    for i in range(20, len(prices)):
        price = prices[i]
        if in_pos:
            peak_price = max(peak_price, price)
            pnl        = (price - entry_price) / entry_price
            mfe_so_far = (peak_price - entry_price) / entry_price
            drawdown   = (peak_price - price) / peak_price if peak_price > 0 else 0
            if pnl >= ratchet_pct and drawdown >= trail_pct:
                trades.append({"pnl": pnl, "entry_hour": entry_hour,
                                "entry_day": entry_day, "mfe": mfe_so_far,
                                "mae": min(0, pnl), "exit": "trail"})
                in_pos = False
            elif pnl <= -stop_pct:
                trades.append({"pnl": pnl, "entry_hour": entry_hour,
                                "entry_day": entry_day, "mfe": mfe_so_far,
                                "mae": pnl, "exit": "stop"})
                in_pos = False
            elif i - entry_idx >= 60:  # max 60 bars = 60 min on 1-min data
                trades.append({"pnl": pnl, "entry_hour": entry_hour,
                                "entry_day": entry_day, "mfe": mfe_so_far,
                                "mae": min(0, pnl), "exit": "timeout"})
                in_pos = False
        else:
            window = prices[max(0, i-14):i+1].tolist()
            rsi    = calc_rsi(window)
            if np.isnan(rsi):
                continue
            if rsi < rsi_threshold and i >= 2 and prices[i] > prices[i-2]:
                in_pos      = True
                entry_price = price
                peak_price  = price
                entry_idx   = i
                entry_hour  = hr_cst(times[i])
                entry_day   = day_cst(times[i])
    return trades

def analyze_hour_day(trades, sym):
    if not trades:
        return {}, {}
    hours     = {}
    days      = {}
    day_names = {0:"Mon", 1:"Tue", 2:"Wed", 3:"Thu", 4:"Fri"}
    for t in trades:
        h = t.get("entry_hour", -1)
        d = t.get("entry_day",  -1)
        if 8 <= h <= 14:
            if h not in hours:
                hours[h] = {"wins": 0, "losses": 0, "pnl": 0.0, "mfe": [], "mae": []}
            if t["pnl"] > 0:
                hours[h]["wins"]   += 1
            else:
                hours[h]["losses"] += 1
            hours[h]["pnl"]  += t["pnl"]
            hours[h]["mfe"].append(t.get("mfe", 0))
            hours[h]["mae"].append(t.get("mae", 0))
        if 0 <= d <= 4:
            dn = day_names[d]
            if dn not in days:
                days[dn] = {"wins": 0, "losses": 0, "pnl": 0.0}
            if t["pnl"] > 0:
                days[dn]["wins"]   += 1
            else:
                days[dn]["losses"] += 1
            days[dn]["pnl"] += t["pnl"]
    return hours, days

def analyze_stop_trail(trades):
    if not trades:
        return {}
    maes = [abs(t.get("mae", 0)) for t in trades if t.get("mae", 0) < 0]
    mfes = [t.get("mfe", 0)     for t in trades if t.get("mfe", 0) > 0]
    if not maes or not mfes:
        return {}
    mae_arr = np.array(maes)
    mfe_arr = np.array(mfes)
    return {
        "avg_mae":         round(float(mae_arr.mean())  * 100, 3),
        "p75_mae":         round(float(np.percentile(mae_arr, 75)) * 100, 3),
        "p90_mae":         round(float(np.percentile(mae_arr, 90)) * 100, 3),
        "avg_mfe":         round(float(mfe_arr.mean())  * 100, 3),
        "p25_mfe":         round(float(np.percentile(mfe_arr, 25)) * 100, 3),
        "p75_mfe":         round(float(np.percentile(mfe_arr, 75)) * 100, 3),
        "optimal_stop":    round(float(np.percentile(mae_arr, 80)) * 100, 2),
        "optimal_ratchet": round(float(np.percentile(mfe_arr, 25)) * 100, 2),
        "optimal_trail":   round(float(mfe_arr.mean()) * 0.15 * 100, 2),
    }

def analyze_reversal_pattern(bear_df, bull_df):
    if bear_df is None or bull_df is None:
        return {}
    common = bear_df.index.intersection(bull_df.index)
    if len(common) < 200:
        return {}
    bear   = bear_df.loc[common, "close"].values
    bull   = bull_df.loc[common, "close"].values
    events = []
    for ob_level in [68, 70, 72, 75]:
        hits = []
        for i in range(20, len(bull) - 30):
            bull_win = bull[max(0,i-14):i+1].tolist()
            bull_rsi = calc_rsi(bull_win)
            if np.isnan(bull_rsi) or bull_rsi < ob_level:
                continue
            reversal_bars = None
            for j in range(1, 31):
                if i+j >= len(bull):
                    break
                if (bull[i] - bull[i+j]) / bull[i] > 0.01:
                    reversal_bars = j
                    break
            bear_start = bear[i]
            if bear_start <= 0:
                continue
            max_bear_bounce = max(
                (bear[i+k] - bear_start) / bear_start
                for k in range(1, min(31, len(bear)-i))
            ) if i + 1 < len(bear) else 0
            hits.append({
                "reversal_bars": reversal_bars,
                "bear_bounce":   max_bear_bounce,
                "win":           max_bear_bounce > 0.005,
            })
        if hits:
            rev_bars = [h["reversal_bars"] for h in hits if h["reversal_bars"]]
            bounces  = [h["bear_bounce"]   for h in hits]
            events.append({
                "ob_level":        ob_level,
                "occurrences":     len(hits),
                "win_rate":        pct(sum(1 for h in hits if h["win"]), len(hits)),
                "avg_bear_bounce": round(np.mean(bounces) * 100, 2),
                "avg_bars_to_rev": round(np.mean(rev_bars), 1) if rev_bars else None,
                "p75_bounce":      round(float(np.percentile(bounces, 75)) * 100, 2),
            })
    return {"reversal_data": events}

def analyze_volume_spikes(df, sym):
    if df is None or "volume" not in df.columns or len(df) < 200:
        return {}
    prices  = df["close"].values
    volumes = df["volume"].values
    results = {}
    for mult in [1.5, 2.0, 2.5, 3.0]:
        trades  = []
        in_pos  = False
        entry_price = peak_price = 0.0
        entry_idx   = 0
        for i in range(20, len(prices) - 12):
            price = prices[i]
            vol   = volumes[i]
            if in_pos:
                peak_price = max(peak_price, price)
                pnl        = (price - entry_price) / entry_price
                drawdown   = (peak_price - price) / peak_price if peak_price > 0 else 0
                if (pnl >= 0.015 and drawdown >= 0.004) or pnl <= -0.015 or i - entry_idx > 12:
                    trades.append({"pnl": pnl, "win": pnl > 0})
                    in_pos = False
            else:
                avg_vol    = np.mean(volumes[max(0,i-10):i]) if i > 0 else 0
                if avg_vol <= 0:
                    continue
                vol_spike  = vol >= avg_vol * mult
                price_move = abs(prices[i] - prices[max(0,i-5)]) / prices[max(0,i-5)] >= 0.005 if prices[max(0,i-5)] > 0 else False
                ma20       = np.mean(prices[i-20:i]) if i >= 20 else price
                if vol_spike and price_move and price > ma20:
                    in_pos      = True
                    entry_price = price
                    peak_price  = price
                    entry_idx   = i
        if trades:
            results[f"mult_{mult}x"] = {
                "multiplier":   mult,
                "trades":       len(trades),
                "win_rate":     pct(sum(1 for t in trades if t["win"]), len(trades)),
                "avg_pnl":      round(np.mean([t["pnl"] for t in trades]) * 100, 3),
                "ev_per_trade": round(np.mean([t["pnl"] for t in trades]) * 100, 4),
            }
    return results

def write_section(f, title):
    bar = "=" * 70
    print_and_log(f"\n{bar}", f)
    print_and_log(f"  {title}", f)
    print_and_log(bar, f)

def write_subsection(f, title):
    print_and_log(f"\n-- {title} " + "-" * (60 - len(title)), f)

def main():
    print("\nNEXUS MARKET ANALYZER — 1-MIN RAILWAY", flush=True)
    print("=" * 50, flush=True)
    print(f"Output directory: {OUTPUT_DIR}", flush=True)

    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

    updated_recipes = {}

    with open(out_file, "w", encoding="utf-8") as f:
        ts = datetime.now(tz=CENTRAL).strftime("%Y-%m-%d %H:%M CST")
        print_and_log(f"NEXUS MARKET ANALYZER 1-MIN -- {ts}", f)
        print_and_log(f"Data: {LOOKBACK_YEARS} year of 1-min bars from Alpaca (SIP)", f)

        print_and_log("\nFetching market data...", f)
        all_needed = list(set(ALL_SYMBOLS + list(PAIRS.values()) + ["SPY", "QQQ"]))
        data = fetch_bars(all_needed, client, years=LOOKBACK_YEARS, log_f=f)
        print_and_log(f"\nLoaded {len(data)} symbols", f)

        if len(data) == 0:
            print_and_log("ERROR: No data returned. Check API keys.", f)
            return

        print_and_log(f"Symbols analyzed: {len(data)}", f)

        # ===================================================================
        # 1. SCALPER ANALYSIS
        # ===================================================================
        write_section(f, "1. SCALPER -- FULL SYMBOL ANALYSIS (1-MIN)")
        scalper_summary = {}

        for sym in SCALPER_SYMBOLS:
            if sym not in data:
                continue
            write_subsection(f, sym)
            df     = data[sym]
            trades = simulate_dip_trades(df)
            if not trades:
                print_and_log(f"  {sym}: insufficient signal data", f)
                continue

            wins   = sum(1 for t in trades if t["pnl"] > 0)
            losses = len(trades) - wins
            wr     = pct(wins, len(trades))
            avg_w  = np.mean([t["pnl"] for t in trades if t["pnl"] > 0]) * 100 if wins else 0
            avg_l  = np.mean([t["pnl"] for t in trades if t["pnl"] <= 0]) * 100 if losses else 0
            ev     = round((wins/len(trades)) * avg_w + (losses/len(trades)) * avg_l, 3) if trades else 0

            print_and_log(f"  Base: {len(trades)} trades | {wr}% WR | "
                          f"avg W: +{round(avg_w,2)}% | avg L: {round(avg_l,2)}% | EV: {ev}%", f)

            st = analyze_stop_trail(trades)
            if st:
                print_and_log(f"  MAE: avg={st['avg_mae']}% p75={st['p75_mae']}% p90={st['p90_mae']}%", f)
                print_and_log(f"  MFE: avg={st['avg_mfe']}% p25={st['p25_mfe']}% p75={st['p75_mfe']}%", f)
                print_and_log(f"  Optimal: stop={st['optimal_stop']}%  "
                              f"ratchet={st['optimal_ratchet']}%  trail={st['optimal_trail']}%", f)

            hours, days = analyze_hour_day(trades, sym)
            if hours:
                print_and_log("  Hour breakdown (CDT):", f)
                for h in sorted(hours):
                    hd   = hours[h]
                    t    = hd["wins"] + hd["losses"]
                    hr   = pct(hd["wins"], t)
                    flag = " AVOID" if hr < 45 else " BEST" if hr > 60 else ""
                    print_and_log(f"    {h}:00 -- {t:3d} trades | {hr:4.1f}% WR{flag}", f)

            if days:
                print_and_log("  Day breakdown:", f)
                for dn in ["Mon","Tue","Wed","Thu","Fri"]:
                    if dn in days:
                        dd   = days[dn]
                        t    = dd["wins"] + dd["losses"]
                        dr   = pct(dd["wins"], t)
                        flag = " AVOID" if dr < 45 else " BEST" if dr > 62 else ""
                        print_and_log(f"    {dn} -- {t:3d} trades | {dr:4.1f}% WR{flag}", f)

            bull_sym = PAIRS.get(sym)
            if bull_sym and bull_sym in data:
                rev = analyze_reversal_pattern(data[sym], data[bull_sym])
                if rev.get("reversal_data"):
                    print_and_log(f"  Reversal (when {bull_sym} overbought):", f)
                    for rd in rev["reversal_data"]:
                        print_and_log(
                            f"    RSI>{rd['ob_level']}: {rd['occurrences']} events | "
                            f"{rd['win_rate']}% WR | avg bounce +{rd['avg_bear_bounce']}% | "
                            f"p75 +{rd['p75_bounce']}% | avg bars to rev: {rd['avg_bars_to_rev']}", f)

            scalper_summary[sym] = {"trades": len(trades), "wr": wr, "ev": ev,
                                     "stop_trail": st, "hours": hours, "days": days}

        # ===================================================================
        # 2. SCANNER VOLUME SPIKE OPTIMIZATION (1-MIN)
        # ===================================================================
        write_section(f, "2. SCANNER -- VOLUME SPIKE OPTIMIZATION (1-MIN)")
        for sym in SCANNER_PRIORITY:
            if sym not in data:
                continue
            vol_results = analyze_volume_spikes(data[sym], sym)
            if vol_results:
                best     = max(vol_results.values(), key=lambda x: x["ev_per_trade"])
                best_key = [k for k, v in vol_results.items() if v == best][0]
                print_and_log(f"  {sym}: optimal={best['multiplier']}x | "
                              f"{best['trades']} trades | {best['win_rate']}% WR | "
                              f"EV: {best['ev_per_trade']}%/trade", f)
                for mk, mv in sorted(vol_results.items(), key=lambda x: x[1]["multiplier"]):
                    flag = " <- OPTIMAL" if mk == best_key else ""
                    print_and_log(f"    {mv['multiplier']}x: {mv['trades']} trades | "
                                  f"{mv['win_rate']}% WR | avg PnL: {mv['avg_pnl']}%{flag}", f)

        # ===================================================================
        # 3. RECIPE RECOMMENDATIONS
        # ===================================================================
        write_section(f, "3. RECOMMENDED RECIPE UPDATES (1-MIN DATA)")
        print_and_log("  Compare vs 5-min recipes — use tighter values where 1-min confirms", f)

        for sym in SCALPER_SYMBOLS:
            s = scalper_summary.get(sym)
            if not s:
                continue
            st         = s.get("stop_trail", {})
            hours_data = s.get("hours", {})
            days_data  = s.get("days",  {})

            avoid_h = [h for h, hd in hours_data.items()
                       if pct(hd["wins"], hd["wins"]+hd["losses"]) < 45
                       and hd["wins"]+hd["losses"] >= 10]
            avoid_d = [{"Mon":0,"Tue":1,"Wed":2,"Thu":3,"Fri":4}[dn]
                       for dn, dd in days_data.items()
                       if pct(dd["wins"], dd["wins"]+dd["losses"]) < 45
                       and dd["wins"]+dd["losses"] >= 10]

            recipe_rec = {
                "stop_loss":      round(st.get("optimal_stop",    2.0) / 100, 3) if st else 0.020,
                "profit_ratchet": round(st.get("optimal_ratchet", 0.75) / 100, 4) if st else 0.0075,
                "trailing_stop":  round(st.get("optimal_trail",   0.4) / 100, 4) if st else 0.004,
                "avoid_hours":    avoid_h,
                "avoid_days":     avoid_d,
            }
            updated_recipes[sym] = recipe_rec
            print_and_log(f"\n  {sym}:", f)
            print_and_log(f"    stop_loss:      {recipe_rec['stop_loss']}", f)
            print_and_log(f"    profit_ratchet: {recipe_rec['profit_ratchet']}", f)
            print_and_log(f"    trailing_stop:  {recipe_rec['trailing_stop']}", f)
            print_and_log(f"    avoid_hours:    {recipe_rec['avoid_hours']}", f)
            print_and_log(f"    avoid_days:     {recipe_rec['avoid_days']}", f)

        print_and_log("\n\n" + "="*70, f)
        print_and_log("  END OF NEXUS 1-MIN ANALYZER REPORT", f)
        print_and_log("="*70, f)

    with open(recipe_file, "w", encoding="utf-8") as f:
        json.dump(updated_recipes, f, indent=2)

    print(f"\nReport saved:  {out_file}", flush=True)
    print(f"Recipes saved: {recipe_file}", flush=True)
    print("\nAnalysis complete. Service can be stopped.", flush=True)

if __name__ == "__main__":
    main()

