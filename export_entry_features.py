#!/usr/bin/env python3
"""READ-ONLY exporter (DRAFT, untested on Railway). Extends export_carry_trades.py.
Re-runs the V4 live-policy replay (no DB access, no writes) and prints every trade as CSV with
causal entry-time features for the entry-filter family (RVOL, VWAP, driver, minutes, ORB30, news count).
All features use only 1-min bars that CLOSED before the entry minute.
Run inside nexus-backtester from /app (needs its Alpaca data keys; it reads them from env, never prints them):
  python3 export_entry_features.py > /tmp/v4_entry_features.csv ; wc -l /tmp/v4_entry_features.csv
Optional:  --no-news   (skip Alpaca News API calls)
"""
import csv, sys, os
from datetime import datetime, timedelta, timezone
import pandas as pd
import nexus_analyzer_1min_railway as nx
import berserker_live_sim as sim
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed, Adjustment

DAYS = 730
DRV = {"MARA": "BTC/USD", "CLSK": "BTC/USD", "MSTR": "BTC/USD", "SMCI": "SMH", "NVDA": "SMH", "NUE": "XLB",
       "AAPL": "QQQ", "PLTR": "QQQ", "TSLA": "QQQ", "SPCX": "QQQ", "CXW": "SPY", "GEO": "SPY"}
CT = "America/Chicago"

bars = nx.fetch_all_bars(DAYS)                      # SYMBOLS + SPY + QQQ, IEX 1-min (same as V4)
earn = nx.fetch_earnings_dates(nx.SYMBOLS)
costs = sim.resolve_costs(list(nx.SYMBOLS), nx._closes_for_costs(bars), quote_half={})
tr = sim.replay(bars, earn, symbols=list(nx.SYMBOLS), recipes=nx.BERSERKER_RECIPES,
                trump_theme=list(nx.TRUMP_THEME), costs=costs, validate_mode=False)

end = datetime.now(timezone.utc); start = end - timedelta(days=DAYS)
sc = StockHistoricalDataClient(nx.ALPACA_API_KEY, nx.ALPACA_SECRET)
for e in ["SMH", "XLB"]:
    if e not in bars:
        df = sc.get_stock_bars(StockBarsRequest(symbol_or_symbols=e, timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                               start=start, end=end, feed=DataFeed.IEX, adjustment=Adjustment.ALL)).df
        bars[e] = df.xs(e, level=0) if hasattr(df.index, "levels") else df
cc = CryptoHistoricalDataClient()
df = cc.get_crypto_bars(CryptoBarsRequest(symbol_or_symbols="BTC/USD", timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                        start=start, end=end)).df
bars["BTC/USD"] = df.xs("BTC/USD", level=0) if hasattr(df.index, "levels") else df

def prep(df):
    d = df.copy(); d.index = pd.DatetimeIndex(d.index).tz_convert(CT)
    d = d.between_time("08:30", "14:59"); d["day"] = d.index.date
    d["slot"] = d.index.hour * 60 + d.index.minute
    px = d["vwap"] if "vwap" in d else (d.high + d.low + d.close) / 3
    d["cumv"] = d.groupby("day").volume.cumsum()
    d["svwap"] = (px * d.volume).groupby(d.day).cumsum() / d.groupby("day").volume.cumsum()
    d["r30"] = d.groupby("day").close.transform(lambda s: s / s.shift(30) - 1)
    d["open830"] = d.groupby("day").open.transform("first")
    trng = pd.concat([d.high - d.low, (d.high - d.close.shift()).abs(), (d.low - d.close.shift()).abs()], axis=1).max(axis=1)
    d["atr5_pct"] = trng.rolling(5).sum().rolling(14).mean() / d.close * 100   # ~ATR14 on 5-min equivalent
    return d
P = {s: prep(bars[s]) for s in list(nx.SYMBOLS) + ["SPY", "QQQ", "SMH", "XLB", "BTC/USD"] if s in bars}
CUMPIV = {s: P[s].pivot_table(index="day", columns="slot", values="cumv").ffill(axis=1).shift(1).rolling(14, min_periods=14).mean()
          for s in nx.SYMBOLS if s in P}

DAYLIST = {s: sorted(set(P[s].day)) for s in P}
news_ok = "--no-news" not in sys.argv
NEWS = {}
def news_times(sym):
    """All Alpaca News timestamps for sym in [start, end]. Plain GETs to data.alpaca.markets,
    paced under the 200/min limit, with backoff on 429. None if the symbol could not be fetched."""
    if sym in NEWS: return NEWS[sym]
    import requests, time as _t
    url = "https://data.alpaca.markets/v1beta1/news"
    hdr = {"APCA-API-KEY-ID": nx.ALPACA_API_KEY, "APCA-API-SECRET-KEY": nx.ALPACA_SECRET}
    out, tok, pages = [], None, 0
    try:
        while True:
            q = {"symbols": sym, "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "limit": 50, "sort": "asc", "include_content": "false"}
            if tok: q["page_token"] = tok
            for attempt in range(12):
                r = requests.get(url, headers=hdr, params=q, timeout=30)
                if r.status_code == 429:
                    _t.sleep(min(60, 5 * (attempt + 1))); continue
                r.raise_for_status(); break
            else:
                raise RuntimeError("429 after retries")
            j = r.json(); pages += 1
            out += [pd.Timestamp(n["created_at"]).tz_convert(CT) for n in (j.get("news") or [])]
            tok = j.get("next_page_token")
            _t.sleep(0.4)
            if not tok: break
        NEWS[sym] = pd.DatetimeIndex(sorted(out))
        print(f"# news {sym}: {len(out)} articles, {pages} pages", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"# news {sym}: FAILED ({e}); news_n blank for this symbol", file=sys.stderr, flush=True)
        NEWS[sym] = None
    return NEWS[sym]

print(f"# replay trades: {len(tr)}", file=sys.stderr, flush=True)
if news_ok:
    for _s in sorted({t["symbol"] for t in tr}): news_times(_s)

def feat(t):
    s = t["symbol"]; et = pd.Timestamp(t["entry_ts_ep"], unit="s", tz="UTC").tz_convert(CT)
    d = P[s]; pre = d[(d.index < et.floor("min")) & (d.day == et.date())]
    f = dict(entry_ct=et.strftime("%Y-%m-%d %H:%M"), min_since_open=et.hour * 60 + et.minute - 510)
    if pre.empty: return f
    last = pre.iloc[-1]; sl = int(last.slot); day = et.date()
    a = CUMPIV[s].loc[day, sl] if (day in CUMPIV[s].index and sl in CUMPIV[s].columns) else float("nan")
    f.update(rvol=round(last.cumv / a, 3) if a and a > 0 else "", vwap_dev_pct=round((t["entry_price"] / last.svwap - 1) * 100, 3),
             atr5_pct=round(last.atr5_pct, 3), orb30_hi=round(pre.between_time("08:30", "08:59").high.max(), 4))
    f["orb30_pass"] = int(t["entry_price"] > f["orb30_hi"]) if et.hour * 60 + et.minute >= 540 else ""
    dv = P.get(DRV.get(s, "SPY"))
    if dv is not None:
        dp = dv[(dv.index < et.floor("min")) & (dv.day == day)]
        if not dp.empty:
            l = dp.iloc[-1]
            f["drv_above_ref"] = int(l.close > (l.open830 if DRV.get(s, "SPY") == "BTC/USD" else l.svwap))
            f["drv_r30_pos"] = int(l.r30 > 0) if pd.notna(l.r30) else ""
    if news_ok:
        nt = news_times(s); prev = [x for x in DAYLIST[s] if x < day]
        if prev and nt is not None:
            lo = pd.Timestamp(datetime.combine(prev[-1], datetime.min.time())).tz_localize(CT) + pd.Timedelta(hours=15)
            f["news_n"] = int(((nt > lo) & (nt <= et)).sum())
    return f

cols = ["symbol", "entry_ts_ep", "exit_ts_ep", "entry_price", "exit_price", "pnl_pct", "exit_reason", "overnight", "holdout",
        "half_spread", "mfe", "mae", "hold_min", "close_1458", "spy_momentum", "vix_at_entry", "entry_ct", "min_since_open", "rvol", "vwap_dev_pct", "atr5_pct", "orb30_hi", "orb30_pass",
        "drv_above_ref", "drv_r30_pos", "news_n"]
w = csv.DictWriter(sys.stdout, fieldnames=cols, extrasaction="ignore"); w.writeheader()
idx = {x: sim._index_rth(sim._bars_from_obj(bars.get(x))) for x in nx.SYMBOLS}
for t in tr:
    t["close_1458"] = ""
    if t["overnight"]:
        dd = sim.to_ct(sim.datetime.fromtimestamp(t["entry_ts_ep"], sim.timezone.utc)).date()
        c = [b for e, b in idx[t["symbol"]].items()
             if sim.to_ct(b[0]).date() == dd and sim.minute_of_day(b[0]) <= sim.EOD_MINUTE]
        if c: t["close_1458"] = max(c, key=lambda b: b[0])[4]
    t.update(feat(t)); w.writerow(t)
