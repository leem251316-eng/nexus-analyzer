"""
fill_reconcile.py  V1.0  — read-only, Gate-0 style
Reconciles every Berserker trade in the Aug 31 – Sep 4 2026 T-Bone log
against Alpaca IEX minute bars.  No DB, no writes.  Runs in the analyst
Railway console.

Per trade prints:
  entry-bar close (proxy entry), implied exit price = entry*(1+reported pnl),
  whether that price sits inside the exit-minute bar range (±2 min),
  MFE / MAE between entry and exit, trail giveback (peak -> exit),
  and for overnight carries: prior close -> next open gap.

Alpaca creds: tries APCA_API_KEY_ID/APCA_API_SECRET_KEY, then
ALPACA_API_KEY/ALPACA_SECRET_KEY, ALPACA_KEY/ALPACA_SECRET, ALPACA_KEY_ID/ALPACA_SECRET_KEY.
"""
import os, sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

# (symbol, entry CT, exit CT, exit_type, reported_pnl_pct)
TRADES = [
    # Aug 31 (Mon) — first two are Fri->Mon carries; entry times derived from watchdog held-minutes
    ("MARA", "2026-08-28 14:00", "2026-08-31 08:30", "trail", -0.57),
    ("NUE",  "2026-08-28 09:56", "2026-08-31 13:56", "trail", -0.52),
    ("CLSK", "2026-08-31 09:22", "2026-08-31 10:19", "stop",  -1.17),
    ("TSLA", "2026-08-31 09:33", "2026-08-31 14:58", "eod",   +0.96),
    ("NVDA", "2026-08-31 10:26", "2026-08-31 13:42", "stop",  -1.02),
    # Sep 1 (Tue)
    ("MARA", "2026-09-01 09:01", "2026-09-01 09:32", "stop",  -0.98),
    ("MSTR", "2026-09-01 09:02", "2026-09-01 10:01", "trail", -0.96),
    ("CLSK", "2026-09-01 09:07", "2026-09-01 09:48", "tp",    +1.37),
    ("SMCI", "2026-09-01 09:36", "2026-09-01 10:38", "tp",    +1.58),
    ("NVDA", "2026-09-01 10:00", "2026-09-01 13:38", "trail", -0.51),
    ("CLSK", "2026-09-01 10:25", "2026-09-01 11:16", "stop",  -1.02),
    ("GEO",  "2026-09-01 10:38", "2026-09-01 10:46", "stop",  -1.36),
    ("CXW",  "2026-09-01 11:39", "2026-09-01 12:12", "tp",    +1.54),
    ("SPCX", "2026-09-01 11:40", "2026-09-01 14:58", "eod",   -0.74),
    ("TSLA", "2026-09-01 12:12", "2026-09-01 13:44", "stop",  -1.05),
    ("CLSK", "2026-09-01 13:50", "2026-09-01 14:58", "eod",   -0.14),
    ("SMCI", "2026-09-01 13:51", "2026-09-02 08:30", "tp",    +4.12),   # carry, DynTP 2.0
    # Sep 2 (Wed)
    ("CLSK", "2026-09-02 09:05", "2026-09-02 10:13", "trail", -0.63),
    ("CXW",  "2026-09-02 09:06", "2026-09-02 11:27", "stop",  -1.15),   # HOT
    ("MSTR", "2026-09-02 09:07", "2026-09-02 09:54", "stop",  -0.98),
    ("GEO",  "2026-09-02 10:16", "2026-09-02 11:28", "stop",  -1.18),
    ("AAPL", "2026-09-02 10:16", "2026-09-02 14:58", "eod",   +0.04),
    ("GEO",  "2026-09-02 13:31", "2026-09-03 09:19", "tp",    +2.14),   # carry, DynTP 2.0
    ("CXW",  "2026-09-02 13:32", "2026-09-02 14:58", "eod",   -0.34),
    # Sep 3 (Thu)
    ("CLSK", "2026-09-03 09:12", "2026-09-03 09:18", "tp",    +1.54),
    ("MSTR", "2026-09-03 09:13", "2026-09-03 09:28", "tp",    +1.56),
    ("SMCI", "2026-09-03 09:19", "2026-09-03 09:32", "stop",  -1.10),
    ("TSLA", "2026-09-03 09:23", "2026-09-03 14:54", "trail", -0.10),
    ("CXW",  "2026-09-03 09:48", "2026-09-03 14:27", "trail", -0.43),
    ("CLSK", "2026-09-03 09:49", "2026-09-03 11:04", "trail", -0.08),
    ("AAPL", "2026-09-03 11:14", "2026-09-03 13:52", "stop",  -1.01),
    ("SMCI", "2026-09-03 14:04", "2026-09-03 14:58", "eod",   -0.58),
    ("NVDA", "2026-09-03 14:28", "2026-09-03 14:58", "eod",   -0.43),
    # Sep 4 (Fri)
    ("MSTR", "2026-09-04 09:02", "2026-09-04 09:17", "stop",  -1.15),
    ("GEO",  "2026-09-04 09:24", "2026-09-04 14:21", "tp",    +1.50),
    ("CLSK", "2026-09-04 09:28", "2026-09-04 13:51", "stop",  -1.07),
    ("CXW",  "2026-09-04 09:29", "2026-09-04 13:08", "trail", -0.50),
    ("PLTR", "2026-09-04 13:09", "2026-09-04 14:58", "eod",   -0.01),
]

def creds():
    for k, s in (("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"),
                 ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
                 ("ALPACA_KEY", "ALPACA_SECRET"),
                 ("ALPACA_KEY_ID", "ALPACA_SECRET_KEY")):
        if os.getenv(k) and os.getenv(s):
            return os.getenv(k), os.getenv(s)
    sys.exit("No Alpaca creds in env (tried APCA_*/ALPACA_*).")

def get_bars(sym, start_utc, end_utc):
    """Returns list of (ts_ct, o, h, l, c). Tries alpaca_trade_api, then alpaca-py."""
    key, sec = creds()
    try:
        from alpaca_trade_api import REST, TimeFrame
        api = REST(key, sec, base_url="https://paper-api.alpaca.markets")
        df = api.get_bars(sym, TimeFrame.Minute, start_utc.isoformat(), end_utc.isoformat(),
                          feed="iex", adjustment="all").df
        return [(ts.tz_convert(CT).to_pydatetime(), float(r.open), float(r.high), float(r.low), float(r.close))
                for ts, r in df.iterrows()]
    except ImportError:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        cl = StockHistoricalDataClient(key, sec)
        req = StockBarsRequest(symbol_or_symbols=sym, timeframe=TimeFrame.Minute,
                               start=start_utc, end=end_utc, feed="iex", adjustment="all")
        bars = cl.get_stock_bars(req).data.get(sym, [])
        return [(b.timestamp.astimezone(CT), float(b.open), float(b.high), float(b.low), float(b.close))
                for b in bars]

def rth(ts):
    """Regular-hours filter, CT."""
    t = ts.hour * 60 + ts.minute
    return 8 * 60 + 30 <= t < 15 * 60

def nearest(bars, ts, tol_min=2):
    best, bd = None, None
    for b in bars:
        d = abs((b[0] - ts).total_seconds()) / 60
        if d <= tol_min and (bd is None or d < bd):
            best, bd = b, d
    return best

def main():
    print("FILL RECONCILE V1.0  (CT, IEX minute bars, ±2 min tolerance)")
    print("sym   exit  entry_ts          entry_px  rep%   impl_exit  in_bar  exitbar_lo-hi         MFE%   MAE%  giveback%  gap%")
    flags = []
    for sym, e_s, x_s, xtype, rep in TRADES:
        e = datetime.strptime(e_s, "%Y-%m-%d %H:%M").replace(tzinfo=CT)
        x = datetime.strptime(x_s, "%Y-%m-%d %H:%M").replace(tzinfo=CT)
        bars = [b for b in get_bars(sym, (e - timedelta(minutes=5)).astimezone(UTC),
                                     (x + timedelta(minutes=5)).astimezone(UTC)) if rth(b[0])]
        eb, xb = nearest(bars, e), nearest(bars, x)
        if not eb or not xb:
            print(f"{sym:5} {xtype:5} {e_s}  NO BARS (entry={bool(eb)} exit={bool(xb)})")
            flags.append((sym, e_s, "no bars"))
            continue
        entry = eb[4]
        impl = entry * (1 + rep / 100)
        in_bar = xb[3] * 0.998 <= impl <= xb[2] * 1.002
        window = [b for b in bars if eb[0] <= b[0] <= xb[0]]
        mfe = max(b[2] for b in window) / entry * 100 - 100
        mae = min(b[3] for b in window) / entry * 100 - 100
        giveback = mfe - rep if xtype == "trail" else float("nan")
        gap = float("nan")
        if e.date() != x.date():
            prev_close = max((b for b in window if b[0].date() == e.date()), key=lambda b: b[0])[4]
            next_open = min((b for b in window if b[0].date() == x.date()), key=lambda b: b[0])[1]
            gap = next_open / prev_close * 100 - 100
        print(f"{sym:5} {xtype:5} {e_s}  {entry:8.2f}  {rep:+5.2f}  {impl:8.2f}   {'Y' if in_bar else 'N'}   "
              f"{xb[3]:8.2f}-{xb[2]:<8.2f}  {mfe:+5.2f}  {mae:+5.2f}   {giveback:+6.2f}   {gap:+5.2f}")
        if not in_bar:
            flags.append((sym, e_s, f"implied exit {impl:.2f} outside exit bar {xb[3]:.2f}-{xb[2]:.2f}"))
        if xtype == "trail" and mfe >= 1.0:
            flags.append((sym, e_s, f"trail exit {rep:+.2f} after MFE {mfe:+.2f} — {giveback:.2f}% given back"))
        if xtype in ("stop", "trail") and mfe >= 1.5:
            flags.append((sym, e_s, f"MFE {mfe:+.2f} reached but TP did not fire"))
    print("\nFLAGS:")
    for f in flags or [("-", "-", "none")]:
        print("  ", *f)

if __name__ == "__main__":
    main()
