#!/usr/bin/env python3
"""
fill_reconcile.py  V1.2  -- read-only, Gate-0 style  (Sep 13 2026)
=====================================================================
Reconciles every LIVE Berserker trade closed in a date window against
Alpaca IEX minute bars.  Writes NOTHING.  Runs in the nexus-analyst
Railway console (has DATABASE_URL + Alpaca keys).

V1.2: MFE cross-check flags only when the BOT's MFE exceeds IEX's (a
peak IEX never printed = the NUE ghost-mark class). Bot MFE below IEX MFE
is expected: 30s sampling, and the exit bar's high is included in the IEX
window (SMCI Sep 11: TP filled +1.65, the same minute then printed +2.72).

V1.1: TRADES now come from berserker_trade_fingerprints (is_paper=FALSE,
no bt_, won IS NOT NULL, exit_ts in window) -- no more hand-typing the
T-Bone log every Monday.  Per trade the bot's own entry_price, pnl_pct,
exit_reason, mfe, mae are the record; IEX bars are the independent check.

  Default window: the most recent Mon..Fri (CT) that has fully elapsed.
  Override:       python3 fill_reconcile.py --from 2026-09-08 --to 2026-09-11

Per trade prints: bot entry px vs entry-minute bar range, implied exit
(entry*(1+pnl)) vs exit-minute bar range (+-2 min), IEX MFE/MAE from the
bot's entry price, bot-MFE minus IEX-MFE, trail giveback (IEX peak -> exit),
and for overnight carries the prior close -> next open gap.
Then the WEEK line: n, W/L, WR, avg W, avg L, expectancy, breakeven WR,
and a per-exit-type table.  Then FLAGS.

Cross-check the trade COUNT against the T-Bone daily reports: a trade the
bot announced but did not fingerprint (SNAP-style) will be MISSING here,
not flagged.  Add such trades to TRADES_OVERRIDE below.

Alpaca creds: tries APCA_API_KEY_ID/APCA_API_SECRET_KEY, then
ALPACA_API_KEY/ALPACA_SECRET_KEY, ALPACA_PHASE4_API_KEY/_SECRET_KEY,
ALPACA_KEY/ALPACA_SECRET, ALPACA_KEY_ID/ALPACA_SECRET_KEY.
"""
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

import requests
import psycopg2

CT  = ZoneInfo("America/Chicago")
UTC = timezone.utc

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Manual extras the DB missed: (symbol, entry CT "YYYY-MM-DD HH:MM",
# exit CT, exit_type in {tp,stop,trail,eod,manual}, reported_pnl_pct,
# entry_price or None). Normally empty.
TRADES_OVERRIDE = []

REASON_MAP = {"take-profit": "tp", "stop-loss": "stop", "trailing-stop": "trail",
              "eod-autoclose": "eod", "manual-close": "manual"}
MFE_TOL   = 0.30   # pct-points: flag if bot MFE > IEX MFE by more (ghost peak)
BAR_TOL   = 0.002  # 0.2% slack on bar containment (IEX lone prints)


def creds():
    for k, s in (("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"),
                 ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
                 ("ALPACA_PHASE4_API_KEY", "ALPACA_PHASE4_SECRET_KEY"),
                 ("ALPACA_KEY", "ALPACA_SECRET"),
                 ("ALPACA_KEY_ID", "ALPACA_SECRET_KEY")):
        if os.getenv(k) and os.getenv(s):
            return os.getenv(k), os.getenv(s)
    sys.exit("No Alpaca creds in env (tried APCA_*/ALPACA_*).")


def q(sql, params=()):
    c = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    c.close()
    return rows


def default_window():
    """Most recent fully-elapsed Mon..Fri in CT."""
    d = datetime.now(CT).date() - timedelta(days=1)
    while d.weekday() != 4:
        d -= timedelta(days=1)
    return d - timedelta(days=4), d


def fetch_minute_bars(symbol, start_ep, end_ep):
    """1-min IEX bars, adjustment=all, oldest-first: [(dt_ct, o, h, l, c)]."""
    key, sec = creds()
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}
    out, token = [], None
    while True:
        params = {"timeframe": "1Min", "adjustment": "all", "feed": "iex",
                  "limit": 10000,
                  "start": datetime.fromtimestamp(start_ep, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "end":   datetime.fromtimestamp(end_ep, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}
        if token:
            params["page_token"] = token
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        for b in j.get("bars") or []:
            dt = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(CT)
            out.append((dt, float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])))
        token = j.get("next_page_token")
        if not token:
            break
    return out


def rth(dt):
    t = dt.hour * 60 + dt.minute
    return 8 * 60 + 30 <= t < 15 * 60


def nearest(bars, dt, tol_min=2):
    best, bd = None, None
    for b in bars:
        d = abs((b[0] - dt).total_seconds()) / 60
        if d <= tol_min and (bd is None or d < bd):
            best, bd = b, d
    return best


def load_trades(d_from, d_to):
    start = int(datetime.combine(d_from, datetime.min.time(), CT).timestamp())
    end   = int(datetime.combine(d_to + timedelta(days=1), datetime.min.time(), CT).timestamp())
    rows = q("""
        SELECT trade_id, symbol, entry_ts, exit_ts, entry_price, pnl_pct,
               exit_reason, mfe, mae
        FROM berserker_trade_fingerprints
        WHERE is_paper = FALSE AND trade_id NOT LIKE 'bt_%%'
          AND won IS NOT NULL AND exit_ts >= %s AND exit_ts < %s
        ORDER BY entry_ts
    """, (start, end))
    trades = []
    for tid, sym, e_ts, x_ts, e_px, pnl, reason, mfe, mae in rows:
        trades.append({
            "id": tid, "sym": sym,
            "entry": datetime.fromtimestamp(int(e_ts), CT),
            "exit":  datetime.fromtimestamp(int(x_ts), CT),
            "entry_px": float(e_px) if e_px is not None else None,
            "pnl": float(pnl), "type": REASON_MAP.get(reason or "", reason or "?"),
            "bot_mfe": float(mfe) if mfe is not None else None,
            "bot_mae": float(mae) if mae is not None else None,
        })
    for sym, e_s, x_s, xtype, rep, e_px in TRADES_OVERRIDE:
        trades.append({
            "id": "override", "sym": sym,
            "entry": datetime.strptime(e_s, "%Y-%m-%d %H:%M").replace(tzinfo=CT),
            "exit":  datetime.strptime(x_s, "%Y-%m-%d %H:%M").replace(tzinfo=CT),
            "entry_px": e_px, "pnl": float(rep), "type": xtype,
            "bot_mfe": None, "bot_mae": None,
        })
    still_open = q("""
        SELECT symbol, entry_ts FROM berserker_trade_fingerprints
        WHERE is_paper = FALSE AND trade_id NOT LIKE 'bt_%%'
          AND won IS NULL AND entry_ts >= %s AND entry_ts < %s
        ORDER BY entry_ts
    """, (start, end))
    return trades, still_open


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    a = ap.parse_args()
    if not DATABASE_URL:
        sys.exit("DATABASE_URL missing -- run in the nexus-analyst console.")
    if a.d_from and a.d_to:
        d_from, d_to = date.fromisoformat(a.d_from), date.fromisoformat(a.d_to)
    else:
        d_from, d_to = default_window()

    trades, still_open = load_trades(d_from, d_to)
    print(f"FILL RECONCILE V1.2  window {d_from}..{d_to} CT  |  {len(trades)} closed live trades "
          f"from berserker_trade_fingerprints  |  IEX minute bars, +-2 min")
    if not trades:
        print("No closed live trades in window. Check the window, or the fingerprint writer.")
        return

    # One bar fetch per symbol spanning that symbol's earliest entry -> latest exit
    span = {}
    for t in trades:
        lo, hi = span.get(t["sym"], (t["entry"], t["exit"]))
        span[t["sym"]] = (min(lo, t["entry"]), max(hi, t["exit"]))
    bars_by_sym = {}
    for sym, (lo, hi) in span.items():
        try:
            raw = fetch_minute_bars(sym, int((lo - timedelta(minutes=5)).timestamp()),
                                    int((hi + timedelta(minutes=5)).timestamp()))
            bars_by_sym[sym] = [b for b in raw if rth(b[0])]
        except Exception as e:
            print(f"  bars {sym}: FETCH FAILED ({e})")
            bars_by_sym[sym] = []

    print("sym   type   entry CT           bot_px  in_ebar  rep%    impl_x   in_xbar  xbar lo-hi          IEX_MFE  IEX_MAE  botMFE-IEX  giveback  gap")
    flags = []
    for t in trades:
        sym, e, x = t["sym"], t["entry"], t["exit"]
        bars = bars_by_sym.get(sym, [])
        eb, xb = nearest(bars, e), nearest(bars, x)
        e_s = e.strftime("%Y-%m-%d %H:%M")
        if not eb or not xb:
            print(f"{sym:5} {t['type']:6} {e_s}  NO BARS (entry={bool(eb)} exit={bool(xb)})")
            flags.append((sym, e_s, "no bars at entry/exit minute"))
            continue
        entry = t["entry_px"] if t["entry_px"] else eb[4]
        in_ebar = eb[3] * (1 - BAR_TOL) <= entry <= eb[2] * (1 + BAR_TOL)
        impl = entry * (1 + t["pnl"] / 100)
        in_xbar = xb[3] * (1 - BAR_TOL) <= impl <= xb[2] * (1 + BAR_TOL)
        window = [b for b in bars if eb[0] <= b[0] <= xb[0]] or [eb, xb]
        mfe = max(b[2] for b in window) / entry * 100 - 100
        mae = min(b[3] for b in window) / entry * 100 - 100
        d_mfe = (t["bot_mfe"] - mfe) if t["bot_mfe"] is not None else float("nan")
        giveback = mfe - t["pnl"] if t["type"] == "trail" else float("nan")
        gap = float("nan")
        if e.date() != x.date():
            day_e = [b for b in window if b[0].date() == e.date()]
            day_x = [b for b in window if b[0].date() == x.date()]
            if day_e and day_x:
                prev_close = max(day_e, key=lambda b: b[0])[4]
                next_open  = min(day_x, key=lambda b: b[0])[1]
                gap = next_open / prev_close * 100 - 100
        print(f"{sym:5} {t['type']:6} {e_s}  {entry:8.2f}   {'Y' if in_ebar else 'N'}    "
              f"{t['pnl']:+6.2f}  {impl:8.2f}    {'Y' if in_xbar else 'N'}    "
              f"{xb[3]:8.2f}-{xb[2]:<8.2f}  {mfe:+6.2f}   {mae:+6.2f}    {d_mfe:+6.2f}    "
              f"{giveback:+6.2f}   {gap:+5.2f}")
        if not in_ebar:
            flags.append((sym, e_s, f"bot entry {entry:.2f} outside entry bar {eb[3]:.2f}-{eb[2]:.2f}"))
        if not in_xbar:
            flags.append((sym, e_s, f"implied exit {impl:.2f} outside exit bar {xb[3]:.2f}-{xb[2]:.2f}"))
        if t["bot_mfe"] is not None and d_mfe > MFE_TOL:
            flags.append((sym, e_s, f"bot MFE {t['bot_mfe']:+.2f} ABOVE IEX MFE {mfe:+.2f} (ghost peak? diff {d_mfe:+.2f})"))
        if t["type"] == "trail" and mfe >= 1.0:
            flags.append((sym, e_s, f"trail exit {t['pnl']:+.2f} after MFE {mfe:+.2f} -- {giveback:.2f}% given back"))
        if t["type"] in ("stop", "trail") and mfe >= 1.5:
            flags.append((sym, e_s, f"MFE {mfe:+.2f} reached but TP did not fire"))

    # ── Week summary (the numbers the handoff computed by hand) ────────────
    wins  = [t["pnl"] for t in trades if t["pnl"] > 0]
    loss  = [t["pnl"] for t in trades if t["pnl"] <= 0]
    n     = len(trades)
    avg_w = sum(wins) / len(wins) if wins else 0.0
    avg_l = sum(loss) / len(loss) if loss else 0.0
    expct = sum(t["pnl"] for t in trades) / n
    be_wr = (-avg_l) / (avg_w - avg_l) * 100 if (avg_w - avg_l) > 0 else float("nan")
    print(f"\nWEEK: n={n}  {len(wins)}W/{len(loss)}L  WR={100*len(wins)/n:.1f}%  "
          f"avgW={avg_w:+.2f}  avgL={avg_l:+.2f}  expectancy={expct:+.3f}%/trade  "
          f"breakeven WR={be_wr:.1f}%")
    print("by exit type:")
    for typ in ("tp", "stop", "trail", "eod", "manual"):
        grp = [t for t in trades if t["type"] == typ]
        if not grp:
            continue
        avg = sum(t["pnl"] for t in grp) / len(grp)
        gw  = sum(1 for t in grp if t["pnl"] > 0)
        print(f"  {typ:6} n={len(grp):2}  {gw}W  avg={avg:+.2f}%  sum={sum(t['pnl'] for t in grp):+.2f}%")
    if still_open:
        print("still open (entered in window, no exit yet):")
        for sym, e_ts in still_open:
            print(f"  {sym} entered {datetime.fromtimestamp(int(e_ts), CT).strftime('%Y-%m-%d %H:%M')}")

    print("\nFLAGS:")
    for f in flags or [("-", "-", "none")]:
        print("  ", *f)
    print("\nCheck: trade count above vs T-Bone daily reports for the same days.")
    print("Note: /buy manual positions are NOT in this table (manual_buy_equity never calls "
          "record_entry, main.py V10.67) -- their P&L only appears in T-Bone and the equity delta.")


if __name__ == "__main__":
    main()
