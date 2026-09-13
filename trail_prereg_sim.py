#!/usr/bin/env python3
"""
trail_prereg_sim.py V1.0 -- TRAIL GEOMETRY PRE-REG SIM (registered Sep 13 2026)
================================================================================
Implements TRAIL_PREREG_Sep13.md. READ-ONLY: reads berserker_trade_fingerprints,
fetches IEX 1-min bars, writes NOTHING. Runs in the nexus-analyst console.

  python3 trail_prereg_sim.py            # TRAIN (Aug 17-28) + reported windows
  python3 trail_prereg_sim.py --oos      # adds OOS (Sep 14-25); refuses before
                                         # Sep 25 15:00 CT unless --force-oos

REGISTERED (binding, copied from the doc):
  Control  = live V10.67 semantics: arm on CURRENT profit, A=0.015, T=0.005,
             base trail 0.015, TP 0.015 (SPCX sl 0.015 else 0.010), MIN_HOLD 20m
             trail suppression, EOD 14:58 CT with carry iff pnl>0 AND close >
             close ~3 bars earlier, RTH-only management.
  Primary  = arm on PEAK, A=0.010, T=0.005. Grid A{0.008,0.010} x T{0.004,0.005}.
  Null     = arm on PEAK, A=0.015 -> must reproduce control on every trade.
  Fee/slip = 0.05% per exit, both arms (cancels in the delta; shown for levels).
  Windows  = TRAIN entered Aug 17-28; REPORTED-ONLY Aug 31-Sep 4 and Sep 8-11
             (inspected before binding); OOS entered Sep 14-25.
  Floors   = >=30 armed trades pooled (train+oos), OOS armed >=12.
  Fidelity = control replica matches realized exit_reason on >=80% AND realized
             pnl within +-0.25 on >=80%, else VOID. DynTP (realized take-profit
             with pnl>=1.85) and manual-close excluded from BOTH arms; >15%
             DynTP exclusions -> VOID pending dynamic_tp persistence.
  Bars     = within a bar: gap-through at the open resolves first (TP if open
             >= TP, SL at the actual open if open <= SL); then the LOW is tested
             against SL and the trail using the PRIOR peak (the low happens
             before the high); then the HIGH updates the peak and tests TP.
             The entry minute itself is not tested. This ordering exposes the
             candidate's tighter trail to more lows than the control's -- it is
             pessimistic against the candidate by construction.
  Verdict  = KILL  if train delta (primary - control, expectancy/trade) < +0.10
             KEEP  if train delta >= +0.10 AND OOS delta > 0 AND >=3/4 grid
                   cells positive on train (ridge) -> proceed to SHADOW
             PARK  if floors unmet after OOS closes, or KEEP carried by one
                   symbol (per-symbol table printed).
"""
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone, date, time as dtime
from zoneinfo import ZoneInfo

import requests
import psycopg2

CT  = ZoneInfo("America/Chicago")
UTC = timezone.utc
DATABASE_URL = os.environ.get("DATABASE_URL", "")

TP, BASE_TRAIL, SL_DEFAULT = 0.015, 0.015, 0.010
SYM_SL   = {"SPCX": 0.015}
MIN_HOLD = 20
FEE      = 0.05                  # pct-points per exit
DYNTP_MIN = 1.85
CONTROL  = ("control",  False, 0.015, 0.005)   # (name, arm_on_peak, A, T)
NULL     = ("null_A15", True,  0.015, 0.005)
PRIMARY  = ("A10_T05",  True,  0.010, 0.005)
GRID     = [("A08_T04", True, 0.008, 0.004), ("A08_T05", True, 0.008, 0.005),
            ("A10_T04", True, 0.010, 0.004), PRIMARY]

WINDOWS = {
    "TRAIN":     (date(2026, 8, 17), date(2026, 8, 28)),
    "REPORTED1": (date(2026, 8, 31), date(2026, 9, 4)),
    "REPORTED2": (date(2026, 9, 8),  date(2026, 9, 11)),
    "OOS":       (date(2026, 9, 14), date(2026, 9, 25)),
}
OOS_UNLOCK = datetime(2026, 9, 25, 15, 0, tzinfo=CT)
REASON_MAP = {"take-profit": "tp", "stop-loss": "sl", "trailing-stop": "trail",
              "eod-autoclose": "eod", "manual-close": "manual"}


def creds():
    for k, s in (("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"),
                 ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
                 ("ALPACA_PHASE4_API_KEY", "ALPACA_PHASE4_SECRET_KEY")):
        if os.getenv(k) and os.getenv(s):
            return os.getenv(k), os.getenv(s)
    sys.exit("No Alpaca creds in env.")


def q(sql, params=()):
    c = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    c.close()
    return rows


def fetch_bars(symbol, d_from, d_to):
    """RTH IEX 1-min bars [(dt_ct, o, h, l, c)] for [d_from, d_to] CT dates."""
    key, sec = creds()
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}
    start = datetime.combine(d_from, dtime(8, 0), CT).astimezone(UTC)
    end   = datetime.combine(d_to, dtime(15, 30), CT).astimezone(UTC)
    out, token = [], None
    while True:
        params = {"timeframe": "1Min", "adjustment": "all", "feed": "iex", "limit": 10000,
                  "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "end": end.strftime("%Y-%m-%dT%H:%M:%SZ")}
        if token:
            params["page_token"] = token
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        for b in j.get("bars") or []:
            dt = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(CT)
            t = dt.hour * 60 + dt.minute
            if 8 * 60 + 30 <= t < 15 * 60:
                out.append((dt, float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])))
        token = j.get("next_page_token")
        if not token:
            break
    return out


def replay(bars, entry_dt, entry_px, sl_pct, arm_on_peak, A, T):
    """Returns (reason, pnl_pct, armed, exit_dt) or None if unresolved."""
    tp_px, sl_px = entry_px * (1 + TP), entry_px * (1 - sl_pct)
    peak, armed = entry_px, False
    entry_min = entry_dt.replace(second=0, microsecond=0)
    closes, skip_day = [], None
    for i, (dt, o, h, l, c) in enumerate(bars):
        if dt < entry_min:
            continue
        if skip_day == dt.date():
            continue
        closes.append(c)
        if dt == entry_min:                       # entry minute: not tested
            peak = max(peak, h)
            continue
        held = (dt - entry_dt).total_seconds() / 60.0
        # 1. gap-through at the open
        if o >= tp_px:
            return "tp", TP * 100, armed, dt
        if o <= sl_px:
            return "sl", (o / entry_px - 1) * 100, armed, dt
        # 2. low vs SL, then trail on the PRIOR peak
        prior_peak = peak
        if l <= sl_px:
            return "sl", -sl_pct * 100, armed, dt
        arm_ref = (prior_peak / entry_px - 1) if arm_on_peak else (l / entry_px - 1)
        if arm_ref >= A:
            armed = True
        trailing = T if arm_ref >= A else BASE_TRAIL
        trail_px = prior_peak * (1 - trailing)
        if l <= trail_px and held >= MIN_HOLD:
            return "trail", (trail_px / entry_px - 1) * 100, armed, dt
        # 3. high updates peak, tests TP
        peak = max(peak, h)
        if (peak / entry_px - 1) >= A:
            armed = True
        if h >= tp_px:
            return "tp", TP * 100, armed, dt
        # 4. EOD decision bar
        if dt.hour == 14 and dt.minute >= 58:
            profit = c / entry_px - 1
            mom_ok = len(closes) >= 4 and c > closes[-4]
            if profit > 0 and mom_ok:
                skip_day = dt.date()              # carry: resume next session
                continue
            return "eod", profit * 100, armed, dt
    return None


def load_window(d_from, d_to):
    start = int(datetime.combine(d_from, dtime(0, 0), CT).timestamp())
    end   = int(datetime.combine(d_to + timedelta(days=1), dtime(0, 0), CT).timestamp())
    rows = q("""
        SELECT trade_id, symbol, entry_ts, exit_ts, entry_price, pnl_pct, exit_reason
        FROM berserker_trade_fingerprints
        WHERE is_paper = FALSE AND trade_id NOT LIKE 'bt_%%' AND won IS NOT NULL
          AND entry_ts >= %s AND entry_ts < %s AND entry_price > 0
        ORDER BY entry_ts
    """, (start, end))
    return [{"id": t, "sym": s, "entry": datetime.fromtimestamp(int(e), CT),
             "exit": datetime.fromtimestamp(int(x), CT), "px": float(p),
             "pnl": float(pnl), "reason": REASON_MAP.get(r or "", r or "?")}
            for t, s, e, x, p, pnl, r in rows]


def run_window(name, trades, bars_by_sym, show_symbols=False):
    print(f"\n{'=' * 72}\n{name}: {len(trades)} live trades")
    if not trades:
        return None
    # exclusions declared up front
    kept, dyntp, manual = [], 0, 0
    for t in trades:
        if t["reason"] == "manual":
            manual += 1
        elif t["reason"] == "tp" and t["pnl"] >= DYNTP_MIN:
            dyntp += 1
        else:
            kept.append(t)
    print(f"  excluded: DynTP {dyntp}, manual {manual} -> {len(kept)} scored")
    if trades and dyntp / len(trades) > 0.15:
        print("  !! DynTP exclusions > 15% -> window VOID pending dynamic_tp persistence")
        return None

    results = {}      # cell name -> list of (pnl_net, armed) aligned with kept
    unresolved = 0
    for cell in [CONTROL, NULL] + GRID:
        cname, on_peak, A, T = cell
        out = []
        for t in kept:
            r = replay(bars_by_sym.get(t["sym"], []), t["entry"], t["px"],
                       SYM_SL.get(t["sym"], SL_DEFAULT), on_peak, A, T)
            out.append(r)
        results[cname] = out
    # fidelity on the control
    ctrl = results["control"]
    scored_idx = [i for i, r in enumerate(ctrl) if r is not None]
    unresolved = len(kept) - len(scored_idx)
    reason_ok = sum(1 for i in scored_idx if ctrl[i][0] == kept[i]["reason"])
    pnl_ok    = sum(1 for i in scored_idx if abs(ctrl[i][1] - kept[i]["pnl"]) <= 0.25)
    n = len(scored_idx)
    fr = 100.0 * reason_ok / n if n else 0.0
    fp = 100.0 * pnl_ok / n if n else 0.0
    print(f"  fidelity: reason match {reason_ok}/{n} ({fr:.0f}%) | pnl within 0.25: "
          f"{pnl_ok}/{n} ({fp:.0f}%) | unresolved {unresolved}")
    for i in scored_idx:
        if ctrl[i][0] != kept[i]["reason"] or abs(ctrl[i][1] - kept[i]["pnl"]) > 0.25:
            t = kept[i]
            print(f"    mismatch {t['sym']} {t['entry'].strftime('%m-%d %H:%M')}: "
                  f"real {t['reason']} {t['pnl']:+.2f} | replica {ctrl[i][0]} {ctrl[i][1]:+.2f}")
    fidelity_ok = n >= 5 and fr >= 80 and fp >= 80
    # null must equal control
    null_diff = sum(1 for i in scored_idx if results["null_A15"][i] is None
                    or abs(results["null_A15"][i][1] - ctrl[i][1]) > 1e-9
                    or results["null_A15"][i][0] != ctrl[i][0])
    print(f"  null check (A=1.5% arm-on-peak == control): {'PASS' if null_diff == 0 else f'FAIL ({null_diff} differ)'}")
    if not fidelity_ok:
        print("  !! FIDELITY FAILED -> window VOID. Investigate the replica before reading cells.")
        return None
    if null_diff:
        print("  !! NULL CHECK FAILED -> sim defective -> VOID.")
        return None

    def expectancy(cname):
        vals = [results[cname][i][1] - FEE for i in scored_idx if results[cname][i] is not None]
        return sum(vals) / len(vals) if vals else 0.0, len(vals)
    ctrl_e, _ = expectancy("control")
    print(f"  control expectancy (net): {ctrl_e:+.3f}%/trade over n={n}")
    print(f"  {'cell':9} {'exp':>8} {'delta':>8} {'armed':>6} {'tp':>4} {'trail':>6} {'sl':>4} {'eod':>4}")
    deltas, armed_primary = {}, 0
    for cell in GRID:
        cname = cell[0]
        e, _ = expectancy(cname)
        rs = [results[cname][i] for i in scored_idx if results[cname][i] is not None]
        armed = sum(1 for r in rs if r[2])
        cnt = {k: sum(1 for r in rs if r[0] == k) for k in ("tp", "trail", "sl", "eod")}
        deltas[cname] = e - ctrl_e
        if cname == PRIMARY[0]:
            armed_primary = armed
        print(f"  {cname:9} {e:+8.3f} {e - ctrl_e:+8.3f} {armed:6d} {cnt['tp']:4d} {cnt['trail']:6d} {cnt['sl']:4d} {cnt['eod']:4d}")
    if show_symbols:
        print("  primary per symbol (delta, n):")
        by = {}
        for i in scored_idx:
            p, c = results[PRIMARY[0]][i], ctrl[i]
            if p is None:
                continue
            by.setdefault(kept[i]["sym"], []).append(p[1] - c[1])
        for sym, ds in sorted(by.items(), key=lambda kv: -sum(kv[1])):
            print(f"    {sym:5} {sum(ds) / len(ds):+.3f}  n={len(ds)}  sum={sum(ds):+.2f}")
    return {"n": n, "ctrl": ctrl_e, "deltas": deltas, "armed": armed_primary}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oos", action="store_true")
    ap.add_argument("--force-oos", action="store_true")
    a = ap.parse_args()
    if not DATABASE_URL:
        sys.exit("DATABASE_URL missing.")
    print("TRAIL PRE-REG SIM V1.0 | registered Sep 13 2026 | read-only | verdicts bind")

    windows = ["TRAIN", "REPORTED1", "REPORTED2"]
    if a.oos:
        if datetime.now(CT) < OOS_UNLOCK and not a.force_oos:
            print(f"OOS locked until {OOS_UNLOCK:%b %d %H:%M} CT. Reading it early contaminates it.")
        else:
            if datetime.now(CT) < OOS_UNLOCK:
                print("!! --force-oos before the window closed: this read is CONTAMINATING. Say so in the ledger.")
            windows.append("OOS")

    all_trades = {w: load_window(*WINDOWS[w]) for w in windows}
    # one bar fetch per symbol across every window in play (+2 sessions for carries)
    syms = sorted({t["sym"] for ts in all_trades.values() for t in ts})
    d_lo = min(WINDOWS[w][0] for w in windows)
    d_hi = max(WINDOWS[w][1] for w in windows) + timedelta(days=4)
    bars_by_sym = {}
    for s in syms:
        try:
            bars_by_sym[s] = fetch_bars(s, d_lo, d_hi)
            print(f"  bars {s}: {len(bars_by_sym[s])}")
        except Exception as e:
            print(f"  bars {s}: FETCH FAILED ({e}) -- its trades unresolved")
            bars_by_sym[s] = []

    res = {w: run_window(w, all_trades[w], bars_by_sym, show_symbols=(w in ("TRAIN", "OOS")))
           for w in windows}

    print(f"\n{'=' * 72}\nVERDICT (train binds now; OOS binds after Sep 25)")
    tr = res.get("TRAIN")
    if tr is None:
        print("TRAIN: VOID or empty -> no verdict. Fix the replica, then re-run.")
        return
    d = tr["deltas"][PRIMARY[0]]
    ridge = sum(1 for v in tr["deltas"].values() if v > 0)
    print(f"TRAIN primary delta {d:+.3f}%/trade | armed {tr['armed']} | ridge {ridge}/4 positive")
    if d < 0.10:
        print("TRAIN: KILL (delta < +0.10). Binding. The trail change does not ship.")
        return
    oo = res.get("OOS")
    if oo is None:
        print("TRAIN: PASS bar. Awaiting OOS (Sep 14-25). No verdict yet.")
        return
    od = oo["deltas"][PRIMARY[0]]
    armed_total = tr["armed"] + oo["armed"]
    print(f"OOS primary delta {od:+.3f} | OOS armed {oo['armed']} | pooled armed {armed_total}")
    if armed_total < 30 or oo["armed"] < 12:
        print("VERDICT: PARK -- floors unmet (pooled >=30, OOS >=12).")
    elif od > 0 and ridge >= 3:
        print("VERDICT: KEEP -> SHADOW stage. Check the per-symbol tables: one-symbol carries are PARK.")
    else:
        print("VERDICT: KILL -- OOS sign or ridge failed. Binding.")


if __name__ == "__main__":
    main()
