#!/usr/bin/env python3
"""
fill_truth.py  V1.0  -- read-only  (Sep 24 2026)
=====================================================================
Rebuilds every live Berserker round trip from Alpaca's own FILL activity
feed and diffs it against berserker_trade_fingerprints. Writes NOTHING.
Runs in the nexus-analyst console (Alpaca keys + DATABASE_URL).

Why: the sell-fill poll had a 3s ceiling until V10.68; on timeout the
QUOTE estimate was recorded as fill_pnl. PLTR Sep 15: recorded +0.16,
filled $171.54 = -1.00%. This script finds every row like it.

  python3 fill_truth.py                 # since 2026-07-29 (wall live)
  python3 fill_truth.py --since 2026-08-17

Round trip = fills for one symbol from the first BUY until net qty returns
to ~0 (Berserker holds one position per symbol; partial fills of one order
share a timestamp and are merged). Entry = cost/qty, exit = proceeds/qty.
Match to a fingerprint: same symbol, |entry_ts - first buy fill| <= 300s.

Prints: per-trade table (recorded vs broker, delta), MISMATCHES
(|delta| >= 0.25 pct-pts or sign flip), broker-only trips (no fingerprint:
manual buys, pre-fix gaps), fingerprint-only rows (no fills found), then
recorded vs broker expectancy over the matched set, by exit type.

Keys: needs the LIVE account's key (activities are per account). Tries
APCA_API_KEY_ID/SECRET, ALPACA_API_KEY/SECRET_KEY, ALPACA_PHASE4_*.
A 401/403 or an empty feed for a week with known trades means the key on
this service is not the Berserker account -- say so, don't guess.
"""
import os
import sys
import argparse
from datetime import datetime, date, timezone, time as dtime
from zoneinfo import ZoneInfo

import requests
import psycopg2

CT  = ZoneInfo("America/Chicago")
UTC = timezone.utc
DATABASE_URL = os.environ.get("DATABASE_URL", "")
SYMBOLS = ["CLSK", "MARA", "PLTR", "GEO", "CXW", "NUE", "MSTR", "NVDA", "TSLA", "AAPL", "SMCI", "SPCX"]
MATCH_S = 300
TOL     = 0.25
REASON  = {"take-profit": "tp", "stop-loss": "sl", "trailing-stop": "tr",
           "eod-autoclose": "eod", "manual-close": "man"}


def creds():
    for k, s in (("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"),
                 ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
                 ("ALPACA_PHASE4_API_KEY", "ALPACA_PHASE4_SECRET_KEY")):
        if os.getenv(k) and os.getenv(s):
            return os.getenv(k), os.getenv(s), k
    sys.exit("No Alpaca creds in env.")


def q(sql, params=()):
    c = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    c.close()
    return rows


def fetch_fills(since):
    key, sec, which = creds()
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}
    url = "https://api.alpaca.markets/v2/account/activities/FILL"
    after = datetime.combine(since, dtime(0, 0), CT).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    out, token = [], None
    while True:
        params = {"after": after, "direction": "asc", "page_size": 100}
        if token:
            params["page_token"] = token
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code in (401, 403):
            sys.exit(f"Alpaca {r.status_code} on activities with {which} -- this key is not a live "
                     f"account key (or not the Berserker account). Stop here.")
        r.raise_for_status()
        page = r.json() or []
        for a in page:
            sym = str(a.get("symbol", "")).upper()
            if sym not in SYMBOLS:
                continue
            try:
                ts = datetime.fromisoformat(str(a["transaction_time"]).replace("Z", "+00:00")).astimezone(CT)
                out.append({"sym": sym, "side": str(a.get("side", "")).lower(),
                            "qty": float(a.get("qty", 0)), "px": float(a.get("price", 0)),
                            "ts": ts, "id": str(a.get("id", ""))})
            except Exception:
                continue
        if len(page) < 100:
            break
        token = page[-1].get("id")
        if not token:
            break
    return sorted(out, key=lambda f: f["ts"]), which


def round_trips(fills):
    """Walk each symbol's fills; a trip closes when net qty returns to ~0."""
    trips, state = [], {}
    for f in fills:
        st = state.setdefault(f["sym"], {"qty": 0.0, "cost": 0.0, "proceeds": 0.0,
                                         "sold": 0.0, "t0": None, "t1": None})
        if f["side"].startswith("buy"):
            if st["qty"] < 1e-6 and st["t0"] is None:
                st["t0"] = f["ts"]
            st["qty"]  += f["qty"]
            st["cost"] += f["qty"] * f["px"]
        else:
            st["qty"]      -= f["qty"]
            st["sold"]     += f["qty"]
            st["proceeds"] += f["qty"] * f["px"]
            st["t1"] = f["ts"]
            if st["qty"] < 1e-4 and st["sold"] > 0:
                bought = st["cost"] / max(st["sold"] + st["qty"], 1e-9)
                entry  = st["cost"] / (st["sold"] + max(st["qty"], 0.0))
                exit_  = st["proceeds"] / st["sold"]
                trips.append({"sym": f["sym"], "t0": st["t0"], "t1": st["t1"],
                              "entry": entry, "exit": exit_,
                              "pnl": (exit_ / entry - 1) * 100 if entry else 0.0})
                state[f["sym"]] = {"qty": 0.0, "cost": 0.0, "proceeds": 0.0,
                                   "sold": 0.0, "t0": None, "t1": None}
    open_syms = [s for s, st in state.items() if st["qty"] > 1e-4]
    return trips, open_syms


def load_fps(since):
    start = int(datetime.combine(since, dtime(0, 0), CT).timestamp())
    rows = q("""
        SELECT trade_id, symbol, entry_ts, exit_ts, entry_price, pnl_pct, exit_reason
        FROM berserker_trade_fingerprints
        WHERE is_paper = FALSE AND trade_id NOT LIKE 'bt_%%'
          AND won IS NOT NULL AND entry_ts >= %s
        ORDER BY entry_ts
    """, (start,))
    return [{"id": t, "sym": s, "t0": datetime.fromtimestamp(int(e), CT),
             "t1": datetime.fromtimestamp(int(x), CT), "entry": float(p or 0),
             "pnl": float(pnl), "type": REASON.get(r or "", r or "?")}
            for t, s, e, x, p, pnl, r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-07-29")
    a = ap.parse_args()
    if not DATABASE_URL:
        sys.exit("DATABASE_URL missing.")
    since = date.fromisoformat(a.since)
    fills, which = fetch_fills(since)
    trips, open_syms = round_trips(fills)
    fps = load_fps(since)
    print(f"FILL TRUTH V1.0 | since {since} | key {which} | fills {len(fills)} | "
          f"broker round trips {len(trips)} | fingerprints {len(fps)} | still open at broker: {open_syms or '-'}")
    if not fills:
        print("No fills returned. Either no activity or this key is not the Berserker account.")
        return

    used, matched, mism = set(), [], []
    print(f"\n{'sym':5} {'type':4} {'entry CT':16} {'rec_px':>8} {'brk_px':>8} {'rec%':>7} {'brk%':>7} {'delta':>7}")
    for fp in fps:
        best, bd = None, None
        for i, tr in enumerate(trips):
            if i in used or tr["sym"] != fp["sym"]:
                continue
            d = abs((tr["t0"] - fp["t0"]).total_seconds())
            if d <= MATCH_S and (bd is None or d < bd):
                best, bd = i, d
        if best is None:
            continue
        used.add(best)
        tr = trips[best]
        delta = tr["pnl"] - fp["pnl"]
        flip  = (tr["pnl"] > 0) != (fp["pnl"] > 0)
        matched.append((fp, tr, delta, flip))
        flag = " <-- FLIP" if flip else (" <-- off" if abs(delta) >= TOL else "")
        print(f"{fp['sym']:5} {fp['type']:4} {fp['t0'].strftime('%m-%d %H:%M'):16} {fp['entry']:8.2f} {tr['entry']:8.2f} "
              f"{fp['pnl']:+7.2f} {tr['pnl']:+7.2f} {delta:+7.2f}{flag}")
        if flip or abs(delta) >= TOL:
            mism.append((fp, tr, delta, flip))

    fp_only = [fp for fp in fps if not any(m[0] is fp for m in matched)]
    br_only = [tr for i, tr in enumerate(trips) if i not in used]

    print(f"\nMATCHED {len(matched)}/{len(fps)} fingerprints to broker trips")
    print(f"MISMATCHES (|delta| >= {TOL} or sign flip): {len(mism)}  "
          f"flips: {sum(1 for m in mism if m[3])}")
    for fp, tr, delta, flip in mism:
        print(f"  {fp['sym']} {fp['t0'].strftime('%m-%d %H:%M')} {fp['type']}: recorded {fp['pnl']:+.2f} "
              f"broker {tr['pnl']:+.2f} (delta {delta:+.2f}){' FLIP' if flip else ''}")
    if br_only:
        print(f"\nBROKER-ONLY trips (no fingerprint -- manual buys, pre-fix gaps): {len(br_only)}")
        for tr in br_only:
            print(f"  {tr['sym']} {tr['t0'].strftime('%m-%d %H:%M')} -> {tr['t1'].strftime('%m-%d %H:%M')} "
                  f"{tr['pnl']:+.2f}%")
    if fp_only:
        print(f"\nFINGERPRINT-ONLY rows (no fills within {MATCH_S}s): {len(fp_only)}")
        for fp in fp_only:
            print(f"  {fp['sym']} {fp['t0'].strftime('%m-%d %H:%M')} {fp['type']} {fp['pnl']:+.2f}")

    if matched:
        n = len(matched)
        rec = sum(m[0]["pnl"] for m in matched) / n
        brk = sum(m[1]["pnl"] for m in matched) / n
        print(f"\nEXPECTANCY over matched n={n}: recorded {rec:+.3f}%/trade | broker {brk:+.3f}%/trade | "
              f"gap {brk - rec:+.3f}")
        print("by exit type (recorded -> broker):")
        for typ in ("tp", "sl", "tr", "eod", "man"):
            g = [m for m in matched if m[0]["type"] == typ]
            if g:
                r_ = sum(m[0]["pnl"] for m in g) / len(g)
                b_ = sum(m[1]["pnl"] for m in g) / len(g)
                print(f"  {typ:4} n={len(g):3}  {r_:+.3f} -> {b_:+.3f}  (gap {b_ - r_:+.3f})  "
                      f"off: {sum(1 for m in g if abs(m[2]) >= TOL)}")
    print("\nRead-only. Corrections, if any, go to a NEW column (pnl_pct_broker) by a "
          "separate registered step -- never overwrite pnl_pct.")


if __name__ == "__main__":
    main()
