#!/usr/bin/env python3
"""
thorn_extended_thesis.py — READ-ONLY. The Jul 29 crypto batch (A1–A7),
executed per NEXUS_Jul29_Preregistration.md (written Jul 22, before looking).

A1  24h/48h forward returns via self-join     (KILL if 48h fee-clearance <4%)
A2  Dip-recovery, TROUGH-anchored redesign    (+ pullback-after-confirmation)
A3  Session-conditioned 24h forwards          (US-peak vs Asia >= 0.5%)
A4  Funding-rate extremes, contrarian         (tails separate from middle 80%)
A5  Liquidation snap-back (vol_spike+wick)    (positive 4-24h forwards)
A6  BTC lead/lag (btc_ret_5m on alts)         (conditioning separates forwards)
A7  Maker feasibility from spread_bps         (halved bar + 25%/50% adverse-
                                               selection haircuts)
Fee bar: 2.40% RT taker; maker ~1.20% RT.
All SELECTs. Self-joins done in-memory (pair -> sorted ts).

USAGE:  python3 thorn_extended_thesis.py
"""
import os
import sys
from bisect import bisect_left
from collections import defaultdict

try:
    import psycopg2
except ImportError:
    print("psycopg2 missing — run on a NEXUS console.")
    sys.exit(1)

FEE_RT, MAKER_RT = 2.40, 1.20
H24, H48, TOL = 86400, 172800, 900     # +-15 min matching window


def stats(v, bar=None):
    if not v:
        return None
    v = sorted(v)
    n = len(v)
    out = {"n": n, "mean": sum(v) / n, "med": v[n // 2],
           "pos": sum(1 for x in v if x > 0) * 100.0 / n}
    if bar is not None:
        out["clear"] = sum(1 for x in v if x > bar) * 100.0 / n
    return out


def line(tag, s, bar=False):
    if not s:
        return f"  {tag:28s} n=0"
    extra = f"  >fee={s['clear']:4.1f}%" if bar and "clear" in s else ""
    return (f"  {tag:28s} n={s['n']:6d}  mean={s['mean']:+.3f}%  "
            f"med={s['med']:+.3f}%  pos={s['pos']:4.1f}%{extra}")


def main():
    db = os.environ.get("DATABASE_URL", "")
    if not db:
        print("DATABASE_URL not set."); sys.exit(1)
    conn = psycopg2.connect(db, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ts, pair, price, session, hour_cdt, funding, vol_spike,
                   wick_frac, btc_ret_5m, spread_bps, fwd_4h, rsi_5m
            FROM crypto_thorn_observations
            WHERE price > 0 ORDER BY pair, ts
        """)
        rows = cur.fetchall()
    conn.close()
    print(f"loaded {len(rows)} observations")

    by_pair_ts, by_pair_px = defaultdict(list), defaultdict(list)
    for r in rows:
        by_pair_ts[r[1]].append(r[0])
        by_pair_px[r[1]].append(r[2])

    def fwd(pair, ts, px, horizon):
        arr = by_pair_ts[pair]
        i = bisect_left(arr, ts + horizon - TOL)
        if i < len(arr) and arr[i] <= ts + horizon + TOL:
            return (by_pair_px[pair][i] / px - 1) * 100
        return None

    # decorate every row with 24/48h forwards once
    R = []
    for ts, pair, px, session, hour, funding, vspike, wick, btc5, spread, f4h, rsi in rows:
        R.append({"ts": ts, "pair": pair, "px": px, "session": session,
                  "hour": hour, "funding": funding, "vspike": vspike,
                  "wick": wick, "btc5": btc5, "spread": spread,
                  "f4h": f4h, "rsi": rsi,
                  "f24": fwd(pair, ts, px, H24), "f48": fwd(pair, ts, px, H48)})
    n24 = sum(1 for r in R if r["f24"] is not None)
    n48 = sum(1 for r in R if r["f48"] is not None)
    print(f"self-join coverage: 24h={n24}  48h={n48}")

    # ---------- A1 ----------
    print("=" * 74); print("A1. EXTENDED-HORIZON FORWARDS (pooled)")
    s4  = stats([r["f4h"] for r in R if r["f4h"] is not None], FEE_RT)
    s24 = stats([r["f24"] for r in R if r["f24"] is not None], FEE_RT)
    s48 = stats([r["f48"] for r in R if r["f48"] is not None], FEE_RT)
    print(line("fwd 4h", s4, True)); print(line("fwd 24h", s24, True)); print(line("fwd 48h", s48, True))
    a1_clear = s48["clear"] if s48 else 0.0
    print(f"  VERDICT A1: {'KEEP — horizon extension works' if a1_clear >= 8 else 'PARK — partial' if a1_clear >= 4 else 'KILL'}"
          f" (48h fee-clearance {a1_clear:.1f}%, bars: KEEP>=8, KILL<4)")

    # ---------- A2 ----------
    print("=" * 74); print("A2. DIP-RECOVERY, TROUGH-ANCHORED (dip = 1h-ago->trough, K = trough->now)")
    idx = {p: {t: i for i, t in enumerate(by_pair_ts[p])} for p in by_pair_ts}
    a2 = defaultdict(list)   # (dipband, kband, horizon) -> vals
    pullback = {"conf": [], "pull": []}
    for r in R:
        p, ts = r["pair"], r["ts"]
        i = idx[p].get(ts)
        if i is None or i < 12:
            continue
        win = by_pair_px[p][i - 12:i + 1]           # ~1h of 5-min obs
        tmin = min(range(len(win)), key=lambda j: win[j])
        trough = win[tmin]
        dip = (trough / win[0] - 1) * 100           # 1h-ago -> trough
        if dip > -0.5:
            continue
        k = 0                                        # rises since trough
        for j in range(tmin + 1, len(win)):
            if win[j] > win[j - 1]:
                k += 1
            else:
                k = 0
        band = "<=-2" if dip <= -2 else "-2..-1" if dip <= -1 else "-1..-0.5"
        kb = "K0" if k == 0 else "K1" if k == 1 else "K2" if k == 2 else "K3+"
        for h, key in (("f4h", "4h"), ("f24", "24h"), ("f48", "48h")):
            if r[h] is not None:
                a2[(band, kb, key)].append(r[h])
        # pre-registered sub-claim: K>=2 -> entering on first pullback beats at-confirmation
        if k >= 2 and r["f4h"] is not None:
            pullback["conf"].append(r["f4h"])
        if k == 0 and tmin < len(win) - 3 and r["f4h"] is not None:
            # proxy for "pullback after a confirmed run": trough older, currently red candle
            if sum(1 for j in range(tmin + 1, len(win)) if win[j] > win[j - 1]) >= 2:
                pullback["pull"].append(r["f4h"])
    for band in ("<=-2", "-2..-1", "-1..-0.5"):
        for h in ("4h", "24h", "48h"):
            cells = "  ".join(
                f"{kb}:{(sum(a2[(band,kb,h)])/len(a2[(band,kb,h)])):+.2f}%/{len(a2[(band,kb,h)])}"
                if a2[(band, kb, h)] else f"{kb}:--"
                for kb in ("K0", "K1", "K2", "K3+"))
            print(f"  dip {band:8s} {h:3s}  {cells}")
    sc, sp = stats(pullback["conf"]), stats(pullback["pull"])
    print(line("  at-confirmation (K>=2)", sc)); print(line("  pullback-after-run", sp))
    deep_k = [kb for kb in ("K2", "K3+") if a2[("<=-2", kb, "4h")]]
    print(f"  VERDICT A2: deep-dip confirmation cells now "
          f"{'POPULATED — trough anchor fixed the census' if deep_k else 'still empty — park deeper'}; "
          f"ordering + fee read above.")

    # ---------- A3 ----------
    print("=" * 74); print("A3. SESSION-CONDITIONED 24h FORWARDS")
    us  = stats([r["f24"] for r in R if r["f24"] is not None and r["hour"] in (8, 9, 10, 11)])
    asia = stats([r["f24"] for r in R if r["f24"] is not None and r["hour"] in (20, 21, 22, 23)])
    print(line("US-peak (8-11 CDT)", us)); print(line("Asia (20-23 CDT)", asia))
    if us and asia:
        d = us["mean"] - asia["mean"]
        print(f"  VERDICT A3: spread {d:+.3f}% at 24h — "
              f"{'FILTER-CONFIRMED (>=0.5)' if d >= 0.5 else 'below the 0.5% bar — persists as 1h effect only' if d > 0 else 'KILL at 24h'}")

    # ---------- A4 ----------
    print("=" * 74); print("A4. FUNDING EXTREMES (contrarian)")
    fund = sorted([r for r in R if r["funding"] is not None and r["f24"] is not None],
                  key=lambda r: r["funding"])
    if len(fund) >= 100:
        dec = len(fund) // 10
        lo, hi, mid = fund[:dec], fund[-dec:], fund[dec:-dec]
        sl, sh, sm = (stats([r["f24"] for r in c]) for c in (lo, hi, mid))
        print(line("bottom decile funding", sl)); print(line("middle 80%", sm)); print(line("top decile funding", sh))
        ok = sl and sm and sl["mean"] - sm["mean"] >= 0.4
        ok2 = sh and sm and sm["mean"] - sh["mean"] >= 0.0
        print(f"  VERDICT A4: {'KEEP — bottom tail separates >= +0.4% vs middle' if ok else 'KILL — tails do not separate'}"
              f"{' (top-tail negative-side consistent)' if ok and ok2 else ''}")
    else:
        print(f"  VERDICT A4: INSUFFICIENT — only {len(fund)} funded rows with 24h forwards")

    # ---------- A5 ----------
    print("=" * 74); print("A5. LIQUIDATION SNAP-BACK (vol_spike top-decile AND wick_frac top-decile)")
    vs = [r for r in R if r["vspike"] is not None and r["wick"] is not None]
    if len(vs) >= 100:
        vq = sorted(x["vspike"] for x in vs)[int(len(vs) * 0.9)]
        wq = sorted(x["wick"] for x in vs)[int(len(vs) * 0.9)]
        hits = [r for r in vs if r["vspike"] >= vq and r["wick"] >= wq]
        for h, key in (("f4h", "4h"), ("f24", "24h")):
            print(line(f"snap-back candidates {key}", stats([r[h] for r in hits if r[h] is not None], FEE_RT), True))
        base4 = stats([r["f4h"] for r in vs if r["f4h"] is not None])
        hit4 = stats([r["f4h"] for r in hits if r["f4h"] is not None])
        ok = hit4 and base4 and hit4["n"] >= 30 and hit4["mean"] > base4["mean"] + 0.2
        print(f"  VERDICT A5: {'KEEP — snap-back beats baseline by >0.2% at 4h' if ok else ('INSUFFICIENT n' if hit4 and hit4['n'] < 30 else 'KILL')}")
    else:
        print("  VERDICT A5: INSUFFICIENT — vol/wick fields too sparse")

    # ---------- A6 ----------
    print("=" * 74); print("A6. BTC LEAD/LAG on alts (btc_ret_5m conditioning)")
    alts = [r for r in R if r["pair"] != "BTC-USDC" and r["btc5"] is not None and r["f4h"] is not None]
    if len(alts) >= 200:
        up  = stats([r["f4h"] for r in alts if r["btc5"] > 0.15])
        dn  = stats([r["f4h"] for r in alts if r["btc5"] < -0.15])
        fl  = stats([r["f4h"] for r in alts if -0.15 <= r["btc5"] <= 0.15])
        print(line("BTC +0.15%+ prior 5m", up)); print(line("BTC flat", fl)); print(line("BTC -0.15%- prior 5m", dn))
        ok = up and dn and (up["mean"] - dn["mean"]) >= 0.15
        print(f"  VERDICT A6: {'KEEP — BTC move conditions alt forwards (spread >= 0.15%)' if ok else 'KILL — no usable lead/lag at 4h'}")
    else:
        print("  VERDICT A6: INSUFFICIENT")

    # ---------- A7 ----------
    print("=" * 74); print("A7. MAKER FEASIBILITY (spread_bps + adverse-selection haircuts)")
    sp = [r["spread"] for r in R if r["spread"] is not None and 0 < r["spread"] < 500]
    ssp = stats(sp)
    if ssp:
        print(f"  spread_bps: n={ssp['n']}  mean={ssp['mean']:.1f}  med={ssp['med']:.1f}")
        f24v = [r["f24"] for r in R if r["f24"] is not None]
        for hc, lbl in ((0.0, "no haircut"), (0.25, "25% haircut"), (0.50, "50% haircut")):
            eff_bar = MAKER_RT / (1 - hc) if hc < 1 else 999
            cl = sum(1 for x in f24v if x > eff_bar) * 100.0 / len(f24v) if f24v else 0
            print(f"  24h clearance vs maker bar ({lbl}, eff {eff_bar:.2f}%): {cl:.1f}%")
        cl25 = sum(1 for x in f24v if x > MAKER_RT / 0.75) * 100.0 / len(f24v) if f24v else 0
        print(f"  VERDICT A7: {'MAKER ROUTE VIABLE for contrarian entries' if cl25 >= 8 else 'KILL maker route (25%-haircut clearance < 8%)'}"
              f" — mean spread supports post-only pricing" if ssp['med'] < 20 else "")
    else:
        print("  VERDICT A7: INSUFFICIENT — spread_bps not captured yet (V5.18 field)")

    print("=" * 74)
    print("Batch complete. Verdicts bind per the Jul 22 pre-registration.")


if __name__ == "__main__":
    main()
