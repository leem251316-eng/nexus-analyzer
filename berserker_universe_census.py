#!/usr/bin/env python3
"""
berserker_universe_census.py — READ-ONLY CENSUS. Runs the V3.3 Berserker
backtest engine over a 40-symbol candidate universe to ask ONE question:
does Berserker's entry logic find edge outside its 12-symbol home?

Pre-registered per BERSERKER_UNIVERSE_CENSUS_PREREG.md (committed before
this run). NOT a thesis script in the banked sense — passing symbols are
PAPER-ROTATION CANDIDATES only, never live adds.

DESIGN:
- Imports the deployed V3.3 engine (nexus_analyzer_1min_railway.py must sit
  in cwd — fetch it first) and drives fetch_all_bars/replay_berserker
  directly. write_fingerprints is NEVER called: zero DB writes, the
  C-thesis census stays clean.
- 5 batches of 8 candidates. Every batch also includes the 7 TRUMP_THEME
  anchors so the sector-health regime gate behaves exactly as production,
  and the anchors double as a calibration check: if an anchor's full-train
  WR drifts >3pts from the latest Sunday run, the batch is flagged VOID
  (data problem, not market answer).
- Candidates run on DEFAULT recipe (tp 1.5% / sl 1.0%, no avoid-hours) —
  the census measures raw entry-logic fit, deliberately understating tuned
  potential.
- PASS requires ALL FOUR pre-registered bars (see prereg):
  OOS WR >= 42% | OOS avg PnL > 0 | OOS n >= 100 | |train-OOS WR| <= 6pts

USAGE (analyzer console, after the weekly suite — API budget):
  python3 -c "import urllib.request as u; [u.urlretrieve('https://raw.githubusercontent.com/leem251316-eng/nexus-analyzer/main/'+f,f) for f in ['nexus_analyzer_1min_railway.py','berserker_universe_census.py']]"
  python3 berserker_universe_census.py            # all 5 batches (~2h)
  python3 berserker_universe_census.py --batch 1  # single batch (~25m)
"""
import argparse
import time
import sys

CANDIDATES = [
    # miners / crypto-adjacent
    "RIOT", "HUT", "CIFR", "WULF", "IREN", "COIN", "HOOD",
    # semis
    "AMD", "AVGO", "MU", "MRVL", "ARM", "INTC",
    # high-beta tech
    "META", "GOOGL", "AMZN", "NFLX", "CRWD", "SNOW", "DDOG", "SHOP",
    # EV / energy / nuclear
    "RIVN", "LCID", "ENPH", "FSLR", "OKLO", "SMR", "VST", "CEG",
    # space / defense-adjacent
    "RKLB", "ASTS", "LUNR",
    # uranium
    "CCJ", "UEC",
    # quantum / spec
    "IONQ", "RGTI", "QBTS", "QUBT",
    # misc high-beta
    "HIMS", "ZIM",
]
BATCH_SIZE = 8
DAYS       = 730

# pre-registered bars (mirror of the prereg doc — doc is authoritative)
BAR_OOS_WR      = 42.0
BAR_OOS_N       = 100
BAR_CONSISTENCY = 6.0
ANCHOR_DRIFT    = 3.0

DEFAULT_RECIPE = {"avoid_hours": [], "avoid_days": [], "tp": 0.015, "sl": 0.010}


def summarize(trades, symbols):
    out = {}
    for sym in symbols:
        t = [x for x in trades if x.get("symbol") == sym]
        if not t:
            out[sym] = None
            continue
        n  = len(t)
        w  = sum(1 for x in t if x.get("won"))
        pn = sum(float(x.get("pnl_pct", 0)) for x in t) / n
        out[sym] = {"n": n, "wr": w * 100.0 / n, "avg": pn}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=0,
                    help="1-based batch number; 0 = all batches")
    args = ap.parse_args()

    try:
        import nexus_analyzer_1min_railway as bt
    except ImportError:
        print("nexus_analyzer_1min_railway.py not in cwd — fetch it first.")
        sys.exit(1)

    anchors = list(bt.TRUMP_THEME)          # 7 anchors keep regime gate live
    batches = [CANDIDATES[i:i + BATCH_SIZE]
               for i in range(0, len(CANDIDATES), BATCH_SIZE)]
    todo = [args.batch - 1] if args.batch else range(len(batches))

    league = {}
    anchor_track = {}
    for bi in todo:
        cand = batches[bi]
        print("=" * 74)
        print(f"BATCH {bi+1}/{len(batches)}: {' '.join(cand)}  "
              f"(+{len(anchors)} anchors)")
        # drive the engine: swap universe, inject default recipes
        bt.SYMBOLS = anchors + cand
        for sym in cand:
            bt.BERSERKER_RECIPES.setdefault(sym, dict(DEFAULT_RECIPE))
        earnings = {}
        try:
            earnings = bt.fetch_earnings_dates(bt.SYMBOLS)
        except Exception as e:
            print(f"  earnings fetch failed ({e}) — blackout off this batch")
        bars = bt.fetch_all_bars(DAYS)
        got = [s for s in cand if s in bars]
        missing = [s for s in cand if s not in bars]
        if missing:
            print(f"  no bars (skipped): {' '.join(missing)}")

        train = bt.replay_berserker(bars, earnings, validate_mode=False)
        oos   = bt.replay_berserker(bars, earnings, validate_mode=True)
        # NOTE: write_fingerprints deliberately never called.

        tr_s = summarize(train, anchors + got)
        oo_s = summarize(oos,   anchors + got)

        print(f"  anchors (calibration vs Sunday run):")
        for a in anchors:
            s = tr_s.get(a)
            anchor_track[a] = s
            print(f"    {a:6s} train "
                  + (f"WR {s['wr']:.1f}% n={s['n']}" if s else "n=0"))

        for sym in got:
            t, o = tr_s.get(sym), oo_s.get(sym)
            league[sym] = {"train": t, "oos": o}
            if not t or not o:
                print(f"  {sym:6s} INSUFFICIENT (train n={t['n'] if t else 0}, "
                      f"oos n={o['n'] if o else 0})")
                continue
            gap = abs(t["wr"] - o["wr"])
            bars_ok = {
                "oos_wr":  o["wr"] >= BAR_OOS_WR,
                "oos_avg": o["avg"] > 0,
                "oos_n":   o["n"] >= BAR_OOS_N,
                "consist": gap <= BAR_CONSISTENCY,
            }
            tag = "PASS" if all(bars_ok.values()) else \
                  "fail:" + ",".join(k for k, v in bars_ok.items() if not v)
            print(f"  {sym:6s} train WR {t['wr']:5.1f}%/{t['n']:5d} "
                  f"avg {t['avg']:+.3f}% | OOS WR {o['wr']:5.1f}%/{o['n']:4d} "
                  f"avg {o['avg']:+.3f}% | gap {gap:.1f} | {tag}")
        time.sleep(5)

    print("=" * 74)
    print("CENSUS LEAGUE TABLE (all batches this run)")
    passes = []
    for sym, r in sorted(league.items(),
                         key=lambda kv: -(kv[1]["oos"]["wr"] if kv[1]["oos"] else -1)):
        t, o = r["train"], r["oos"]
        if not t or not o:
            continue
        gap = abs(t["wr"] - o["wr"])
        ok = (o["wr"] >= BAR_OOS_WR and o["avg"] > 0
              and o["n"] >= BAR_OOS_N and gap <= BAR_CONSISTENCY)
        if ok:
            passes.append(sym)
        print(f"  {sym:6s} OOS WR {o['wr']:5.1f}% n={o['n']:4d} "
              f"avg {o['avg']:+.3f}% | train {t['wr']:5.1f}% | "
              f"{'★ PASS' if ok else ''}")
    print("=" * 74)
    print(f"PASSES: {' '.join(passes) if passes else 'none'}")
    print(f"Expected false positives at these bars across {len(CANDIDATES)} "
          f"symbols: ~1-2. Passing symbols are PAPER-ROTATION CANDIDATES "
          f"only, per BERSERKER_UNIVERSE_CENSUS_PREREG.md. No DB writes "
          f"occurred in this run.")


if __name__ == "__main__":
    main()
