#!/usr/bin/env python3
"""
opening_delay_verdict.py V1.0 -- V10.61 OPENING-DELAY EXPERIMENT VERDICT
=========================================================================
Pre-registered Jul 29 2026 (main.py V10.61 header). The 15-session gate
window has elapsed. This script computes the two registered gates and
prints the binding verdict. READ-ONLY: reads berserker_shadow_signals +
berserker_trade_fingerprints, fetches bars from Alpaca, writes NOTHING.

REGISTERED GATES (verbatim from V10.61):
  "GATE (15 sessions): post-9:00 WR within 5pts of baseline AND
   shadow-logged signals' forward moves consistent with the measured
   in-window deficit. Fail either -> revert."
  Registered baseline: post-9:00 live 45.2% WR (pre-experiment tape).
  Registered in-window deficit: 12.2% WR / -0.77%/trade (n=41).

OPERATIONALIZATION (declared here, BEFORE running -- binds):
  GATE A (live health): live post-9:00 WR over the experiment window
    must be >= 40.2% (baseline 45.2 - 5.0). Live rows only
    (is_paper=FALSE, no bt_), entered hour_cdt >= 9, Jul 29 -> today.
  GATE B (wall validation): blocked shadow signals, deduped to the FIRST
    signal per symbol per session (the would-be entry; live would have
    held + cooled down), simulated through the standard live bracket
    from the logged signal price on real 1-min bars:
      TP +1.5% / SL -1.0% (recipe defaults), else EOD 14:58 close.
      Ambiguity rule (anti-validation conservative): a bar touching BOTH
      TP and SL counts as a WIN -- pushes sim WR UP, making the wall
      HARDER to validate, never easier.
    Gate B passes if simulated WR < 40.2% (the refused trades really
    were worse than the baseline band the wall was built to protect).
  VERDICT: KEEP if A and B pass. REVERT if either fails.
  Sample floors: >= 15 distinct sessions with shadow rows AND >= 30
  live post-9:00 trades; below either floor -> EXTEND (no verdict).

Run (nexus-analyst console): python3 opening_delay_verdict.py
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [OD-VERDICT] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("odv")

DATABASE_URL   = os.environ.get("DATABASE_URL", "")
ALPACA_API_KEY = (os.environ.get("ALPACA_API_KEY") or
                  os.environ.get("ALPACA_PHASE4_API_KEY", ""))
ALPACA_SECRET  = (os.environ.get("ALPACA_SECRET_KEY") or
                  os.environ.get("ALPACA_PHASE4_SECRET_KEY", ""))
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

CT = ZoneInfo("America/Chicago")

EXPERIMENT_START = int(datetime(2026, 7, 29, 0, 0, tzinfo=CT).timestamp())
BASELINE_WR      = 45.2      # registered pre-experiment post-9:00 WR (%)
GATE_BAND        = 5.0       # registered "within 5pts"
GATE_A_FLOOR     = BASELINE_WR - GATE_BAND      # 40.2%
TP_PCT           = 0.015     # live recipe defaults (all 12 symbols tp=1.5%)
SL_PCT           = 0.010     # live default sl (SPCX 1.5% -- declared: use 1.0%
                             # uniformly; a wider SL only LOWERS sim WR, and
                             # SPCX-specific handling would be post-hoc)
MIN_SESSIONS     = 15
MIN_LIVE_TRADES  = 30


def send_alert(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except Exception:
        pass


def q(sql, params=()):
    c = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    c.close()
    return rows


def fetch_minute_bars(symbol, start_ep, end_ep):
    """1-min adjusted IEX bars, oldest-first: [(epoch, o, h, l, c)]."""
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY,
               "APCA-API-SECRET-KEY": ALPACA_SECRET}
    out, token = [], None
    while True:
        params = {"timeframe": "1Min", "adjustment": "all", "feed": "iex",
                  "limit": 10000,
                  "start": datetime.fromtimestamp(start_ep, timezone.utc)
                                   .strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "end":   datetime.fromtimestamp(end_ep, timezone.utc)
                                   .strftime("%Y-%m-%dT%H:%M:%SZ")}
        if token:
            params["page_token"] = token
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        for b in j.get("bars") or []:
            ep = int(datetime.fromisoformat(b["t"].replace("Z", "+00:00")).timestamp())
            out.append((ep, float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])))
        token = j.get("next_page_token")
        if not token:
            break
    return out


def simulate_bracket(sig_ep, sig_px, bars):
    """Walk forward from the signal through the live bracket.
    Returns (won, pnl_pct) or None if no usable bars.
    Ambiguous bar (touches both) counts WIN -- declared anti-validation."""
    tp = sig_px * (1 + TP_PCT)
    sl = sig_px * (1 - SL_PCT)
    sig_day = datetime.fromtimestamp(sig_ep, CT).date()
    last_close = None
    for ep, o, h, l, c in bars:
        if ep < sig_ep:
            continue
        d = datetime.fromtimestamp(ep, CT)
        if d.date() != sig_day:
            break
        if d.hour == 14 and d.minute >= 58 or d.hour >= 15:
            break
        hit_tp = h >= tp
        hit_sl = l <= sl
        if hit_tp and hit_sl:
            return True, TP_PCT * 100          # ambiguous -> WIN (declared)
        if hit_tp:
            return True, TP_PCT * 100
        if hit_sl:
            return False, -SL_PCT * 100
        last_close = c
    if last_close is None:
        return None
    pnl = (last_close / sig_px - 1) * 100
    return pnl > 0, pnl


def main():
    if not DATABASE_URL or not ALPACA_API_KEY:
        log.error("Missing DATABASE_URL or Alpaca keys")
        sys.exit(1)

    log.info("=" * 60)
    log.info("V10.61 OPENING-DELAY VERDICT | registered Jul 29 2026")
    log.info(f"Gate A floor: {GATE_A_FLOOR}% | Gate B: sim WR < {GATE_A_FLOOR}%")
    log.info("Read-only. Verdict binds per registration.")
    log.info("=" * 60)

    # ── GATE A: live post-9:00 health over the experiment window ────────────
    rows = q("""
        SELECT COUNT(*), COALESCE(SUM(CASE WHEN won THEN 1 ELSE 0 END),0)
        FROM berserker_trade_fingerprints
        WHERE won IS NOT NULL AND is_paper = FALSE
          AND trade_id NOT LIKE 'bt_%%'
          AND entry_ts >= %s AND hour_cdt >= 9
    """, (EXPERIMENT_START,))
    n_live, n_wins = int(rows[0][0]), int(rows[0][1])
    live_wr = round(100.0 * n_wins / n_live, 1) if n_live else 0.0
    log.info(f"GATE A: live post-9:00 trades n={n_live} | WR={live_wr}% "
             f"(floor {GATE_A_FLOOR}%, baseline {BASELINE_WR}%)")
    gate_a = live_wr >= GATE_A_FLOOR

    # ── GATE B: blocked-signal counterfactual ───────────────────────────────
    sig_rows = q("""
        SELECT ts, symbol, price FROM berserker_shadow_signals
        WHERE reason = 'opening-delay' AND ts >= %s
        ORDER BY ts
    """, (EXPERIMENT_START,))
    firsts, seen = [], set()
    for ts, sym, px in sig_rows:
        day = datetime.fromtimestamp(int(ts), CT).date()
        key = (sym, day)
        if key in seen or not px or px <= 0:
            continue
        seen.add(key)
        firsts.append((int(ts), sym, float(px)))
    sessions = len({d for _, d in seen})
    log.info(f"Shadow rows: {len(sig_rows)} raw -> {len(firsts)} first-per-symbol-"
             f"per-session across {sessions} sessions")

    if sessions < MIN_SESSIONS or n_live < MIN_LIVE_TRADES:
        log.info("=" * 60)
        log.info(f"VERDICT: EXTEND -- sample floors not met "
                 f"(sessions {sessions}/{MIN_SESSIONS}, live n {n_live}/{MIN_LIVE_TRADES})")
        log.info("=" * 60)
        send_alert(f"🕰️ OPENING-DELAY VERDICT: EXTEND\n"
                   f"Sessions {sessions}/{MIN_SESSIONS} | live n {n_live}/{MIN_LIVE_TRADES}\n"
                   f"Gates not evaluable yet -- experiment continues.")
        return

    # Fetch bars once per symbol across the whole window
    by_sym = {}
    for ts, sym, px in firsts:
        by_sym.setdefault(sym, []).append((ts, px))
    bars_cache = {}
    end_ep = int(datetime.now(timezone.utc).timestamp())
    for sym in by_sym:
        try:
            bars_cache[sym] = fetch_minute_bars(sym, EXPERIMENT_START, end_ep)
            log.info(f"  bars {sym}: {len(bars_cache[sym])}")
        except Exception as e:
            log.info(f"  bars {sym}: FETCH FAILED ({e}) -- its signals excluded")
            bars_cache[sym] = []

    sims, wins, pnls = 0, 0, []
    for ts, sym, px in firsts:
        res = simulate_bracket(ts, px, bars_cache.get(sym, []))
        if res is None:
            continue
        won, pnl = res
        sims += 1
        wins += 1 if won else 0
        pnls.append(pnl)
    sim_wr  = round(100.0 * wins / sims, 1) if sims else 0.0
    sim_avg = round(sum(pnls) / len(pnls), 3) if pnls else 0.0
    log.info(f"GATE B: simulated blocked entries n={sims} | WR={sim_wr}% | "
             f"avg={sim_avg}%/trade (registered deficit was 12.2% / -0.77%)")
    gate_b = sims >= 10 and sim_wr < GATE_A_FLOOR

    verdict = "KEEP" if (gate_a and gate_b) else "REVERT"
    log.info("=" * 60)
    log.info(f"GATE A (live post-9 WR >= {GATE_A_FLOOR}%): {'PASS' if gate_a else 'FAIL'} ({live_wr}%)")
    log.info(f"GATE B (blocked sim WR < {GATE_A_FLOOR}%):  {'PASS' if gate_b else 'FAIL'} ({sim_wr}%, n={sims})")
    log.info(f"VERDICT: {verdict}")
    log.info("=" * 60)
    send_alert(
        f"🕰️ OPENING-DELAY VERDICT (V10.61, registered Jul 29)\n"
        f"──────────────────\n"
        f"Gate A: live post-9 WR {live_wr}% (n={n_live}) vs floor {GATE_A_FLOOR}% "
        f"{'✅' if gate_a else '❌'}\n"
        f"Gate B: blocked-signal sim WR {sim_wr}% avg {sim_avg}% (n={sims}) "
        f"{'✅' if gate_b else '❌'}\n"
        f"Sessions: {sessions}\n"
        f"──────────────────\n"
        f"VERDICT: {verdict}"
        + ("" if verdict == "KEEP" else "\nAction: revert V10.61 wall in next main.py version")
    )


if __name__ == "__main__":
    main()
