#!/usr/bin/env python3
"""
flat_verdict.py V1.0 -- FLAT_1458 EXIT EXPERIMENT VERDICT
=========================================================
Pre-registered in real_trades_filters_and_carry.md section 4 (Fuzzy,
Oct 19 2026). Modeled on band_verdict.py / orb_verdict.py and reuses
band_verdict's bar fetcher and constants.

READ-ONLY. The database session is opened read-only and only SELECTs run.
It reads berserker_flat1458_shadow (written by Trading-bot main.py at the
14:58 decision) and fetches Alpaca IEX 1-min bars. It writes NOTHING. If
TELEGRAM_* is set it sends one summary message, as band_verdict.py does.

RULE under test (Trading-bot, FLAT_1458_ENABLED=1): at 14:58 CT flatten
every Berserker position; the green-and-rising carry is not applied.

SAMPLE: shadow rows with flag_on = TRUE and action = flat_carry_eligible
(the old rule would have carried), session_date strictly after
FLAT_DEPLOY_DAY. One row per (session, symbol).

MATCHED CONTROL (per row, deterministic, from bars): both arms share the
entry -> 14:58 leg, so the comparison is the overnight leg only.
  flatten outcome   = exit at the 14:58 mark (0 pp from the mark).
  old-rule outcome  = carry from the mark into the NEXT session and exit
    under the live bracket, levels anchored to the original entry:
    TP entry*(1+1.5%), SL entry*(1-1.0%) (SPCX 1.5%), 1.5% trail from the
    running peak (seeded at the mark) once above SL, else the 14:58 mark
    of the next session. A gap through TP or SL at the first bar fills at
    that bar's open. TP wins ambiguous bars (as in band_verdict).
    Carry is capped at one night (a re-carry on day 2 is not modeled).
  diff = old_rule_outcome - flatten_outcome, in pp of the 14:58 mark.
  Fees: one round trip in both arms, so they cancel in the diff.

GATES:
  FLOOR  >= 30 carry-eligible positions resolved. Below it: INTERIM.
  KILL   mean(diff) >= +0.30 pp with n >= 20 -> REVERT (checked first,
         from n = 20 on, even before the floor).
  A      mean(diff) <= 0 AND day-block bootstrap (2000 reps, resample
         sessions, fixed seed) P(mean <= 0) >= 0.80 -> KEEP.
  Otherwise (floor met, no kill, A not met) -> PARK (inconclusive).

Run (nexus-analyst console): FLAT_DEPLOY_DAY=2026-10-19 python3 flat_verdict.py
"""

import os
import sys
import random
import logging
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo

import psycopg2

from band_verdict import (fetch_minute_bars, sl_pct_for, send_alert,
                          TP_PCT, BASE_TRAIL, ALPACA_API_KEY)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FLAT] %(message)s",
                    force=True, datefmt="%H:%M:%S")
log = logging.getLogger("flat")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
CT = ZoneInfo("America/Chicago")

ELIGIBLE = "flat_carry_eligible"
MIN_N = 30
KILL_N = 20
KILL_DIFF = 0.30
GATE_A_PROB = 0.80
BOOT_REPS = 2000
BOOT_SEED = 20261019


# ---------------------------------------------------------------- pure parts

def dedup(rows, deploy_day):
    """rows: (session_date, symbol, entry, mark, action, flag_on). Keeps the
    first eligible flag-on row per (session, symbol) after deploy_day."""
    seen, out = set(), []
    for d, sym, entry, mark, action, flag_on in rows:
        if action != ELIGIBLE or not flag_on or d <= deploy_day:
            continue
        if not entry or not mark or float(entry) <= 0 or float(mark) <= 0:
            continue
        if (d, sym) in seen:
            continue
        seen.add((d, sym))
        out.append((d, sym, float(entry), float(mark)))
    return out


def simulate_carry(entry, mark, next_bars, sl_pct):
    """Old-rule overnight leg. next_bars: next session's bars (epoch, o, h,
    l, c) oldest-first, same CT date. Returns (diff_pp, reason) or None."""
    tp = entry * (1 + TP_PCT)
    sl = entry * (1 - sl_pct)
    peak = mark
    last = None
    first = True
    for ep, o, h, l, c in next_bars:
        t = datetime.fromtimestamp(ep, CT)
        if (t.hour == 14 and t.minute >= 58) or t.hour >= 15:
            break
        if first:
            first = False
            if o >= tp:
                return (o / mark - 1) * 100.0, "gap_tp"
            if o <= sl:
                return (o / mark - 1) * 100.0, "gap_sl"
        trail = peak * (1 - BASE_TRAIL)
        if h >= tp:
            return (tp / mark - 1) * 100.0, "tp"
        if trail > sl and l <= trail:
            return (trail / mark - 1) * 100.0, "trail"
        if l <= sl:
            return (sl / mark - 1) * 100.0, "sl"
        peak = max(peak, h)
        last = c
    if last is None:
        return None
    return (last / mark - 1) * 100.0, "eod"


def bootstrap_prob_le0(by_day, reps=BOOT_REPS, seed=BOOT_SEED):
    """by_day: session -> [diff]. P(mean diff <= 0), day-block. Deterministic."""
    days = sorted(by_day)
    if not days:
        return 0.0
    rng = random.Random(seed)
    hits = 0
    for _ in range(reps):
        s = n = 0
        for _k in range(len(days)):
            v = by_day[days[rng.randrange(len(days))]]
            s += sum(v)
            n += len(v)
        if n and s / n <= 0:
            hits += 1
    return hits / reps


def decide(n, mean, prob):
    if n >= KILL_N and mean is not None and mean >= KILL_DIFF:
        return "REVERT"
    if n < MIN_N:
        return "INTERIM"
    if mean is not None and mean <= 0 and prob >= GATE_A_PROB:
        return "KEEP"
    return "PARK"


def next_session_bars(bars, d):
    """Bars of the first CT date after d that has any bars."""
    later = sorted({datetime.fromtimestamp(b[0], CT).date() for b in bars} - {d})
    later = [x for x in later if x > d]
    if not later:
        return []
    nd = later[0]
    return [b for b in bars if datetime.fromtimestamp(b[0], CT).date() == nd]


# ---------------------------------------------------------------- I/O parts

def q(sql, params=()):
    """Read-only session: the server rejects any write on this connection."""
    c = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    c.set_session(readonly=True, autocommit=True)
    with c.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    c.close()
    return rows


def main():
    if not DATABASE_URL or not ALPACA_API_KEY:
        log.error("Missing DATABASE_URL or Alpaca keys")
        sys.exit(1)
    raw = os.environ.get("FLAT_DEPLOY_DAY", "").strip()
    if not raw:
        log.error("Set FLAT_DEPLOY_DAY=YYYY-MM-DD (the CT date FLAT_1458 went live)")
        sys.exit(1)
    deploy_day = date.fromisoformat(raw)
    today = datetime.now(CT).date()
    log.info("=" * 64)
    log.info("flat_verdict.py V1.0 | FLAT_1458 | READ-ONLY | deploy %s (excluded)", deploy_day)
    log.info("=" * 64)

    exists = q("SELECT to_regclass('public.berserker_flat1458_shadow') IS NOT NULL")[0][0]
    if not exists:
        log.info("No shadow table yet. VERDICT: INTERIM")
        return
    rows = q("""SELECT session_date, symbol, entry_price, mark_price, action, flag_on
                FROM berserker_flat1458_shadow WHERE session_date > %s ORDER BY ts""",
             (deploy_day,))
    sample = [r for r in dedup(rows, deploy_day) if r[0] < today]   # need a next session
    log.info("Shadow rows %d | carry-eligible sessions-symbols %d", len(rows), len(sample))

    bars = {}
    by_day, resolved, unresolved, reasons = {}, [], 0, {}
    for d, sym, entry, mark in sample:
        if sym not in bars:
            start = int(datetime(deploy_day.year, deploy_day.month, deploy_day.day,
                                 tzinfo=CT).timestamp())
            try:
                bars[sym] = fetch_minute_bars(sym, start, int(datetime.now(timezone.utc).timestamp()))
            except Exception as e:
                log.info("  bars %s: FETCH FAILED (%s)", sym, e)
                bars[sym] = []
        res = simulate_carry(entry, mark, next_session_bars(bars[sym], d), sl_pct_for(sym))
        if res is None:
            unresolved += 1
            continue
        resolved.append(res[0])
        by_day.setdefault(d, []).append(res[0])
        reasons[res[1]] = reasons.get(res[1], 0) + 1
        log.info("  %s %-6s entry %.2f mark %.2f -> carry %+.3f pp (%s)",
                 d, sym, entry, mark, res[0], res[1])

    n = len(resolved)
    mean = sum(resolved) / n if n else None
    prob = bootstrap_prob_le0(by_day)
    verdict = decide(n, mean, prob)
    log.info("Resolved %d | unresolved %d | exits %s", n, unresolved, reasons)
    log.info("mean(old_rule - flatten) = %s pp | P(mean <= 0) = %.3f",
             "n/a" if mean is None else f"{mean:+.3f}", prob)
    log.info("Floor %d/%d | kill: >= +%.2f pp with n >= %d", n, MIN_N, KILL_DIFF, KILL_N)
    log.info("=" * 64)
    log.info("VERDICT: %s", verdict)
    note = {
        "INTERIM": f"{n}/{MIN_N} carry-eligible positions. Nothing is decided yet.",
        "REVERT": "Carry beat flattening by >= +0.30 pp. Turn FLAT_1458 off (Matthew's OK).",
        "KEEP": "Flattening does not hurt (Gate A). FLAT_1458 may stay.",
        "PARK": "Floor met, no kill, Gate A not met. Inconclusive.",
    }[verdict]
    log.info(note)
    log.info("=" * 64)
    if verdict != "INTERIM":
        send_alert(f"FLAT_1458 VERDICT: {verdict}\nn={n} mean diff "
                   f"{'n/a' if mean is None else f'{mean:+.3f}'} pp P(<=0)={prob:.2f}\n{note}")


if __name__ == "__main__":
    main()
