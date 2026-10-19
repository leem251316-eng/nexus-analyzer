#!/usr/bin/env python3
"""
orb_verdict.py V1.0 -- ORB_CONFIRM ENTRY EXPERIMENT VERDICT
===========================================================
Pre-registered in next_wr_experiment_prereg.md (Fuzzy, Oct 19 2026).
Cloned from band_verdict.py and uses its minute-bar exit engine
(simulate_bracket, fetch_minute_bars, sl_pct_for, 0.05 pp fee).

READ-ONLY. The database session is opened read-only and only SELECTs run.
It reads berserker_shadow_signals and berserker_trade_fingerprints and
fetches Alpaca IEX bars. It writes NOTHING. If TELEGRAM_* is set it sends
one summary message, as band_verdict.py does.

Coexistence: Matthew KEPT the 09:05-09:30 BAND_0905 wall (Oct 19), so the
prereg's "revert BAND first" prerequisite is void. ORB runs after both
walls; every signal it saw reached it past them, so both arms come from
the same post-wall population.

RULE under test (Trading-bot, ORB_CONFIRM_ENABLED=1): a Berserker buy
signal passes iff last_print > high of the 08:30-08:59 CT 1-min bars.
Shadow reasons: orb_block (skipped), orb_pass (went on to execute_trade),
orb_nodata (no OR high; traded as today; EXCLUDED here).

DEPLOY DAY: set ORB_DEPLOY_DAY=YYYY-MM-DD (CT) before running. Session 1 is
the first NYSE session after it; the deploy day is excluded. Optional
BEAR_PC_DEPLOY_DAY=YYYY-MM-DD records when the prior-close bear rail went
live, for the confound split.

SAMPLE: one signal per (session, symbol), the FIRST orb row of the
session (block or pass). If that first row is orb_nodata the
symbol-session is excluded. Only sessions 1..30 are read.

WINDOW AND FLOORS:
  Base window 20 sessions; extends to at most 30 only if floors are unmet.
  Floors: >= 15 sessions with valid shadow logging (>= 1 orb row),
  >= 40 blocked, >= 15 passed.
  Before session 20: INTERIM (nothing is decided).
  Floors unmet at 20..29: EXTEND. Floors unmet at 30: PARK.

ENGINE: every first signal, passed or blocked, is replayed from its own
price and time with band_verdict.simulate_bracket (TP +1.5 / SL -1.0,
SPCX -1.5, 1.5% trail after 20 min, EOD 14:58, TP wins ambiguous bars).
Both arms carry the same fill bias. Net = mean - 0.05 pp.

GATES:
  A  net(passed) - net(all signals) >= +0.10 pp/trade AND day-block
     bootstrap (2000 reps, resample sessions, fixed seed) P(diff > 0)
     >= 0.80. "All" = passed + blocked from the same sessions.
  B  net(blocked) <= net(passed) - 0.15 pp AND net(blocked) <= -0.10.
  C  Live realized P&L of passed trades (COALESCE(pnl_pct_broker,
     pnl_pct)) vs the sim of those same trades from their live entry,
     |mean(realized - sim net)| <= 0.30 pp. No matched trade -> C not
     evaluable, treated as FAIL.
  RIDGE  among symbols with >= 3 blocked signals, >= 60% have blocked
     GROSS sim < 0 (no fee, as in BAND). No qualifying symbol -> FAIL.

VERDICT (order): INTERIM / EXTEND / PARK on window and floors;
  C fails -> PARK (sim untrusted, so A and B are not read);
  A fails or B fails -> REVERT;
  ridge fails -> PARK;
  else KEEP.

KILL SWITCHES (prereg section 7, reported every run; any FIRED -> turn
ORB_CONFIRM off). 1: >= 3 window sessions with no orb rows (proxy for a
failed shadow writer). 2: >= 10 sessions and >= 8 passed live trades and
live realized mean <= -1.00%. 3: pass rate < 5% over any 5 consecutive
sessions. 4: orb_nodata > 50% of symbol-sessions over any 3 consecutive
sessions.

Run (nexus-analyst console): ORB_DEPLOY_DAY=2026-10-20 python3 orb_verdict.py
"""

import os
import sys
import random
import logging
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo

import requests
import psycopg2

from band_verdict import (simulate_bracket, fetch_minute_bars, sl_pct_for,
                          send_alert, FEE_PCT, ALPACA_API_KEY, ALPACA_SECRET)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ORB] %(message)s", force=True,
                    datefmt="%H:%M:%S")
log = logging.getLogger("orb")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
CT = ZoneInfo("America/Chicago")
NY = ZoneInfo("America/New_York")

PASS, BLOCK, NODATA = "orb_pass", "orb_block", "orb_nodata"
REASONS = (PASS, BLOCK, NODATA)

BASE_SESSIONS = 20
MAX_SESSIONS = 30
MIN_VALID_SESSIONS = 15
MIN_BLOCKED = 40
MIN_PASSED = 15

GATE_A_DIFF = 0.10
GATE_A_PROB = 0.80
BOOT_REPS = 2000
BOOT_SEED = 20261019
GATE_B_GAP = 0.15
GATE_B_MAX = -0.10
GATE_C_MAX = 0.30
RIDGE_MIN_N = 3
RIDGE_MIN_FRAC = 0.60

KS1_EMPTY_SESSIONS = 3
KS2_MIN_SESSIONS, KS2_MIN_TRADES, KS2_MAX_MEAN = 10, 8, -1.00
KS3_RUN, KS3_MIN_RATE = 5, 0.05
KS4_RUN, KS4_MAX_NODATA = 3, 0.50

BEAR_R = -0.50   # informational split: SPY vs prior close at the signal


# ---------------------------------------------------------------- pure parts

def net(pnls):
    if not pnls:
        return None
    return sum(pnls) / len(pnls) - FEE_PCT


def gross(pnls):
    if not pnls:
        return None
    return sum(pnls) / len(pnls)


def session_of(ts):
    return datetime.fromtimestamp(int(ts), CT).date()


def first_signals(rows, sessions_in_window, excluded=()):
    """rows: (ts, symbol, price, reason) sorted by ts. Returns
    (firsts, nodata_firsts, rows_per_session).
    firsts: list of (ts, sym, px, reason, session) for the first orb row per
    (session, symbol) when it is pass or block. nodata_firsts counts
    symbol-sessions whose first row was orb_nodata, per session."""
    seen, firsts, nodata, per_sess = set(), [], {}, {}
    allowed = set(sessions_in_window)
    for ts, sym, px, reason in rows:
        if reason not in REASONS:
            continue
        d = session_of(ts)
        if d in excluded or d not in allowed:
            continue
        per_sess[d] = per_sess.get(d, 0) + 1
        key = (d, sym)
        if key in seen:
            continue
        if not px or float(px) <= 0:
            continue
        seen.add(key)
        if reason == NODATA:
            nodata[d] = nodata.get(d, 0) + 1
            continue
        firsts.append((int(ts), sym, float(px), reason, d))
    return firsts, nodata, per_sess


def window_status(n_sessions, n_valid, n_blocked, n_passed):
    """INTERIM / EXTEND / PARK / READY."""
    floors = (n_valid >= MIN_VALID_SESSIONS and n_blocked >= MIN_BLOCKED
              and n_passed >= MIN_PASSED)
    if n_sessions < BASE_SESSIONS:
        return "INTERIM", floors
    if floors:
        return "READY", floors
    if n_sessions < MAX_SESSIONS:
        return "EXTEND", floors
    return "PARK", floors


def bootstrap_prob(sims, reps=BOOT_REPS, seed=BOOT_SEED):
    """sims: list of (session, reason, pnl). Day-block bootstrap of
    mean(passed) - mean(all). Returns P(diff > 0). Deterministic."""
    by_day = {}
    for d, reason, pnl in sims:
        by_day.setdefault(d, []).append((reason, pnl))
    days = sorted(by_day)
    if not days:
        return 0.0
    rng = random.Random(seed)
    wins = 0
    for _ in range(reps):
        p_sum = p_n = a_sum = a_n = 0
        for _k in range(len(days)):
            for reason, pnl in by_day[days[rng.randrange(len(days))]]:
                a_sum += pnl
                a_n += 1
                if reason == PASS:
                    p_sum += pnl
                    p_n += 1
        if p_n == 0 or a_n == 0:
            continue                      # counted as not > 0 (conservative)
        if p_sum / p_n - a_sum / a_n > 0:
            wins += 1
    return wins / reps if reps else 0.0


def gate_a(passed, all_pnls, prob):
    np_, na = net(passed), net(all_pnls)
    if np_ is None or na is None:
        return False, None
    diff = np_ - na
    return (diff >= GATE_A_DIFF and prob >= GATE_A_PROB), diff


def gate_b(passed, blocked):
    np_, nb = net(passed), net(blocked)
    if np_ is None or nb is None:
        return False
    return nb <= np_ - GATE_B_GAP and nb <= GATE_B_MAX


def gate_c(pairs):
    """pairs: (realized_pct, sim_gross_pct). Returns (pass, mean_diff, n).
    The sim is taken net of the fee so both sides are after costs."""
    if not pairs:
        return False, None, 0
    diffs = [r - (s - FEE_PCT) for r, s in pairs]
    m = sum(diffs) / len(diffs)
    return abs(m) <= GATE_C_MAX, m, len(pairs)


def ridge(blocked_counts, blocked_by_sym):
    """blocked_counts: sym -> distinct blocked signals. blocked_by_sym:
    sym -> gross sim pnls. Symbols with >= 3 blocked signals qualify; one
    with no resolved sim does not count as negative (as in band_verdict).
    Returns (pass, rows, n_negative); rows are (sym, n_sig, n_sims, gross)."""
    rows, neg = [], 0
    for sym in sorted(blocked_counts):
        n_sig = blocked_counts[sym]
        if n_sig < RIDGE_MIN_N:
            continue
        p = blocked_by_sym.get(sym, [])
        g = (sum(p) / len(p)) if p else None
        is_neg = g is not None and g < 0
        rows.append((sym, n_sig, len(p), g))
        neg += 1 if is_neg else 0
    ok = bool(rows) and neg / len(rows) >= RIDGE_MIN_FRAC
    return ok, rows, neg


def decide(status, c_ok, a_ok, b_ok, ridge_ok):
    if status != "READY":
        return status
    if not c_ok:
        return "PARK"
    if not a_ok or not b_ok:
        return "REVERT"
    if not ridge_ok:
        return "PARK"
    return "KEEP"


def match_live(passed_firsts, live_rows):
    """passed_firsts: (ts, sym, px, reason, session). live_rows:
    (entry_ts, symbol, entry_price, realized). The earliest live entry of
    the same symbol, same session, at or after the pass row. Each live row
    is used once. Returns list of (first, live_row)."""
    used, out = set(), []
    live_sorted = sorted(live_rows, key=lambda r: r[0])
    for f in sorted(passed_firsts, key=lambda x: x[0]):
        ts, sym, _px, _r, d = f
        for i, row in enumerate(live_sorted):
            if i in used:
                continue
            ets, lsym = row[0], row[1]
            if lsym != sym or ets < ts or session_of(ets) != d:
                continue
            used.add(i)
            out.append((f, row))
            break
    return out


def kill_switches(sessions, per_sess_rows, firsts, nodata, live_realized):
    """Returns list of (name, fired, detail). sessions: ordered window."""
    out = []
    empty = [d for d in sessions if per_sess_rows.get(d, 0) == 0]
    out.append(("KS1 sessions with no orb rows", len(empty) >= KS1_EMPTY_SESSIONS,
                f"{len(empty)} (fires at {KS1_EMPTY_SESSIONS})"))
    m = (sum(live_realized) / len(live_realized)) if live_realized else None
    fired2 = (len(sessions) >= KS2_MIN_SESSIONS and len(live_realized) >= KS2_MIN_TRADES
              and m is not None and m <= KS2_MAX_MEAN)
    out.append(("KS2 live passed mean <= -1.00%", fired2,
                f"n={len(live_realized)} mean={'n/a' if m is None else f'{m:+.3f}'}"))
    per = {d: [0, 0] for d in sessions}           # pass, block
    for _ts, _s, _p, reason, d in firsts:
        if d in per:
            per[d][0 if reason == PASS else 1] += 1
    fired3, worst3 = False, None
    for i in range(0, max(0, len(sessions) - KS3_RUN + 1)):
        run = sessions[i:i + KS3_RUN]
        p = sum(per[d][0] for d in run)
        t = p + sum(per[d][1] for d in run)
        if t:
            rate = p / t
            worst3 = rate if worst3 is None else min(worst3, rate)
            if rate < KS3_MIN_RATE:
                fired3 = True
    out.append(("KS3 pass rate < 5% over 5 sessions", fired3,
                f"worst={'n/a' if worst3 is None else f'{worst3:.1%}'}"))
    fired4, worst4 = False, None
    for i in range(0, max(0, len(sessions) - KS4_RUN + 1)):
        run = sessions[i:i + KS4_RUN]
        nd = sum(nodata.get(d, 0) for d in run)
        tot = nd + sum(per[d][0] + per[d][1] for d in run)
        if tot:
            share = nd / tot
            worst4 = share if worst4 is None else max(worst4, share)
            if share > KS4_MAX_NODATA:
                fired4 = True
    out.append(("KS4 OR missing > 50% over 3 sessions", fired4,
                f"worst={'n/a' if worst4 is None else f'{worst4:.1%}'}"))
    return out


def trading_sessions_after(deploy_day, today, calendar_days=None):
    """NYSE sessions strictly after deploy_day and up to today (the last one
    only if it is not today, i.e. closed). calendar_days: iterable of dates
    from Alpaca's calendar; falls back to weekdays."""
    if calendar_days is None:
        days, d = [], deploy_day + timedelta(days=1)
        while d < today:
            if d.weekday() < 5:
                days.append(d)
            d += timedelta(days=1)
        return days
    return sorted(d for d in calendar_days if deploy_day < d < today)


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


def fetch_calendar(start, end):
    for host in ("https://api.alpaca.markets", "https://paper-api.alpaca.markets"):
        try:
            r = requests.get(f"{host}/v2/calendar",
                             headers={"APCA-API-KEY-ID": ALPACA_API_KEY,
                                      "APCA-API-SECRET-KEY": ALPACA_SECRET},
                             params={"start": start.isoformat(), "end": end.isoformat()},
                             timeout=20)
            if r.status_code == 200:
                return [date.fromisoformat(x["date"]) for x in r.json()]
        except Exception:
            continue
    return None


def fetch_daily_closes(symbol, start_ep, end_ep):
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}
    params = {"timeframe": "1Day", "adjustment": "all", "feed": "iex", "limit": 1000,
              "start": datetime.fromtimestamp(start_ep, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "end": datetime.fromtimestamp(end_ep, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    out = {}
    for b in r.json().get("bars") or []:
        d = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(NY).date()
        out[d] = float(b["c"])
    return out


def spy_r_at(ts, spy_bars, closes):
    """SPY % vs prior session close at the last bar at or before ts."""
    d = session_of(ts)
    prior = [k for k in closes if k < d]
    if not prior:
        return None
    pc = closes[max(prior)]
    last = None
    for ep, _o, _h, _l, c in spy_bars:
        if ep > ts:
            break
        if session_of(ep) == d:
            last = c
    if last is None or pc <= 0:
        return None
    return (last / pc - 1.0) * 100.0


def _fmt(x, nd=4):
    return "n/a" if x is None else f"{x:+.{nd}f}"


def main():
    if not DATABASE_URL or not ALPACA_API_KEY:
        log.error("Missing DATABASE_URL or Alpaca keys")
        sys.exit(1)
    raw = os.environ.get("ORB_DEPLOY_DAY", "").strip()
    if not raw:
        log.error("Set ORB_DEPLOY_DAY=YYYY-MM-DD (the CT date ORB_CONFIRM went live)")
        sys.exit(1)
    deploy_day = date.fromisoformat(raw)
    bear_raw = os.environ.get("BEAR_PC_DEPLOY_DAY", "").strip()
    bear_day = date.fromisoformat(bear_raw) if bear_raw else None
    today = datetime.now(CT).date()

    cal = fetch_calendar(deploy_day, today)
    all_sessions = trading_sessions_after(deploy_day, today, cal)
    window = all_sessions[:MAX_SESSIONS]
    deploy_ep = int(datetime(deploy_day.year, deploy_day.month, deploy_day.day,
                             tzinfo=CT).timestamp())
    end_ep = int(datetime.now(timezone.utc).timestamp())

    log.info("=" * 64)
    log.info("orb_verdict.py V1.0 | ORB_CONFIRM | READ-ONLY")
    log.info("Deploy %s (excluded) | sessions elapsed %d (calendar: %s) | window %d..%d",
             deploy_day, len(all_sessions), "alpaca" if cal is not None else "weekdays",
             BASE_SESSIONS, MAX_SESSIONS)
    log.info("BAND_0905 wall KEPT; ORB sits after both walls.")
    log.info("=" * 64)

    rows = q("""SELECT ts, symbol, price, reason FROM berserker_shadow_signals
                WHERE reason IN (%s, %s, %s) AND ts >= %s ORDER BY ts""",
             (PASS, BLOCK, NODATA, deploy_ep))
    firsts, nodata, per_sess = first_signals(rows, window, {deploy_day})
    valid = [d for d in window if per_sess.get(d, 0) > 0]
    passed_f = [f for f in firsts if f[3] == PASS]
    blocked_f = [f for f in firsts if f[3] == BLOCK]
    log.info("Shadow rows %d | first signals: passed %d, blocked %d | "
             "nodata symbol-sessions %d (excluded)",
             len(rows), len(passed_f), len(blocked_f), sum(nodata.values()))
    status, floors = window_status(len(window), len(valid), len(blocked_f), len(passed_f))
    log.info("Floors: valid sessions %d/%d | blocked %d/%d | passed %d/%d -> %s",
             len(valid), MIN_VALID_SESSIONS, len(blocked_f), MIN_BLOCKED,
             len(passed_f), MIN_PASSED, "MET" if floors else "UNMET")

    # live rows (for Gate C and kill switch 2)
    cols = {r[0] for r in q("SELECT column_name FROM information_schema.columns "
                            "WHERE table_name = 'berserker_trade_fingerprints'")}
    pnl_sql = ("COALESCE(pnl_pct_broker, pnl_pct)" if "pnl_pct_broker" in cols
               else "pnl_pct")
    live = q(f"""SELECT entry_ts, symbol, entry_price, {pnl_sql}
                 FROM berserker_trade_fingerprints
                 WHERE won IS NOT NULL AND is_paper = FALSE
                   AND trade_id NOT LIKE 'bt_%%' AND entry_ts >= %s
                   AND entry_price > 0""", (deploy_ep,))
    live = [(int(a), s, float(p), float(r)) for a, s, p, r in live
            if r is not None and session_of(a) in set(window)]
    matched = match_live(passed_f, live)

    # bars
    syms = sorted({f[1] for f in firsts} | {m[1][1] for m in matched})
    bars = {}
    for s in syms:
        try:
            bars[s] = fetch_minute_bars(s, deploy_ep, end_ep)
        except Exception as e:
            log.info("  bars %s: FETCH FAILED (%s)", s, e)
            bars[s] = []

    sims = []          # (session, reason, gross pnl)
    by_sym_block = {}
    unresolved = 0
    for ts, sym, px, reason, d in firsts:
        res = simulate_bracket(ts, px, bars.get(sym, []), sl_pct_for(sym))
        if res is None:
            unresolved += 1
            continue
        sims.append((d, reason, res[1]))
        if reason == BLOCK:
            by_sym_block.setdefault(sym, []).append(res[1])
    p_p = [p for _d, r, p in sims if r == PASS]
    p_b = [p for _d, r, p in sims if r == BLOCK]
    p_all = [p for _d, _r, p in sims]
    prob = bootstrap_prob(sims)
    a_ok, a_diff = gate_a(p_p, p_all, prob)
    b_ok = gate_b(p_p, p_b)
    pairs = []
    for f, row in matched:
        res = simulate_bracket(row[0], row[2], bars.get(row[1], []), sl_pct_for(row[1]))
        if res is not None:
            pairs.append((row[3], res[1]))
    c_ok, c_diff, c_n = gate_c(pairs)
    blocked_counts = {}
    for f in blocked_f:
        blocked_counts[f[1]] = blocked_counts.get(f[1], 0) + 1
    r_ok, r_rows, r_neg = ridge(blocked_counts, by_sym_block)

    log.info("Sims resolved %d | unresolved %d", len(sims), unresolved)
    log.info("PASSED  n=%d net %s | BLOCKED n=%d net %s | ALL n=%d net %s",
             len(p_p), _fmt(net(p_p)), len(p_b), _fmt(net(p_b)), len(p_all), _fmt(net(p_all)))
    log.info("GATE A: diff %s pp (need >= +%.2f), P(diff>0)=%.3f (need >= %.2f): %s",
             _fmt(a_diff), GATE_A_DIFF, prob, GATE_A_PROB, "PASS" if a_ok else "FAIL")
    log.info("GATE B: blocked <= passed - %.2f and <= %.2f: %s",
             GATE_B_GAP, GATE_B_MAX, "PASS" if b_ok else "FAIL")
    log.info("GATE C: matched live passed n=%d | mean(realized - sim net) %s (need |x| <= %.2f): %s",
             c_n, _fmt(c_diff), GATE_C_MAX, "PASS" if c_ok else "FAIL")
    log.info("RIDGE: %d/%d qualifying names negative gross (need >= %.0f%%): %s",
             r_neg, len(r_rows), RIDGE_MIN_FRAC * 100, "PASS" if r_ok else "FAIL")
    for sym, n_sig, n_sim, g in r_rows:
        log.info("  %-6s blocked=%d sims=%d gross=%s %s", sym, n_sig, n_sim, _fmt(g),
                 "NEG" if (g is not None and g < 0) else "")

    # confound split (informational, not a gate)
    try:
        spy_bars = fetch_minute_bars("SPY", deploy_ep - 10 * 86400, end_ep)
        closes = fetch_daily_closes("SPY", deploy_ep - 10 * 86400, end_ep)
        split = {}
        for (ts, sym, px, reason, d) in firsts:
            res = simulate_bracket(ts, px, bars.get(sym, []), sl_pct_for(sym))
            if res is None:
                continue
            r = spy_r_at(ts, spy_bars, closes)
            tag = "SPY<=-0.5" if (r is not None and r <= BEAR_R) else "SPY>-0.5"
            if bear_day:
                tag += " post-rail" if d >= bear_day else " pre-rail"
            split.setdefault((tag, reason), []).append(res[1])
        for (tag, reason) in sorted(split):
            v = split[(tag, reason)]
            log.info("  split %-22s %-9s n=%d net %s", tag, reason, len(v), _fmt(net(v)))
    except Exception as e:
        log.info("  regime split unavailable: %s", e)

    ks = kill_switches(window, per_sess, firsts, nodata, [row[3] for _f, row in matched])
    for name, fired, detail in ks:
        log.info("%s: %s (%s)", name, "FIRED -> turn ORB_CONFIRM off" if fired else "ok", detail)

    verdict = decide(status, c_ok, a_ok, b_ok, r_ok)
    log.info("=" * 64)
    log.info("VERDICT: %s", verdict)
    note = {
        "INTERIM": f"Session {len(window)}/{BASE_SESSIONS}. Nothing is decided yet.",
        "EXTEND": f"Floors unmet at {len(window)} sessions. Keep collecting (max {MAX_SESSIONS}).",
        "PARK": "Floors unmet at 30, sim untrusted (Gate C), or ridge failed.",
        "REVERT": "Gate A or B failed. Turn ORB_CONFIRM off.",
        "KEEP": "A, B, C and ridge hold. ORB_CONFIRM may stay (Matthew's OK).",
    }[verdict]
    log.info(note)
    log.info("=" * 64)
    if verdict not in ("INTERIM",) or any(f for _n, f, _d in ks):
        send_alert(
            f"ORB_CONFIRM VERDICT: {verdict}\n"
            f"Sessions {len(window)} (valid {len(valid)}) | passed {len(passed_f)} | "
            f"blocked {len(blocked_f)}\n"
            f"A: diff {_fmt(a_diff, 3)} P={prob:.2f} {'PASS' if a_ok else 'FAIL'} | "
            f"B {'PASS' if b_ok else 'FAIL'} | C n={c_n} {_fmt(c_diff, 3)} "
            f"{'PASS' if c_ok else 'FAIL'} | ridge {r_neg}/{len(r_rows)} "
            f"{'PASS' if r_ok else 'FAIL'}\n"
            + "".join(f"{n}: FIRED\n" for n, f, _d in ks if f)
            + note)


if __name__ == "__main__":
    main()
