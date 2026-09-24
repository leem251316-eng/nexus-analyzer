#!/usr/bin/env python3
"""
band_verdict.py V1.0 -- BAND_0905 SECOND OPENING WALL VERDICT
=============================================================
Pre-registered in BAND_0905_PREREG_Sep24.md. Parameterized cousin of
opening_delay_verdict.py. READ-ONLY: reads berserker_shadow_signals and
berserker_trade_fingerprints, fetches Alpaca bars, writes NOTHING.

reason = 'band_0905'

DEPLOY: Thu Sep 24 2026 evening (V10.68 live), near the close.
SESSION 1: Fri Sep 25 2026.
VERDICT: after the close of Thu Oct 15 2026 (the 15th session). No NYSE
holiday falls in that window. EXCLUDED_SESSIONS = {2026-09-24} so a
deploy-day shadow row cannot open the sample early. No other date is
excluded. Shadow rows on that day are counted in the log and dropped.

FLOORS (both required): 15 sessions AND >= 30 distinct blocked
(session, symbol). Sessions = distinct dates with >= 1 blocked signal
after the exclusion. n = distinct (session, symbol); the FIRST tick's
price is the would-be entry; raw ticks are liveness only. Below either
floor -> PARK and extend once by 10 sessions (15 -> 25), then decide.
The bracket replay is withheld until both floors are met, so Gate B is
not read on a short sample (same operational pattern as
opening_delay_verdict.py).

GATE A (the block did not hurt what still trades):
  Live expectancy of entries OUTSIDE [09:05, 09:30) CT, entry_ts >=
  session 1, >= +0.00%/trade net, and n >= 40.
  Live rows only: is_paper = FALSE, trade_id not like 'bt_%', closed
  (won IS NOT NULL).
  P&L: COALESCE(pnl_pct_broker, pnl_pct) when pnl_pct_broker exists
  (Sep 24 fill audit), else pnl_pct.
  Clock: hour_cdt plus a minute column (minute_cdt, entry_minute,
  minute, or cdt_minute) when both exist. Otherwise minute-of-day is
  derived from entry_ts in America/Chicago, which is how
  entry_minute_census.py and berserker_session_thesis.py place entries.
  hour_cdt alone cannot split 09:00-09:04 from 09:05-09:29.

GATE B (what was blocked would have lost):
  berserker_shadow_signals with reason = 'band_0905', deduped to the
  FIRST tick per (session, symbol), replayed from that price on real
  1-min IEX bars (fetch_minute_bars, same Alpaca call as
  opening_delay_verdict.py).
  PASS if simulated expectancy <= -0.10%/trade net. WR is printed and
  is not the gate.

  Binding bracket, extending opening_delay_verdict.simulate_bracket:
    TP +1.5% / SL -1.0%, and SPCX SL -1.5% on this path (binding, not
    a sensitivity).
    Base trail = 1.5% off the PRIOR bar's peak, and only once the bar
    is >= MIN_HOLD (20) minutes after the signal. That is TRAILING_STOP
    from trail_prereg_sim.py's control replay. The tight 0.5% ratchet
    arms only once profit is already +1.5%, which is the TP and is
    resolved on the same bar, so it cannot change an exit and is not
    applied. Trail eligibility is MIN_HOLD, not a separate peak-MFE
    threshold (a +1.5% MFE gate would sit on the TP and the trail
    would never be reached).
    The trail fills at peak * (1 - 0.015) when that level is above the
    hard SL. SL and TP fill at their levels, not at the bar extreme.
    EOD is the last close before 14:58 CT, same session only. No
    overnight carry (the registered bracket list ends at EOD 14:58;
    opening_delay_verdict stops the same way).
    Ambiguous bar: a high that touches TP while the low touches SL
    and/or the trail counts as a WIN at +1.5%. Combined with level
    fills, this flatters the blocked band. Gate B is harder to pass.
    That is the declared anti-validation. The low is tested against
    the prior peak, matching trail_prereg_sim (a high later in the
    same bar does not set the trail the low already traded through).

RIDGE (time effect, not a thin name):
  Among symbols with >= 3 blocked distinct signals, at least 5 must
  show negative GROSS simulated expectancy. Otherwise PARK regardless
  of Gate A and Gate B. The fee haircut is not applied to this sign: a
  flat subtraction would push every name toward negative and weaken
  the guard. The prereg puts the fee on the net expectancy bars.

NET: Gate A and Gate B subtract 0.05 percentage points (fee/slip) from
  mean P&L. The registered claim and the Gate B bar are net. The same
  haircut is applied to both so the two bars share one scale.

VERDICT, overlap resolved in this order:
  floors unmet -> PARK (extend once by 10 sessions)
  ridge unmet  -> PARK regardless of A and B (same extension)
  Gate A fails OR Gate B fails -> REVERT
  else KEEP

An empty replay (floors met, zero resolved sims) is PARK with a re-run
message. That is a data failure, not the 10-session extension and not
a revert.

Run (nexus-analyst console): python3 band_verdict.py
"""

import os
import sys
import logging
from datetime import datetime, timezone, date
from zoneinfo import ZoneInfo

import requests
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BAND] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("band")

DATABASE_URL   = os.environ.get("DATABASE_URL", "")
ALPACA_API_KEY = (os.environ.get("ALPACA_API_KEY") or
                  os.environ.get("ALPACA_PHASE4_API_KEY", ""))
ALPACA_SECRET  = (os.environ.get("ALPACA_SECRET_KEY") or
                  os.environ.get("ALPACA_PHASE4_SECRET_KEY", ""))
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

CT = ZoneInfo("America/Chicago")

DEPLOY_DAY = date(2026, 9, 24)
SESSION_1  = date(2026, 9, 25)
VERDICT_DAY = date(2026, 10, 15)          # 15th weekday session; Thursday
# Deploy landed near the close. Drop that date so it cannot be session 1.
EXCLUDED_SESSIONS = {DEPLOY_DAY}
DEPLOY_EP    = int(datetime(2026, 9, 24, 0, 0, tzinfo=CT).timestamp())
SESSION_1_EP = int(datetime(2026, 9, 25, 0, 0, tzinfo=CT).timestamp())

REASON = "band_0905"

# [09:05, 09:30) CT, minutes after midnight.
BAND_LO = 9 * 60 + 5
BAND_HI = 9 * 60 + 30

TP_PCT     = 0.015
SL_PCT     = 0.010
SPCX_SL    = 0.015               # binding on this path
BASE_TRAIL = 0.015
MIN_HOLD   = 20                  # minutes before the base trail can fire
FEE_PCT    = 0.05                # percentage points, both gate expectancies

MIN_SESSIONS = 15
MIN_BLOCKED  = 30                # distinct (session, symbol)
MIN_LIVE     = 40                # Gate A outside-band n
GATE_A_MIN   = 0.00              # net %/trade
GATE_B_MAX   = -0.10             # net %/trade
RIDGE_MIN_N  = 3
RIDGE_MIN_NEG = 5

MINUTE_COLUMNS = ("minute_cdt", "entry_minute", "minute", "cdt_minute")
EXTEND_ONCE = ("Extend once by 10 sessions (floor 15 -> 25), then decide.")


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
    out.sort(key=lambda b: b[0])
    return out


def in_band(minute_of_day):
    """True when entry minute-of-day is in [09:05, 09:30) CT."""
    return BAND_LO <= minute_of_day < BAND_HI


def row_minute(entry_ts, hour, minute):
    """Minute-of-day. hour+minute columns when both are present, else entry_ts CT."""
    if hour is not None and minute is not None:
        hour_i, minute_i = int(hour), int(minute)
        if minute_i > 59:                 # column already stores minute-of-day
            return minute_i
        return hour_i * 60 + minute_i
    dt = datetime.fromtimestamp(int(entry_ts), CT)
    return dt.hour * 60 + dt.minute


def minute_from_ts(entry_ts):
    dt = datetime.fromtimestamp(int(entry_ts), CT)
    return dt.hour * 60 + dt.minute


def sl_pct_for(symbol):
    """Binding stop. SPCX uses the live 1.5% recipe; every other name uses 1.0%."""
    return SPCX_SL if symbol == "SPCX" else SL_PCT


def simulate_bracket(sig_ep, sig_px, bars, sl_pct=None):
    """Walk one would-be entry through the live bracket.

    Returns (won, pnl_pct, reason) or None when no same-day bar is usable.
    pnl_pct is GROSS of the 0.05 fee. reason is tp, sl, trail, or eod.

    See the module docstring for the trail rule. Short form: after
    MIN_HOLD minutes, a low at or below prior_peak * (1 - 0.015) exits
    at that level when the level is above the hard SL. A bar whose high
    touches TP counts as a win even if the low also touches SL or the
    trail. Same-day only; the walk stops before 14:58 CT and marks the
    last prior close.
    """
    sl_pct = SL_PCT if sl_pct is None else sl_pct
    tp = sig_px * (1 + TP_PCT)
    sl = sig_px * (1 - sl_pct)
    sig_day = datetime.fromtimestamp(sig_ep, CT).date()
    peak = sig_px
    last_close = None
    for ep, _o, h, l, c in bars:
        if ep < sig_ep:
            continue
        d = datetime.fromtimestamp(ep, CT)
        if d.date() != sig_day:
            break
        if (d.hour == 14 and d.minute >= 58) or d.hour >= 15:
            break
        held_min = (ep - sig_ep) / 60.0
        prior_peak = peak
        trail_px = prior_peak * (1.0 - BASE_TRAIL)
        hit_tp = h >= tp
        hit_sl = l <= sl
        hit_trail = held_min >= MIN_HOLD and trail_px > sl and l <= trail_px
        # Anti-validation: TP on this bar wins ties with SL and the trail.
        if hit_tp and hit_sl:
            return True, TP_PCT * 100.0, "tp"
        if hit_tp and hit_trail:
            return True, TP_PCT * 100.0, "tp"
        if hit_tp:
            return True, TP_PCT * 100.0, "tp"
        if hit_trail:
            pnl = (trail_px / sig_px - 1.0) * 100.0
            return pnl > 0, pnl, "trail"
        if hit_sl:
            return False, -sl_pct * 100.0, "sl"
        peak = max(peak, h)
        last_close = c
    if last_close is None:
        return None
    pnl = (last_close / sig_px - 1.0) * 100.0
    return pnl > 0, pnl, "eod"


def net_expectancy(pnls):
    """Mean P&L minus the 0.05 pct-pt fee, or None when pnls is empty."""
    if not pnls:
        return None
    return sum(pnls) / len(pnls) - FEE_PCT


def gate_a_pass(pnls):
    exp = net_expectancy(pnls)
    return exp is not None and len(pnls) >= MIN_LIVE and exp >= GATE_A_MIN


def gate_b_pass(pnls):
    exp = net_expectancy(pnls)
    return exp is not None and exp <= GATE_B_MAX


def dedup_firsts(sig_rows, excluded):
    """FIRST priced tick per (symbol, session). Rows are pre-sorted by ts.

    Invalid prices are skipped, so a later tick can supply the price.
    That matches opening_delay_verdict.py. Returns
    (firsts, excluded_rows, per_session, sessions).
    """
    firsts, seen, excluded_rows, per_session = [], set(), 0, {}
    for ts, sym, px in sig_rows:
        day = datetime.fromtimestamp(int(ts), CT).date()
        if day in excluded:
            excluded_rows += 1
            continue
        key = (sym, day)
        if key in seen or not px or px <= 0:
            continue
        seen.add(key)
        firsts.append((int(ts), sym, float(px)))
        per_session[day] = per_session.get(day, 0) + 1
    return firsts, excluded_rows, per_session, len(per_session)


def ridge_table(firsts, sim_by_sym):
    """Symbols with >= 3 blocked signals, and how many have gross exp < 0.

    A symbol with no resolved replay does not count as negative.
    Returns (rows, n_negative). Each row is
    (symbol, n_signals, n_sims, gross_exp or None, is_negative).
    """
    counts = {}
    for _ts, sym, _px in firsts:
        counts[sym] = counts.get(sym, 0) + 1
    rows, n_neg = [], 0
    for sym in sorted(counts):
        n_sig = counts[sym]
        if n_sig < RIDGE_MIN_N:
            continue
        pnls = sim_by_sym.get(sym, [])
        if not pnls:
            rows.append((sym, n_sig, 0, None, False))
            continue
        exp = sum(pnls) / len(pnls)
        neg = exp < 0.0
        if neg:
            n_neg += 1
        rows.append((sym, n_sig, len(pnls), exp, neg))
    return rows, n_neg


def decide(floors_met, gate_a, gate_b, ridge_ok):
    """PARK on floors or ridge. REVERT when a gate fails and the ridge holds."""
    if not floors_met:
        return "PARK"
    if not ridge_ok:
        return "PARK"
    if not gate_a or not gate_b:
        return "REVERT"
    return "KEEP"


def fingerprint_clock():
    """Which columns to use for the live entry clock and for P&L."""
    cols = {r[0] for r in q(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'berserker_trade_fingerprints'")}
    minute_col = next((c for c in MINUTE_COLUMNS if c in cols), None)
    use_cols = "hour_cdt" in cols and minute_col is not None
    has_broker = "pnl_pct_broker" in cols
    pnl_sql = "COALESCE(pnl_pct_broker, pnl_pct)" if has_broker else "pnl_pct"
    src = "broker" if has_broker else "recorded"
    return cols, use_cols, minute_col, pnl_sql, src


def load_live(pnl_sql, use_cols, minute_col):
    """Closed live trades from session 1 onward: list of (minute, pnl, sym)."""
    fields = ["entry_ts", "symbol", pnl_sql]
    if use_cols:
        fields.extend(["hour_cdt", minute_col])
    rows = q(f"""
        SELECT {", ".join(fields)}
        FROM berserker_trade_fingerprints
        WHERE won IS NOT NULL AND is_paper = FALSE
          AND trade_id NOT LIKE 'bt_%%'
          AND entry_ts >= %s
    """, (SESSION_1_EP,))
    out, skipped = [], 0
    for row in rows:
        entry_ts, sym, pnl = row[0], row[1], row[2]
        if pnl is None:
            skipped += 1
            continue
        if use_cols:
            mod = row_minute(entry_ts, row[3], row[4])
        else:
            mod = minute_from_ts(entry_ts)
        out.append((mod, float(pnl), sym))
    return out, skipped


def main():
    if not DATABASE_URL or not ALPACA_API_KEY:
        log.error("Missing DATABASE_URL or Alpaca keys")
        sys.exit(1)

    log.info("=" * 64)
    log.info("band_verdict.py V1.0 | BAND_0905 | reason='%s'", REASON)
    log.info("Deploy Thu %s (V10.68) -> session 1 Fri %s -> verdict after Thu %s close",
             DEPLOY_DAY, SESSION_1, VERDICT_DAY)
    log.info("Minute-bar fills at the level flatter the blocked band "
             "(declared anti-validation). Gate B is harder to pass.")
    log.info("Net of %.2f pct-pt fee/slip on Gate A and Gate B. READ-ONLY.", FEE_PCT)
    log.info("Excluded sessions: %s", sorted(EXCLUDED_SESSIONS))
    log.info("=" * 64)

    _cols, use_cols, minute_col, pnl_sql, pnl_src = fingerprint_clock()
    if use_cols:
        log.info("Clock: hour_cdt + %s", minute_col)
    else:
        log.info("Clock: entry_ts America/Chicago "
                 "(no entry-minute column beside hour_cdt; "
                 "hour alone cannot place 09:05)")
    log.info("P&L: %s (%s)", pnl_sql, pnl_src)

    live, skipped_pnl = load_live(pnl_sql, use_cols, minute_col)
    outside = [pnl for mod, pnl, _sym in live if not in_band(mod)]
    n_in = sum(1 for mod, _pnl, _sym in live if in_band(mod))
    gross_a = (sum(outside) / len(outside)) if outside else None
    exp_a = net_expectancy(outside)
    wins_a = sum(1 for pnl in outside if pnl > 0)
    wr_a = 100.0 * wins_a / len(outside) if outside else 0.0
    gate_a = gate_a_pass(outside)
    exp_a_s = f"{exp_a:+.4f}" if exp_a is not None else "n/a"
    gross_a_s = f"{gross_a:+.4f}" if gross_a is not None else "n/a"
    log.info("GATE A pool: %d closed live since %s | in-band excluded %d | "
             "null pnl skipped %d", len(live), SESSION_1, n_in, skipped_pnl)
    log.info("GATE A: outside-band n=%d | WR=%.1f%% | gross %s | net %s%%/trade "
             "(need net >= %+.2f and n >= %d)",
             len(outside), wr_a, gross_a_s, exp_a_s, GATE_A_MIN, MIN_LIVE)
    log.info("GATE A (block did not hurt what still trades): %s",
             "PASS" if gate_a else "FAIL")

    sig_rows = q("""
        SELECT ts, symbol, price FROM berserker_shadow_signals
        WHERE reason = %s AND ts >= %s
        ORDER BY ts
    """, (REASON, DEPLOY_EP))
    firsts, excluded_rows, per_session, sessions = dedup_firsts(
        sig_rows, EXCLUDED_SESSIONS)
    n_blocked = len(firsts)
    log.info("Shadow rows: %d raw ticks (liveness only; %d on excluded sessions %s)",
             len(sig_rows), excluded_rows, sorted(EXCLUDED_SESSIONS))
    log.info("n = %d distinct (session, symbol) across %d sessions",
             n_blocked, sessions)
    for d in sorted(per_session):
        log.info("  %s  distinct symbols: %d", d, per_session[d])

    floors_met = sessions >= MIN_SESSIONS and n_blocked >= MIN_BLOCKED
    log.info("Floors: sessions %d/%d | blocked %d/%d | %s",
             sessions, MIN_SESSIONS, n_blocked, MIN_BLOCKED,
             "MET" if floors_met else "UNMET")

    if not floors_met:
        log.info("GATE B (blocked sim exp <= %.2f%% net): NOT EVALUATED "
                 "(floors unmet; bar replay withheld)", GATE_B_MAX)
        log.info("RIDGE (>= %d symbols with >= %d blocked signals negative): "
                 "NOT EVALUATED (floors unmet)", RIDGE_MIN_NEG, RIDGE_MIN_N)
        log.info("=" * 64)
        log.info("VERDICT: PARK")
        log.info("%s", EXTEND_ONCE)
        log.info("=" * 64)
        send_alert(
            f"BAND_0905 VERDICT: PARK\n"
            f"Deploy {DEPLOY_DAY} -> session 1 {SESSION_1} -> after {VERDICT_DAY} close\n"
            f"Sessions {sessions}/{MIN_SESSIONS} | blocked {n_blocked}/{MIN_BLOCKED}\n"
            f"Gate A: outside n={len(outside)} net {exp_a_s}% "
            f"{'PASS' if gate_a else 'FAIL'} (not binding while floors are open)\n"
            f"Gate B / ridge: not evaluated (replay withheld)\n"
            f"{EXTEND_ONCE}"
        )
        return

    by_sym = {}
    for ts, sym, px in firsts:
        by_sym.setdefault(sym, []).append((ts, px))
    bars_cache, failed_syms = {}, []
    end_ep = int(datetime.now(timezone.utc).timestamp())
    for sym in by_sym:
        try:
            bars_cache[sym] = fetch_minute_bars(sym, DEPLOY_EP, end_ep)
            log.info("  bars %s: %d", sym, len(bars_cache[sym]))
        except Exception as e:
            log.info("  bars %s: FETCH FAILED (%s) -- its signals unresolved", sym, e)
            bars_cache[sym] = []
            failed_syms.append(sym)

    pnls, wins = [], 0
    reasons = {"tp": 0, "sl": 0, "trail": 0, "eod": 0}
    sim_by_sym, unresolved = {}, 0
    for ts, sym, px in firsts:
        res = simulate_bracket(ts, px, bars_cache.get(sym, []), sl_pct_for(sym))
        if res is None:
            unresolved += 1
            continue
        won, pnl, reason = res
        pnls.append(pnl)
        wins += 1 if won else 0
        reasons[reason] = reasons.get(reason, 0) + 1
        sim_by_sym.setdefault(sym, []).append(pnl)

    sims = len(pnls)
    exp_b = net_expectancy(pnls)
    gross_b = (sum(pnls) / sims) if sims else None
    wr_b = 100.0 * wins / sims if sims else 0.0
    gate_b = gate_b_pass(pnls)
    exp_b_s = f"{exp_b:+.4f}" if exp_b is not None else "n/a"
    gross_b_s = f"{gross_b:+.4f}" if gross_b is not None else "n/a"
    log.info("GATE B: simulated blocked n=%d | unresolved %d | WR=%.1f%% | "
             "gross %s | net %s%%/trade | tp=%d sl=%d trail=%d eod=%d",
             sims, unresolved, wr_b, gross_b_s, exp_b_s,
             reasons["tp"], reasons["sl"], reasons["trail"], reasons["eod"])
    log.info("GATE B (blocked sim exp <= %.2f%% net): %s",
             GATE_B_MAX, "PASS" if gate_b else "FAIL")
    if failed_syms:
        log.info("Bar fetch failed: %s", sorted(failed_syms))

    ridge_rows, n_neg = ridge_table(firsts, sim_by_sym)
    ridge_ok = n_neg >= RIDGE_MIN_NEG
    log.info("RIDGE: %d symbols with >= %d blocked signals | %d negative gross "
             "(need >= %d). Fee is not applied to this sign.",
             len(ridge_rows), RIDGE_MIN_N, n_neg, RIDGE_MIN_NEG)
    for sym, n_sig, n_sims, exp, neg in ridge_rows:
        exp_s = f"{exp:+.4f}" if exp is not None else "n/a"
        flag = "NEG" if neg else "non-negative"
        log.info("  %-6s signals=%d sims=%d gross=%s %s", sym, n_sig, n_sims, exp_s, flag)
    log.info("RIDGE (time effect, not a thin name): %s",
             "PASS" if ridge_ok else "FAIL")

    if sims == 0:
        log.info("=" * 64)
        log.info("VERDICT: PARK")
        log.info("No resolved bracket replays. Re-run when IEX bars are available. "
                 "Do not extend the sample and do not revert on an empty sim.")
        log.info("=" * 64)
        send_alert(
            f"BAND_0905 VERDICT: PARK\n"
            f"Floors met (sessions {sessions}, blocked {n_blocked}) but "
            f"0 replays resolved.\n"
            f"Re-run when IEX bars are available. Do not extend and do not revert "
            f"on an empty sim.\n"
            f"Gate A: outside n={len(outside)} net {exp_a_s}% "
            f"{'PASS' if gate_a else 'FAIL'}"
        )
        return

    verdict = decide(True, gate_a, gate_b, ridge_ok)
    log.info("=" * 64)
    log.info("GATE A (outside live net >= %+.2f, n >= %d): %s (%s%%, n=%d)",
             GATE_A_MIN, MIN_LIVE, "PASS" if gate_a else "FAIL", exp_a_s, len(outside))
    log.info("GATE B (blocked sim net <= %.2f): %s (%s%%, n=%d, WR=%.1f%%)",
             GATE_B_MAX, "PASS" if gate_b else "FAIL", exp_b_s, sims, wr_b)
    log.info("RIDGE (>= %d of >=%d-signal names negative): %s (%d/%d)",
             RIDGE_MIN_NEG, RIDGE_MIN_N, "PASS" if ridge_ok else "FAIL",
             n_neg, len(ridge_rows))
    log.info("VERDICT: %s", verdict)
    if verdict == "PARK":
        log.info("%s Ridge failed, so this PARK stands regardless of Gate A/B.",
                 EXTEND_ONCE)
    elif verdict == "REVERT":
        log.info("A gate failed and the ridge held. Revert the V10.68 band_0905 wall.")
    else:
        log.info("Gate A, Gate B, and the ridge all hold. The band stays.")
    log.info("=" * 64)

    tail = {
        "KEEP": "The band stays.",
        "REVERT": "Revert the V10.68 band_0905 wall.",
        "PARK": EXTEND_ONCE + " Ridge failed, regardless of Gate A/B.",
    }[verdict]
    send_alert(
        f"BAND_0905 VERDICT: {verdict}\n"
        f"Deploy {DEPLOY_DAY} -> session 1 {SESSION_1} -> after {VERDICT_DAY} close\n"
        f"Gate A: outside n={len(outside)} WR {wr_a:.1f}% net {exp_a_s}% "
        f"{'PASS' if gate_a else 'FAIL'}\n"
        f"Gate B: sim n={sims} WR {wr_b:.1f}% net {exp_b_s}% "
        f"{'PASS' if gate_b else 'FAIL'}\n"
        f"Ridge: {n_neg}/{len(ridge_rows)} names negative "
        f"{'PASS' if ridge_ok else 'FAIL'}\n"
        f"Sessions {sessions} | blocked {n_blocked} | Sep 24 excluded\n"
        f"Level fills flatter the blocked band (anti-validation).\n"
        f"{tail}"
    )


if __name__ == "__main__":
    main()
