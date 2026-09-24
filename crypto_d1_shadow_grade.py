#!/usr/bin/env python3
"""
crypto_d1_shadow_grade.py V1.1 — READ-ONLY Phase 2 D1 shadow grader.

V1.1 (Sep 24 2026): the live writer stores the shadow limit in
limit_price. CRYPTO_P2_SHADOW_PREREG.md names that field `limit`
(`limit` is reserved SQL). The required-column check and the SELECT
read limit_price. The prereg name `limit` is accepted only when
limit_price is absent. No other column is renamed, and the limit is
still not recomputed from spread_bps.

Implements CRYPTO_P2_SHADOW_PREREG.md (Aug 13 2026) as closely as the
schema allows. Verdicts bind when the clock says they bind. This script
does not start Phase 3, does not write shadow rows, and does not change
the lock.

WHAT IT READS
  crypto_shadow_signals — live writer columns (Matthew, Sep 24 2026):
      id, ts, pair, price, limit_price, spread_bps, funding, t48, rel_strength
  Required for a grade: ts, pair, price, limit_price, spread_bps, funding,
  t48, rel_strength. `id` may be present and is not selected.
  The prereg's parenthetical list says `limit`. That is the same field.
  Production named it limit_price. If limit_price is absent and a column
  named limit exists, that prereg alias is read instead. Nothing else
  is aliased.
  crypto_thorn_observations — the Thorn tape used by crypto_p1_thesis.py
      and thorn_extended_thesis.py. Required columns: ts, pair, price.
  If either table or any required column is missing, the script exits
  with a SCHEMA FAIL naming the table, the missing names, and the
  columns that are actually present. It does not invent a third name
  for the limit and it does not recompute a limit to fill a hole.
  NULL ts, pair, price, or limit_price on any row is a DATA FAIL (exit 2).
  A stored limit_price is never rebuilt from spread_bps.

FILL AND 48h (prereg maker-fill simulation)
  FILLED if some Thorn print for that pair has price <= stored limit
  and signal_ts < print_ts <= signal_ts + 30 minutes.
  The signal bar itself is not a fill: the limit sits inside the touch
  (price * (1 - spread_bps/2/10000)), so the signal price is above it.
  Fill price is that first qualifying print.
  48h forward is from the FILL price, not the signal price:
      ret_pct = (price_at(fill_ts + 48h) / fill_price - 1) * 100
  The 48h print uses the same ±900s window as crypto_p1_thesis.py and
  thorn_extended_thesis.py (first print in the window, not nearest).
  Frame filters (funding p90, hour-20 CDT, 24h dedup) are the writer's
  job. Rows are graded as logged. They are not re-filtered here.
  If any signal's pair has zero Thorn rows, the binding verdict is
  EXTEND (JOIN INCOMPLETE). Those rows are not counted as failed fills
  and are not dropped from the denominator — a partial join could mint
  a KEEP. No symbol remap (BTC-USDC stays BTC-USDC).

GATES (from the prereg; floors are absolute)
  Clock anchor: 2026-08-13 00:00 America/Chicago — the prereg date,
  written before any live shadow data. "3 weeks OR n>=25 filled,
  whichever later" means BOTH must be true before KEEP / KILL / PARK.
  FILL VIABILITY   fill rate >= 40% of gradable signals.
  EDGE KEEP        on n >= 25 resolved fills (48h print exists):
                   mean >= +1.00% AND mean > 0 AND positive rate >= 60%.
                   Positive = 48h return strictly > 0.
                   n for this gate is resolved fills, not raw touches.
                   Touches whose 48h print is not on the tape yet do not
                   enter the mean.
  REGIME CHECK     a capitulation cluster is an ISO week (America/Chicago
                   dates) with >= 3 distinct signal days. If any cluster
                   exists, report the resolved-fill mean with it and
                   without the single largest cluster (most signals, then
                   most days, then earliest week). The ex-cluster mean
                   must be > 0. The exclusion set does not have to hold
                   n >= 25. No cluster → regime passes and is reported
                   as such. One crash week cannot carry the verdict.
  INSUFFICIENT     at 6 weeks with n_filled < 25: shadow continues, no
                   KEEP/KILL. Printed as EXTEND.

VERDICT LINES (priority once the window is open)
  EXTEND   before the later of (3 weeks, n_filled >= 25); or 3–6 weeks
           with n_filled < 25; or 6 weeks with n_filled < 25
           (INSUFFICIENT); or n_filled >= 25 but n_resolved < 25
           (48h not mature).
  PARK     window open, n_resolved >= 25, fill rate < 40%.
           Prereg: maker economics do not exist; D1 returns to the queue
           re-costed at the 2.4% taker bar (cell cleared taker at 28.8%
           — likely fatal). This is not an edge KILL and not an unlock.
  KILL     window open, fill rate >= 40%, and the edge gate or the
           regime gate fails. Any KILL: D1 dies or re-registers per the
           batch clock. The lock does not move.
  KEEP     fill, edge, and regime all pass. Prereg next leg is Phase 3
           paper in CRYPTO_REVIVAL_PLAN.md. This script does not start
           it.

D3 (rel_strength) is printed as a descriptive split only. It is not a gate.

BAND / LOCK
  Banner: observation-only; does not change CRYPTO_BUYS_DISABLED; not a
  live unlock. BAND_0905 owns the live WR-touching slot. Shadow grading
  may be read while that experiment is open; it is not a second live
  WR experiment and it is not a crypto unlock.

USAGE
  python3 crypto_d1_shadow_grade.py
  python3 crypto_d1_shadow_grade.py --self-test

Read-only session. Idempotent. No Telegram (thesis graders in this repo
print; they do not alert).
"""

import argparse
import os
import sys
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

VERSION = "1.1"
CENTRAL = ZoneInfo("America/Chicago")
# Prereg written Aug 13 2026, before any live shadow rows existed.
ANCHOR = datetime(2026, 8, 13, 0, 0, tzinfo=CENTRAL)

FILL_WINDOW_S = 30 * 60
H48 = 172800
TOL = 900  # ±15 min, same matching window as the Thorn thesis scripts

FILL_MIN = 0.40
EDGE_MEAN = 1.00          # percentage points
POS_MIN = 0.60
N_MIN = 25
WEEKS_OPEN = 3.0
WEEKS_INSUFFICIENT = 6.0
CLUSTER_DAYS = 3

SHADOW_TABLE = "crypto_shadow_signals"
THORN_TABLE = "crypto_thorn_observations"
# Everything except the limit. The limit column is resolved separately.
SHADOW_REQUIRED = (
    "ts", "pair", "price", "spread_bps", "funding", "t48", "rel_strength",
)
# Live writer column first. The prereg names this field `limit`; accept
# that spelling only when limit_price is not on the table.
LIMIT_PRICE_COLUMNS = ("limit_price", "limit")
THORN_COLS = ("ts", "pair", "price")


def shadow_select_columns(present):
    """Map live crypto_shadow_signals columns to the SELECT list.

    `present` is the column names on the table (extras such as id are
    ignored). Returns (columns, missing). `columns` is None when the
    table cannot be graded. The limit slot is limit_price when that
    column exists, otherwise the prereg name `limit`.
    """
    have = {}
    for name in present:
        have[str(name).lower()] = str(name)
    missing = [c for c in SHADOW_REQUIRED if c not in have]
    limit_key = next((c for c in LIMIT_PRICE_COLUMNS if c in have), None)
    if limit_key is None:
        missing.append("limit_price")
    if missing:
        return None, missing
    columns = [
        have["ts"], have["pair"], have["price"], have[limit_key],
        have["spread_bps"], have["funding"], have["t48"], have["rel_strength"],
    ]
    return columns, []


def weeks_since(now, anchor=ANCHOR):
    return (now - anchor).total_seconds() / (7.0 * 86400.0)


def to_epoch(value):
    """Normalize a shadow/tape timestamp to unix seconds. Fail on junk."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    try:
        v = float(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"unrecognized timestamp {value!r}") from e
    if v > 1e12:  # millisecond epoch
        v = v / 1000.0
    return v


def first_fill(ts_list, px_list, signal_ts, limit):
    """First tape print strictly after the signal, within 30 minutes, at or below limit."""
    if limit is None or not ts_list:
        return None
    i = bisect_right(ts_list, signal_ts)
    end = signal_ts + FILL_WINDOW_S
    while i < len(ts_list) and ts_list[i] <= end:
        px = px_list[i]
        if px is not None and px <= limit:
            return ts_list[i], px
        i += 1
    return None


def px_at(ts_list, px_list, target):
    """First print in [target-TOL, target+TOL]. Matches the Thorn scripts."""
    if not ts_list:
        return None
    i = bisect_left(ts_list, target - TOL)
    if i < len(ts_list) and ts_list[i] <= target + TOL:
        return px_list[i]
    return None


def forward_from_fill(ts_list, px_list, fill_ts, fill_px):
    if not fill_px or fill_px <= 0:
        return None
    fwd = px_at(ts_list, px_list, fill_ts + H48)
    if fwd is None or fwd <= 0:
        return None
    return (fwd / fill_px - 1.0) * 100.0


def capitulation_clusters(signal_epochs):
    """ISO weeks (CDT/CST dates) with >= 3 distinct signal days.

    Returns (clusters, largest_key). clusters maps (iso_year, iso_week)
    -> {"days": set, "idxs": [signal index...]}. largest is None if no
    week qualifies. Largest = most signals, then most days, then earliest.
    """
    buckets = defaultdict(lambda: {"days": set(), "idxs": []})
    for i, ts in enumerate(signal_epochs):
        day = datetime.fromtimestamp(ts, tz=CENTRAL).date()
        iso = day.isocalendar()
        key = (iso[0], iso[1])
        buckets[key]["days"].add(day)
        buckets[key]["idxs"].append(i)
    clusters = {k: v for k, v in buckets.items() if len(v["days"]) >= CLUSTER_DAYS}
    if not clusters:
        return {}, None
    largest = max(
        clusters.items(),
        key=lambda kv: (len(kv[1]["idxs"]), len(kv[1]["days"]), -kv[0][0], -kv[0][1]),
    )[0]
    return clusters, largest


def _mean(vals):
    if not vals:
        return None
    return sum(vals) / len(vals)


def _pos_rate(vals):
    if not vals:
        return None
    return sum(1 for v in vals if v > 0) / len(vals)


def evaluation_verdict(now, n_filled, n_resolved, fill_rate, mean, pos_rate,
                       cluster_present, ex_mean, ex_n):
    """Return (verdict, reason).

    verdict is KEEP, KILL, PARK, or EXTEND.
    See the module docstring for the priority.
    """
    weeks = weeks_since(now)
    window = weeks >= WEEKS_OPEN and n_filled >= N_MIN

    if not window:
        if weeks >= WEEKS_INSUFFICIENT and n_filled < N_MIN:
            return (
                "EXTEND",
                "INSUFFICIENT: 6 weeks from the Aug 13 2026 anchor and "
                f"n_filled={n_filled} < {N_MIN}. Shadow continues. No KEEP/KILL. "
                "The market did not offer enough fills. Lock unchanged.",
            )
        if weeks < WEEKS_OPEN and n_filled >= N_MIN:
            return (
                "EXTEND",
                f"n_filled={n_filled} >= {N_MIN} but only {weeks:.2f} weeks "
                "since Aug 13 2026. Prereg: 3 weeks OR n>=25, whichever later. "
                "No verdict yet.",
            )
        if WEEKS_OPEN <= weeks < WEEKS_INSUFFICIENT and n_filled < N_MIN:
            return (
                "EXTEND",
                f"{weeks:.2f} weeks elapsed and n_filled={n_filled} < {N_MIN}. "
                "Continue until n_filled >= 25 or the 6-week mark.",
            )
        return (
            "EXTEND",
            f"{weeks:.2f} weeks since Aug 13 2026, n_filled={n_filled}. "
            "Before the evaluation window (3 weeks AND n_filled >= 25).",
        )

    if n_resolved < N_MIN:
        return (
            "EXTEND",
            f"n_filled={n_filled} >= {N_MIN} but n_resolved={n_resolved} < {N_MIN}. "
            "48h forward from the fill is not on the tape yet for enough fills. "
            "No edge verdict on an immature window.",
        )

    fill_ok = fill_rate is not None and fill_rate + 1e-12 >= FILL_MIN
    edge_ok = (
        mean is not None
        and mean + 1e-9 >= EDGE_MEAN
        and mean > 0
        and pos_rate is not None
        and pos_rate + 1e-12 >= POS_MIN
    )
    if cluster_present:
        regime_ok = ex_n > 0 and ex_mean is not None and ex_mean > 0
    else:
        regime_ok = True

    if not fill_ok:
        extra = ""
        if not edge_ok:
            extra = " Edge gate would also fail; PARK is the prior gate (fill)."
        if cluster_present and not regime_ok:
            extra += " Regime check would also fail."
        rate = "n/a" if fill_rate is None else f"{fill_rate * 100:.1f}%"
        return (
            "PARK",
            f"FILL VIABILITY failed: fill rate {rate} < 40%. "
            "Maker economics do not exist. D1 returns to the queue re-costed "
            "at the 2.4% taker bar (prereg: the discovery cell cleared that bar "
            "at only 28.8% — likely fatal). Lock unchanged."
            + extra,
        )
    if not edge_ok:
        return (
            "KILL",
            "EDGE KEEP failed: need filled 48h mean >= +1.00% AND > 0 absolute "
            f"AND positive rate >= 60% on n_resolved >= {N_MIN}. "
            "D1 dies or re-registers per the batch clock. Lock unchanged.",
        )
    if not regime_ok:
        shown = "undefined" if ex_mean is None else f"{ex_mean:+.3f}%"
        return (
            "KILL",
            "REGIME CHECK failed: edge must be > 0 excluding the single largest "
            f"capitulation cluster (ex-cluster mean {shown}, n={ex_n}). "
            "One crash week cannot carry the verdict. Lock unchanged.",
        )
    return (
        "KEEP",
        "All gates pass (fill >= 40%, filled 48h mean >= +1.00% and > 0, "
        "positive rate >= 60%, n_resolved >= 25, regime > 0 ex-cluster or no "
        "cluster). Prereg next leg is Phase 3 paper per CRYPTO_REVIVAL_PLAN.md. "
        "This script does not start it. CRYPTO_BUYS_DISABLED unchanged. "
        "BAND_0905 owns the live WR slot.",
    )


def _fmt(v, digits=3, signed=True):
    if v is None:
        return "—"
    if signed:
        return f"{v:+.{digits}f}%"
    return f"{v:.{digits}f}%"


def _rate(v):
    if v is None:
        return "—"
    return f"{v * 100:.1f}%"


def grade_signals(signals, tape, now):
    """signals: list of dicts with epoch ts, pair, price, limit, and covariates.
    tape: pair -> (ts_list, px_list), each sorted by ts.

    Returns a report dict. Does not touch the DB.
    """
    pairs_with_tape = {p for p, (ts, _px) in tape.items() if ts}
    gradable = []
    uncovered = []
    for s in signals:
        if s["pair"] not in pairs_with_tape:
            uncovered.append(s)
        else:
            gradable.append(s)

    filled = []
    for s in gradable:
        ts_list, px_list = tape[s["pair"]]
        hit = first_fill(ts_list, px_list, s["ts"], s["limit"])
        if hit is None:
            s = dict(s)
            s["filled"] = False
            s["ret48"] = None
            filled.append(s)
            continue
        fill_ts, fill_px = hit
        ret = forward_from_fill(ts_list, px_list, fill_ts, fill_px)
        s = dict(s)
        s["filled"] = True
        s["fill_ts"] = fill_ts
        s["fill_px"] = fill_px
        s["ret48"] = ret
        filled.append(s)

    n_signals = len(signals)
    n_gradable = len(gradable)
    n_filled = sum(1 for s in filled if s["filled"])
    resolved_vals = [s["ret48"] for s in filled if s["filled"] and s["ret48"] is not None]
    n_resolved = len(resolved_vals)
    fill_rate = (n_filled / n_gradable) if n_gradable else None
    mean = _mean(resolved_vals)
    pos = _pos_rate(resolved_vals)

    epochs = [s["ts"] for s in signals]
    clusters, largest = capitulation_clusters(epochs) if epochs else ({}, None)
    excluded = set(clusters[largest]["idxs"]) if largest is not None else set()
    # `filled` follows gradable order, a subsequence of `signals`. Map back by index
    # so the jackknife drops the same signal rows the cluster definition counted.
    graded_by_index = {}
    g_i = 0
    for i, s in enumerate(signals):
        if s["pair"] in pairs_with_tape:
            graded_by_index[i] = filled[g_i]
            g_i += 1
    ex_vals = []
    for i, s in enumerate(signals):
        if i in excluded:
            continue
        g = graded_by_index.get(i)
        if g and g["filled"] and g["ret48"] is not None:
            ex_vals.append(g["ret48"])
    ex_mean = _mean(ex_vals)
    ex_n = len(ex_vals)

    subset_verdict, subset_reason = evaluation_verdict(
        now, n_filled, n_resolved, fill_rate, mean, pos,
        bool(clusters), ex_mean, ex_n,
    )
    # A pair with zero Thorn rows is a broken join, not a failed fill.
    # Dropping it would shrink the denominator and could mint a KEEP.
    if uncovered:
        missing = ", ".join(sorted({s["pair"] for s in uncovered}))
        verdict = "EXTEND"
        reason = (
            f"JOIN INCOMPLETE: {len(uncovered)} signal(s) have no Thorn rows "
            f"for pair key(s) {missing}. No symbol remap is applied. "
            f"The joined subset would have been {subset_verdict} — not binding. "
            + subset_reason
        )
    else:
        verdict, reason = subset_verdict, subset_reason
    return {
        "n_signals": n_signals,
        "n_gradable": n_gradable,
        "n_uncovered": len(uncovered),
        "n_filled": n_filled,
        "n_resolved": n_resolved,
        "fill_rate": fill_rate,
        "mean": mean,
        "pos_rate": pos,
        "clusters": clusters,
        "largest": largest,
        "ex_mean": ex_mean,
        "ex_n": ex_n,
        "ex_pos": _pos_rate(ex_vals),
        "verdict": verdict,
        "reason": reason,
        "uncovered_pairs": sorted({s["pair"] for s in uncovered}),
        "resolved_vals": resolved_vals,
    }


def render(report, now, null_counts, limit_check_note, covariate_note):
    weeks = weeks_since(now)
    lines = []
    lines.append("=" * 78)
    lines.append(f"D1 SHADOW GRADE V{VERSION} — observation-only")
    lines.append(
        "Banner: observation-only; does not change CRYPTO_BUYS_DISABLED; "
        "not a live unlock."
    )
    lines.append(
        "BAND_0905 owns the live WR-touching slot. This grade is not a crypto unlock "
        "and not a second live WR experiment."
    )
    lines.append("Source: CRYPTO_P2_SHADOW_PREREG.md. Read-only. Writes nothing.")
    lines.append("=" * 78)
    lines.append(
        f"Clock anchor: 2026-08-13 00:00 America/Chicago | "
        f"now {now.strftime('%Y-%m-%d %H:%M %Z')} | {weeks:.2f} weeks"
    )
    lines.append(
        f"Signals: {report['n_signals']} | gradable (pair has Thorn tape): "
        f"{report['n_gradable']} | uncovered pairs: {report['n_uncovered']}"
    )
    if report["uncovered_pairs"]:
        lines.append(
            "JOIN NOTE: no Thorn rows for: " + ", ".join(report["uncovered_pairs"])
            + ". Thorn scripts key crypto_thorn_observations.pair as BTC-USDC. "
            "No symbol remap is applied. Those signals are excluded from the fill rate."
        )
    lines.append(
        f"Filled within 30m: {report['n_filled']} | "
        f"fill rate: {_rate(report['fill_rate'])} of gradable "
        f"(floor {FILL_MIN * 100:.0f}%)"
    )
    lines.append(
        f"Resolved 48h from fill: n={report['n_resolved']} | "
        f"mean {_fmt(report['mean'])} | positive rate {_rate(report['pos_rate'])}"
    )
    lines.append(
        "48h is measured from the fill price. ±900s tape match, same as "
        "crypto_p1_thesis.py. Unresolved fills are not in the mean."
    )
    lines.append("-" * 78)
    lines.append("CAPITULATION CLUSTERS (>=3 distinct signal days in one CDT/CST ISO week)")
    if not report["clusters"]:
        lines.append("None. Regime check passes vacuously (nothing to exclude).")
    else:
        for key, bucket in sorted(report["clusters"].items()):
            mark = "  LARGEST — excluded from jackknife" if key == report["largest"] else ""
            days = ", ".join(d.isoformat() for d in sorted(bucket["days"]))
            lines.append(
                f"  {key[0]}-W{key[1]:02d}: {len(bucket['days'])} signal days, "
                f"{len(bucket['idxs'])} signals ({days}){mark}"
            )
        lines.append(
            f"Jackknife ex-largest: n_resolved={report['ex_n']} | "
            f"mean {_fmt(report['ex_mean'])} | positive rate {_rate(report['ex_pos'])}"
        )
        lines.append(
            "Regime floor: ex-cluster mean > 0. It does not re-impose the +1.00% "
            "or n>=25 floors (those apply to the full resolved set)."
        )
    lines.append("-" * 78)
    lines.append(limit_check_note)
    lines.append(covariate_note)
    if null_counts:
        bits = [f"{k}={v}" for k, v in null_counts.items()]
        lines.append("Null covariate counts (columns exist; not gates): " + ", ".join(bits))
    lines.append("-" * 78)
    lines.append(f"VERDICT: {report['verdict']}")
    lines.append(report["reason"])
    lines.append(
        "Banner: observation-only; does not change CRYPTO_BUYS_DISABLED; "
        "not a live unlock."
    )
    lines.append("=" * 78)
    return "\n".join(lines) + "\n"


def _connect():
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 not installed. On the Railway console it is already present.")
        sys.exit(1)
    db = os.environ.get("DATABASE_URL", "").strip()
    if not db:
        print("DATABASE_URL not set. Run from a NEXUS service console.")
        sys.exit(1)
    try:
        conn = psycopg2.connect(db, connect_timeout=15)
    except Exception as e:
        print(f"DB connect failed: {e}")
        sys.exit(1)
    try:
        conn.set_session(readonly=True, autocommit=True)
    except Exception as e:
        conn.close()
        print(f"Could not set a read-only session: {e}")
        print("Refusing to continue — this script must not write.")
        sys.exit(1)
    return conn


def _table_columns(conn, table):
    """Return (schema, {column: data_type}) or exit 2 if the table is missing."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY table_schema, ordinal_position
            """,
            (table,),
        )
        rows = cur.fetchall()
    if not rows:
        print(f"SCHEMA FAIL: table {table} not found in any schema.")
        if table == SHADOW_TABLE:
            print(
                "Check the V5.24 shadow writer. Live crypto_shadow_signals "
                "columns are ts, pair, price, limit_price, spread_bps, funding, "
                "t48, rel_strength (id optional). The prereg calls limit_price "
                "`limit`. This grader does not create the table."
            )
        else:
            print(
                "Check the Thorn writer. Existing analyzer scripts read "
                "crypto_thorn_observations (ts, pair, price, hour_cdt, funding, "
                "spread_bps, …). This grader needs ts, pair, price to test "
                "fill-within-30m and the 48h forward."
            )
        sys.exit(2)
    by_schema = defaultdict(dict)
    for schema, name, dtype in rows:
        by_schema[schema][name] = dtype
    if "public" in by_schema:
        schema = "public"
    elif len(by_schema) == 1:
        schema = next(iter(by_schema))
    else:
        print(
            f"SCHEMA FAIL: {table} exists in multiple schemas: "
            + ", ".join(sorted(by_schema))
        )
        print("Refusing to guess which one the writer uses.")
        sys.exit(2)
    return schema, by_schema[schema]


def _require_columns(schema, table, cols, required):
    missing = [c for c in required if c not in cols]
    if missing:
        print(f"SCHEMA FAIL: {schema}.{table} is missing columns: {', '.join(missing)}")
        print("Present columns: " + ", ".join(cols))
        print(f"Required: {', '.join(required)}")
        print("Refusing to invent stand-ins. Check the writer.")
        sys.exit(2)


def _fail_bad_ts(which, value):
    print(f"DATA FAIL: {which} timestamp {value!r} is not a usable epoch or datetime.")
    print("Check the writer. Refusing to grade on a guessed clock.")
    sys.exit(2)


def load_and_grade(conn, now):
    from psycopg2 import sql

    shadow_schema, shadow_cols = _table_columns(conn, SHADOW_TABLE)
    thorn_schema, thorn_cols = _table_columns(conn, THORN_TABLE)
    _require_columns(thorn_schema, THORN_TABLE, thorn_cols, THORN_COLS)
    select_cols, missing = shadow_select_columns(shadow_cols)
    if select_cols is None:
        print(
            f"SCHEMA FAIL: {shadow_schema}.{SHADOW_TABLE} is missing columns: "
            + ", ".join(missing)
        )
        print("Present columns: " + ", ".join(shadow_cols))
        print(
            "Required: " + ", ".join(SHADOW_REQUIRED)
            + ", limit_price (prereg name `limit` is accepted only if "
            "limit_price is absent)"
        )
        print(
            "The live writer column is limit_price. This grader does not "
            "rename the table and does not invent any other alias."
        )
        sys.exit(2)
    limit_col = select_cols[3]

    shadow_sql = sql.SQL("SELECT {cols} FROM {tbl} ORDER BY {ts}").format(
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in select_cols),
        tbl=sql.Identifier(shadow_schema, SHADOW_TABLE),
        ts=sql.Identifier("ts"),
    )
    with conn.cursor() as cur:
        cur.execute(shadow_sql)
        raw = cur.fetchall()

    signals = []
    null_limit = null_price = null_ts = null_pair = 0
    null_counts = {"spread_bps": 0, "funding": 0, "t48": 0, "rel_strength": 0}
    formula_deltas = []
    for ts, pair, price, limit, spread_bps, funding, t48, rel_strength in raw:
        if ts is None:
            null_ts += 1
            continue
        if pair is None:
            null_pair += 1
            continue
        if price is None:
            null_price += 1
            continue
        if limit is None:
            null_limit += 1
            continue
        try:
            epoch = to_epoch(ts)
        except ValueError:
            _fail_bad_ts("crypto_shadow_signals.ts", ts)
        if epoch is None or epoch < 1577836800 or epoch > 1893456000:
            _fail_bad_ts("crypto_shadow_signals.ts", ts)
        for key, val in (
            ("spread_bps", spread_bps),
            ("funding", funding),
            ("t48", t48),
            ("rel_strength", rel_strength),
        ):
            if val is None:
                null_counts[key] += 1
        if spread_bps is not None and price:
            expected = float(price) * (1.0 - float(spread_bps) / 2.0 / 10000.0)
            formula_deltas.append(abs(float(limit) - expected))
        signals.append({
            "ts": epoch,
            "pair": str(pair),
            "price": float(price),
            "limit": float(limit),
            "spread_bps": None if spread_bps is None else float(spread_bps),
            "funding": None if funding is None else float(funding),
            "t48": None if t48 is None else float(t48),
            "rel_strength": None if rel_strength is None else float(rel_strength),
        })

    bad = []
    if null_ts:
        bad.append(f"ts NULL on {null_ts} rows")
    if null_pair:
        bad.append(f"pair NULL on {null_pair} rows")
    if null_price:
        bad.append(f"price NULL on {null_price} rows")
    if null_limit:
        bad.append(f"{limit_col} NULL on {null_limit} rows")
    if bad:
        print("DATA FAIL: crypto_shadow_signals has rows that cannot be graded:")
        for b in bad:
            print(f"  - {b}")
        print(
            f"Fill uses stored {limit_col}. It is not recomputed from "
            "spread_bps. Check the V5.24 shadow writer."
        )
        sys.exit(2)

    if formula_deltas:
        formula_deltas.sort()
        med = formula_deltas[len(formula_deltas) // 2]
        limit_note = (
            f"Stored {limit_col} vs prereg formula price*(1-spread_bps/2/10000): "
            f"median abs diff {med:.6g} on n={len(formula_deltas)} "
            f"(rows with both price and spread_bps). Grading uses stored {limit_col}."
        )
    else:
        limit_note = (
            f"Stored-{limit_col} vs formula check skipped: no row has both price and spread_bps. "
            f"Grading uses stored {limit_col}."
        )

    rels = [s["rel_strength"] for s in signals if s["rel_strength"] is not None]
    if not signals:
        covariate_note = "D3 covariate: no signals. rel_strength is not a gate."
    elif not rels:
        covariate_note = (
            "D3 covariate: rel_strength column is present but every value is NULL. "
            "Not measurable. Not a gate."
        )
    else:
        covariate_note = (
            f"D3 covariate (descriptive only, not a gate): rel_strength non-null "
            f"{len(rels)}/{len(signals)}, mean {sum(rels)/len(rels):+.4f}. "
            "The prereg logs it so D3 can be read at verdict time; it does not move KEEP/KILL."
        )

    pairs = sorted({s["pair"] for s in signals})
    tape = {}
    if pairs:
        thorn_sql = sql.SQL(
            """
            SELECT {ts}, {pair}, {price}
            FROM {tbl}
            WHERE {price} > 0 AND {pair} = ANY(%s)
            ORDER BY {pair}, {ts}
            """
        ).format(
            ts=sql.Identifier("ts"),
            pair=sql.Identifier("pair"),
            price=sql.Identifier("price"),
            tbl=sql.Identifier(thorn_schema, THORN_TABLE),
        )
        with conn.cursor() as cur:
            cur.execute(thorn_sql, (pairs,))
            thorn_rows = cur.fetchall()
        grouped_ts = defaultdict(list)
        grouped_px = defaultdict(list)
        for ts, pair, price in thorn_rows:
            try:
                epoch = to_epoch(ts)
            except ValueError:
                _fail_bad_ts("crypto_thorn_observations.ts", ts)
            if epoch is None:
                continue
            grouped_ts[str(pair)].append(epoch)
            grouped_px[str(pair)].append(float(price))
        for pair in pairs:
            tape[pair] = (grouped_ts.get(pair, []), grouped_px.get(pair, []))

    report = grade_signals(signals, tape, now)
    # Attach D3 split on resolved fills, still descriptive.
    if rels:
        resolved = []
        # Reconstruct resolved (rel, ret) from a second pass would need grade_signals
        # to return them. Compute a simple overall note only; the split needs rets.
        # grade_signals doesn't return per-row rel. Extend via local recompute below.
        covariate_note = _d3_note(signals, tape, covariate_note)
    text = render(report, now, null_counts, limit_note, covariate_note)
    return text, report


def _d3_note(signals, tape, base_note):
    """Descriptive rel_strength split on resolved fills. Not a gate."""
    rows = []
    for s in signals:
        if s["rel_strength"] is None or s["pair"] not in tape or not tape[s["pair"]][0]:
            continue
        ts_list, px_list = tape[s["pair"]]
        hit = first_fill(ts_list, px_list, s["ts"], s["limit"])
        if hit is None:
            continue
        ret = forward_from_fill(ts_list, px_list, hit[0], hit[1])
        if ret is None:
            continue
        rows.append((s["rel_strength"], ret))
    if len(rows) < 2:
        return base_note + " Resolved-fill split not shown (fewer than 2 rows with both)."
    rel_sorted = sorted(r for r, _ in rows)
    med = rel_sorted[len(rel_sorted) // 2]
    lo = [ret for rel, ret in rows if rel <= med]
    hi = [ret for rel, ret in rows if rel > med]
    return (
        base_note
        + f" Resolved fills split at median rel_strength {med:+.4f}: "
        + f"at-or-below n={len(lo)} mean {_fmt(_mean(lo))}; "
        + f"above n={len(hi)} mean {_fmt(_mean(hi))}. Descriptive only."
    )


def _self_test():
    # Live columns Matthew printed from public.crypto_shadow_signals.
    # The prereg says `limit`; the writer stored limit_price. id is extra.
    live = [
        "id", "ts", "pair", "price", "limit_price", "spread_bps",
        "funding", "t48", "rel_strength",
    ]
    cols, missing = shadow_select_columns(live)
    assert cols == [
        "ts", "pair", "price", "limit_price", "spread_bps",
        "funding", "t48", "rel_strength",
    ], cols
    assert missing == []
    # Prereg spelling still readable when that is the only limit column.
    cols_alias, missing_alias = shadow_select_columns([
        "ts", "pair", "price", "limit", "spread_bps", "funding", "t48", "rel_strength",
    ])
    assert cols_alias[3] == "limit" and missing_alias == []
    # Writer name wins when both are present.
    cols_both, _ = shadow_select_columns(live + ["limit"])
    assert cols_both[3] == "limit_price"
    # The V1.0 required list (column named limit, no limit_price) is not
    # what production has. Production's list must pass; a table with
    # neither spelling must fail asking for limit_price.
    cols_neither, missing_neither = shadow_select_columns([
        "id", "ts", "pair", "price", "spread_bps", "funding", "t48", "rel_strength",
    ])
    assert cols_neither is None and missing_neither == ["limit_price"]

    # Fee-free verdict matrix. now is injected; anchor is Aug 13 2026.
    early = datetime(2026, 8, 20, tzinfo=CENTRAL)          # ~1 week
    mid = datetime(2026, 9, 10, tzinfo=CENTRAL)            # ~4 weeks
    late = datetime(2026, 9, 24, 12, tzinfo=CENTRAL)       # ~6.1 weeks

    def V(**kw):
        defaults = dict(
            n_filled=30, n_resolved=30, fill_rate=0.50, mean=1.20, pos_rate=0.70,
            cluster_present=False, ex_mean=None, ex_n=0,
        )
        defaults.update(kw)
        return evaluation_verdict(defaults.pop("now"), **defaults)

    assert V(now=early)[0] == "EXTEND"
    assert V(now=mid, n_filled=10, n_resolved=10)[0] == "EXTEND"
    assert V(now=late, n_filled=10, n_resolved=10)[0] == "EXTEND"
    assert "INSUFFICIENT" in V(now=late, n_filled=10, n_resolved=10)[1]
    assert V(now=late, n_resolved=10)[0] == "EXTEND"  # filled 30, resolved 10
    assert V(now=late, fill_rate=0.39, mean=2.0)[0] == "PARK"
    assert V(now=late, fill_rate=0.40)[0] == "KEEP"
    assert V(now=late, mean=0.99)[0] == "KILL"
    assert V(now=late, mean=1.0, pos_rate=0.60)[0] == "KEEP"
    assert V(now=late, pos_rate=0.599)[0] == "KILL"
    assert V(now=late, cluster_present=True, ex_mean=0.2, ex_n=5)[0] == "KEEP"
    assert V(now=late, cluster_present=True, ex_mean=0.0, ex_n=5)[0] == "KILL"
    assert V(now=late, cluster_present=True, ex_mean=-0.1, ex_n=5)[0] == "KILL"
    assert V(now=late, cluster_present=True, ex_mean=None, ex_n=0)[0] == "KILL"
    # Fill gate is prior to edge: both failing → PARK, and the reason mentions edge.
    parked = V(now=late, fill_rate=0.10, mean=-1.0, pos_rate=0.2)
    assert parked[0] == "PARK" and "Edge gate would also fail" in parked[1]

    # Fill simulation: signal bar is not a fill; first later print at/under limit is.
    ts = [1000.0, 1100.0, 1100.0 + H48]
    px = [100.0, 99.0, 101.0]
    hit = first_fill(ts, px, 1000.0, 99.5)
    assert hit == (1100.0, 99.0), hit
    ret = forward_from_fill(ts, px, hit[0], hit[1])
    assert abs(ret - ((101 / 99 - 1) * 100)) < 1e-9, ret
    assert first_fill(ts, px, 1000.0, 98.0) is None  # 99 is above 98
    assert first_fill([1000.0], [99.0], 1000.0, 99.0) is None  # signal bar excluded
    assert first_fill([1000.0 + FILL_WINDOW_S + 1], [90.0], 1000.0, 99.0) is None
    assert first_fill([1000.0 + FILL_WINDOW_S], [90.0], 1000.0, 99.0) == (
        1000.0 + FILL_WINDOW_S, 90.0
    )

    # Cluster: three CDT days in one ISO week, and the larger week is excluded.
    # 2026-07-20 is a Monday. Three days that week vs a later week with more days
    # but fewer signals should lose to the signal-count key.
    def ep(y, m, d, hour=12):
        return datetime(y, m, d, hour, tzinfo=CENTRAL).timestamp()

    epochs = [
        ep(2026, 7, 20), ep(2026, 7, 21), ep(2026, 7, 22),  # 3 days, 3 signals
        ep(2026, 7, 20, 18),                                 # extra signal, same week
        ep(2026, 8, 3), ep(2026, 8, 4), ep(2026, 8, 5),      # 3 days, 3 signals
    ]
    clusters, largest = capitulation_clusters(epochs)
    assert len(clusters) == 2, clusters.keys()
    assert len(clusters[largest]["idxs"]) == 4  # July week has 4 signals

    # Eight-day spacing: at most one signal day per ISO week, so the regime
    # gate is vacuous and the fill/edge floors are what the verdict tests.
    base = ep(2026, 9, 1)
    signals = []
    tape_ts = defaultdict(list)
    tape_px = defaultdict(list)
    for i in range(40):
        ts = base + i * 8 * 86400
        signals.append({
            "ts": ts, "pair": "BTC-USDC", "price": 100.0, "limit": 99.0,
            "rel_strength": 0.0,
        })
        tape_ts["BTC-USDC"].append(ts + 60)  # one minute later
        if i % 2 == 0:
            tape_px["BTC-USDC"].append(98.0)  # fills
            tape_ts["BTC-USDC"].append(ts + 60 + H48)
            tape_px["BTC-USDC"].append(98.0 * 1.015)  # +1.5%
        else:
            tape_px["BTC-USDC"].append(101.0)  # above limit, no fill
    tape = {"BTC-USDC": (tape_ts["BTC-USDC"], tape_px["BTC-USDC"])}
    report = grade_signals(signals, tape, late)
    assert report["n_filled"] == 20, report["n_filled"]
    assert report["fill_rate"] == 0.5
    assert report["n_resolved"] == 20
    assert report["clusters"] == {}
    assert report["verdict"] == "EXTEND"  # 20 < 25

    signals = []
    tape_ts = defaultdict(list)
    tape_px = defaultdict(list)
    for i in range(50):
        ts = base + i * 8 * 86400
        signals.append({
            "ts": ts, "pair": "ETH-USDC", "price": 100.0, "limit": 99.0,
            "rel_strength": -1.0 if i < 25 else 1.0,
        })
        tape_ts["ETH-USDC"].append(ts + 60)
        if i < 25:
            tape_px["ETH-USDC"].append(98.0)
            tape_ts["ETH-USDC"].append(ts + 60 + H48)
            tape_px["ETH-USDC"].append(98.0 * 1.012)  # +1.2%
        else:
            tape_px["ETH-USDC"].append(110.0)
    tape = {"ETH-USDC": (tape_ts["ETH-USDC"], tape_px["ETH-USDC"])}
    report = grade_signals(signals, tape, late)
    assert report["n_filled"] == 25 and report["n_resolved"] == 25
    assert report["clusters"] == {}
    assert report["verdict"] == "KEEP", report["reason"]

    # Regime: one Mon–Wed cluster of +5% fills carries the pooled mean.
    # Isolated −2% fills outside it must pull the jackknife through 0 → KILL.
    monday = datetime(2026, 7, 20, 12, tzinfo=CENTRAL)
    while monday.weekday() != 0:
        monday += timedelta(days=1)
    signals = []
    events = []
    for i in range(20):
        ts = (monday + timedelta(days=i % 3, minutes=i)).timestamp()
        signals.append({
            "ts": ts, "pair": "SOL-USDC", "price": 100.0, "limit": 99.0,
            "rel_strength": None,
        })
        events.append((ts + 60, 98.0))
        events.append((ts + 60 + H48, 98.0 * 1.05))
    for i in range(10):
        ts = (datetime(2026, 5, 4, 12, tzinfo=CENTRAL) + timedelta(days=14 * i)).timestamp()
        signals.append({
            "ts": ts, "pair": "SOL-USDC", "price": 100.0, "limit": 99.0,
            "rel_strength": None,
        })
        events.append((ts + 60, 98.0))
        events.append((ts + 60 + H48, 98.0 * 0.98))
    for i in range(10):
        ts = (datetime(2026, 5, 4, 18, tzinfo=CENTRAL) + timedelta(days=14 * i)).timestamp()
        signals.append({
            "ts": ts, "pair": "SOL-USDC", "price": 100.0, "limit": 99.0,
            "rel_strength": None,
        })
        events.append((ts + 60, 150.0))  # above the limit: not a fill
    events.sort()
    tape = {"SOL-USDC": ([t for t, _ in events], [p for _, p in events])}
    report = grade_signals(signals, tape, late)
    assert report["n_filled"] == 30, report["n_filled"]
    assert report["fill_rate"] == 0.75
    assert report["largest"] is not None
    assert report["ex_n"] == 10, report["ex_n"]
    assert report["ex_mean"] < 0
    assert report["mean"] > 1.0
    assert report["verdict"] == "KILL", report["reason"]

    bare = grade_signals(
        [{"ts": ep(2026, 8, 1), "pair": "NOPE-USDC", "price": 1.0,
          "limit": 0.9, "rel_strength": None}],
        {},
        late,
    )
    assert bare["verdict"] == "EXTEND" and "JOIN INCOMPLETE" in bare["reason"]

    text = render(
        report, late, {"funding": 0}, "limit note", "covariate note",
    )
    assert "VERDICT: KILL" in text
    assert "does not change CRYPTO_BUYS_DISABLED" in text
    assert "BAND_0905" in text
    assert "not a live unlock" in text
    print("crypto_d1_shadow_grade self-test OK")


def main():
    ap = argparse.ArgumentParser(description="Read-only D1 shadow grader V1.0")
    ap.add_argument("--self-test", action="store_true",
                    help="Check gates, fill window, and cluster jackknife; no DB")
    args = ap.parse_args()
    if args.self_test:
        _self_test()
        return
    now = datetime.now(CENTRAL)
    conn = _connect()
    try:
        text, _report = load_and_grade(conn, now)
    finally:
        conn.close()
    sys.stdout.write(text)


if __name__ == "__main__":
    main()
