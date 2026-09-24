#!/usr/bin/env python3
"""
crypto_sunday_lock_memo.py V1.0 — READ-ONLY Sunday lock memo.

Prints the fee-aware Sunday book stored in crypto_trade_fingerprints
(trade_id LIKE 'bt_%' AND won IS NOT NULL). Those rows are what
crypto_backtester.py V3.1 writes. This script does not replay bars and
does not scrape orchestrator logs.

GATES / BANNER (current fee, not the counterfactual)
  LOCK REINFORCED  aggregate avg pnl_pct < 0, or exactly 0.
                   Zero is not a positive print. The review trigger is
                   strict: avg > 0.
  REVIEW           aggregate avg pnl_pct > 0 under the current fee model.
                   Human review only. Not an unlock.
  NO SAMPLE        no scored rows, or avg pnl is undefined. Neither
                   banner. Lock unchanged.

FEE MATH
  Per side: CB_TAKER_FEE_PCT, default 0.012 (1.2%). Same env var V3.1
  and live crypto.py key off.
  current_rt = 2 * CB_TAKER_FEE_PCT          # fraction; default 0.024
  Stored pnl_pct is NET percentage points (V3.1: fees + slippage already
  inside the number; won is the net label).

  Alpaca-like comparison RT is 0.005 (0.50% round trip). That is tier-1
  taker 25 bps per side × 2, i.e. a 0.50% RT, not 0.50% per side.
  The add-back is exactly:

      add_back_pp = (current_rt - 0.005) * 100
      cf_avg      = stored_avg_pnl_pct + add_back_pp

  Example at the default: (0.024 - 0.005) * 100 = +1.900 percentage
  points. A stored mean of -2.410% becomes a counterfactual -0.510%.
  Slippage baked into the stored print is left alone. The adjustment
  assumes the rows were netted at the current env fee. If Sunday ran
  under a different CB_TAKER_FEE_PCT, the add-back is mis-scaled.

  Illustrative venue sensitivity. Not a claim of edge.

STRATEGY SPLIT
  MEAN_REVERSION vs MOMENTUM is printed only when a column named
  strategy, strategy_at_entry, or entry_strategy exists. V3.1's INSERT
  does not persist strategy (it is console-only in crypto_backtester.py).
  If none of those columns exist, the split is skipped with that note.
  No value is inferred from exit_reason or entry_mode.

BAND / LOCK
  Observation-only. Writes nothing (session is read-only).
  Does not read or set CRYPTO_BUYS_DISABLED. Does not propose unlocking it.
  BAND_0905 owns the live WR-touching slot. This memo is not that experiment.

USAGE
  python3 crypto_sunday_lock_memo.py
  python3 crypto_sunday_lock_memo.py --markdown sunday_lock.md
  python3 crypto_sunday_lock_memo.py --self-test

Telegram: if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID are set, a short summary
is sent (fail-open, same pattern as crypto_backtester.py). Idempotent.
"""

import argparse
import os
import sys
from collections import defaultdict

VERSION = "1.0"
ALPACA_RT = 0.005  # 0.50% round trip (25 bps/side × 2). See module docstring.

# Canonical V3.1 universe (crypto_backtester.ALL_PAIRS). POL/SUI are the
# known Alpaca map gap (MATIC/USD, SUI) called out in the Sep 24 deep dive.
PAIRS = (
    "BTC-USDC", "ETH-USDC", "SOL-USDC", "DOGE-USDC",
    "XRP-USDC", "DOT-USDC", "ADA-USDC", "LTC-USDC",
    "POL-USDC", "SUI-USDC",
)
KNOWN_GAP = ("POL-USDC", "SUI-USDC")
STRATEGY_CANDIDATES = ("strategy", "strategy_at_entry", "entry_strategy")
REQUIRED_COLS = ("trade_id", "pair", "won", "pnl_pct")


def taker_fee_pct():
    raw = os.environ.get("CB_TAKER_FEE_PCT", "0.012")
    try:
        fee = float(raw)
    except ValueError:
        print(f"CB_TAKER_FEE_PCT={raw!r} is not a float. Refusing to invent a fee.")
        sys.exit(1)
    if fee < 0 or fee > 0.2:
        print(f"CB_TAKER_FEE_PCT={fee} is outside 0..0.20. Refusing to run.")
        sys.exit(1)
    return fee


def fee_addback_pp(current_rt, alpaca_rt=ALPACA_RT):
    """Percentage points added to a stored net mean. See module docstring."""
    return (current_rt - alpaca_rt) * 100.0


def counterfactual_avg(stored_avg, current_rt, alpaca_rt=ALPACA_RT):
    if stored_avg is None:
        return None
    return stored_avg + fee_addback_pp(current_rt, alpaca_rt)


def banner_for(avg):
    """Return (code, sentence). code is LOCK / REVIEW / NO_SAMPLE."""
    if avg is None:
        return (
            "NO_SAMPLE",
            "NO SAMPLE — aggregate avg pnl is undefined. "
            "Lock unchanged. Not a review and not an unlock.",
        )
    if avg > 0:
        return (
            "REVIEW",
            "REVIEW — aggregate avg pnl > 0 under the current fee. "
            "Human review only. Not an unlock. CRYPTO_BUYS_DISABLED unchanged.",
        )
    if avg < 0:
        return (
            "LOCK",
            "LOCK REINFORCED — aggregate avg pnl < 0 under the current fee. "
            "Lock stays.",
        )
    return (
        "LOCK",
        "LOCK REINFORCED — aggregate avg pnl == 0, which is not > 0. "
        "Review trigger is strict. Lock stays.",
    )


def _fmt_pct(value, digits=3, signed=True):
    if value is None:
        return "—"
    if signed:
        return f"{value:+.{digits}f}%"
    return f"{value:.{digits}f}%"


def _fmt_wr(wr):
    if wr is None:
        return "—"
    return f"{wr * 100:.1f}%"


def render(per_pair, aggregate, strat_rows, strat_note, current_rt):
    add_pp = fee_addback_pp(current_rt)
    lines = []
    lines.append("=" * 78)
    lines.append(f"CRYPTO SUNDAY LOCK MEMO V{VERSION} — observation-only")
    lines.append("BAND_0905 owns the live WR slot. Does not change CRYPTO_BUYS_DISABLED.")
    lines.append("Not a live unlock. Writes nothing to the DB.")
    lines.append("=" * 78)
    lines.append(
        f"Source: crypto_trade_fingerprints WHERE trade_id LIKE 'bt_%' "
        f"AND won IS NOT NULL"
    )
    lines.append(
        f"Fee: CB_TAKER_FEE_PCT={current_rt / 2:.4f} per side → "
        f"RT={current_rt * 100:.3f}% ({current_rt:.4f} fraction)"
    )
    lines.append(
        f"Alpaca-like counterfactual RT = {ALPACA_RT * 100:.3f}% "
        f"({ALPACA_RT:.4f} fraction; 25 bps/side × 2)"
    )
    lines.append(
        f"Add-back = (current_rt - {ALPACA_RT:.3f}) * 100 "
        f"= {add_pp:+.3f} percentage points"
    )
    lines.append(
        "Arithmetic: stored avg pnl_pct is already net percentage points. "
        "cf_avg = stored_avg + add-back."
    )
    lines.append(
        "Illustrative only — not an edge claim. Slippage inside the stored "
        "print is not removed."
    )
    lines.append("-" * 78)
    lines.append(
        f"{'pair':<12} {'n':>6} {'WR':>8} {'avg pnl':>12} {'cf avg':>12}  flag"
    )
    seen = set()
    for pair in PAIRS:
        seen.add(pair)
        row = per_pair.get(pair)
        lines.append(_pair_line(pair, row, current_rt))
    extras = sorted(p for p in per_pair if p not in seen)
    if extras:
        lines.append("other pairs present in bt_ rows (not in the V3.1 universe):")
        for pair in extras:
            lines.append(_pair_line(pair, per_pair[pair], current_rt))
    lines.append("-" * 78)
    n, wr, avg = aggregate
    cf = counterfactual_avg(avg, current_rt)
    lines.append(
        f"{'AGGREGATE':<12} {n:>6} {_fmt_wr(wr):>8} {_fmt_pct(avg):>12} "
        f"{_fmt_pct(cf):>12}"
    )
    gap = [p for p in KNOWN_GAP if per_pair.get(p, {}).get("n", 0) == 0]
    if gap:
        lines.append(
            "EMPTY FLAG: " + ", ".join(gap)
            + " have zero bt_ rows (known Alpaca map gap: POL→MATIC/USD, SUI)."
        )
    other_empty = [
        p for p in PAIRS
        if p not in KNOWN_GAP and per_pair.get(p, {}).get("n", 0) == 0
    ]
    if other_empty:
        lines.append("EMPTY FLAG: " + ", ".join(other_empty) + " have zero bt_ rows.")
    lines.append("-" * 78)
    lines.append("MEAN_REVERSION vs MOMENTUM")
    lines.append(strat_note)
    if strat_rows:
        lines.append(f"{'strategy':<24} {'n':>6} {'WR':>8} {'avg pnl':>12} {'cf avg':>12}")
        for name, sn, swr, savg in strat_rows:
            label = "(null)" if name is None else str(name)
            lines.append(
                f"{label:<24} {sn:>6} {_fmt_wr(swr):>8} {_fmt_pct(savg):>12} "
                f"{_fmt_pct(counterfactual_avg(savg, current_rt)):>12}"
            )
    lines.append("-" * 78)
    _code, sentence = banner_for(avg if n else None)
    lines.append("BANNER: " + sentence)
    lines.append(
        "Observation-only. Idempotent. CRYPTO_BUYS_DISABLED is not read or set. "
        "BAND_0905 owns the live WR slot."
    )
    lines.append("=" * 78)
    return "\n".join(lines) + "\n"


def _pair_line(pair, row, current_rt):
    n = 0 if not row else row["n"]
    if n == 0:
        flag = "EMPTY"
        if pair in KNOWN_GAP:
            flag = "EMPTY (POL/SUI map gap)"
        return f"{pair:<12} {0:>6} {'—':>8} {'—':>12} {'—':>12}  {flag}"
    cf = counterfactual_avg(row["avg"], current_rt)
    return (
        f"{pair:<12} {n:>6} {_fmt_wr(row['wr']):>8} {_fmt_pct(row['avg']):>12} "
        f"{_fmt_pct(cf):>12}"
    )


def send_alert(text):
    token = os.environ.get("TELEGRAM_TOKEN", "")
    chat = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat:
        return
    try:
        import requests
    except ImportError:
        print("[ALERT] requests not installed; telegram skipped")
        return
    body = text if len(text) <= 3500 else text[:3500] + "\n…(truncated)"
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat, "text": body},
            timeout=8,
        )
    except Exception:
        pass


def _columns(conn, table):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY table_schema, ordinal_position
            """,
            (table,),
        )
        rows = cur.fetchall()
    if not rows:
        return None, []
    by_schema = defaultdict(list)
    for schema, name in rows:
        by_schema[schema].append(name)
    if "public" in by_schema:
        schema = "public"
    elif len(by_schema) == 1:
        schema = next(iter(by_schema))
    else:
        print(
            "SCHEMA FAIL: crypto_trade_fingerprints exists in multiple schemas: "
            + ", ".join(sorted(by_schema))
        )
        print("Check which schema the Sunday writer uses. Refusing to guess.")
        sys.exit(2)
    return schema, by_schema[schema]


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


def load(conn):
    schema, cols = _columns(conn, "crypto_trade_fingerprints")
    if schema is None:
        print("SCHEMA FAIL: table crypto_trade_fingerprints not found.")
        print("Check the Sunday writer in crypto_backtester.py V3.1.")
        sys.exit(2)
    have = {c.lower() for c in cols}
    missing = [c for c in REQUIRED_COLS if c not in have]
    if missing:
        print("SCHEMA FAIL: crypto_trade_fingerprints is missing columns: "
              + ", ".join(missing))
        print("Present columns: " + ", ".join(cols))
        print("Check crypto_backtester.py V3.1 INSERT "
              "(trade_id, pair, won, pnl_pct).")
        sys.exit(2)

    from psycopg2 import sql
    tbl = sql.Identifier(schema, "crypto_trade_fingerprints")
    where = sql.SQL("trade_id LIKE 'bt_%' AND won IS NOT NULL")

    per_pair = {}
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT pair,
                       COUNT(*)::int,
                       AVG(CASE WHEN won THEN 1.0 ELSE 0.0 END),
                       AVG(pnl_pct)
                FROM {}
                WHERE {}
                GROUP BY pair
                """
            ).format(tbl, where)
        )
        for pair, n, wr, avg in cur.fetchall():
            per_pair[pair] = {
                "n": n,
                "wr": None if wr is None else float(wr),
                "avg": None if avg is None else float(avg),
            }
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)::int,
                       AVG(CASE WHEN won THEN 1.0 ELSE 0.0 END),
                       AVG(pnl_pct)
                FROM {}
                WHERE {}
                """
            ).format(tbl, where)
        )
        n, wr, avg = cur.fetchone()
        aggregate = (
            int(n or 0),
            None if wr is None else float(wr),
            None if avg is None else float(avg),
        )

    strat_col = next((c for c in STRATEGY_CANDIDATES if c in have), None)
    strat_rows = []
    if strat_col is None:
        note = (
            "Skipped — no strategy column on crypto_trade_fingerprints. "
            "Looked for: " + ", ".join(STRATEGY_CANDIDATES) + ". "
            "V3.1 INSERT (crypto_backtester.py) stores the trade but not "
            "MEAN_REVERSION vs MOMENTUM; that split is console-only in the "
            "backtester summary. Not inferred from other columns."
        )
    else:
        note = f"Grouped by column {strat_col}."
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT {col},
                           COUNT(*)::int,
                           AVG(CASE WHEN won THEN 1.0 ELSE 0.0 END),
                           AVG(pnl_pct)
                    FROM {tbl}
                    WHERE {where}
                    GROUP BY {col}
                    ORDER BY {col}
                    """
                ).format(
                    col=sql.Identifier(strat_col),
                    tbl=tbl,
                    where=where,
                )
            )
            for name, sn, swr, savg in cur.fetchall():
                strat_rows.append((
                    name,
                    int(sn),
                    None if swr is None else float(swr),
                    None if savg is None else float(savg),
                ))
    return per_pair, aggregate, strat_rows, note


def _self_test():
    # Default Coinbase Intro: 1.2%/side → 2.4% RT. Alpaca-like RT 0.50%.
    add = fee_addback_pp(0.024)
    assert abs(add - 1.9) < 1e-12, add
    cf = counterfactual_avg(-2.410, 0.024)
    assert abs(cf - (-0.510)) < 1e-12, cf
    # Cheaper-than-Alpaca env would subtract, not add.
    assert fee_addback_pp(0.004) < 0
    assert banner_for(-2.4)[0] == "LOCK"
    assert banner_for(0.0)[0] == "LOCK"
    assert banner_for(0.001)[0] == "REVIEW"
    assert banner_for(None)[0] == "NO_SAMPLE"
    text = render(
        {"BTC-USDC": {"n": 2, "wr": 0.5, "avg": -2.4}, "ETH-USDC": {"n": 1, "wr": 0.0, "avg": -1.0}},
        (3, 1 / 3, -2.0),
        [],
        "Skipped — test",
        0.024,
    )
    assert "LOCK REINFORCED" in text
    assert "POL-USDC" in text and "EMPTY" in text
    assert "SUI-USDC" in text
    assert "+1.900 percentage points" in text
    # Positive aggregate must say REVIEW and must not say unlock-as-action.
    up = render(
        {"BTC-USDC": {"n": 4, "wr": 0.75, "avg": 0.4}},
        (4, 0.75, 0.4),
        [("MEAN_REVERSION", 4, 0.75, 0.4)],
        "Grouped by column strategy.",
        0.024,
    )
    assert "BANNER: REVIEW" in up
    assert "Not an unlock" in up
    assert "MEAN_REVERSION" in up
    print("crypto_sunday_lock_memo self-test OK")


def main():
    ap = argparse.ArgumentParser(description="Read-only Sunday crypto lock memo V1.0")
    ap.add_argument("--markdown", metavar="PATH",
                    help="Also write the report to this markdown path")
    ap.add_argument("--self-test", action="store_true",
                    help="Check fee math and banners; no DB")
    args = ap.parse_args()
    if args.self_test:
        _self_test()
        return

    current_rt = 2.0 * taker_fee_pct()
    conn = _connect()
    try:
        per_pair, aggregate, strat_rows, note = load(conn)
    finally:
        conn.close()
    for pair in PAIRS:
        per_pair.setdefault(pair, {"n": 0, "wr": None, "avg": None})
    text = render(per_pair, aggregate, strat_rows, note, current_rt)
    sys.stdout.write(text)
    if args.markdown:
        with open(args.markdown, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"wrote {args.markdown}", file=sys.stderr)
    send_alert(text)


if __name__ == "__main__":
    main()
