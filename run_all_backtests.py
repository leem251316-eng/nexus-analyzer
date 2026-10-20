#!/usr/bin/env python3
"""
run_all_backtests.py — NEXUS Backtester Orchestrator
=====================================================
Entry point for the nexus-backtester Railway worker (Procfile).
A deploy starts this process once. The Sunday cron is no longer attached,
so this file is what a merge to main will run.

Database writes are off unless BACKTEST_WRITE_DB is 1, true, yes, or on.
When it is off, every stage is launched with --dry-run. Each stage also
refuses DELETE/INSERT on its own if the variable is off, including a
direct `python phase4_backtester.py`. Telegram summaries are marked
DRY RUN. The suite still runs and still reports.

Order:
  1. Berserker (nexus_analyzer_1min_railway.py V4.0) — baseline plus research replays
  2. Phase4 (phase4_backtester.py V1.2)             — pure-Python indicators
  3. Crypto (crypto_backtester.py V3.0)             — pure-Python RSI
  4. Scanner (scanner_backtester.py V1.0)
  Per-stage backstop is 3 hours (was 2). Stages return when they finish.

Each sends its own T-Bone alerts on start/finish.
This script sends a combined summary at the end.

V1.1 (Jun 30 2026): Added Scanner stage. Scanner went live on day one with
almost no backtest evidence (SCANNER_VOL_MULT's comments reference a prior
2yr backtest for only 6 of 44 symbols, and that backtester no longer exists
in the codebase). scanner_backtester.py V1.0 gives it the same evidence
base Berserker, Phase4, and Crypto already have.

Environment: all env vars from individual backtester scripts.
"""

import os
import sys
import time
import subprocess
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

import backtest_safety
import suite_digest

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
CENTRAL          = ZoneInfo("America/Chicago")


def send_alert(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[ALERT] {msg}", flush=True)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=8
        )
    except Exception:
        pass


# Oct 11 dry run: Phase4's holdout walk and crypto's LTC pass were both
# killed at 7200s, so those stages never finished. The indicator loops no
# longer build a pandas object per bar, which is what filled the 2 hours.
# 3 hours is the backstop so a slow box still reaches the holdout and the
# last pairs. Stages return as soon as they finish.
STAGE_TIMEOUT_SEC = 10800


def run_script(script: str, label: str, extra_args: list) -> tuple:
    """Run a backtester script as a subprocess. Returns (success, elapsed_sec)."""
    cmd = [sys.executable, script, *extra_args]
    print(f"\n{'='*60}", flush=True)
    print(f"[ORCHESTRATOR] Starting {label}: {' '.join(cmd)}", flush=True)
    print(f"{'='*60}", flush=True)
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            check=True,
            timeout=STAGE_TIMEOUT_SEC,
        )
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ✅ {label} complete in {elapsed}s", flush=True)
        return True, elapsed
    except subprocess.TimeoutExpired:
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ⏱ {label} TIMED OUT after {elapsed}s", flush=True)
        send_alert(f"⏱ NEXUS BACKTEST STAGE TIMED OUT: {label} after {elapsed}s")
        return False, elapsed
    except subprocess.CalledProcessError as e:
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ❌ {label} FAILED (exit {e.returncode}) after {elapsed}s",
              flush=True)
        send_alert(f"❌ NEXUS BACKTEST STAGE FAILED: {label} (exit {e.returncode}) after {elapsed}s")
        return False, elapsed
    except Exception as e:
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ❌ {label} ERROR: {e}", flush=True)
        send_alert(f"❌ NEXUS BACKTEST STAGE ERROR: {label}: {e}")
        return False, elapsed


def main():
    now = datetime.now(tz=CENTRAL)
    writing = backtest_safety.writes_enabled()
    extra = [] if writing else ["--dry-run"]
    print(f"[ORCHESTRATOR] NEXUS Backtester Orchestrator", flush=True)
    print(f"[ORCHESTRATOR] Start: {now.strftime('%Y-%m-%d %H:%M:%S CDT')}", flush=True)
    print(
        f"[ORCHESTRATOR] BACKTEST_WRITE_DB={'ON' if writing else 'OFF (DRY RUN)'}",
        flush=True,
    )

    # One Telegram per run: children buffer COMPLETE blocks here and drop
    # STARTING messages; failures still alert on their own.
    digest_path = f"/tmp/nexus_suite_digest_{os.getpid()}.txt"
    try:
        os.remove(digest_path)
    except FileNotFoundError:
        pass
    os.environ[suite_digest.ENV] = digest_path

    wall_start = time.time()
    results    = {}

    # 1. Berserker
    ok, elapsed = run_script("nexus_analyzer_1min_railway.py", "BERSERKER V4.0", extra)
    results["Berserker"] = {"ok": ok, "elapsed": elapsed}

    # 2. Phase4
    ok, elapsed = run_script("phase4_backtester.py", "PHASE4 V1.2", extra)
    results["Phase4"] = {"ok": ok, "elapsed": elapsed}

    # 3. Crypto
    ok, elapsed = run_script("crypto_backtester.py", "CRYPTO V3.0", extra)
    results["Crypto"] = {"ok": ok, "elapsed": elapsed}

    # 4. Scanner
    ok, elapsed = run_script("scanner_backtester.py", "SCANNER V1.0", extra)
    results["Scanner"] = {"ok": ok, "elapsed": elapsed}

    # Final summary
    wall_elapsed = round(time.time() - wall_start)
    blocks = suite_digest.read_blocks(digest_path)
    os.environ.pop(suite_digest.ENV, None)
    send_alert(suite_digest.build_summary(
        backtest_safety.banner(writing), blocks, results, wall_elapsed, writing))
    print(f"\n[ORCHESTRATOR] All backtests complete. Total: {wall_elapsed}s", flush=True)


if __name__ == "__main__":
    main()
