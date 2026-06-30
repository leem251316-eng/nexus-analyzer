#!/usr/bin/env python3
"""
run_all_backtests.py — NEXUS Backtester Orchestrator V1.1
==========================================================
Entry point for nexus-analyzer Railway worker (genuine-reverence).
Runs all four backtests in sequence every Sunday 11pm UTC.

Railway cron: 0 23 * * 0

Order:
  1. Berserker (nexus_analyzer_1min_railway.py V3.0) — ~35-45 min
  2. Phase4 (phase4_backtester.py V1.2)             — ~45-60 min
  3. Crypto (crypto_backtester.py V3.0)             — ~15-25 min
  4. Scanner (scanner_backtester.py V1.0)           — ~15-25 min

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


def run_script(script: str, label: str) -> tuple:
    """Run a backtester script as a subprocess. Returns (success, elapsed_sec)."""
    print(f"\n{'='*60}", flush=True)
    print(f"[ORCHESTRATOR] Starting {label}", flush=True)
    print(f"{'='*60}", flush=True)
    t0 = time.time()
    try:
        result = subprocess.run(
            [sys.executable, script],
            check=True,
            timeout=7200,   # 2hr hard limit per script
        )
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ✅ {label} complete in {elapsed}s", flush=True)
        return True, elapsed
    except subprocess.TimeoutExpired:
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ⏱ {label} TIMED OUT after {elapsed}s", flush=True)
        return False, elapsed
    except subprocess.CalledProcessError as e:
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ❌ {label} FAILED (exit {e.returncode}) after {elapsed}s",
              flush=True)
        return False, elapsed
    except Exception as e:
        elapsed = round(time.time() - t0)
        print(f"[ORCHESTRATOR] ❌ {label} ERROR: {e}", flush=True)
        return False, elapsed


def main():
    now = datetime.now(tz=CENTRAL)
    print(f"[ORCHESTRATOR] NEXUS Backtester Orchestrator V1.1", flush=True)
    print(f"[ORCHESTRATOR] Start: {now.strftime('%Y-%m-%d %H:%M:%S CDT')}", flush=True)

    send_alert(
        f"🚀 NEXUS BACKTESTER SUITE STARTING\n"
        f"4 systems: Berserker → Phase4 → Crypto → Scanner\n"
        f"ETA: ~2.5-3 hours total\n"
        f"{now.strftime('%H:%M CDT')}"
    )

    wall_start = time.time()
    results    = {}

    # 1. Berserker
    ok, elapsed = run_script("nexus_analyzer_1min_railway.py", "BERSERKER V3.0")
    results["Berserker"] = {"ok": ok, "elapsed": elapsed}

    # 2. Phase4
    ok, elapsed = run_script("phase4_backtester.py", "PHASE4 V1.2")
    results["Phase4"] = {"ok": ok, "elapsed": elapsed}

    # 3. Crypto
    ok, elapsed = run_script("crypto_backtester.py", "CRYPTO V3.0")
    results["Crypto"] = {"ok": ok, "elapsed": elapsed}

    # 4. Scanner
    ok, elapsed = run_script("scanner_backtester.py", "SCANNER V1.0")
    results["Scanner"] = {"ok": ok, "elapsed": elapsed}

    # Final summary
    wall_elapsed = round(time.time() - wall_start)
    lines = ["📊 NEXUS BACKTEST SUITE COMPLETE", "──────────────────"]
    for name, r in results.items():
        status = "✅" if r["ok"] else "❌"
        lines.append(f"{status} {name}: {r['elapsed']}s")
    lines.append(f"──────────────────")
    lines.append(f"Total: {wall_elapsed}s ({round(wall_elapsed/60)}min)")
    lines.append(f"Next run: Sunday 11pm UTC")

    send_alert("\n".join(lines))
    print(f"\n[ORCHESTRATOR] All backtests complete. Total: {wall_elapsed}s", flush=True)


if __name__ == "__main__":
    main()
