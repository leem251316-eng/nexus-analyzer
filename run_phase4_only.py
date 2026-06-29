#!/usr/bin/env python3
"""
run_phase4_only.py — Run Phase4 backtester in isolation for testing.
Drop into nexus-analyzer repo, change Railway worker start command to:
    python run_phase4_only.py
Then switch back to run_all_backtests.py when Phase4 is working.
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

def main():
    now = datetime.now(tz=CENTRAL)
    print(f"[P4-TEST] Phase4 backtester isolated test run", flush=True)
    print(f"[P4-TEST] Start: {now.strftime('%Y-%m-%d %H:%M:%S CDT')}", flush=True)
    send_alert(f"🔧 PHASE4 BACKTEST TEST\nStarting isolated run\n{now.strftime('%H:%M CDT')}")

    t0 = time.time()
    try:
        result = subprocess.run(
            [sys.executable, "phase4_backtester.py"],
            check=True,
            timeout=7200,
        )
        elapsed = round(time.time() - t0)
        print(f"[P4-TEST] ✅ Phase4 complete in {elapsed}s", flush=True)
        send_alert(f"✅ PHASE4 BACKTEST DONE\nElapsed: {elapsed}s")
    except subprocess.CalledProcessError as e:
        elapsed = round(time.time() - t0)
        print(f"[P4-TEST] ❌ Phase4 FAILED (exit {e.returncode}) after {elapsed}s", flush=True)
        send_alert(f"❌ PHASE4 BACKTEST FAILED\nExit: {e.returncode} | {elapsed}s")
    except Exception as e:
        print(f"[P4-TEST] ❌ Phase4 ERROR: {e}", flush=True)
        send_alert(f"❌ PHASE4 BACKTEST ERROR\n{e}")

if __name__ == "__main__":
    main()
