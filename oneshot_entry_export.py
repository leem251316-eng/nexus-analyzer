#!/usr/bin/env python3
"""ONE-SHOT, READ-ONLY entry-feature export (temporary; reverted after one run).

run_all_backtests.py hands off here while this file's hook is in place. It:
  * forces BACKTEST_WRITE_DB=0 and blanks DATABASE_URL / Telegram vars,
  * replaces psycopg2.connect with a function that raises,
  * blocks every requests call that is not a GET, and any Alpaca host other
    than data.alpaca.markets (no trading API, no orders),
  * runs export_entry_features.py with stdout captured,
  * prints the CSV to the deploy log as gzip+base64 chunks (EFX lines),
  * exits 0 so Railway does not restart and rerun it.
"""
import base64, gzip, hashlib, io, os, sys, time, traceback, contextlib
from urllib.parse import urlparse

for k in ("DATABASE_URL", "DATABASE_PUBLIC_URL", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "TELEGRAM_BOT_TOKEN"):
    os.environ[k] = ""
os.environ["BACKTEST_WRITE_DB"] = "0"

import psycopg2, requests
def _blocked(*a, **k): raise RuntimeError("blocked by read-only guard")
psycopg2.connect = _blocked
_orig_request = requests.Session.request
def _guarded(self, method, url, *a, **k):
    host = (urlparse(url).hostname or "").lower()
    if str(method).upper() != "GET":
        raise RuntimeError(f"read-only guard: {method} {host} blocked")
    if "alpaca.markets" in host and host != "data.alpaca.markets":
        raise RuntimeError(f"read-only guard: Alpaca host {host} blocked")
    return _orig_request(self, method, url, *a, **k)
requests.Session.request = _guarded
requests.post = requests.put = requests.delete = requests.patch = _blocked


def main():
    t0 = time.time()
    print("[EFX] one-shot entry-feature export starting (read-only guard on)", flush=True)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "export_entry_features.py")
    buf = io.StringIO()
    sys.argv = [path]
    try:
        with contextlib.redirect_stdout(buf):
            exec(compile(open(path).read(), path, "exec"), {"__name__": "__export__", "__file__": path})
    except BaseException:
        print("[EFX] FAILED:\n" + traceback.format_exc(), flush=True)
        partial = buf.getvalue()
        print(f"[EFX] partial rows captured: {max(partial.count(chr(10)) - 1, 0)}", flush=True)
        if not partial:
            return
    data = buf.getvalue().encode()
    rows = max(data.count(b"\n") - 1, 0)
    b64 = base64.b64encode(gzip.compress(data, 9)).decode()
    n = 1500
    chunks = [b64[i:i + n] for i in range(0, len(b64), n)]
    print(f"[EFX] BEGIN rows={rows} bytes={len(data)} sha256={hashlib.sha256(data).hexdigest()} "
          f"chunks={len(chunks)} elapsed={round(time.time() - t0)}s", flush=True)
    for i, c in enumerate(chunks):
        print(f"EFX {i:04d}/{len(chunks):04d} {c}", flush=True)
        time.sleep(0.05)
    print(f"[EFX] END rows={rows}", flush=True)


if __name__ == "__main__":
    main()
