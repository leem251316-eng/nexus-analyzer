"""One Telegram message per backtest-suite run.

run_all_backtests.py sets NEXUS_SUITE_DIGEST to a file path for its child
stages. While it is set, a stage's send_alert() hands its message here:
  - a STARTING message is dropped,
  - a COMPLETE message is appended to the digest file (not sent),
  - anything else (failure, crash, warning) returns False so the stage
    still sends it as its own Telegram message.
The orchestrator then sends one summary built from the captured blocks.
Run a stage on its own (no env var) and it alerts exactly as before.
"""

import os

ENV = "NEXUS_SUITE_DIGEST"
SEP = "\x1e\n"
TG_LIMIT = 4000
_BANNER = "DRY RUN — no fingerprint deletes or inserts"


def capture(msg: str) -> bool:
    """True when the message was absorbed and must not be sent."""
    path = os.environ.get(ENV, "")
    if not path:
        return False
    head = "\n".join(msg.splitlines()[:3])
    if "STARTING" in head:
        return True
    if "COMPLETE" in head:
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(msg.rstrip("\n") + "\n" + SEP)
            return True
        except Exception:
            return False  # can't buffer: fall back to sending it
    return False


def read_blocks(path: str) -> list:
    try:
        with open(path, encoding="utf-8") as fh:
            raw = fh.read()
    except FileNotFoundError:
        return []
    out = []
    for blk in raw.split(SEP):
        lines = [ln for ln in blk.splitlines()
                 if ln.strip() and ln.strip() != _BANNER]
        if lines:
            out.append("\n".join(lines))
    return out


def build_summary(banner: str, blocks: list, results: dict,
                  wall_elapsed: int, writing: bool) -> str:
    lines = []
    if banner.strip():
        lines.append(banner.strip())
    lines += ["📊 NEXUS BACKTEST SUITE COMPLETE", "══════════════════"]
    for b in blocks:
        lines.append(b)
        lines.append("══════════════════")
    for name, r in results.items():
        lines.append(f"{'✅' if r['ok'] else '❌'} {name}: {r['elapsed']}s")
    lines.append(f"Total: {wall_elapsed}s ({round(wall_elapsed/60)}min)")
    if writing:
        lines.append("Writes: ON (BACKTEST_WRITE_DB). Closed bt_% rows were replaced.")
    else:
        lines.append("Writes: OFF. No fingerprint rows were deleted or inserted.")
    text = "\n".join(lines)
    if len(text) > TG_LIMIT:
        tail = "\n".join(lines[-(len(results) + 2):])
        text = text[: TG_LIMIT - len(tail) - 6] + "\n…\n" + tail
    return text
