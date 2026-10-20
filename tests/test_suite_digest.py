import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import suite_digest as sd

BANNER = "DRY RUN — no fingerprint deletes or inserts\n"


def test_no_env_sends_everything(monkeypatch):
    monkeypatch.delenv(sd.ENV, raising=False)
    assert sd.capture(BANNER + "⚡ PHASE4 BACKTESTER V1.6 STARTING") is False
    assert sd.capture(BANNER + "✅ PHASE4 BACKTESTER V1.6 COMPLETE") is False


def test_starting_dropped_complete_buffered_failure_sent(tmp_path, monkeypatch):
    p = tmp_path / "d.txt"
    monkeypatch.setenv(sd.ENV, str(p))
    assert sd.capture(BANNER + "🔥 NEXUS BERSERKER BACKTESTER V4.0 STARTING\nx") is True
    assert sd.capture(BANNER + "✅ NEXUS BERSERKER BACKTESTER V4.0 COMPLETE\n"
                      "Holdout:   1061 trades  avg -0.075%  PF 0.86\n"
                      "Carry-all holdout: 1033\nDrop-bottom holdout: 488") is True
    assert sd.capture(BANNER + "✅ PHASE4 BACKTESTER V1.6 COMPLETE\nValidation (last 25%): 55.2% WR") is True
    assert sd.capture("❌ CRYPTO crashed: KeyError") is False
    blocks = sd.read_blocks(str(p))
    assert len(blocks) == 2
    assert all("DRY RUN" not in b for b in blocks)
    assert "Carry-all" in blocks[0] and "Drop-bottom" in blocks[0]


def test_summary_has_numbers_and_writes_off(tmp_path, monkeypatch):
    p = tmp_path / "d.txt"
    monkeypatch.setenv(sd.ENV, str(p))
    sd.capture("✅ NEXUS CRYPTO BACKTESTER V3.0 COMPLETE\nOverall WR: 18.8%\n  MOMENTUM: 576 trades | 18.8% WR | avg -2.429%")
    sd.capture("✅ SCANNER BACKTESTER V1.1 COMPLETE\nTraining: 15141 trades | 37.4% WR\nOOS:      695 trades | 37.8% WR")
    res = {"Berserker": {"ok": True, "elapsed": 417}, "Crypto": {"ok": False, "elapsed": 9}}
    txt = sd.build_summary(BANNER, sd.read_blocks(str(p)), res, 2366, False)
    assert txt.startswith("DRY RUN")
    assert txt.count("DRY RUN") == 1
    for s in ["Overall WR: 18.8%", "avg -2.429%", "OOS:", "Total: 2366s (39min)",
              "Writes: OFF", "❌ Crypto: 9s", "✅ Berserker: 417s"]:
        assert s in txt


def test_summary_truncates_but_keeps_tail():
    res = {"A": {"ok": True, "elapsed": 1}}
    txt = sd.build_summary("", ["x" * 5000], res, 60, True)
    assert len(txt) <= sd.TG_LIMIT
    assert txt.endswith("Writes: ON (BACKTEST_WRITE_DB). Closed bt_% rows were replaced.")
    assert "✅ A: 1s" in txt
