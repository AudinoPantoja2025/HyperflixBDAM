"""Pruebas del orquestador (sin ejecutar la cadena real)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

sys.path.insert(0, str(ROOT / "scripts"))
import automatizacion as auto


def test_full_steps_orden_y_completitud():
    keys = [s["key"] for s in auto.FULL_STEPS]
    assert keys == ["estructura", "olap", "etl", "dw", "benchmark",
                    "spark", "dashboard"]
    assert len(set(keys)) == len(keys)
    for s in auto.FULL_STEPS:
        assert s["timeout"] > 0 and s["desc"]
        assert s["cmd"][0] == sys.executable
        target = (ROOT / s["cmd"][1]).as_posix()
        assert Path(target).exists(), f"falta el script del paso: {target}"


def test_full_apunta_a_sample_mflix():
    dw = next(s for s in auto.FULL_STEPS if s["key"] == "dw")
    assert "sample_mflix" in dw["cmd"]
    dash = next(s for s in auto.FULL_STEPS if s["key"] == "dashboard")
    assert "sample_mflix" in dash["cmd"]


def test_run_step_ok_y_fallo():
    ok_step = {"key": "t-ok", "desc": "ok",
               "cmd": [sys.executable, "-c", "print('ok')"], "timeout": 60}
    ok, secs = auto.run_step(ok_step)
    assert ok is True and secs >= 0
    bad_step = {"key": "t-bad", "desc": "bad",
                "cmd": [sys.executable, "-c", "raise SystemExit(3)"],
                "timeout": 60}
    ok, _ = auto.run_step(bad_step)
    assert ok is False
