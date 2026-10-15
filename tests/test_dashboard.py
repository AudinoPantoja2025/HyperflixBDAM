"""Pruebas del dashboard (puras + PNG con Agg, sin Mongo real)."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

sys.path.insert(0, str(ROOT / "scripts"))
import dashboard_monitoreo as dash


def test_calcular_costo_mensual():
    assert dash.calcular_costo_mensual(0.39) == 57.04
    assert dash.calcular_costo_mensual(0.0) == 57.0


def test_estado_sistema():
    assert dash.estado_sistema([200.0, 240.0, 230.0]) == "OPERATIVO"
    assert dash.estado_sistema([900.0, 100.0]) == "REVISAR INDICES"
    assert dash.estado_sistema([]) == "SIN DATOS"


def test_benchmark_son_10_y_con_allowdiskuse(tmp_path=None):
    qs = dash.construir_benchmark_queries()
    assert len(qs) == 10
    assert all({"nombre", "coleccion", "pipeline"} <= set(q) for q in qs)


def test_generar_dashboard_crea_png(tmp_path):
    out = tmp_path / "dash.png"
    stats = {
        "conteos": {"users": 185, "movies": 21349, "comments": 41079},
        "total": 62513, "gb": 0.39, "costo": 57.04,
        "tendencia": [("2026-01", 100.0), ("2026-02", 120.0)],
        "meses": 2,
        "latencias": [{"nombre": "A", "ms": 200.0, "filas": 5},
                      {"nombre": "B", "ms": 240.0, "filas": 3},
                      {"nombre": "C", "ms": 230.0, "filas": 4}],
        "lat_prom": 223.33, "estado": "OPERATIVO",
    }
    assert dash.generar_dashboard(stats, str(out)) == str(out)
    assert out.exists() and out.stat().st_size > 0
