"""Pruebas Fase 3 Data Warehouse (sin conexion real, con mocks)."""

from unittest.mock import MagicMock

import pytest

from src import data_warehouse as dw


def _mock_db(existing=("interacciones_analiticas",), counts=None):
    db = MagicMock()
    db.name = "sample_mflix"
    db.list_collection_names.return_value = list(existing)
    counts = counts or {}
    db.__getitem__.side_effect = lambda n: _mock_coll(n, counts.get(n, 0))
    return db


def _mock_coll(name, count=0):
    coll = MagicMock()
    coll.count_documents.return_value = count
    coll.aggregate.return_value = []
    return coll


def test_pipelines_usan_group_project_merge_y_operadores_doc():
    t = dw.build_dim_tiempo_pipeline()
    assert [list(e)[0] for e in t] == ["$group", "$project", "$merge"]
    project = t[1]["$project"]
    assert "$ceil" in str(project) and "$divide" in str(project)  # trimestre
    assert "$dayOfWeek" in str(t[1]) and "$cond" in str(t[1])  # fin de semana correcto

    c = dw.build_dim_usuario_pipeline()
    assert "$first" in str(c) and "$sum" in str(c)
    assert "$avg" in str(c) and "$round" in str(c)

    p = dw.build_dim_pelicula_pipeline()
    assert "$sum" in str(p) and "$ifNull" in str(p[0])  # nulos -> SIN_PELICULA

    f = dw.build_fact_interacciones_pipeline()
    proj = f[0]["$project"]
    assert {"interaccion_id", "fecha_id", "usuario_id", "pelicula_id",
            "cantidad", "rating_pelicula", "longitud_comentario"} <= set(proj)
    assert t[2]["$merge"]["whenMatched"] == "replace"
    assert c[2]["$merge"]["whenMatched"] == "replace"
    assert p[2]["$merge"]["whenMatched"] == "replace"
    assert f[1]["$merge"]["whenMatched"] == "replace"


def test_run_ejecuta_4_agregaciones_con_allow_disk_use_y_devuelve_counts():
    counts = {"interacciones_analiticas": 6, "dim_tiempo": 4,
              "dim_usuario": 4, "dim_pelicula": 3, "fact_interacciones": 6}
    colls = {n: _mock_coll(n, counts[n]) for n in counts}
    db = MagicMock()
    db.name = "sample_mflix"
    db.list_collection_names.return_value = ["interacciones_analiticas"]
    db.__getitem__.side_effect = lambda n: colls[n]

    out = dw.run_data_warehouse(db, drop_first=True, allow_disk_use=True)
    assert colls["interacciones_analiticas"].aggregate.call_count == 4
    for call in colls["interacciones_analiticas"].aggregate.call_args_list:
        assert call.kwargs.get("allowDiskUse") is True
    assert out["counts"]["fact_interacciones"] == 6
    for n in ("dim_tiempo", "dim_usuario", "dim_pelicula", "fact_interacciones"):
        colls[n].drop.assert_called_once()


def test_run_falla_si_no_hay_fuente_y_sin_seed():
    db = _mock_db(existing=[])
    with pytest.raises(dw.DataWarehouseError):
        dw.run_data_warehouse(db, seed_if_empty=False)


def test_ensure_seed_solo_si_vacia():
    db = _mock_db()
    coll = _mock_coll("interacciones_analiticas", count=5)
    db.__getitem__.side_effect = lambda n: coll
    assert dw.ensure_seed(db) == 0
    coll2 = _mock_coll("interacciones_analiticas", count=0)
    db.__getitem__.side_effect = lambda n: coll2
    assert dw.ensure_seed(db) == len(dw.demo_seed_docs())
    coll2.insert_many.assert_called_once()
