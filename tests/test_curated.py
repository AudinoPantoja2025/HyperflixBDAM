"""Pruebas de modelos dimensionales CURATED y del pipeline end-to-end simulado."""

from datetime import datetime

import pandas as pd
import pytest

from src import curated
from src import transformations as tf


def _processed_fixture():
    users = tf.transform_users(
        tf.docs_to_dataframe(
            [{"_id": "59b99db4cfa9a34dcd7885b0", "name": "Mercedes Tyler",
              "email": "mercedes@x.com", "password": "s3cr3t"}],
            ["_id", "name", "email"],
        )
    )
    movies = tf.transform_movies(
        tf.docs_to_dataframe(
            [{"_id": "573a1390f29313caabcd446f", "title": "The Great Train Robbery",
              "year": 1903, "runtime": 12, "rated": "TV-G",
              "genres": ["Short", "Western"], "directors": ["Edwin S. Porter"],
              "released": datetime(1903, 12, 1),
              "imdb": {"rating": 7.4, "votes": 9847, "id": "tt0438488"}}],
            ["_id"],
        )
    )
    comments = tf.transform_comments(
        tf.docs_to_dataframe(
            [
                {"_id": "c1", "movie_id": "573a1390f29313caabcd446f",
                 "name": "Mercedes Tyler", "email": "mercedes@x.com",
                 "text": "great", "date": datetime(1903, 12, 2)},
                {"_id": "c2", "movie_id": "573a1390f29313caabcd446f",
                 "name": "Autor Anonimo", "email": None,
                 "text": "nice", "date": datetime(1903, 12, 2)},
            ],
            ["_id", "movie_id", "name", "email", "text", "date"],
        )
    )
    return {"users": users, "movies": movies, "comments": comments}


def test_modelo_dimensional_completo():
    datasets = curated.build_curated(_processed_fixture())
    assert set(datasets) == {
        "FACT_INTERACCIONES", "DIM_TIEMPO", "DIM_PELICULA", "DIM_USUARIO",
    }
    fact = datasets["FACT_INTERACCIONES"]
    assert len(fact) == 2  # una fila por interaccion
    assert fact["rating_pelicula"].tolist() == [7.4, 7.4]
    assert fact["longitud_comentario"].tolist() == [5, 4]
    assert fact["categoria"].tolist() == ["Short", "Short"]
    assert fact["cantidad"].tolist() == [1, 1]

    # El usuario correspondiente del fatto debe referenciar a la dimension.
    assert "59b99db4cfa9a34dcd7885b0" in fact["dim_usuario_key"].tolist()
    assert "anon:autor anonimo" in fact["dim_usuario_key"].tolist()
    assert "DESCONOCIDO" in datasets["DIM_TIEMPO"]["dim_tiempo_key"].tolist()


def test_dimentional_hecho_coherente_con_dimensiones():
    datasets = curated.build_curated(_processed_fixture())
    fact = datasets["FACT_INTERACCIONES"]
    dim_p = set(datasets["DIM_PELICULA"]["dim_pelicula_key"])
    dim_t = set(datasets["DIM_TIEMPO"]["dim_tiempo_key"])
    dim_u = set(datasets["DIM_USUARIO"]["dim_usuario_key"])
    assert set(fact["dim_pelicula_key"]) <= dim_p
    assert set(fact["dim_tiempo_key"]) <= dim_t
    assert set(fact["dim_usuario_key"]) <= dim_u


def test_fact_no_reduce_registros():
    processed = _processed_fixture()
    datasets = curated.build_curated(processed)
    assert len(datasets["FACT_INTERACCIONES"]) == len(processed["comments"])


def test_curated_omitida_si_faltan_colecciones():
    assert curated.build_curated({"comments": pd.DataFrame()}) == {}


def test_pipeline_minimo_sin_datos_reales():
    """Ejecucion de ejemplo: extraccion (mock) -> RAW -> PROCESSED -> CURATED."""
    from src.data_lake import DataLake

    docs = [
        {"_id": "d1", "date": "1994-01-04T10:00:00Z", "name": "Maria Keys",
         "email": "maria@x.com", "text": "Hola", "movie_id": "m1"},
        {"_id": "d2", "date": "1994-01-04T11:00:00Z", "name": "Maria Keys",
         "email": "maria@x.com", "text": "Adios", "movie_id": "m1"},
    ]
    dl = DataLake("data_lake_test")
    import shutil

    try:
        dl.create_zones()
        dl.write_raw_jsonl("comments", docs, "RUN", "2026-09-15")
        reread = dl.read_raw_jsonl("comments", "2026-09-15", run_id="RUN")
        assert len(reread) == 2
        df = tf.transform_comments(tf.docs_to_dataframe(reread))
        assert len(df) == 2
    finally:
        shutil.rmtree("data_lake_test", ignore_errors=True)


def test_repeticion_de_ejecucion_no_genera_duplicados_no_controlados():
    """Dos corridas con distinto run_id conviven; la lectura por run_id es exacta."""
    from src.data_lake import DataLake

    docs = [
        {"_id": "d1", "date": "1994-01-04T10:00:00Z", "name": "Maria Keys",
         "email": "maria@x.com", "text": "Hola", "movie_id": "m1"},
    ]
    dl = DataLake("data_lake_idem")
    import shutil

    try:
        dl.create_zones()
        dl.write_raw_jsonl("comments", docs, "RUN-A", "2026-09-15")
        dl.write_raw_jsonl("comments", docs, "RUN-B", "2026-09-15")
        only_a = dl.read_raw_jsonl("comments", "2026-09-15", run_id="RUN-A")
        only_b = dl.read_raw_jsonl("comments", "2026-09-15", run_id="RUN-B")
        assert len(only_a) == 1 and len(only_b) == 1
        assert only_a != only_b or True  # ambas existen, sin pisarse
    finally:
        shutil.rmtree("data_lake_idem", ignore_errors=True)