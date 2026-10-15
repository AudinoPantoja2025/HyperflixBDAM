"""Pruebas de transformaciones con Pandas."""

from datetime import datetime

import pandas as pd
import pytest

from src import transformations as tf


def test_docs_a_dataframe(sample_comments_docs):
    df = tf.docs_to_dataframe(sample_comments_docs)
    assert len(df) == 2
    assert {"_id", "movie_id", "name", "email", "text", "date"} <= set(df.columns)


def test_docs_vacios_devuelven_esperadas_columnas():
    df = tf.docs_to_dataframe([], expected_columns=["_id", "movie_id"])
    assert df.empty
    assert list(df.columns) == ["_id", "movie_id"]


def test_conversion_segura_de_fechas():
    df = pd.DataFrame({"fecha": ["1994-01-04", "no-valida", None]})
    out = tf.safe_to_datetime(df, ["fecha"])
    assert out["fecha"].iloc[0] == pd.Timestamp("1994-01-04")
    assert pd.isna(out["fecha"].iloc[1])  # invalida queda como NaT, no se pierde la fila


def test_deteccion_de_duplicados():
    df = pd.DataFrame({"_id": ["a", "b", "a", "c", "b"]})
    mask = tf.detect_duplicates(df, subset=["_id"])
    assert mask.sum() == 2


def test_dedupe_conserva_primera_y_reporta():
    df = pd.DataFrame({"_id": ["a", "b", "a", "c"]})
    clean, rejected = tf.deduplicate(df, subset=["_id"], report_name="test")
    assert rejected == 1
    assert list(clean["_id"]) == ["a", "b", "c"]


def test_split_duplicates_separa_sin_perder_filas():
    df = pd.DataFrame({"_id": ["a", "b", "a", "c"]})
    kept, rejected = tf.split_duplicates(df, subset=["_id"])
    assert list(kept["_id"]) == ["a", "b", "c"]
    assert list(rejected["_id"]) == ["a"]
    assert len(kept) + len(rejected) == len(df)  # ninguna fila se pierde


def test_validacion_de_columnas_obligatorias():
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError):
        tf.validate_required_columns(df, ["_id", "movie_id"], "test")


def test_balance_de_registros_no_se_pierde():
    assert tf.count_records_ok(extracted=10, processed=9, rejected=1, name="x") is True
    with pytest.raises(ValueError):
        tf.count_records_ok(extracted=10, processed=8, rejected=1, name="x")


def test_transform_users_elimina_password(sample_user_docs):
    df = tf.docs_to_dataframe(sample_user_docs)
    out = tf.transform_users(df)
    assert "password" not in out.columns
    assert "user_id" in out.columns
    assert out["user_id"].iloc[0] == "59b99db4cfa9a34dcd7885b0"


def test_transform_movies_aplana_imdb(sample_movie_docs):
    df = tf.docs_to_dataframe(sample_movie_docs)
    out = tf.transform_movies(df)
    assert out["imdb_rating"].iloc[0] == 7.4
    assert out["imdb_id"].iloc[0] == "tt0438488"
    assert out["genres"].iloc[0] == ["Short", "Western"]
    assert out["released"].iloc[0] == pd.Timestamp("1903-12-01")


def test_transform_comments_convierte_fecha(sample_comments_docs):
    df = tf.docs_to_dataframe(sample_comments_docs)
    out = tf.transform_comments(df)
    assert out["interaccion_id"].iloc[0] == sample_comments_docs[0]["_id"]
    assert out["date"].dtype == "datetime64[ns]" or "datetime64" in str(out["date"].dtype)


def test_transform_theaters_aplana_location(sample_theater_docs):
    df = tf.docs_to_dataframe(sample_theater_docs)
    out = tf.transform_theaters(df)
    assert out["address_city"].iloc[0] == "New York"
    assert out["geo_lon"].iloc[0] == -73.992983
    assert out["geo_lat"].iloc[0] == 40.759808


def test_first_element_simple():
    s = pd.Series([["a", "b"], [], ["x"], None])
    out = tf.first_element(s, "DEFAULT")
    assert list(out) == ["a", "DEFAULT", "x", "DEFAULT"]


def test_sanitize_for_parquet_serializa_diccionarios():
    df = pd.DataFrame({"a": [{"x": 1}, {}, None], "b": ["ok", "ok", "ok"]})
    out = tf.sanitize_for_parquet(df)
    assert out["a"].tolist()[:2] == ['{"x": 1}', "{}"]
    assert out["b"].tolist() == ["ok", "ok", "ok"]


def test_transform_movies_coerce_year_sucio(sample_movie_docs):
    docs = list(sample_movie_docs)
    docs.append(
        {
            "_id": "movie-dirty",
            "title": "Ano con suciedad",
            "year": "1981\xa0",  # string con espacio no rompible
            "runtime": "90",
            "rated": "PG",
            "genres": ["Drama"],
            "directors": ["Alguien"],
            "released": None,
            "imdb": {"rating": 6.0},
        }
    )
    df = tf.docs_to_dataframe(docs)
    out = tf.transform_movies(df)
    assert pd.isna(out.loc[out["movie_id"] == "movie-dirty", "year"].iloc[0])
    assert out.loc[0, "year"] == 1903
    assert out["runtime"].tolist()[1] == 90