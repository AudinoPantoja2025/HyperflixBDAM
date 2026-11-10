"""Pruebas aisladas del Quality Gate Spark."""

import os
import sys

import pytest
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import Row

from src.data_quality_gate import simulate_dirty_data, validate_collection


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.Builder().master("local[2]")
        .appName("quality-gate-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_quality_gate_rechaza_pelicula_con_titulo_rating_y_runtime_invalidos(spark):
    rows = [
        Row(_id="m1", title="Película válida", runtime=100, year=2020, imdb=Row(rating=8.0)),
        Row(_id="m2", title=None, runtime=-1, year=1800, imdb=Row(rating=11.0)),
    ]
    result = validate_collection(spark.createDataFrame(rows), "movies")

    assert result.metrics["total_records"] == 2
    assert result.metrics["valid_records"] == 1
    assert result.metrics["rejected_records"] == 1
    rejected = result.rejected_df.select("quality_errors").first()
    assert rejected is not None
    assert "movie_title_required" in rejected["quality_errors"]


def test_quality_gate_detecta_duplicados_y_conserva_cuarentena(spark):
    rows = [
        Row(_id="m1", title="Uno", runtime=90, year=2020, imdb=Row(rating=7.0)),
        Row(_id="m1", title="Uno", runtime=90, year=2020, imdb=Row(rating=7.0)),
    ]
    result = validate_collection(spark.createDataFrame(rows), "movies")

    assert result.metrics["valid_records"] == 0
    assert result.rejected_df.count() == 2
    assert result.quarantine_df.select("record_id", "original_record").count() == 2


def test_simulacion_no_modifica_dataframe_original(spark):
    original = spark.createDataFrame(
        [Row(_id="m1", title="Uno", runtime=90, year=2020, imdb=Row(rating=7.0))]
    )
    dirty = simulate_dirty_data(original, "movies")

    assert original.count() == 1
    assert dirty.count() == 3
    assert dirty.where("_simulated_dirty_record").count() == 2


def test_simulacion_movies_usa_imdb_rating_cuando_imdb_es_string(spark):
    original = spark.createDataFrame(
        [
            Row(
                _id="m1",
                title="Uno",
                runtime=90.0,
                year=2020.0,
                imdb='{"rating": 7.0}',
                imdb_rating=7.0,
            )
        ]
    )

    dirty = simulate_dirty_data(original, "movies")

    assert dirty.where("_simulated_dirty_reason = 'invalid_imdb_rating'").count() == 2
    assert dirty.where("imdb_rating = 11.0").count() == 2


def test_comments_valida_referencia_a_movies(spark):
    movies = spark.createDataFrame([Row(_id="m1", title="Uno")])
    comments = spark.createDataFrame(
        [Row(_id="c1", movie_id="missing", text="comentario")]
    )

    result = validate_collection(comments, "comments", movies_df=movies)

    assert result.metrics["rejected_records"] == 1
    rejected = result.rejected_df.first()
    assert rejected is not None
    assert "comment_movie_reference_exists" in rejected["quality_errors"]
