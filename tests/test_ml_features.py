"""Pruebas del feature engineering de ML."""

import os
import sys

import pytest

from src.ml_features import SparkJobError, dataset_ml_churn, ml_dataset_metrics, run_dataset_ml_churn


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        pyspark.sql.SparkSession.Builder()
        .master("local[2]")
        .appName("ml-features-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_run_dataset_falla_si_no_hay_parquet_en_curated(tmp_path):
    with pytest.raises(SparkJobError, match="No hay Parquet"):
        run_dataset_ml_churn(tmp_path, master=None)


def test_dataset_rfa_etiqueta_audiencia_inactiva(spark):
    rows = [
        ("i1", "2026-01-01", "u1", 8.0, 10, 1),
        ("i2", "2026-04-01", "u1", 6.0, 20, 1),
        ("i3", "2026-06-15", "u2", 9.0, 30, 1),
    ]
    fact = spark.createDataFrame(
        rows,
        [
            "interaccion_id",
            "dim_tiempo_key",
            "dim_usuario_key",
            "rating_pelicula",
            "longitud_comentario",
            "cantidad",
        ],
    )
    dataset = dataset_ml_churn(
        spark, fact, dias_inactividad=90, fecha_referencia="2026-07-15"
    )
    metrics = ml_dataset_metrics(dataset, 90)
    values = {row["dim_usuario_key"]: row.asDict() for row in dataset.collect()}

    assert values["u1"]["frequency"] == 2
    assert values["u1"]["recency"] == 105
    assert values["u1"]["churn"] == 1
    assert values["u2"]["recency"] == 30
    assert metrics["total_usuarios"] == 2
    assert metrics["usuarios_activos"] == 1
    assert metrics["usuarios_inactivos"] == 1


def test_dataset_rfa_rechaza_fecha_referencia_invalida(spark):
    fact = spark.createDataFrame(
        [("i1", "2026-01-01", "u1", 8.0, 10, 1)],
        [
            "interaccion_id",
            "dim_tiempo_key",
            "dim_usuario_key",
            "rating_pelicula",
            "longitud_comentario",
            "cantidad",
        ],
    )
    with pytest.raises(SparkJobError, match="fecha_referencia"):
        dataset_ml_churn(spark, fact, fecha_referencia="fecha-invalida")
