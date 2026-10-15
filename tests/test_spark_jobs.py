"""Pruebas sin requerir Java ni una instalacion local de PySpark."""

import pytest

from src.spark_jobs import SparkJobError, latest_parquet


def test_latest_parquet_elige_la_fotografia_mas_reciente(tmp_path):
    root = tmp_path / "curated" / "FACT_INTERACCIONES"
    old = root / "ingestion_date=2026-09-15" / "run_20260915_200158.parquet"
    latest = root / "ingestion_date=2026-09-16" / "run_20260916_080000.parquet"
    old.parent.mkdir(parents=True)
    latest.parent.mkdir(parents=True)
    old.touch()
    latest.touch()

    assert latest_parquet(root) == latest


def test_latest_parquet_falla_si_no_hay_entradas(tmp_path):
    with pytest.raises(SparkJobError, match="No hay Parquet"):
        latest_parquet(tmp_path / "inexistente")
