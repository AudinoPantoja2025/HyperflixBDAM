"""Pruebas de validadores de configuracion y calidad."""

import pandas as pd
import pytest

from src.validators import (
    check_dataframe_schema,
    check_parquet_readable,
    validate_settings,
)


def test_validacion_configuracion_ok(settings):
    assert validate_settings(settings) is True


def test_validacion_incompleta_lanza_error(settings):
    class Incompleto:
        mongodb_uri = ""
        mongodb_database = "sample_mflix"
        mongodb_collections = ["comments"]
        batch_size = 1000

    with pytest.raises(ValueError) as exc:
        validate_settings(Incompleto())
    assert "MONGODB_URI" in str(exc.value)


def test_esquema_esperado_ok():
    df = pd.DataFrame({"_id": ["a"], "name": ["n"]})
    assert check_dataframe_schema(df, ["_id", "name"], "test") is True


def test_esquema_faltante_lanza_error():
    df = pd.DataFrame({"_id": ["a"]})
    with pytest.raises(ValueError):
        check_dataframe_schema(df, ["_id", "name"], "test")


def test_parquet_legible(tmp_path):
    df = pd.DataFrame({"x": [1, 2, 3]})
    df.to_parquet(tmp_path / "check.parquet", engine="pyarrow", compression="snappy")
    assert check_parquet_readable(tmp_path / "check.parquet") == 3