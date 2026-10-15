"""Pruebas del escritor Parquet (Snappy) y su relectura."""

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.parquet_writer import ParquetWriteError, parquet_file_info, read_parquet, write_parquet


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "id": ["a1", "a2", "a3"],
            "importe": [10.5, 20.0, 7.25],
            "ok": [True, False, True],
        }
    )


def test_escritura_parquet_snappy(df, tmp_path):
    meta = write_parquet(df, tmp_path, run_id="RUN-1", dataset_name="test")
    assert meta["records"] == 3
    assert meta["compression"] == "snappy"
    assert tmp_path / "run_RUN-1.parquet" in list(tmp_path.glob("*.parquet"))

    info = parquet_file_info(tmp_path / "run_RUN-1.parquet")
    assert info["rows"] == 3
    assert info["compression"] == "SNAPPY"


def test_relectura_correcta(df, tmp_path):
    write_parquet(df, tmp_path, run_id="RUN-2", dataset_name="test")
    back = read_parquet(tmp_path / "run_RUN-2.parquet")
    pd.testing.assert_frame_equal(df, back)


def test_lectura_preserva_tipos_y_esquema(df, tmp_path):
    write_parquet(df, tmp_path, run_id="RUN-3", dataset_name="test")
    pf = pq.ParquetFile(tmp_path / "run_RUN-3.parquet")
    assert sorted(pf.schema.names) == ["id", "importe", "ok"]
    assert pf.metadata.num_rows == 3


def test_no_sobrescribe_corridas_previas(df, tmp_path):
    write_parquet(df, tmp_path, run_id="RUN-1", dataset_name="test")
    with pytest.raises(ParquetWriteError):
        write_parquet(df, tmp_path, run_id="RUN-1", dataset_name="test")


def test_datos_vacios_tambien_son_validos(tmp_path):
    empty = pd.DataFrame({"a": pd.Series(dtype="object"), "b": pd.Series(dtype="object")})
    meta = write_parquet(empty, tmp_path, run_id="EMPTY", dataset_name="test")
    assert meta["records"] == 0
    back = read_parquet(tmp_path / "run_EMPTY.parquet")
    assert back.empty