"""Escritura de DataFrames en Apache Parquet con compresion Snappy.

Soporta dos backends de almacenamiento:
- Local: rutas Path normales (comportamiento original).
- S3: rutas ``s3://bucket/prefijo/`` via ``s3fs`` y ``pyarrow``.

El contrato es estricto: no se sobrescriben archivos de corridas anteriores
(el nombre incluye el ``run_id``), y todo archivo escrito se relee de inmediato
para verificar que sea valido (cantidad de registros y esquema).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
import pyarrow.parquet as pq
import s3fs

from src.logger import get_logger

logger = get_logger(__name__)


class ParquetWriteError(Exception):
    """Error al escribir o verificar un archivo Parquet."""


def _get_s3fs(region: str = "us-east-2", profile: Optional[str] = None) -> s3fs.S3FileSystem:
    """Devuelve un filesystem S3 autenticado.

    Usa el perfil AWS indicado (o el perfil/credenciales por defecto del
    entorno si no se especifica ninguno).
    """
    kwargs: dict = {"anon": False}
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
        creds = session.get_credentials().resolve()
        kwargs.update(
            key=creds.access_key,
            secret=creds.secret_key,
            token=creds.token,
        )
    return s3fs.S3FileSystem(**kwargs)


def _is_s3_path(path: str | Path) -> bool:
    return str(path).startswith("s3://")


def write_parquet(
    df: pd.DataFrame,
    output_dir: str | Path,
    run_id: str,
    compression: str = "snappy",
    dataset_name: str = "",
    s3_region: str = "us-east-2",
    aws_profile: Optional[str] = None,
) -> dict:
    """Escribe un DataFrame como Parquet con compresion Snappy y lo verifica.

    ``output_dir`` puede ser:
    - Una ruta local: ``data_lake/processed/movies/ingestion_date=2026-10-14``
    - Una ruta S3:    ``s3://mi-bucket/processed/movies/ingestion_date=2026-10-14``

    Retorna metadatos: ruta, registros, bytes y columnas.
    """
    output_dir_str = str(output_dir)
    file_path = output_dir_str.rstrip("/") + f"/run_{run_id}.parquet"

    if _is_s3_path(file_path):
        return _write_parquet_s3(
            df, file_path, run_id, compression, dataset_name, s3_region, aws_profile
        )
    return _write_parquet_local(
        df, Path(output_dir), run_id, compression, dataset_name
    )


# ----------------------------------------------------------- backend local
def _write_parquet_local(
    df: pd.DataFrame,
    output_dir: Path,
    run_id: str,
    compression: str,
    dataset_name: str,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"run_{run_id}.parquet"

    if file_path.exists():
        raise ParquetWriteError(
            f"El archivo destino ya existe y no se sobrescribe (corrida previa): {file_path}"
        )

    try:
        df.to_parquet(file_path, engine="pyarrow", compression=compression, index=False)
    except Exception as exc:
        raise ParquetWriteError(
            f"Error escribiendo Parquet local '{dataset_name}': {exc}"
        ) from exc

    # Verificacion inmediata
    back = read_parquet(str(file_path))
    if len(back) != len(df):
        raise ParquetWriteError(
            f"Fallo la verificacion de '{file_path}': se leyeron {len(back)} "
            f"registros tras escribir {len(df)}."
        )

    schema = pq.read_schema(str(file_path))
    size = file_path.stat().st_size
    _log_written(dataset_name or file_path.parent.name, file_path, len(df), size, compression, len(schema.names))
    return {
        "path": str(file_path),
        "records": len(df),
        "bytes": size,
        "columns": list(schema.names),
        "compression": compression,
        "storage": "local",
    }


# ------------------------------------------------------------ backend S3
def _write_parquet_s3(
    df: pd.DataFrame,
    file_path: str,
    run_id: str,
    compression: str,
    dataset_name: str,
    s3_region: str,
    aws_profile: Optional[str],
) -> dict:
    fs = _get_s3fs(region=s3_region, profile=aws_profile)

    if fs.exists(file_path):
        raise ParquetWriteError(
            f"El archivo S3 ya existe y no se sobrescribe (corrida previa): {file_path}"
        )

    try:
        df.to_parquet(
            file_path,
            engine="pyarrow",
            compression=compression,
            index=False,
            storage_options={"anon": False},
        )
    except Exception as exc:
        raise ParquetWriteError(
            f"Error escribiendo Parquet en S3 '{dataset_name}': {exc}"
        ) from exc

    # Verificacion inmediata
    back = read_parquet(file_path)
    if len(back) != len(df):
        raise ParquetWriteError(
            f"Fallo la verificacion S3 de '{file_path}': se leyeron {len(back)} "
            f"registros tras escribir {len(df)}."
        )

    info = fs.info(file_path)
    size = info.get("size", 0)

    schema = pq.read_schema(
        file_path,
        filesystem=pq.ParquetDataset(file_path, filesystem=fs).schema_arrow if False else None,
    )
    # Leer esquema desde S3
    with fs.open(file_path, "rb") as fh:
        schema = pq.read_schema(fh)

    _log_written(dataset_name or file_path.split("/")[-2], file_path, len(df), size, compression, len(schema.names))
    return {
        "path": file_path,
        "records": len(df),
        "bytes": size,
        "columns": list(schema.names),
        "compression": compression,
        "storage": "s3",
    }


# --------------------------------------------------------------- lectura
def read_parquet(path: str | Path, aws_profile: Optional[str] = None) -> pd.DataFrame:
    """Lee un Parquet desde ruta local o S3."""
    path_str = str(path)
    if _is_s3_path(path_str):
        return pd.read_parquet(path_str, engine="pyarrow", storage_options={"anon": False})
    return pd.read_parquet(path_str, engine="pyarrow")


def parquet_file_info(path: str | Path) -> dict:
    """Esquema y resolucion de compresion de un archivo Parquet (local)."""
    path_str = str(path)
    if _is_s3_path(path_str):
        fs = _get_s3fs()
        with fs.open(path_str, "rb") as fh:
            pf = pq.ParquetFile(fh)
            compression = None
            if pf.metadata.num_row_groups > 0:
                compression = pf.metadata.row_group(0).column(0).compression
            return {
                "path": path_str,
                "rows": pf.metadata.num_rows,
                "columns": pf.schema.names,
                "compression": compression,
                "storage": "s3",
            }
    pf = pq.ParquetFile(path_str)
    compression = None
    if pf.metadata.num_row_groups > 0:
        compression = pf.metadata.row_group(0).column(0).compression
    return {
        "path": path_str,
        "rows": pf.metadata.num_rows,
        "columns": pf.schema.names,
        "compression": compression,
        "storage": "local",
    }


def _log_written(name, path, records, size, compression, ncols):
    logger.info(
        "%s: Parquet -> %s | registros=%d | bytes=%d | compresion='%s' | columnas=%d",
        name,
        path,
        records,
        size,
        compression,
        ncols,
    )
