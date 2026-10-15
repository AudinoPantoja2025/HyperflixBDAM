"""Escritura de DataFrames en Apache Parquet con compresion Snappy.

El contrato es estricto: no se sobrescriben archivos de corridas anteriores
(el nombre incluye el ``run_id``), y todo archivo escrito se relee de inmediato
para verificar que sea valido (cantidad de registros y esquema).
"""

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.logger import get_logger

logger = get_logger(__name__)


class ParquetWriteError(Exception):
    """Error al escribir o verificar un archivo Parquet."""


def write_parquet(
    df: pd.DataFrame,
    output_dir,
    run_id: str,
    compression: str = "snappy",
    dataset_name: str = "",
) -> dict:
    """Escribe un DataFrame como Parquet con compresion Snappy y lo verifica.

    Retorna metadatos: ruta, registros, bytes y columnas.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"run_{run_id}.parquet"

    if file_path.exists():
        raise ParquetWriteError(
            f"El archivo destino ya existe y no se sobrescribe (corrida previa): {file_path}"
        )

    try:
        df.to_parquet(file_path, engine="pyarrow", compression=compression, index=False)
    except Exception as exc:  # noqa: BLE001
        raise ParquetWriteError(
            f"Error escribiendo Parquet '{dataset_name}': {exc}"
        ) from exc

    # Verificacion inmediata de legibilidad y conteo.
    back = read_parquet(file_path)
    if len(back) != len(df):
        raise ParquetWriteError(
            f"Fallo la verificacion de '{file_path}': se leyeron {len(back)} registros "
            f"tras escribir {len(df)}."
        )

    schema = pq.read_schema(file_path)
    size = file_path.stat().st_size
    logger.info(
        "%s: Parquet -> %s | registros=%d | bytes=%d | compresion='%s' | columnas=%d",
        dataset_name or file_path.parent.name,
        file_path,
        len(df),
        size,
        compression,
        len(schema.names),
    )
    return {
        "path": str(file_path),
        "records": len(df),
        "bytes": size,
        "columns": list(schema.names),
        "compression": compression,
    }


def read_parquet(path) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow")


def parquet_file_info(path) -> dict:
    """Esquema y resolucion de compresion de un archivo Parquet."""
    pf = pq.ParquetFile(path)
    compression = None
    if pf.metadata.num_row_groups > 0:
        compression = pf.metadata.row_group(0).column(0).compression
    return {
        "path": str(path),
        "rows": pf.metadata.num_rows,
        "columns": pf.schema.names,
        "compression": compression,
    }