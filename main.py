"""Punto de entrada unico del pipeline ETL: MongoDB Atlas -> Data Lake -> Parquet.

Ejecucion:
    python main.py [--env .env] [--root data_lake] [--collections c1,c2] [--batch-size 1000]

Flujo:
    1.  Cargar configuracion (variables de entorno).
    2.  Validar variables obligatorias.
    3.  Conectar a MongoDB Atlas (ping + base).
    4.  Extraer las colecciones configuradas.
    5.  Guardar originales en RAW (JSON Lines / extended JSON).
    6.  Releer RAW y Transformar a DataFrames (Pandas).
    7.  Guardar PROCESSED en Parquet (Snappy).
    8.  Construir datasets CURATED (modelo dimensional).
    9.  Exportar CURATED en Parquet (Snappy).
    10. Validar los archivos generados (relectura).
    11. Escribir manifiesto de auditoria y resumen.
    12. Cerrar la conexion a MongoDB.
"""

import argparse
import json
import sys
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from src import transformations as tf
from src.config import ConfigError, load_settings
from src.curated import build_curated
from src.data_lake import DataLake
from src.extractor import ExtractionError, extract_collection
from src.logger import setup_logging, get_logger
from src.mongo_client import MongoConnection, MongoConnectionError
from src.parquet_writer import ParquetWriteError, write_parquet
from src.validators import check_dataframe_schema, validate_settings

logger = get_logger("pipeline")

REQUIRED_COLUMNS = {
    "users": ["_id", "name", "email"],
    "movies": ["_id"],
    "comments": ["_id", "movie_id", "name", "email", "text", "date"],
    "theaters": ["_id"],
}


def _read_rows(path) -> int:
    import pandas as pd

    return len(pd.read_parquet(path, engine="pyarrow"))


def run_pipeline(args=None):
    """Orquesta el pipeline completo. Retorna el dict de auditoria (o None si la config falla)."""
    cli = _parse_args(args)

    try:
        settings = load_settings(env_path=cli.env)
        if cli.root:
            settings = replace(settings, data_lake_root=Path(cli.root))
        if cli.collections:
            colls = [c.strip() for c in cli.collections.split(",") if c.strip()]
            settings = replace(settings, mongodb_collections=colls)
        if cli.batch_size and cli.batch_size > 0:
            settings = replace(settings, batch_size=cli.batch_size)
        validate_settings(settings)
    except (ConfigError, ValueError) as exc:
        logger.error("Configuracion invalida: %s", exc)
        return None

    setup_logging(log_dir="logs")
    total_t0 = time.perf_counter()
    audit = _execute(settings)
    audit["total_seconds"] = round(time.perf_counter() - total_t0, 3)
    _write_audit_file(DataLake(settings.data_lake_root), audit)
    _print_summary(audit)
    return audit


def _parse_args(args):
    parser = argparse.ArgumentParser(
        description="Pipeline ETL MongoDB Atlas -> Data Lake -> Parquet"
    )
    parser.add_argument("--env", default=None, help="Ruta al archivo .env (default: ./.env)")
    parser.add_argument("--root", default=None, help="Raiz del Data Lake (default: data_lake)")
    parser.add_argument("--collections", default=None, help="Colecciones separadas por coma")
    parser.add_argument("--batch-size", type=int, default=None, help="Tamano de lote de lectura")
    return parser.parse_args(args)


def _execute(settings):
    started_at = datetime.now()
    stages = {}
    audit = {
        "run_id": settings.run_id,
        "ingestion_date": settings.ingestion_date,
        "started_at": started_at.isoformat(),
        "database": settings.mongodb_database,
        "collections": settings.mongodb_collections,
        "stages": stages,
        "files": [],
        "warnings": [],
        "error": None,
    }

    def timed(stage, fn):
        t0 = time.perf_counter()
        try:
            value = fn()
        except Exception:
            stages[stage] = round(time.perf_counter() - t0, 3)
            raise
        stages[stage] = round(time.perf_counter() - t0, 3)
        return value

    dl = DataLake(settings.data_lake_root)
    timed("create_zones", dl.create_zones)

    conn = MongoConnection(settings.mongodb_uri, settings.mongodb_database, retries=3)
    try:
        _, db = timed("connect", conn.connect)

        processed = {}
        for coll in settings.mongodb_collections:
            docs = timed(
                f"extract:{coll}",
                lambda c=coll: extract_collection(db, c, settings.batch_size),
            )
            timed(
                f"write_raw:{coll}",
                lambda c=coll, d=docs: dl.write_raw_jsonl(
                    c, d, settings.run_id, settings.ingestion_date
                ),
            )

            # PROCESSED se construye releyendo RAW (capa intermedia del Data
            # Lake): el archivo es la unica fuente de verdad de la corrida.
            raw_docs = timed(
                f"read_raw:{coll}",
                lambda c=coll: dl.read_raw_jsonl(
                    c, settings.ingestion_date, settings.run_id
                ),
            )
            required = REQUIRED_COLUMNS.get(coll, [])
            raw_df = tf.docs_to_dataframe(raw_docs, required)
            kept, rejected_df = tf.split_duplicates(raw_df, subset=["_id"])
            if len(rejected_df):
                timed(
                    f"write_rejected:{coll}",
                    lambda r=rejected_df, c=coll: dl.write_rejected(
                        c, r, settings.run_id, settings.ingestion_date
                    ),
                )
            df = tf.transform_collection(coll, kept)
            tf.count_records_ok(len(raw_docs), len(df), len(rejected_df), name=coll)
            check_dataframe_schema(df, required, name=coll)
            processed[coll] = df

            meta = timed(
                f"write_processed:{coll}",
                lambda d=df, c=coll: write_parquet(
                    d,
                    dl.processed_partition_dir(coll, settings.ingestion_date),
                    settings.run_id,
                    dataset_name=f"PROCESSED:{coll}",
                ),
            )
            audit["files"].append({"zone": "processed", "collection": coll, **meta})

        datasets = timed("build_curated", lambda: build_curated(processed))
        for name, df in datasets.items():
            meta = timed(
                f"write_curated:{name}",
                lambda d=df, n=name: write_parquet(
                    d,
                    dl.curated_partition_dir(n, settings.ingestion_date),
                    settings.run_id,
                    dataset_name=f"CURATED:{n}",
                ),
            )
            audit["files"].append({"zone": "curated", "collection": name, **meta})

        for f in audit["files"]:
            path = Path(f["path"])
            if not path.exists():
                raise ValueError(f"Falta el archivo esperado: {path}")
            rows = _read_rows(path)
            if rows != f["records"]:
                audit["warnings"].append(
                    f"Relectura de {path}: {rows} filas (esperado {f['records']})"
                )

        audit["finished_at"] = datetime.now().isoformat()
        return audit
    except BaseException:
        audit["error"] = "flujo abortado; ver logs"
        raise
    finally:
        conn.close()
        timed("close_connection", lambda: None)


def _write_audit_file(dl, audit):
    audit_dir = dl.root / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    out = audit_dir / f"run_{audit.get('run_id', 'unknown')}.json"
    with out.open("w", encoding="utf-8") as fh:
        json.dump(audit, fh, indent=2, ensure_ascii=False, default=str)


def _print_summary(audit):
    print("\n" + "=" * 72)
    print(f"RESUMEN DEL PIPELINE  |  run_id={audit.get('run_id')}")
    print("=" * 72)
    stages = dict(sorted((audit.get("stages") or {}).items()))
    for key, seconds in stages.items():
        print(f"  {key:<30} {seconds:>8.3f} s")
    print("-" * 72)
    print(f"  {'total':<30} {audit.get('total_seconds', 'n/a'):>8} s")
    if audit.get("error"):
        print(f"\n  ERROR: {audit['error']}")
    else:
        files = audit.get("files") or []
        if files:
            print("\n  Archivos generados:")
            for f in files:
                label = f.get("collection") or f.get("dataset")
                print(
                    f"    [{f['zone']:>9}] {label:<18} "
                    f"registros={f['records']:>6} bytes={f['bytes']:>9} "
                    f"compresion={f.get('compression')}"
                )
    warnings = audit.get("warnings") or []
    for w in warnings:
        print(f"  ADVERTENCIA: {w}")
    print("=" * 72)
    print("  Estado:", "COMPLETADO OK" if not audit.get("error") else "FINALIZADO CON ERRORES")
    print("=" * 72)


if __name__ == "__main__":
    sys.exit(0 if run_pipeline() is not None else 1)