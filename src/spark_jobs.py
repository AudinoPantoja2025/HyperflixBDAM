"""Jobs PySpark para la fase analítica del Data Lake.

Estos jobs consumen CURATED ya generado por el ETL; no reemplazan sus reglas
de negocio. La salida es una capa de consumo para Data Warehouse/BI.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from src.logger import get_logger

logger = get_logger(__name__)


class SparkJobError(Exception):
    """Error de configuracion, dependencia o ejecucion de un job Spark."""


def latest_parquet(dataset_root: Path) -> Path:
    """Devuelve la fotografia Parquet mas reciente de un dataset CURATED.

    La particion de ingreso y el ``run_id`` tienen formato ordenable; por eso
    la seleccion es determinista y no depende de modificar archivos anteriores.
    """
    root = Path(dataset_root)
    candidates = list(root.glob("ingestion_date=*/run_*.parquet"))
    if not candidates:
        raise SparkJobError(f"No hay Parquet disponible para procesar en: {root}")
    return max(candidates, key=lambda item: (item.parent.name, item.name))


def create_spark_session(app_name: str = "BDParcial-SparkInteracciones", master: Optional[str] = None):
    """Crea una sesion Spark solo al ejecutar el job (importacion diferida)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SparkJobError(
            "PySpark no esta instalado. Ejecute: pip install -r requirements.txt"
        ) from exc

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def interacciones_por_categoria(spark, input_path: Path):
    """Genera metricas de actividad por categoria para DW/BI.

    Transformaciones distribuidas aplicadas:
    1. ``filter`` y ``select``: deja hechos validos y las columnas necesarias.
    2. ``withColumn``: normaliza categoria y medidas a tipos analiticos.
    3. ``groupBy`` + ``agg``: consolida interacciones, rating y longitud.
    4. ``orderBy``: presenta la metrica priorizada para BI.
    """
    try:
        from pyspark.sql import functions as F
    except ImportError as exc:  # Protección si se llama sin dependencia instalada.
        raise SparkJobError("PySpark no esta instalado") from exc

    fact = spark.read.parquet(str(input_path))
    required = {"categoria", "rating_pelicula", "interaccion_id", "longitud_comentario"}
    missing = required.difference(fact.columns)
    if missing:
        raise SparkJobError(
            "FACT_INTERACCIONES no contiene columnas requeridas: " + ", ".join(sorted(missing))
        )

    base = (
        fact.filter(F.col("cantidad") == F.lit(1))
        .select("interaccion_id", "categoria", "rating_pelicula", "longitud_comentario")
        .withColumn("categoria", F.coalesce(F.col("categoria"), F.lit("Sin categoria")))
        .withColumn("rating_pelicula", F.coalesce(F.col("rating_pelicula").cast("double"), F.lit(0.0)))
        .withColumn("longitud_comentario", F.coalesce(F.col("longitud_comentario").cast("int"), F.lit(0)))
    )
    return (
        base.groupBy("categoria")
        .agg(
            F.count("interaccion_id").alias("cantidad_interacciones"),
            F.round(F.avg("rating_pelicula"), 2).alias("rating_promedio"),
            F.round(F.avg("longitud_comentario"), 1).alias("longitud_promedio"),
        )
        .orderBy(F.desc("cantidad_interacciones"), F.asc("categoria"))
    )


def run_interacciones_por_categoria(
    lake_root: Path,
    output_root: Optional[Path] = None,
    master: Optional[str] = None,
    single_file: bool = False,
) -> dict:
    """Ejecuta el job y escribe una nueva salida Parquet, sin sobrescrituras."""
    lake_root = Path(lake_root)
    source = latest_parquet(lake_root / "curated" / "FACT_INTERACCIONES")
    target_root = Path(output_root) if output_root else lake_root / "spark_curated"
    now = datetime.now()
    target = (
        target_root
        / "INTERACCIONES_CATEGORIA"
        / f"ingestion_date={now:%Y-%m-%d}"
        / f"run_{now:%Y%m%d_%H%M%S}.parquet"
    )
    spark = create_spark_session(master=master)
    try:
        result = interacciones_por_categoria(spark, source)
        rows = result.count()  # Materializa y permite auditar la salida.
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise SparkJobError(f"La salida ya existe y no se sobrescribe: {target}")

        if single_file:
            # Modo didactico y portable en Windows: Spark ejecuta todas las
            # transformaciones y Pandas escribe el resultado agregado como un
            # unico archivo Parquet. Evita la dependencia winutils.exe del
            # escritor Hadoop local. Debe usarse solo con resultados pequenos.
            # No se usa DataFrame.toPandas(): PySpark 3.5 lo implementa con
            # ``distutils``, modulo retirado en Python 3.12. ``collect`` trae
            # solo el agregado final (no la FACT completa) y preserva el mismo
            # resultado para esta salida de demostracion.
            import pandas as pd

            pandas_result = pd.DataFrame(
                [row.asDict(recursive=True) for row in result.collect()],
                columns=result.columns,
            )
            pandas_result.to_parquet(
                target, engine="pyarrow", compression="snappy", index=False
            )
        else:
            # Modo distribuido para un cluster Spark/Hadoop correctamente
            # configurado; genera un directorio con uno o varios part-files.
            result.write.mode("errorifexists").parquet(str(target), compression="snappy")
        logger.info("Spark Job listo: %s -> %s (%d filas)", source, target, rows)
        return {"source": str(source), "output": str(target), "records": rows}
    finally:
        spark.stop()
