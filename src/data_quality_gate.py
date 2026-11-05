"""Quality Gate Spark para datos de ``sample_mflix``.

Este módulo es deliberadamente independiente del ETL existente. Recibe
DataFrames de la zona PROCESSED, valida copias de trabajo y devuelve datos
válidos, rechazos, cuarentena y métricas sin escribir en MongoDB.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


QUALITY_COLUMNS = (
    "quality_status",
    "quality_errors",
    "validation_timestamp",
    "source_collection",
)


@dataclass
class QualityGateResult:
    """Resultado completo de una validación de una colección."""

    valid_df: DataFrame
    rejected_df: DataFrame
    quarantine_df: DataFrame
    rule_counts_df: DataFrame
    metrics: Dict[str, Any]


def _text_present(column: str) -> Column:
    return F.col(column).isNotNull() & (F.length(F.trim(F.col(column).cast("string"))) > 0)


def _movie_rules(df: DataFrame) -> list[tuple[str, Column]]:
    rules: list[tuple[str, Column]] = []
    if "title" in df.columns:
        rules.append(("movie_title_required", ~_text_present("title")))
    if "imdb_rating" in df.columns:
        rating = F.col("imdb_rating").cast("double")
        rules.append(
            ("movie_imdb_rating_range", rating.isNotNull() & ((rating < 0) | (rating > 10)))
        )
    elif "imdb" in df.columns and isinstance(df.schema["imdb"].dataType, StructType):
        rating = F.col("imdb.rating").cast("double")
        rules.append(
            ("movie_imdb_rating_range", rating.isNotNull() & ((rating < 0) | (rating > 10)))
        )
    if "runtime" in df.columns:
        runtime = F.col("runtime").cast("double")
        rules.append(("movie_runtime_positive", runtime.isNotNull() & (runtime <= 0)))
    if "year" in df.columns:
        year = F.col("year").cast("int")
        current_year = datetime.now(timezone.utc).year
        rules.append(
            ("movie_year_reasonable", year.isNotNull() & ((year < 1888) | (year > current_year + 1)))
        )
    if "num_mflix_comments" in df.columns:
        comments = F.col("num_mflix_comments").cast("long")
        rules.append(
            ("movie_comment_count_non_negative", comments.isNotNull() & (comments < 0))
        )
    return rules


def _comments_rules(
    df: DataFrame,
) -> list[tuple[str, Column]]:
    rules: list[tuple[str, Column]] = []
    if "movie_id" in df.columns:
        rules.append(("comment_movie_id_required", F.col("movie_id").isNull()))
    if "text" in df.columns:
        rules.append(("comment_text_required", ~_text_present("text")))
    if "date" in df.columns:
        date_as_string = F.col("date").cast("string")
        rules.append(
            (
                "comment_date_valid",
                F.col("date").isNotNull()
                & F.to_timestamp(date_as_string).isNull(),
            )
        )
    if "_missing_movie_reference" in df.columns:
        rules.append(
            (
                "comment_movie_reference_exists",
                F.col("_missing_movie_reference"),
            )
        )
    return rules


def _user_rules(df: DataFrame) -> list[tuple[str, Column]]:
    rules: list[tuple[str, Column]] = []
    if "name" in df.columns:
        rules.append(("user_name_required", ~_text_present("name")))
    if "email" in df.columns:
        rules.append(
            (
                "user_email_format",
                F.col("email").isNotNull()
                & ~F.col("email").cast("string").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
            )
        )
    return rules


def _theater_rules(df: DataFrame) -> list[tuple[str, Column]]:
    rules: list[tuple[str, Column]] = []
    if "theaterId" in df.columns:
        rules.append(
            (
                "theater_id_numeric",
                F.col("theaterId").isNotNull()
                & F.col("theaterId").cast("long").isNull(),
            )
        )
    return rules


def _rules_for(
    df: DataFrame, collection: str
) -> list[tuple[str, Column]]:
    rules: list[tuple[str, Column]] = []
    if "_id" in df.columns:
        rules.append(("record_id_required", F.col("_id").isNull()))
        duplicate_window = Window.partitionBy(F.col("_id").cast("string"))
        rules.append(
            (
                "record_id_unique",
                F.col("_id").isNotNull()
                & (F.count(F.lit(1)).over(duplicate_window) > 1),
            )
        )
    if collection == "movies":
        rules.extend(_movie_rules(df))
    elif collection == "comments":
        rules.extend(_comments_rules(df))
    elif collection == "users":
        rules.extend(_user_rules(df))
    elif collection == "theaters":
        rules.extend(_theater_rules(df))
    else:
        raise ValueError(f"Colección no soportada por el Quality Gate: {collection}")
    return rules


def _with_quality_columns(
    df: DataFrame, collection: str, rules: Iterable[tuple[str, Column]]
) -> DataFrame:
    error_array = F.array(
        *[
            F.when(condition, F.lit(rule_name)).otherwise(F.lit(None))
            for rule_name, condition in rules
        ]
    )
    return (
        df.withColumn("quality_errors_array", F.filter(error_array, lambda item: item.isNotNull()))
        .withColumn("quality_errors", F.array_join("quality_errors_array", ";"))
        .withColumn(
            "quality_status",
            F.when(F.size("quality_errors_array") == 0, F.lit("valid")).otherwise(
                F.lit("rejected")
            ),
        )
        .withColumn("validation_timestamp", F.current_timestamp())
        .withColumn("source_collection", F.lit(collection))
        .drop("quality_errors_array")
    )


def _rule_counts(rejected_df: DataFrame) -> DataFrame:
    return (
        rejected_df.select(F.explode(F.split("quality_errors", ";")).alias("rule"))
        .groupBy("rule")
        .count()
        .withColumnRenamed("count", "rejected_records")
        .orderBy("rule")
    )


def _quarantine(rejected_df: DataFrame) -> DataFrame:
    original_columns = [
        column for column in rejected_df.columns if column not in QUALITY_COLUMNS
    ]
    return rejected_df.select(
        F.col("_id").cast("string").alias("record_id")
        if "_id" in rejected_df.columns
        else F.lit(None).cast("string").alias("record_id"),
        "source_collection",
        F.to_json(F.struct(*[F.col(column) for column in original_columns])).alias(
            "original_record"
        ),
        "quality_errors",
        "validation_timestamp",
    )


def validate_collection(
    df: DataFrame,
    collection: str,
    movies_df: Optional[DataFrame] = None,
) -> QualityGateResult:
    """Valida una colección y separa registros válidos de rechazados.

    ``movies_df`` solo es necesario para la regla referencial de ``comments``.
    La función no realiza escrituras ni muta el DataFrame recibido.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df debe ser un pyspark.sql.DataFrame")
    working_df = df
    if (
        collection == "comments"
        and movies_df is not None
        and "movie_id" in df.columns
        and "_id" in movies_df.columns
    ):
        movie_ids = movies_df.select(
            F.col("_id").cast("string").alias("_movie_id")
        ).distinct()
        working_df = (
            df.withColumn("_movie_id", F.col("movie_id").cast("string"))
            .join(movie_ids.withColumn("_movie_exists", F.lit(True)), "_movie_id", "left")
            .withColumn(
                "_missing_movie_reference",
                F.col("movie_id").isNotNull() & F.col("_movie_exists").isNull(),
            )
            .drop("_movie_id", "_movie_exists")
        )
    rules = _rules_for(working_df, collection)
    evaluated = _with_quality_columns(working_df, collection, rules)
    if "_missing_movie_reference" in evaluated.columns:
        evaluated = evaluated.drop("_missing_movie_reference")
    valid = evaluated.where(F.col("quality_status") == "valid")
    rejected = evaluated.where(F.col("quality_status") == "rejected")
    return QualityGateResult(
        valid_df=valid,
        rejected_df=rejected,
        quarantine_df=_quarantine(rejected),
        rule_counts_df=_rule_counts(rejected),
        metrics=quality_metrics(evaluated),
    )


def quality_metrics(evaluated_df: DataFrame) -> Dict[str, Any]:
    """Calcula métricas reales sin recolectar los registros completos."""
    total = evaluated_df.count()
    valid = evaluated_df.where(F.col("quality_status") == "valid").count()
    rejected = total - valid
    return {
        "total_records": total,
        "valid_records": valid,
        "rejected_records": rejected,
        "valid_percentage": round(valid * 100.0 / total, 2) if total else 0.0,
        "rejected_percentage": round(rejected * 100.0 / total, 2) if total else 0.0,
    }


def simulate_dirty_data(df: DataFrame, collection: str) -> DataFrame:
    """Crea una copia con anomalías reproducibles; nunca modifica el origen."""
    if not isinstance(df, DataFrame):
        raise TypeError("df debe ser un pyspark.sql.DataFrame")
    if collection not in {"movies", "comments", "users", "theaters"}:
        raise ValueError(f"Colección no soportada: {collection}")

    sample = df.limit(1)
    if not df.columns:
        return df

    dirty = sample
    dirty_reason = f"{collection}_quality_anomaly"
    if collection == "movies":
        if "title" in df.columns:
            dirty = dirty.withColumn("title", F.lit(None).cast(df.schema["title"].dataType))
            dirty_reason = "missing_title"
        if "runtime" in df.columns:
            dirty = dirty.withColumn("runtime", F.lit(-1).cast(df.schema["runtime"].dataType))
            dirty_reason = "negative_runtime"
        if "year" in df.columns:
            dirty = dirty.withColumn("year", F.lit(1800).cast(df.schema["year"].dataType))
            dirty_reason = "invalid_year"
        if "imdb_rating" in df.columns:
            dirty = dirty.withColumn(
                "imdb_rating",
                F.lit(11.0).cast(df.schema["imdb_rating"].dataType),
            )
            dirty_reason = "invalid_imdb_rating"
        elif "imdb" in df.columns and isinstance(df.schema["imdb"].dataType, StructType):
            dirty = dirty.withColumn(
                "imdb",
                F.col("imdb").withField("rating", F.lit(11.0)),
            )
            dirty_reason = "invalid_imdb_rating"
    elif collection == "comments":
        if "movie_id" in df.columns:
            dirty = dirty.withColumn("movie_id", F.lit("__missing_movie__"))
            dirty_reason = "missing_movie_reference"
        if "text" in df.columns:
            dirty = dirty.withColumn("text", F.lit(""))
            dirty_reason = "empty_comment_text"
    elif collection == "users" and "name" in df.columns:
        dirty = dirty.withColumn("name", F.lit(""))
        dirty_reason = "missing_user_name"
    elif collection == "theaters" and "theaterId" in df.columns:
        dirty = dirty.withColumn("theaterId", F.lit("not-a-number"))
        dirty_reason = "invalid_theater_id"

    dirty = (
        dirty.withColumn("_simulated_dirty_record", F.lit(True))
        .withColumn("_simulated_dirty_reason", F.lit(dirty_reason))
    )
    original = df.withColumn("_simulated_dirty_record", F.lit(False)).withColumn(
        "_simulated_dirty_reason", F.lit(None).cast("string")
    )
    # La fila sucia y la copia duplicada hacen visibles dos categorías del taller
    # sin alterar ningún documento de MongoDB.
    return original.unionByName(dirty, allowMissingColumns=False).unionByName(
        dirty, allowMissingColumns=False
    )


def validate_all_collections(
    dataframes: Mapping[str, DataFrame],
) -> Dict[str, QualityGateResult]:
    """Ejecuta el Quality Gate sobre las colecciones disponibles."""
    required = {"movies", "comments", "users", "theaters"}
    unknown = set(dataframes) - required
    if unknown:
        raise ValueError(f"Colecciones no soportadas: {sorted(unknown)}")
    movies = dataframes.get("movies")
    return {
        collection: validate_collection(
            df,
            collection,
            movies_df=movies if collection == "comments" else None,
        )
        for collection, df in dataframes.items()
    }


def metrics_dataframe(results: Mapping[str, QualityGateResult]) -> DataFrame:
    """Construye un reporte Spark consolidado a partir de métricas ejecutadas."""
    if not results:
        raise ValueError("Se requiere al menos un resultado de calidad")
    spark = next(iter(results.values())).valid_df.sparkSession
    rows = [
        (
            collection,
            int(result.metrics["total_records"]),
            int(result.metrics["valid_records"]),
            int(result.metrics["rejected_records"]),
            float(result.metrics["valid_percentage"]),
            float(result.metrics["rejected_percentage"]),
        )
        for collection, result in sorted(results.items())
    ]
    schema = StructType(
        [
            StructField("collection", StringType(), False),
            StructField("total_records", LongType(), False),
            StructField("valid_records", LongType(), False),
            StructField("rejected_records", LongType(), False),
            StructField("valid_percentage", DoubleType(), False),
            StructField("rejected_percentage", DoubleType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def write_quality_outputs(
    results: Mapping[str, QualityGateResult],
    quality_root: str | Path,
    mode: str = "overwrite",
) -> Dict[str, str]:
    """Escribe resultados Spark separados y verifica que puedan releerse."""
    if mode not in {"overwrite", "append", "error", "ignore"}:
        raise ValueError(f"Modo de escritura inválido: {mode}")
    root = str(quality_root).rstrip("/")
    paths: Dict[str, str] = {}
    for collection, result in results.items():
        outputs = {
            f"valid_{collection}": (result.valid_df, f"{root}/valid/{collection}"),
            f"quarantine_{collection}": (
                result.quarantine_df,
                f"{root}/quarantine/{collection}",
            ),
            f"rules_{collection}": (
                result.rule_counts_df,
                f"{root}/metrics/{collection}_rules",
            ),
        }
        for name, (frame, path) in outputs.items():
            frame.write.mode(mode).parquet(path)
            frame.sparkSession.read.parquet(path).limit(1).count()
            paths[name] = path
    summary = metrics_dataframe(results)
    summary_path = f"{root}/metrics/run_summary"
    summary.write.mode(mode).parquet(summary_path)
    summary.sparkSession.read.parquet(summary_path).limit(1).count()
    paths["run_summary"] = summary_path
    return paths
