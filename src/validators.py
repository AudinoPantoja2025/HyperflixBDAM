"""Validaciones de calidad y auditoria del pipeline.

- Validacion de la configuracion (variables de entorno).
- Validacion de esquemas de columnas esperadas.
- Verificacion de que los Parquet generados se puedan abrir.
"""

from src.logger import get_logger

logger = get_logger(__name__)


def validate_settings(settings) -> bool:
    """Valida que la configuracion sea suficiente para ejecutar."""
    missing = []
    if not settings.mongodb_uri:
        missing.append("MONGODB_URI")
    if not settings.mongodb_database:
        missing.append("MONGODB_DATABASE")
    if not settings.mongodb_collections:
        missing.append("MONGODB_COLLECTIONS")
    if missing:
        raise ValueError("Configuracion incompleta. Faltan: " + ", ".join(missing))
    if settings.batch_size <= 0:
        raise ValueError("MONGO_BATCH_SIZE debe ser mayor que 0")
    return True


def check_dataframe_schema(df, expected_columns, name=""):
    """Verifica que el DataFrame contenga al menos las columnas esperadas."""
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"'{name}': el DataFrame no contiene las columnas esperadas: {sorted(missing)}"
        )
    return True


def check_parquet_readable(path):
    """Reabre un Parquet y confirma cuantos registros posee."""
    import pandas as pd

    df = pd.read_parquet(path, engine="pyarrow")
    logger.info("Parquet legible: '%s' (%d registros)", path, len(df))
    return len(df)