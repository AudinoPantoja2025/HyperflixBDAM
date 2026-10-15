"""Configuracion segura del pipeline mediante variables de entorno.

Carga el archivo ``.env`` con ``python-dotenv`` y expone un objeto
inmutable ``Settings`` que el resto del pipeline consume. Ninguna
credencial se imprime ni se escribe en los logs.
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from src.logger import get_logger

logger = get_logger(__name__)

DEFAULT_DATABASE = "sample_mflix"
DEFAULT_COLLECTIONS = ["users", "movies", "comments", "theaters"]
REQUIRED_ENV_VARS = ("MONGODB_URI", "MONGODB_DATABASE")

# El proyecto existente usa historicamente MONGO_URI; se mantiene como
# alternativa respaldada (MONGODB_URI tiene prioridad).
LEGACY_URI_VAR = "MONGO_URI"


class ConfigError(Exception):
    """Configuracion invalida o incompleta."""


@dataclass(frozen=True)
class Settings:
    mongodb_uri: str
    mongodb_database: str
    mongodb_collections: List[str]
    data_lake_root: Path
    batch_size: int
    run_id: str
    ingestion_date: str
    required_env_vars: List[str] = field(default_factory=lambda: list(REQUIRED_ENV_VARS))


def mask_uri(uri: str) -> str:
    """Devuelve la URI sin credenciales para registros de auditoria.

    Ej.: ``mongodb+srv://cluster0.abc.mongodb.net/`` en lugar de
    ``mongodb+srv://usuario:clave@cluster0.abc.mongodb.net/``.
    """
    try:
        scheme, resto = uri.split("://", 1)
        host = resto.split("@")[-1]
        return f"{scheme}://{host}"
    except (ValueError, AttributeError):
        return "<uri-no-legible>"


def _reset_env():
    """Limpia las variables del pipeline para una carga deterministica."""
    keys = list(REQUIRED_ENV_VARS) + [
        LEGACY_URI_VAR,  # MONGO_URI tambien debe ser aislada de entornos heredados
        "MONGODB_COLLECTIONS",
        "DATA_LAKE_ROOT",
        "MONGO_BATCH_SIZE",
    ]
    for key in keys:
        os.environ.pop(key, None)


def load_settings(env_path: Optional[str] = None) -> Settings:
    """Carga, valida y construye la configuracion del pipeline.

    Lanza ``ConfigError`` si faltan variables obligatorias.
    """
    _reset_env()
    load_dotenv(dotenv_path=env_path)

    missing = []
    if not (os.getenv("MONGODB_URI") or os.getenv(LEGACY_URI_VAR)):
        missing.append("MONGODB_URI (o MONGO_URI)")
    if not os.getenv("MONGODB_DATABASE"):
        missing.append("MONGODB_DATABASE")
    if missing:
        raise ConfigError("Variables de entorno faltantes: " + ", ".join(missing))

    uri = (os.getenv("MONGODB_URI") or os.getenv(LEGACY_URI_VAR) or "").strip()
    database = (os.getenv("MONGODB_DATABASE") or "").strip()
    raw_collections = os.getenv("MONGODB_COLLECTIONS", "")
    collections = [
        c.strip()
        for c in raw_collections.split(",")
        if c.strip()
    ] or list(DEFAULT_COLLECTIONS)
    root = Path(os.getenv("DATA_LAKE_ROOT", "data_lake"))
    batch_size = int(os.getenv("MONGO_BATCH_SIZE", "1000") or "1000")

    now = datetime.now()
    settings = Settings(
        mongodb_uri=uri,
        mongodb_database=database,
        mongodb_collections=collections,
        data_lake_root=root,
        batch_size=batch_size,
        run_id=now.strftime("%Y%m%d_%H%M%S"),
        ingestion_date=now.strftime("%Y-%m-%d"),
    )
    logger.info(
        "Configuracion cargada: db='%s', colecciones=%s, root='%s', uri=%s",
        settings.mongodb_database,
        settings.mongodb_collections,
        settings.data_lake_root,
        mask_uri(settings.mongodb_uri),
    )
    return settings