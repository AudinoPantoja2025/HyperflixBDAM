"""Conexion a MongoDB Atlas con PyMongo.

Mejora y centraliza la conexion que el proyecto ya tenia en ``conexion.py``:

- Manejador de contexto para cierre garantizado.
- Reintentos con espera ante fallos transitorios.
- ``ping`` obligatorio antes de usar la base.
- Errores tipificados (autenticacion, conexion, configuracion).
- Ninguna credencial se imprime en los logs.
"""

import time

from pymongo import MongoClient
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure

from src.logger import get_logger

logger = get_logger(__name__)


class MongoConnectionError(Exception):
    """Error de conexion/autenticacion con MongoDB MongoDB Atlas."""


class MongoConnection:
    """Conexion a una base MongoDB Atlas con control de ciclo de vida."""

    def __init__(self, uri, db_name, timeout_ms=5000, retries=3, retry_delay=2):
        self._uri = uri
        self._db_name = db_name
        self._timeout_ms = timeout_ms
        self._retries = max(1, retries)
        self._retry_delay = max(0, retry_delay)
        self._client = None
        self._db = None

    def connect(self):
        for attempt in range(1, self._retries + 1):
            logger.info(
                "Intentando conectar a MongoDB Atlas (intento %d/%d)...",
                attempt,
                self._retries,
            )
            try:
                client = MongoClient(self._uri, serverSelectionTimeoutMS=self._timeout_ms)
                client.admin.command("ping")
                self._client = client
                self._db = client[self._db_name]
                logger.info("Conexion exitosa a la base '%s'", self._db_name)
                return client, self._db
            except OperationFailure as exc:
                # Autenticacion, permisos o comando rechazado.
                logger.error("Error de autenticacion u operacion en MongoDB: %s", exc)
            except (ConnectionFailure, ConfigurationError) as exc:
                logger.error("Fallo de conexion/configuracion en MongoDB: %s", exc)
            except Exception as exc:  # noqa: BLE001 - cualquier fallo del driver
                logger.error("Error inesperado al conectar a MongoDB: %s", exc)

            if attempt < self._retries:
                logger.info("Reintentando en %d segundos...", self._retry_delay)
                time.sleep(self._retry_delay)

        raise MongoConnectionError(
            f"No fue posible conectar a MongoDB tras {self._retries} intentos "
            f"(base '{self._db_name}'). Verifique MONGODB_URI, credenciales y red."
        )

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
            logger.info("Conexion a MongoDB cerrada")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()