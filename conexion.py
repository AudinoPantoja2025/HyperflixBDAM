"""Conexion a MongoDB Atlas (wrapper de compatibilidad).

Esta version reutiliza el modulo mejorado ``src.mongo_client`` y conserva la
misma interfaz publica que tenia este archivo (``conectar_mongodb``,
``DB_NAME`` y ``COLECCIONES_OLTP``) para no romper los scripts existentes.

Ejecutar como script para probar el ping:
    python conexion.py
"""

import os

from dotenv import load_dotenv

from src import config
from src.logger import get_logger
from src.mongo_client import MongoConnection

logger = get_logger(__name__)

DB_NAME = config.DEFAULT_DATABASE
COLECCIONES_OLTP = list(config.DEFAULT_COLLECTIONS)


def conectar_mongodb(db_name=DB_NAME, reintentos=5):
    """Conecta a MongoDB Atlas y devuelve (client, db).

    Reutiliza la URI de ``MONGODB_URI``/``MONGO_URI`` del archivo .env.
    Retorna ``(None, None)`` si la configuracion es invalida.
    """
    load_dotenv()
    uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
    if not uri:
        logger.error("No se encontro MONGODB_URI (ni MONGO_URI) en el archivo .env")
        return None, None
    try:
        conn = MongoConnection(uri, db_name, retries=reintentos)
        return conn.connect()
    except Exception as exc:  # noqa: BLE001 - compatibilidad con scripts existentes
        logger.error("No fue posible conectar a MongoDB: %s", exc)
        return None, None


if __name__ == "__main__":
    client, db = conectar_mongodb()
    if client and db is not None:
        print("Ping OK: '%s' accesible" % db.name)
        client.close()
        print("Conexion cerrada")