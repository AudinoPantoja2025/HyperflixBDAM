"""Extraccion de colecciones desde MongoDB Atlas con PyMongo.

- Lee documento por documento (streaming con ``batch_size``) para no
  cargar colecciones enteras en memoria sin control.
- Conserva el ``_id`` (convertido a cadena para su uso analitico).
- Preserva los campos originales.
- Maneja colecciones vacias e inexistentes.
- Registra la cantidad extraida.
"""

from typing import Iterator, List

from pymongo.errors import PyMongoError

from src.logger import get_logger

logger = get_logger(__name__)


class ExtractionError(Exception):
    """Error durante la extraccion desde MongoDB."""


def list_collections(db) -> List[str]:
    try:
        return sorted(db.list_collection_names())
    except PyMongoError as exc:
        raise ExtractionError(f"No se pudieron listar las colecciones: {exc}") from exc


def collection_exists(db, collection_name: str) -> bool:
    return collection_name in list_collections(db)


def extract_collection_stream(db, collection_name: str, batch_size: int = 1000) -> Iterator[dict]:
    """Generador de documentos de una coleccion (un dict por documento).

    La conversion de ``_id`` (ObjectId) a ``str`` se hace aqui para que las
    siguientes etapas trabajen con un identificador estable y serializable.
    """
    if not collection_exists(db, collection_name):
        raise ExtractionError(
            f"La coleccion '{collection_name}' no existe en la base '{db.name}'"
        )
    try:
        cursor = db[collection_name].find().batch_size(max(1, batch_size))
        for doc in cursor:
            doc = dict(doc)
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            yield doc
    except PyMongoError as exc:
        raise ExtractionError(
            f"Error extrayendo la coleccion '{collection_name}': {exc}"
        ) from exc


def extract_collection(db, collection_name: str, batch_size: int = 1000) -> List[dict]:
    """Extrae una coleccion completa como lista de diccionarios.

    Para colecciones muy grandes conviene usar ``extract_collection_stream``
    y persistir en RAW por lotes; aqui se mantiene la lista por simplicidad
    dado el volumen actual del proyecto (~41K docs en la coleccion mayor).
    """
    docs: List[dict] = []
    for doc in extract_collection_stream(db, collection_name, batch_size=batch_size):
        docs.append(doc)
    logger.info(
        "Extraccion completada: '%s' -> %d documento(s)",
        collection_name,
        len(docs),
    )
    return docs