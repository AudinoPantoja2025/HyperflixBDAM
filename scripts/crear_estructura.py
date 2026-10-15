import os
import time
import logging

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_NAME = "sample_mflix"
COLECCIONES_OLTP = ["users", "movies", "comments", "theaters"]
COLECCION_OLAP = "interacciones_analiticas"


def conectar():
    load_dotenv()
    uri = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
    if not uri:
        logger.error("No se encontro la variable MONGO_URI (ni MONGODB_URI) en el archivo .env")
        return None, None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        return client, client[DB_NAME]
    except (ConnectionFailure, ConfigurationError, OperationFailure) as e:
        logger.error("Error al conectar a MongoDB: %s", e)
        return None, None


def verificar_oltp_existente(db):
    print("\n[1/5] Verificando colecciones OLTP existentes en sample_mflix...")
    for col in COLECCIONES_OLTP:
        if col in db.list_collection_names():
            print(f"  Coleccion '{col}' OK ({db[col].count_documents({})} documento(s))")
        else:
            print(f"  Advertencia: la coleccion '{col}' no existe en sample_mflix")


def crear_indices_oltp(db):
    print("\n[2/5] Creando indices sobre las colecciones OLTP existentes...")
    db.users.create_index([("email", 1)], unique=True)
    print("  Indice unico creado en users.email")

    db.movies.create_index([("title", 1)])
    print("  Indice simple creado en movies.title")

    db.comments.create_index([("movie_id", 1)])
    print("  Indice simple creado en comments.movie_id")

    db.comments.create_index([("date", -1)])
    print("  Indice descendente creado en comments.date")


def crear_coleccion_olap(db):
    print("\n[3/5] Creando coleccion analitica (OLAP)...")
    if COLECCION_OLAP in db.list_collection_names():
        db.drop_collection(COLECCION_OLAP)
        print(f"  Coleccion '{COLECCION_OLAP}' previa eliminada (para reinicio limpio)")
    db.create_collection(COLECCION_OLAP)
    print(f"  Coleccion '{COLECCION_OLAP}' creada")


def configurar_pipeline(db):
    print("\n[4/5] Configurando referencia al pipeline OLTP -> OLAP...")
    print("  Fuente OLTP: users (audiencia) + movies (catalogo) + comments (interacciones)")
    print("  Destino OLAP: interacciones_analiticas (modelo estrella)")


def resumen(db):
    print("\n[5/5] Resumen de la estructura")
    print("=" * 60)
    for col in ["users", "movies", "comments"]:
        n = db[col].count_documents({}) if col in db.list_collection_names() else 0
        print(f"  OLTP '{col}': {n} documentos")
    print(f"  OLAP '{COLECCION_OLAP}': {db[COLECCION_OLAP].count_documents({})} documentos")
    print("=" * 60)
    print("Estructura de sample_mflix lista para el pipeline analitico")


def crear_estructura():
    client, db = conectar()
    if not client or db is None:
        return

    verificar_oltp_existente(db)
    crear_indices_oltp(db)
    crear_coleccion_olap(db)
    configurar_pipeline(db)
    resumen(db)

    client.close()


if __name__ == "__main__":
    crear_estructura()