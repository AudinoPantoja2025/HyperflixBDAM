import os
import time
import logging

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_NAME = "sample_mflix"
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


def construir_pipeline():
    return [
        {"$lookup": {"from": "users", "localField": "name", "foreignField": "name", "as": "info_usuario"}},
        {"$unwind": {"path": "$info_usuario", "preserveNullAndEmptyArrays": True}},

        {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "_id", "as": "info_pelicula"}},
        {"$unwind": {"path": "$info_pelicula", "preserveNullAndEmptyArrays": True}},

        {"$addFields": {
            "rating": {"$convert": {"input": "$info_pelicula.imdb.rating",
                                    "to": "double", "onError": 0, "onNull": 0}},
            "categoria": {"$arrayElemAt": [
                {"$ifNull": ["$info_pelicula.genres", ["Sin categoria"]]}, 0]},
            "marca": {"$arrayElemAt": [
                {"$ifNull": ["$info_pelicula.directors", ["Desconocido"]]}, 0]},
            "longitud": {"$strLenCP": {"$ifNull": ["$text", ""]}},
        }},

        {"$project": {
            "_id": 1,
            "fecha": "$date",
            "anio": {"$year": "$date"},
            "mes": {"$month": "$date"},
            "dia": {"$dayOfMonth": "$date"},

            "usuario": {
                "id": {"$ifNull": ["$info_usuario._id", None]},
                "nombre": {"$ifNull": ["$name", "Anonimo"]},
                "segmento": {
                    "$cond": [{"$ifNull": ["$info_usuario._id", False]},
                              "Registrado", "Anonimo"]
                },
            },

            "pelicula": {
                "id": {"$ifNull": ["$info_pelicula._id", None]},
                "titulo": {"$ifNull": ["$info_pelicula.title", "Sin titulo"]},
                "categoria": "$categoria",
                "marca": "$marca",
                "rating": "$rating",
                "clasificacion": {"$ifNull": ["$info_pelicula.rated", "Sin clasificacion"]},
            },

            "cantidad": {"$literal": 1},
            "rating_pelicula": "$rating",
            "longitud_comentario": "$longitud",
        }},

        {"$merge": {"into": COLECCION_OLAP, "on": "_id",
                    "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]


def transformar_datos():
    print("Transformacion OLTP -> OLAP (interacciones de streaming) sobre sample_mflix")
    client, db = conectar()
    if not client or db is None:
        return

    db.drop_collection(COLECCION_OLAP)
    print(f"Coleccion '{COLECCION_OLAP}' limpiada (reinicio de la capa analitica)")

    pipeline = construir_pipeline()
    inicio = time.time()

    for intento in range(1, 6):
        try:
            print("Ejecutando Aggregation Pipeline ($lookup + $unwind + $project + $merge)...")
            list(db.comments.aggregate(pipeline, allowDiskUse=True))
            generados = db[COLECCION_OLAP].count_documents({})
            total_fuente = db.comments.count_documents({})
            if generados > 0 or total_fuente == 0 or intento >= 4:
                break
            print(f"Pipeline termino con 0 registros (intento {intento}), reintentando...")
        except Exception as e:
            logger.warning("Fallo en el pipeline (intento %d): %s", intento, e)
            if intento >= 4:
                break
            time.sleep(2)

    tiempo_total = time.time() - inicio
    total_analitico = db[COLECCION_OLAP].count_documents({})

    print("=" * 60)
    print("TRANSFORMACION OLTP A OLAP COMPLETADA")
    print(f"  Tiempo de procesamiento: {tiempo_total:.2f} segundos")
    print(f"  Interacciones fuente (comments): {db.comments.count_documents({})}")
    print(f"  Registros analiticos generados: {total_analitico}")
    print("=" * 60)

    client.close()


if __name__ == "__main__":
    transformar_datos()
