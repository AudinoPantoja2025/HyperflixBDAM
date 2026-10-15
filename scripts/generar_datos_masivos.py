import os
import time
import logging
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConfigurationError, ConnectionFailure, OperationFailure

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_NAME = "sample_mflix"
COLECCION_OLAP = "interacciones_analiticas"
BATCH_SIZE = 1000
FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"


def conectar():
    load_dotenv()
    uri = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
    if not uri:
        logger.error("No se encontro la variable MONGO_URI (ni MONGODB_URI) en el archivo .env")
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        return client
    except (ConnectionFailure, ConfigurationError, OperationFailure) as e:
        logger.error("Error al conectar a MongoDB: %s", e)
        return None


def leer_todo(client, coleccion, proyeccion):
    intentos = 0
    while intentos < 6:
        try:
            db = client[DB_NAME]
            data = list(db[coleccion].find({}, proyeccion).batch_size(BATCH_SIZE))
            if data or intentos >= 3:
                return data, db
        except Exception as e:
            logger.warning("Fallo de lectura en '%s' (intento %d): %s", coleccion, intentos + 1, e)
        intentos += 1
        time.sleep(2)
    return [], None


def construir_hecho(comment, peliculas, usuarios):
    movie = peliculas.get(comment.get("movie_id"), {})
    fecha = comment.get("date")
    if isinstance(fecha, str):
        try:
            fecha = datetime.strptime(fecha, FORMATO_FECHA)
        except ValueError:
            fecha = None

    rating = movie.get("imdb", {}).get("rating") or 0
    generos = movie.get("genres") or ["Sin categoria"]
    directores = movie.get("directors") or ["Desconocido"]
    autor = comment.get("name") or "Anonimo"
    segmento = "Registrado" if autor in usuarios else "Anonimo"
    texto = comment.get("text") or ""

    return {
        "fecha": fecha,
        "anio": fecha.year if fecha else None,
        "mes": fecha.month if fecha else None,
        "dia": fecha.day if fecha else None,
        "usuario": {
            "id": usuarios.get(autor),
            "nombre": autor,
            "segmento": segmento,
        },
        "pelicula": {
            "id": movie.get("_id"),
            "titulo": movie.get("title") or "Sin titulo",
            "categoria": generos[0],
            "marca": directores[0],
            "rating": rating,
            "clasificacion": movie.get("rated") or "Sin clasificacion",
        },
        "cantidad": 1,
        "rating_pelicula": rating,
        "longitud_comentario": len(texto),
    }


def insertar_en_lotes(coleccion, total_esperado, generador, nombre_entidad):
    buffer = []
    insertados_acumulados = 0
    procesados = 0

    for doc in generador:
        buffer.append(doc)
        procesados += 1
        if len(buffer) >= BATCH_SIZE:
            insertados_acumulados += _volcar_lote(coleccion, buffer, procesados, total_esperado, nombre_entidad)
            buffer = []

    if buffer:
        insertados_acumulados += _volcar_lote(coleccion, buffer, procesados, total_esperado, nombre_entidad)

    print(f"  Insertados {insertados_acumulados}/{total_esperado} en '{nombre_entidad}'")


def _volcar_lote(coleccion, lote, procesados, total_esperado, nombre_entidad):
    try:
        resultado = coleccion.insert_many(lote, ordered=False)
        n = len(resultado.inserted_ids)
    except BulkWriteError as e:
        detalles = e.details or {}
        n = detalles.get("nInserted", 0)
        logger.warning("BulkWriteError en %s (insertados de este lote: %d): %s",
                       nombre_entidad, n, detalles)
    progreso = min(procesados, total_esperado)
    pct = (progreso / total_esperado) * 100 if total_esperado else 0
    print(f"  Progreso: {progreso}/{total_esperado} ({pct:.1f}%)")
    return n


def ingestir_fuentes_existentes(client):
    inicio = time.time()
    db = client[DB_NAME]

    print("--- Cargando catalogo de peliculas ---")
    peliculas, db = leer_todo(client, "movies", {"_id": 1, "title": 1, "genres": 1,
                                                 "directors": 1, "year": 1, "rated": 1,
                                                 "imdb": 1})
    if db is None or not peliculas:
        logger.error("No fue posible leer movies desde sample_mflix")
        return
    catalogo = {p["_id"]: p for p in peliculas}
    logger.info("Catalogo de peliculas: %d", len(catalogo))

    print("--- Cargando audiencia (usuarios) ---")
    usuarios_rows, db = leer_todo(client, "users", {"name": 1, "email": 1})
    if db is None:
        logger.error("No fue posible leer users desde sample_mflix")
        return
    usuarios = {}
    for u in usuarios_rows:
        if u.get("name"):
            usuarios[u["name"]] = u.get("_id")
    logger.info("Usuarios registrados: %d", len(usuarios))

    print("--- Ingesta masiva de comentarios -> interacciones_analiticas ---")
    total_comments = db.comments.count_documents({})
    logger.info("Total de interacciones a procesar: %d (lote=%d)", total_comments, BATCH_SIZE)

    def generador():
        cursor = db.comments.find({}, {"_id": 1, "movie_id": 1, "name": 1,
                                       "email": 1, "date": 1}).batch_size(BATCH_SIZE)
        for c in cursor:
            yield construir_hecho(c, catalogo, usuarios)

    insertar_en_lotes(db[COLECCION_OLAP], total_comments, generador(), "interacciones_analiticas")

    tiempo_total = time.time() - inicio
    print("=" * 60)
    print("INGESTA MASIVA COMPLETADA (datos existentes de sample_mflix)")
    print(f"  Tiempo total de ejecucion: {tiempo_total:.2f} segundos")
    print(f"  Users en sample_mflix: {db.users.count_documents({})}")
    print(f"  Movies en sample_mflix: {db.movies.count_documents({})}")
    print(f"  Comments en sample_mflix: {db.comments.count_documents({})}")
    print(f"  Registros analiticos en interacciones_analiticas: {db[COLECCION_OLAP].count_documents({})}")
    print("=" * 60)


def main():
    client = conectar()
    if not client:
        return
    ingestir_fuentes_existentes(client)
    client.close()


if __name__ == "__main__":
    main()