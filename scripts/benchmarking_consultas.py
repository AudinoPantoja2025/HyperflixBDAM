import os
import time
import json
import logging
from statistics import mean

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


def crear_indices(coleccion):
    print("Fase 1: Aplicando estrategia de indexacion compuesta...")
    coleccion.create_index([("anio", 1), ("mes", 1), ("dia", 1)], name="idx_fecha")
    coleccion.create_index([("pelicula.categoria", 1), ("rating_pelicula", -1)], name="idx_categoria_rating")
    coleccion.create_index([("usuario.segmento", 1)], name="idx_segmento")
    coleccion.create_index([("pelicula.clasificacion", 1), ("usuario.segmento", 1)],
                           name="idx_clasificacion_segmento")
    print("Indices compuestos creados exitosamente.")


def construir_consultas():
    return [
        {
            "nombre": "1. Interacciones totales mensuales",
            "pipeline": [
                {"$group": {"_id": {"anio": "$anio", "mes": "$mes"},
                            "interacciones": {"$sum": 1},
                            "rating_promedio": {"$avg": "$rating_pelicula"}}},
                {"$sort": {"_id.anio": 1, "_id.mes": 1}},
            ],
        },
        {
            "nombre": "2. Top 5 Peliculas por Categoria",
            "pipeline": [
                {"$group": {"_id": {"categoria": "$pelicula.categoria", "pelicula": "$pelicula.titulo"},
                            "interacciones": {"$sum": 1},
                            "rating_promedio": {"$avg": "$rating_pelicula"}}},
                {"$sort": {"_id.categoria": 1, "interacciones": -1}},
                {"$group": {"_id": "$_id.categoria",
                            "todas": {"$push": {"pelicula": "$_id.pelicula",
                                                "interacciones": "$interacciones"}}}},
                {"$project": {"categoria": "$_id", "top_peliculas": {"$slice": ["$todas", 5]}, "_id": 0}},
            ],
        },
        {
            "nombre": "3. Actividad por Segmento de Audiencia",
            "pipeline": [
                {"$group": {"_id": "$usuario.segmento",
                            "interacciones": {"$sum": 1},
                            "longitud_promedio": {"$avg": "$longitud_comentario"}}},
                {"$sort": {"interacciones": -1}},
            ],
        },
        {
            "nombre": "4. Rating Promedio por Clasificacion",
            "pipeline": [
                {"$group": {"_id": "$pelicula.clasificacion",
                            "rating_promedio": {"$avg": "$rating_pelicula"},
                            "total_interacciones": {"$sum": 1}}},
                {"$sort": {"total_interacciones": -1}},
            ],
        },
        {
            "nombre": "5. Actividad por Dia de la Semana",
            "pipeline": [
                {"$match": {"fecha": {"$type": "date"}}},
                {"$addFields": {"dia_semana": {"$dayOfWeek": "$fecha"}}},
                {"$group": {"_id": "$dia_semana",
                            "interacciones": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ],
        },
        {
            "nombre": "6. Top 10 Peliculas con Mas Interacciones",
            "pipeline": [
                {"$group": {"_id": "$pelicula.titulo",
                            "interacciones": {"$sum": 1},
                            "rating": {"$first": "$rating_pelicula"}}},
                {"$sort": {"interacciones": -1}},
                {"$limit": 10},
            ],
        },
        {
            "nombre": "7. Usuarios Registrados Mas Activos",
            "pipeline": [
                {"$match": {"usuario.segmento": "Registrado"}},
                {"$group": {"_id": "$usuario.nombre",
                            "interacciones": {"$sum": 1}}},
                {"$sort": {"interacciones": -1}},
                {"$limit": 10},
            ],
        },
        {
            "nombre": "8. Longitud Promedio por Categoria",
            "pipeline": [
                {"$group": {"_id": "$pelicula.categoria",
                            "longitud_promedio": {"$avg": "$longitud_comentario"},
                            "longitud_maxima": {"$max": "$longitud_comentario"},
                            "interacciones": {"$sum": 1}}},
                {"$sort": {"longitud_promedio": -1}},
            ],
        },
        {
            "nombre": "9. Tendencia Anual de Actividad",
            "pipeline": [
                {"$group": {"_id": "$anio",
                            "interacciones": {"$sum": 1},
                            "rating_promedio": {"$avg": "$rating_pelicula"}}},
                {"$sort": {"_id": 1}},
            ],
        },
        {
            "nombre": "10. Matriz Segmento x Clasificacion",
            "pipeline": [
                {"$group": {"_id": {"segmento": "$usuario.segmento",
                                    "clasificacion": "$pelicula.clasificacion"},
                            "interacciones": {"$sum": 1}}},
                {"$sort": {"interacciones": -1}},
            ],
        },
    ]


def ejecutar_benchmark():
    client, db = conectar()
    if not client or db is None:
        return
    coleccion = db[COLECCION_OLAP]
    print("Iniciando Benchmarking de Consultas Analiticas (sample_mflix)...")

    crear_indices(coleccion)

    consultas = construir_consultas()
    print("\nFase 2: Ejecutando consultas...\n")
    print("-" * 70)

    tiempos = []

    for consulta in consultas:
        print(f"'{consulta['nombre']}'")
        inicio = fin = time.perf_counter()
        resultados = []
        for intento in range(1, 5):
            try:
                inicio = time.perf_counter()
                resultados = list(coleccion.aggregate(consulta["pipeline"], allowDiskUse=True))
                fin = time.perf_counter()
                if resultados or intento >= 3:
                    break
            except Exception as e:
                logger.warning("Fallo al ejecutar consulta (intento %d): %s", intento, e)
                time.sleep(1)

        tiempo_ms = (fin - inicio) * 1000
        tiempos.append(tiempo_ms)
        print(f"  Latencia: {tiempo_ms:.2f} ms | Filas: {len(resultados)}")

        if len(resultados) > 0:
            muestra = json.loads(json.dumps(resultados[0], default=str))
            print(f"  Muestra: {muestra}")
        print("-" * 70)

    total_registros = coleccion.count_documents({})

    print("\n" + "=" * 70)
    print("RESUMEN DE BENCHMARKING")
    print("=" * 70)
    print(f"Consultas ejecutadas: {len(consultas)}")
    print(f"Latencia promedio: {mean(tiempos):.2f} ms")
    print(f"Latencia maxima: {max(tiempos):.2f} ms")
    print(f"Registros analiticos procesados: {total_registros}")
    print("=" * 70)

    client.close()


if __name__ == "__main__":
    ejecutar_benchmark()
