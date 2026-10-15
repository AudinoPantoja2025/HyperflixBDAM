"""Fase 3 — Data Warehouse en MongoDB (modelo estrella de streaming).

Construye el DW sin alterar el ETL existente (``main.py`` -> Data Lake ->
Parquet) ni los jobs Spark/CDC:

- Fuente OLAP: ``interacciones_analiticas`` (base ``sample_mflix``,
  1 doc por comentario: usuario + pelicula + fecha).
- Destino (modelo estrella): ``dim_tiempo``, ``dim_usuario``,
  ``dim_pelicula`` y ``fact_interacciones`` en la misma base.
- Motor: ``aggregate()`` con ``$group`` -> ``$project`` -> ``$merge`` y
  ``allowDiskUse=True`` (agregaciones grandes usan disco temporal).

Aqui nada se vende: las medidas son interacciones (actividad), rating de
contexto y longitud del comentario.

Uso (sustentacion):
    python data_warehouse.py --db sample_mflix
    python -m pytest tests/test_data_warehouse.py -q
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.logger import get_logger

logger = get_logger(__name__)

DW_DATABASE_DEFAULT = "sample_mflix"
SOURCE_COLLECTION = "interacciones_analiticas"
DIM_TIEMPO = "dim_tiempo"
DIM_USUARIO = "dim_usuario"
DIM_PELICULA = "dim_pelicula"
FACT_INTERACCIONES = "fact_interacciones"


class DataWarehouseError(Exception):
    """Error de configuracion o ejecucion del Data Warehouse."""


# ------------------------------------------------------- pipelines (Paso 3-6)
def build_dim_tiempo_pipeline(source: str = SOURCE_COLLECTION,
                              target: str = DIM_TIEMPO) -> List[Dict[str, Any]]:
    """Dimension Tiempo (calendario analitico).

    ``$group`` elimina duplicados por fecha; ``$project`` deriva trimestre
    (``$ceil($divide:[mes,3])``) y fin de semana con ``$cond`` + ``$dayOfWeek``
    sobre ``$fecha``. ``$merge`` = INSERT inteligente (inserta si no existe,
    reemplaza si existe).
    """
    return [
        {"$group": {
            "_id": {
                "fecha": "$fecha",
                "anio": "$anio",
                "mes": "$mes",
                "dia": "$dia",
            }
        }},
        {"$project": {
            "_id": 0,
            "fecha_id": "$_id.fecha",
            "anio": "$_id.anio",
            "mes": "$_id.mes",
            "dia": "$_id.dia",
            "trimestre": {"$ceil": {"$divide": ["$_id.mes", 3]}},
            "fin_de_semana": {"$cond": [
                {"$in": [{"$dayOfWeek": "$_id.fecha"}, [1, 7]]},
                "Si", "No",
            ]},
        }},
        {"$merge": {
            "into": target,
            "on": "fecha_id",
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]


def build_dim_usuario_pipeline(source: str = SOURCE_COLLECTION,
                               target: str = DIM_USUARIO) -> List[Dict[str, Any]]:
    """Dimension Usuario (``$first`` + ``$sum`` + ``$avg``/``$round``).

    No se repite el segmento por cada interaccion: ``$first`` toma el primero
    (atributo descriptivo unico); ``$sum`` cuenta interacciones y ``$avg`` con
    ``$round`` calcula el rating promedio a 2 decimales. Los ids nulos se
    agrupan como ``DESCONOCIDO`` (``$merge`` no acepta nulos en ``on``).
    """
    return [
        {"$group": {
            "_id": {"$ifNull": ["$usuario.id", "DESCONOCIDO"]},
            "nombre": {"$first": "$usuario.nombre"},
            "segmento": {"$first": "$usuario.segmento"},
            "total_interacciones": {"$sum": "$cantidad"},
            "rating_promedio_raw": {"$avg": "$rating_pelicula"},
        }},
        {"$project": {
            "_id": 0,
            "usuario_id": "$_id",
            "nombre": 1,
            "segmento": 1,
            "total_interacciones": 1,
            "rating_promedio": {"$round": ["$rating_promedio_raw", 2]},
        }},
        {"$merge": {
            "into": target,
            "on": "usuario_id",
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]


def build_dim_pelicula_pipeline(source: str = SOURCE_COLLECTION,
                                target: str = DIM_PELICULA) -> List[Dict[str, Any]]:
    """Dimension Pelicula (misma logica, agrupada por pelicula.id)."""
    return [
        {"$group": {
            "_id": {"$ifNull": ["$pelicula.id", "SIN_PELICULA"]},
            "titulo": {"$first": "$pelicula.titulo"},
            "categoria": {"$first": "$pelicula.categoria"},
            "marca": {"$first": "$pelicula.marca"},
            "rating": {"$first": "$pelicula.rating"},
            "num_interacciones": {"$sum": "$cantidad"},
        }},
        {"$project": {
            "_id": 0,
            "pelicula_id": "$_id",
            "titulo": 1,
            "categoria": 1,
            "marca": 1,
            "rating": 1,
            "num_interacciones": 1,
        }},
        {"$merge": {
            "into": target,
            "on": "pelicula_id",
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]


def build_fact_interacciones_pipeline(source: str = SOURCE_COLLECTION,
                                      target: str = FACT_INTERACCIONES) -> List[Dict[str, Any]]:
    """Tabla de hechos (solo claves + medidas, sin duplicar dimensiones).

    Cada documento guarda ``usuario_id`` / ``pelicula_id`` / ``fecha_id`` en
    lugar de toda la informacion del usuario o la pelicula (menos
    duplicacion, modelo estrella clasico listo para BI: Compass, Atlas SQL,
    Power BI, Tableau).
    """
    return [
        {"$project": {
            "_id": 0,
            "interaccion_id": {"$toString": "$_id"},
            "fecha_id": "$fecha",
            "usuario_id": {"$ifNull": ["$usuario.id", "DESCONOCIDO"]},
            "pelicula_id": {"$ifNull": ["$pelicula.id", "SIN_PELICULA"]},
            "cantidad": "$cantidad",
            "rating_pelicula": "$rating_pelicula",
            "longitud_comentario": "$longitud_comentario",
        }},
        {"$merge": {
            "into": target,
            "on": "interaccion_id",
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]


# ------------------------------------------------------------- datos demo
def demo_seed_docs() -> List[Dict[str, Any]]:
    """Interacciones de ejemplo (Mocoa) para sustentar sin datos reales."""
    base = [
        ("m1", "Laptop en apuros", datetime(2026, 8, 1), "u1", "Ana Ruiz",
         "Registrado", 2, 8.5, "Gran pelicula, la recomiendo"),
        ("m1", "Laptop en apuros", datetime(2026, 8, 1), None, "Luis Paz",
         "Anonimo", 1, 8.5, "Buena"),
        ("m2", "Noche de estreno", datetime(2026, 8, 2), "u2", "Marta Gil",
         "Registrado", 3, 7.0, "El final me sorprendio bastante"),
        ("m2", "Noche de estreno", datetime(2026, 4, 10), None, "Pedro N.",
         "Anonimo", 2, 7.0, "Regular"),
        ("m1", "Laptop en apuros", datetime(2026, 12, 5), "u1", "Ana Ruiz",
         "Registrado", 1, 8.5, "La volvi a ver, excelente"),
        ("m3", "Cortos del sur", datetime(2026, 8, 2), None, "Lucia T.",
         "Anonimo", 3, 6.0, "Corto pero entretenido"),
    ]
    docs = []
    for i, (mid, titulo, fecha, uid, nombre, seg, n, rating, texto) in enumerate(base, 1):
        docs.append({
            "fecha": fecha,
            "anio": fecha.year, "mes": fecha.month, "dia": fecha.day,
            "usuario": {"id": uid, "nombre": nombre, "segmento": seg},
            "pelicula": {"id": mid, "titulo": titulo, "categoria": "Drama",
                         "marca": "Sur Films", "rating": rating,
                         "clasificacion": "PG"},
            "cantidad": 1,
            "rating_pelicula": rating,
            "longitud_comentario": len(texto),
        })
    return docs


def ensure_seed(db, source: str = SOURCE_COLLECTION,
                docs: Optional[List[Dict[str, Any]]] = None) -> int:
    """Inserta datos demo solo si la fuente esta vacia. Retorna insertados."""
    coll = db[source]
    try:
        existing = coll.count_documents({})
    except Exception:
        existing = 0
    if existing > 0:
        logger.info("Fuente '%s' ya tiene %d docs; no se inserta seed.", source, existing)
        return 0
    seed = docs if docs is not None else demo_seed_docs()
    if not seed:
        return 0
    coll.insert_many(seed)
    logger.info("Seed insertado en '%s': %d interacciones demo.", source, len(seed))
    return len(seed)


# ------------------------------------------------------------------ runner
def run_data_warehouse(db, source: str = SOURCE_COLLECTION,
                       drop_first: bool = True,
                       allow_disk_use: bool = True,
                       seed_if_empty: bool = False) -> Dict[str, Any]:
    """Ejecuta las 4 agregaciones (``allowDiskUse=True``).

    Con ``allowDiskUse`` MongoDB usa disco temporal si la agregacion supera
    la memoria; sin esto una agregacion grande podria fallar (clave en Big
    Data). Retorna ``count_documents`` por coleccion (resumen final).
    """
    if db is None:
        raise DataWarehouseError("Se requiere una base de datos (db).")
    try:
        names = set(db.list_collection_names())
    except Exception as exc:
        raise DataWarehouseError(f"No se pudo listar colecciones: {exc}") from exc
    if source not in names:
        if seed_if_empty:
            ensure_seed(db, source)
        else:
            raise DataWarehouseError(
                f"Fuente '{source}' no existe en '{db.name}'. "
                "Use --seed para la demo o verifique la base."
            )

    if drop_first:
        # Empezar desde cero: drop + recreacion con indice unico, pues $merge
        # con `on` propio exige indice unico previo (error 51183 si falta).
        for target, key in ((DIM_TIEMPO, "fecha_id"),
                            (DIM_USUARIO, "usuario_id"),
                            (DIM_PELICULA, "pelicula_id"),
                            (FACT_INTERACCIONES, "interaccion_id")):
            try:
                db[target].drop()
                db.create_collection(target)
                db[target].create_index([(key, 1)], unique=True,
                                        name=f"idx_{target}_{key}")
            except Exception as exc:
                raise DataWarehouseError(
                    f"No se pudo limpiar '{target}': {exc}") from exc

    pipelines = {
        DIM_TIEMPO: build_dim_tiempo_pipeline(source, DIM_TIEMPO),
        DIM_USUARIO: build_dim_usuario_pipeline(source, DIM_USUARIO),
        DIM_PELICULA: build_dim_pelicula_pipeline(source, DIM_PELICULA),
        FACT_INTERACCIONES: build_fact_interacciones_pipeline(source, FACT_INTERACCIONES),
    }
    for target, pipeline in pipelines.items():
        logger.info("Agregando '%s' -> '%s' ...", source, target)
        try:
            # allowDiskUse: usa disco temporal ante mucha memoria.
            list(db[source].aggregate(pipeline, allowDiskUse=allow_disk_use))
        except Exception as exc:
            raise DataWarehouseError(
                f"Fallo la agregacion hacia '{target}': {exc}") from exc

    counts = {name: db[name].count_documents({}) for name in
              [source, DIM_TIEMPO, DIM_USUARIO, DIM_PELICULA, FACT_INTERACCIONES]}
    logger.info("DATA WAREHOUSE CONFIGURADO: %s", counts)
    return {"database": db.name, "source": source, "counts": counts}
