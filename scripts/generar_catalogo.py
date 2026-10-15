"""Catalogo de datos automatizado (Gobierno de Datos / Data Discovery).

Genera un Excel con dos pestanas a partir de las colecciones reales de
MongoDB Atlas:

    1. "Resumen Arquitectonico": inventario de colecciones clasificadas por
       su rol (OLTP / OLAP / Data Warehouse) con volumen y tamano.
    2. "Diccionario de Datos": un campo por fila (incluidos los anidados,
       p.ej. ``location.address.city``) con tipo estandarizado, clave
       primaria (PK = ``_id``), claves foraneas (campos ``*_id``), si el
       campo esta indexado y una descripcion de negocio.

Como funciona internamente (ingenieria inversa sin configuracion manual):

    1. Conecta a la base configurada en ``.env`` (p.ej. ``sample_mflix``).
    2. Consulta ``list_collection_names()`` y los metadatos reales:
       ``index_information()`` (indices en Mongo) y ``collstats()`` (volumen).
    3. Toma una muestra de documentos por coleccion y "aplana" los JSON
       anidados con una funcion recursiva (``aplanar_documento``).
    4. Traduce los tipos BSON/Python a tipos de negocio estandar
       (ObjectId -> "String (UUID)", datetime -> "Timestamp", list -> "Array").
    5. Detecta claves: Primary Key = ``_id`` (siempre indexada); Foreign Key
       = cualquier campo que termine en ``_id`` (``movie_id``, ``user_id``...).
    6. Enriquece cada campo con una descripcion de negocio (diccionario
       ``DESCRIPCIONES_CAMPOS``) usando el nombre completo y el nombre hoja.
    7. Exporta a Excel (pandas + openpyxl) con anchos de columna ajustados y
       encabezados congelados.

Uso:
    python scripts/generar_catalogo.py              # base del .env
    python scripts/generar_catalogo.py --db sample_mflix
    python scripts/generar_catalogo.py --muestra 100 --salida catalogo.xlsx

El archivo se escribe en la raiz del proyecto con el nombre
``Catalogo_Datos_VentasDB_YYYYMMDD.xlsx`` por defecto.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

from bson import ObjectId
from bson.decimal128 import Decimal128
from bson.int64 import Int64

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.logger import get_logger  # noqa: E402

logger = get_logger(__name__)

LIMITE_MUESTRA = 50
SUFIJOS_CLAVE_FORANEA = ("_id", "codigo_", "id_")

# -------------------------------------------------------------------------
# Diccionario semantico: campo (o nombre hoja) -> descripcion de negocio.
# -------------------------------------------------------------------------
DESCRIPCIONES_CAMPOS = {
    "_id": "Identificador unico del documento en MongoDB (Primary Key, siempre indexado).",
    "id_usuario": "Clave foranea hacia la dimension de usuario (usuarios del sistema).",
    "id_pelicula": "Clave foranea hacia la dimension de pelicula (catalogo de contenido).",
    "id_tiempo": "Clave foranea hacia la dimension de tiempo (fecha de la interaccion).",
    "movie_id": "Clave foranea hacia la coleccion 'movies' (pelicula comentada).",
    "user_id": "Clave foranea hacia la coleccion 'users' (usuario que interactua).",
    "name": "Nombre completo del registrado en la coleccion.",
    "email": "Correo electronico del usuario (identificador de negocio).",
    "birthdate": "Fecha de nacimiento del usuario.",
    "gender": "Genero del usuario.",
    "location": "Localizacion geografica del usuario (objeto anidado).",
    "preferences": "Preferencias del usuario (objeto anidado de gustos de contenido).",
    "favoritos": "Lista de favoritos o generos preferidos.",
    "title": "Titulo de la pelicula (identificador de negocio del catalogo).",
    "year": "Anio de estreno de la pelicula.",
    "runtime": "Duracion de la pelicula en minutos.",
    "rated": "Clasificacion por edades (G, PG, PG-13, R...).",
    "released": "Fecha de lanzamiento en cines.",
    "plot": "Sinopsis de la pelicula.",
    "genre": "Genero cinematografico de la pelicula.",
    "genres": "Lista de generos cinematograficos.",
    "directors": "Lista de directores de la pelicula.",
    "cast": "Lista de actores participantes.",
    "writers": "Lista de guionistas.",
    "languages": "Idiomas en que esta disponible la pelicula.",
    "countries": "Paises de produccion o distribucion.",
    "comments": "Cantidad de comentarios recibidos.",
    "metacritic": "Puntuacion Metacritic (critica especializada).",
    "awards": "Premios recibidos (objeto anidado).",
    "imdb": "Objeto de calificaciones IMDB (rating y votos).",
    "rating": "Calificacion numerica otorgada.",
    "votes": "Numero de votos recibidos.",
    "tomates": "Objeto con calificaciones tipo Rotten Tomatoes.",
    "viewer": "Calificacion del publico en Rotten Tomatoes.",
    "type": "Tipo de contenido (p.ej. 'movie').",
    "dvd": "Fecha de lanzamiento en DVD.",
    "lastupdated": "Fecha de la ultima actualizacion del registro.",
    "theaterId": "Clave foranea hacia la coleccion 'theaters' (sala de cine).",
    "theater_id": "Clave foranea hacia la coleccion 'theaters' (sala de cine).",
    "city": "Ciudad donde se ubica el teatro o el usuario.",
    "state": "Departamento o estado de la ubicacion.",
    "street": "Calle de la direccion.",
    "zipcode": "Codigo postal de la direccion.",
    "text": "Texto libre del comentario (resena).",
    "date": "Fecha del evento o de la resena.",
    "interacciones": "Conteo de interacciones registradas.",
    "total_venta": "Valor monetario total de la venta/interaccion.",
    "ganancia": "Margen de ganancia estimado de la interaccion.",
    "factura": "Numero de factura asignado a la interaccion.",
    "metodo_pago": "Metodo de pago utilizado en la interaccion.",
    "estado": "Estado del proceso (p.ej. 'Completada').",
    "nombre": "Nombre descriptivo del registro.",
    "fecha": "Fecha del hecho analitico.",
    "cantidad": "Cantidad de unidades involucradas.",
    "precio": "Precio unitario del producto.",
    "sku": "Stock Keeping Unit. Codigo unico de inventario del producto.",
    "producto_id": "Clave foranea hacia el catalogo de productos.",
    "cliente_id": "Clave foranea hacia el maestro de clientes.",
    "direccion": "Direccion del cliente (objeto anidado).",
    "ciudad": "Ciudad de la direccion.",
    "telefono": "Telefono de contacto.",
    "documento": "Documento de identidad del cliente.",
}

# -------------------------------------------------------------------------
# Utilidades de tipado y clasificacion (funciones puras, testeadas).
# -------------------------------------------------------------------------


def aplanar_documento(doc: dict, prefijo: str = "") -> Iterator[Tuple[str, object]]:
    """Aplana recursivamente un documento BSON anidado a campos 'punto'."""
    for clave, valor in (doc or {}).items():
        nombre = f"{prefijo}.{clave}" if prefijo else clave
        if isinstance(valor, dict):
            yield from aplanar_documento(valor, nombre)
        else:
            yield nombre, valor


def tipificar_valor(valor: object) -> str:
    """Traduce el tipo tecnico de Python/BSON a un tipo de negocio estandar."""
    if isinstance(valor, ObjectId):
        return "String (UUID)"
    if isinstance(valor, (datetime,)):
        return "Timestamp"
    if isinstance(valor, (list, tuple)):
        return "Array"
    if isinstance(valor, (dict,)):
        return "Objeto"
    if isinstance(valor, bool):
        return "Boolean"
    if isinstance(valor, (int, Int64)):
        return "Integer"
    if isinstance(valor, (float, Decimal128)):
        return "Double"
    if isinstance(valor, str):
        return "String"
    if valor is None:
        return "Null / Vacio"
    return type(valor).__name__


def clasificar_arquitectura(nombre_coleccion: str) -> str:
    """Clasifica el rol arquitectonico de una coleccion por convencion de nombre."""
    nombre = (nombre_coleccion or "").lower()
    if nombre.startswith(("dim_", "fact_", "fct_", "dw_")):
        return "Data Warehouse (Modelo Estrella)"
    if "analitic" in nombre:
        return "OLAP (Analitico)"
    return "OLTP (Transaccional)"


def es_primary_key(campo: str) -> bool:
    return campo == "_id"


def es_foreign_key(campo: str) -> bool:
    """Foreign Key: campo distinto de '_id' con marca de referencia.

    Cubre las dos convenciones del proyecto: sufijo (``movie_id``,
    ``user_id``) y prefijo del modelo estrella (``id_usuario``,
    ``id_pelicula``, ``id_tiempo``).
    """
    if es_primary_key(campo):
        return False
    hoja = campo.rsplit(".", 1)[-1]
    return any(hoja.startswith(marca) or hoja.endswith(marca)
               for marca in SUFIJOS_CLAVE_FORANEA)


def enriquecer_descripcion(campo: str, coleccion: str) -> str:
    """Descripcion de negocio: busca el nombre completo y luego la hoja."""
    hoja = campo.rsplit(".", 1)[-1]
    descripcion = DESCRIPCIONES_CAMPOS.get(campo) or DESCRIPCIONES_CAMPOS.get(hoja)
    if descripcion:
        return descripcion
    return (f"Campo '{hoja}' de la coleccion '{coleccion}' sin descripcion "
            "semantica asociada en el diccionario de negocio.")


def esta_indexado(campo: str, index_info: dict) -> bool:
    """Determina si el campo participa en algun indice de MongoDB."""
    if es_primary_key(campo):
        return True
    campos_indexados: Set[str] = set()
    for indice in (index_info or {}).values():
        for clave, _directo in indice.get("key", []):
            campos_indexados.add(clave)
    if campo in campos_indexados:
        return True
    return campo.split(".", 1)[0] in campos_indexados


def _formatear_bytes(tamano: Optional[int]) -> str:
    if tamano is None:
        return "n/d"
    if tamano >= 1024 ** 3:
        return f"{tamano / 1024 ** 3:.2f} GB"
    if tamano >= 1024 ** 2:
        return f"{tamano / 1024 ** 2:.2f} MB"
    if tamano >= 1024:
        return f"{tamano / 1024:.1f} KB"
    return f"{tamano} B"


# -------------------------------------------------------------------------
# Construccion del catalogo a partir de la base real.
# -------------------------------------------------------------------------


def construir_resumen(db, colecciones: List[str]) -> List[dict]:
    """Inventario por coleccion con volumen, tamano y numero de indices."""
    resumen: List[dict] = []
    for nombre in colecciones:
        col = db[nombre]
        try:
            stats = col.aggregate([{"$collStats": {"storageStats": {}}}]).next()
            registros = stats["storageStats"]["count"]
            tamano = stats["storageStats"]["size"]
            promedio = (tamano / registros) if registros else 0
        except Exception:  # noqa: BLE001 - sin permisos se degrada a count_documents
            registros = col.count_documents({})
            tamano, promedio = None, None
        indices = col.index_information()
        resumen.append({
            "Coleccion": nombre,
            "Rol Arquitectonico": clasificar_arquitectura(nombre),
            "Registros": registros,
            "Tamano Estimado": _formatear_bytes(tamano),
            "Promedio (bytes)": int(promedio) if promedio is not None else "n/d",
            "Indices": len(indices),
        })
        logger.info(
            "resumen: '%s' -> %d registros, %d indice(s)",
            nombre, registros, len(indices),
        )
    return resumen


def construir_diccionario(db, colecciones: List[str], limite: int = LIMITE_MUESTRA) -> List[dict]:
    """Campo por fila: tipos unicos, claves e indices vistos en el muestreo."""
    filas: List[dict] = []
    for nombre in colecciones:
        col = db[nombre]
        index_info = col.index_information()
        campos: Dict[str, Set[str]] = {}
        for doc in col.find({}, limit=limite):
            for campo, valor in aplanar_documento(doc):
                campos.setdefault(campo, set()).add(tipificar_valor(valor))
        for campo in sorted(campos, key=lambda c: (not es_primary_key(c), c)):
            filas.append({
                "Coleccion": nombre,
                "Campo": campo,
                "Tipo de Dato": ", ".join(sorted(campos[campo])),
                "Es Primary Key (PK)": "Si" if es_primary_key(campo) else "No",
                "Es Foreign Key (FK)": "Si" if es_foreign_key(campo) else "No",
                "Esta Indexado": "Si" if esta_indexado(campo, index_info) else "No",
                "Descripcion de Negocio": enriquecer_descripcion(campo, nombre),
            })
        logger.info("diccionario: '%s' -> %d campo(s) unico(s)", nombre, len(campos))
    return filas


# -------------------------------------------------------------------------
# Exportacion profesional a Excel.
# -------------------------------------------------------------------------


def _estilizar_hoja(ws, encabezados: List[str], filas_preview: List[list]) -> None:
    """Congela encabezados, ajusta anchos y resalta la fila de titulos."""
    from openpyxl.styles import Alignment, Font, PatternFill

    ancho_max: Dict[int, int] = {}
    for i, encabezado in enumerate(encabezados, start=1):
        ancho_max[i] = len(str(encabezado))
    for fila in filas_preview:
        for i, valor in enumerate(fila, start=1):
            ancho = len(str(valor))
            if ancho > ancho_max.get(i, 0):
                ancho_max[i] = ancho

    relleno = PatternFill(start_color="1F4E78", end_color="1F4E78",
                          fill_type="solid")
    fuente = Font(bold=True, color="FFFFFF")
    for celda in ws[1]:
        celda.fill = relleno
        celda.font = fuente
        celda.alignment = Alignment(vertical="center")

    from openpyxl.utils import get_column_letter

    for i, ancho in ancho_max.items():
        ws.column_dimensions[get_column_letter(i)].width = min(ancho + 2, 65)
    ws.freeze_panes = "A2"


def generar_excel(resumen: List[dict], diccionario: List[dict], salida: Path) -> Path:
    """Escribe las dos pestanas del catalogo en un archivo Excel."""
    import pandas as pd

    salida = Path(salida)
    salida.parent.mkdir(parents=True, exist_ok=True)

    df_resumen = pd.DataFrame(resumen)
    df_diccionario = pd.DataFrame(diccionario)

    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        df_resumen.to_excel(writer, sheet_name="Resumen Arquitectonico", index=False)
        df_diccionario.to_excel(writer, sheet_name="Diccionario de Datos", index=False)
        _estilizar_hoja(
            writer.sheets["Resumen Arquitectonico"],
            list(df_resumen.columns), df_resumen.values.tolist(),
        )
        _estilizar_hoja(
            writer.sheets["Diccionario de Datos"],
            list(df_diccionario.columns), df_diccionario.values.tolist(),
        )

    logger.info("Excel generado: %s (resumen=%d, diccionario=%d)",
                salida, len(df_resumen), len(df_diccionario))
    return salida


# -------------------------------------------------------------------------
# Orquestacion.
# -------------------------------------------------------------------------


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Genera el catalogo de datos (gobierno de datos / data discovery)."
    )
    parser.add_argument("--db", default=None, help="base de datos (default: .env)")
    parser.add_argument("--muestra", type=int, default=LIMITE_MUESTRA,
                        help="documentos a muestrear por coleccion")
    parser.add_argument("--salida", default=None,
                        help="ruta del Excel de salida (default: raiz del proyecto)")
    args = parser.parse_args(argv)

    from dotenv import load_dotenv

    from src.config import load_settings, mask_uri
    from src.mongo_client import MongoConnection, MongoConnectionError

    env_file = ROOT / ".env"
    load_dotenv(dotenv_path=str(env_file))
    try:
        settings = load_settings(env_path=str(env_file))
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] configuracion invalida: {exc}")
        return 2

    base = args.db or settings.mongodb_database
    print(f"Conectando a '{base}' ({mask_uri(settings.mongodb_uri)})...")
    conn = MongoConnection(settings.mongodb_uri, base, timeout_ms=6000, retries=2)
    try:
        _client, db = conn.connect()
        colecciones = sorted(db.list_collection_names())
    except MongoConnectionError as exc:
        print(f"[ERROR] no fue posible conectar a MongoDB Atlas: {exc}")
        print("  Revise: (1) su IP actual este en Network Access del cluster;")
        print("  (2) el DNS de su red (pymongo usa mongodb+srv); (3) intente mas tarde.")
        return 2
    # La conexion permanece abierta mientras se construyen el resumen y el diccionario
    # para evitar 'Cannot use MongoClient after close'.

    try:
        print(f"Colecciones encontradas ({len(colecciones)}): {', '.join(colecciones)}")
        resumen = construir_resumen(db, colecciones)
        diccionario = construir_diccionario(db, colecciones, limite=max(1, args.muestra))
    finally:
        conn.close()
    print("\n--- Resumen arquitectonico ---")
    for fila in resumen:
        print(f"  {fila['Coleccion']:<28} {fila['Rol Arquitectonico']:<32} "
              f"{fila['Registros']:>8}  {fila['Tamano Estimado']:>10}  "
              f"{fila['Indices']} indice(s)")

    stamp = datetime.now().strftime("%Y%m%d")
    salida = Path(args.salida) if args.salida else ROOT / f"Catalogo_Datos_MovieLensDB_{stamp}.xlsx"
    generar_excel(resumen, diccionario, salida)

    print(f"\n[OK] Catalogo generado: {salida}")
    print(f"     - 'Resumen Arquitectonico': {len(resumen)} colecciones.")
    print(f"     - 'Diccionario de Datos': {len(diccionario)} campos unicos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())