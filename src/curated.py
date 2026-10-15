"""Modelo dimensional en la zona CURATED.

Cada comentario de ``sample_mflix`` es una interaccion de streaming
(audiencia <-> catalogo) y se modela como hecho de actividad:

Tablas generadas:

- FACT_INTERACCIONES  (un registro por comentario)
- DIM_USUARIO         (audiencia: registrados + autores anonimos)
- DIM_PELICULA        (catalogo: categoria, marca/director, rating)
- DIM_TIEMPO          (calendario derivado de las fechas de interaccion)

Medidas honestas del hecho (sin ventas: aqui nada se vende):

- ``cantidad = 1`` (una interaccion).
- ``rating_pelicula = imdb.rating`` (contexto de calidad, no dinero).
- ``longitud_comentario`` (tamano del texto en caracteres).
- ``categoria = genres[0]`` (genero principal para el BI).
"""

import pandas as pd

from src import transformations as tf
from src.logger import get_logger

logger = get_logger(__name__)


def build_dim_tiempo(dates: pd.Series) -> pd.DataFrame:
    """Calendario analitico derivado de una serie de fechas (datetime)."""
    d = pd.to_datetime(dates, errors="coerce").dropna().drop_duplicates()
    rows = []
    for fecha in d:
        rows.append(
            {
                "dim_tiempo_key": fecha.strftime("%Y-%m-%d"),
                "anio": fecha.year,
                "mes": fecha.month,
                "dia": fecha.day,
                "trimestre": (fecha.month - 1) // 3 + 1,
                "nombre_mes": tf.MESES[fecha.month - 1],
                "dia_semana_num": fecha.weekday(),
                "nombre_dia_semana": tf.DIAS[fecha.weekday()],
            }
        )
    rows.append(
        {
            "dim_tiempo_key": "DESCONOCIDO",
            "anio": None,
            "mes": None,
            "dia": None,
            "trimestre": None,
            "nombre_mes": None,
            "dia_semana_num": None,
            "nombre_dia_semana": None,
        }
    )
    return pd.DataFrame(rows)


def build_dim_pelicula(movies: pd.DataFrame) -> pd.DataFrame:
    """Dimension de pelicula (catalogo) usada por el hecho."""
    df = movies.copy()
    df["dim_pelicula_key"] = df["movie_id"]
    df["titulo"] = df["title"].astype("string")
    df["categoria"] = tf.first_element(df["genres"], "Sin categoria")
    df["marca"] = tf.first_element(df["directors"], "Desconocido")
    df["anio_lanzamiento"] = df["year"]
    df["clasificacion"] = df["rated"].fillna("Sin clasificacion")
    df["duracion_minutos"] = df["runtime"]
    columns = [
        "dim_pelicula_key", "titulo", "categoria", "marca",
        "anio_lanzamiento", "clasificacion", "duracion_minutos",
    ]
    return df[columns].drop_duplicates(subset=["dim_pelicula_key"]).reset_index(drop=True)


def build_dim_usuario(users: pd.DataFrame, comments: pd.DataFrame) -> pd.DataFrame:
    """Dimension de audiencia: registrados + anonimos participantes."""
    usr_ids = users["user_id"].tolist() if len(users) else []
    usr_names = users["name"].tolist() if len(users) else []
    usr_emails = users["email"].tolist() if len(users) else []

    lookup = {}
    for n, uid in zip(usr_names, usr_ids):
        if n is None or pd.isna(n):
            continue
        lookup[str(n)] = str(uid)

    rows = []
    for n, uid, email in zip(usr_names, usr_ids, usr_emails):
        if n is None or pd.isna(n):
            continue
        rows.append(
            {
                "dim_usuario_key": str(uid),
                "nombre": str(n),
                "email": None if email is None or pd.isna(email) else str(email),
                "segmento": "Registrado",
            }
        )

    seen_anon = set()
    if len(comments):
        c_names = comments["name"].tolist()
        c_emails = comments["email"].tolist()
        for n, email in zip(c_names, c_emails):
            if n is None or pd.isna(n):
                continue
            name = str(n)
            if name in lookup:
                continue
            key = "anon:" + name.lower()
            if key in seen_anon:
                continue
            seen_anon.add(key)
            rows.append(
                {
                    "dim_usuario_key": key,
                    "nombre": name,
                    "email": None if email is None or pd.isna(email) else str(email),
                    "segmento": "Anonimo",
                }
            )

    columns = ["dim_usuario_key", "nombre", "email", "segmento"]
    if not rows:
        return pd.DataFrame(columns=pd.Index(columns, dtype="object"))
    return pd.DataFrame(rows)


def build_fact_interacciones(
    comments: pd.DataFrame,
    movies: pd.DataFrame,
    users: pd.DataFrame,
) -> pd.DataFrame:
    """Tabla de hechos: una fila por interaccion (comentario).

    ``users`` se integra via la coincidencia de ``comments.name`` con
    ``users.name``: si coincide -> "Registrado"; si no -> "Anonimo".
    Sin metricas de venta: la actividad se mide en interacciones (cantidad),
    contexto de calidad (rating) y tamano del texto.
    """
    fact = comments.copy()

    dates = pd.to_datetime(fact["date"], errors="coerce")
    fact["dim_tiempo_key"] = dates.dt.strftime("%Y-%m-%d")
    fact["dim_tiempo_key"] = fact["dim_tiempo_key"].fillna("DESCONOCIDO")

    user_lookup = {}
    if len(users):
        for n, uid in zip(users["name"].tolist(), users["user_id"].tolist()):
            if n is not None and not pd.isna(n):
                user_lookup[str(n)] = str(uid)
    fact["dim_usuario_key"] = fact["name"].apply(
        lambda n: user_lookup.get(str(n), "anon:" + str(n).lower())
        if n is not None and not pd.isna(n)
        else "DESCONOCIDO"
    )

    movie_map = (
        movies[["movie_id", "title", "imdb_rating", "genres"]]
        .drop_duplicates(subset=["movie_id"])
    )

    fact = fact.merge(movie_map, how="left", on="movie_id")
    fact = fact.rename(columns={"movie_id": "dim_pelicula_key"})

    fact["cantidad"] = 1
    fact["categoria"] = tf.first_element(
        fact["genres"].apply(lambda v: v if isinstance(v, list) else []),
        "Sin categoria",
    )
    fact["rating_pelicula"] = (
        pd.to_numeric(fact["imdb_rating"], errors="coerce").fillna(0).round(2)
    )
    fact["longitud_comentario"] = (
        fact["text"].astype("string").str.len().fillna(0).astype(int)
    )

    columns = [
        "interaccion_id", "dim_tiempo_key", "dim_usuario_key", "dim_pelicula_key",
        "categoria", "cantidad", "rating_pelicula", "longitud_comentario",
    ]
    return fact[columns].reset_index(drop=True)


def build_curated(processed: dict) -> dict:
    """Construye los 4 datasets del modelo dimensional a partir de PROCESSED.

    Requiere ``comments``, ``movies`` y ``users``. El ``dim_usuario_key`` del
    hecho se calcula con la misma regla usada por los scripts existentes.
    """
    required = ["comments", "movies", "users"]
    missing = [c for c in required if c not in processed]
    if missing:
        logger.warning("CURATED omitido: faltan colecciones: %s", missing)
        return {}

    comments = processed["comments"]
    movies = processed["movies"]
    users = processed["users"]

    fact = build_fact_interacciones(comments, movies, users)

    if len(fact) != len(comments):
        raise ValueError(
            f"FACT_INTERACCIONES: conteo inesperado ({len(fact)} != {len(comments)})"
        )

    datasets = {
        "FACT_INTERACCIONES": fact,
        "DIM_TIEMPO": build_dim_tiempo(comments["date"]),
        "DIM_PELICULA": build_dim_pelicula(movies),
        "DIM_USUARIO": build_dim_usuario(users, comments),
    }
    logger.info(
        "CURATED construido: fact=%d, dim_tiempo=%d, dim_pelicula=%d, dim_usuario=%d",
        len(datasets["FACT_INTERACCIONES"]),
        len(datasets["DIM_TIEMPO"]),
        len(datasets["DIM_PELICULA"]),
        len(datasets["DIM_USUARIO"]),
    )
    return datasets