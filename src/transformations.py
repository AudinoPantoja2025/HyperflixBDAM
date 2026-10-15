"""Transformaciones con Pandas para la capa PROCESSED.

Cada funcion es independiente, reutilizable y conduce a un DataFrame
adecuado para Parquet. Reglas aplicadas (documentadas en el README):

- ``_id`` se conserva como identificador textual (analitico estable).
- ``password`` (users) se elimina en PROCESSED por minimizacion de datos;
  en RAW queda la copia original.
- Fechas se convierten con ``errors="coerce"`` (inconvertibles -> NaT).
- Duplicados se detectan (por ``_id``) y se conserva la primera ocurrencia,
  registrando cuantos se rechazaron; no se pierden silenciosamente.
- Se valida que el conteo resultante corresponda (extraidos = procesados + rechazados).
"""

import json

import pandas as pd

from src.logger import get_logger

logger = get_logger(__name__)

MESES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
DIAS = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]


# --------------------------------------------------------------- basicos
def docs_to_dataframe(docs, expected_columns=None) -> pd.DataFrame:
    """Convierte documentos (lista de dict) a DataFrame.

    Para listas vacias se devuelve un DataFrame con las columnas esperadas,
    de modo que la etapa posterior sea consistente.
    """
    if not docs:
        cols = list(expected_columns) if expected_columns else []
        if not cols:
            return pd.DataFrame(index=pd.RangeIndex(0))
        return pd.DataFrame(columns=pd.Index(cols, dtype="object"))
    df = pd.DataFrame(docs)
    if expected_columns:
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
    return df


def to_str_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    """Convierte columnas al tipo textual nullable (equivalente a VARCHAR NULL)."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def safe_to_datetime(df: pd.DataFrame, columns) -> pd.DataFrame:
    """Convierte columnas a datetime; valores invalidos quedan como NaT."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def detect_duplicates(df: pd.DataFrame, subset=None):
    """Devuelve una Serie bool con las filas duplicadas (conserva la primera)."""
    if df.empty:
        return pd.Series([False] * len(df), index=df.index)
    if subset is None:
        subset = list(df.columns)
    return df.duplicated(subset=subset, keep="first")


def deduplicate(df: pd.DataFrame, subset=None, report_name="") -> tuple:
    """Elimina duplicados registrando cuantos fueron rechazados."""
    mask = detect_duplicates(df, subset)
    n_dups = int(mask.sum())
    if n_dups:
        logger.warning(
            "'%s': %d fila(s) duplicada(s) detectadas; se conserva la primera y "
            "se rechazan las restantes.",
            report_name,
            n_dups,
        )
    return df[~mask].copy(), n_dups


def split_duplicates(df: pd.DataFrame, subset=None) -> tuple:
    """Separa (conservadas, rechazadas) sin descartar las filas duplicadas.

    Las filas rechazadas se devuelven completas para que la orquestacion pueda
    persistirlas (no se eliminan silenciosamente).
    """
    mask = detect_duplicates(df, subset)
    kept = df[~mask].copy()
    rejected = df[mask].copy()
    if len(rejected):
        logger.warning(
            "'%s': %d fila(s) duplicada(s) separadas para persistir en rejected/.",
            subset,
            len(rejected),
        )
    return kept, rejected


def validate_required_columns(df: pd.DataFrame, required, name=""):
    """Valida que el DataFrame contenga las columnas obligatorias."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"'{name}': faltan columnas obligatorias: {missing}")
    return df


def count_records_ok(extracted: int, processed: int, rejected: int = 0, name=""):
    """Verifica que no se pierdan registros: extraidos == procesados + rechazados."""
    expected = extracted - rejected
    if processed != expected:
        raise ValueError(
            f"'{name}': balance de registros invalido "
            f"(extraidos={extracted}, rechazados={rejected}, procesados={processed})"
        )
    return True


def first_element(series, default):
    """Primer elemento de listas (categoria/marca), con valor alternativo."""
    out = []
    for v in series:
        if isinstance(v, list) and v:
            out.append(v[0])
        else:
            out.append(default)
    return pd.Series(out, index=series.index)


# ------------------------------------------------------------ por coleccion
def transform_users(df: pd.DataFrame, drop_password: bool = True) -> pd.DataFrame:
    """Limpieza de la coleccion users."""
    validate_required_columns(df, ["_id", "name", "email"], "users")
    df = df.copy()
    if drop_password and "password" in df.columns:
        df = df.drop(columns=["password"])
    df["user_id"] = df["_id"].astype("string")
    df = to_str_columns(df, ["name", "email"])
    return df


def transform_movies(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizacion de la coleccion movies."""
    validate_required_columns(df, ["_id"], "movies")
    df = df.copy()
    df["movie_id"] = df["_id"].astype("string")

    imdb = df["imdb"].apply(lambda v: v if isinstance(v, dict) else {})
    df["imdb_rating"] = pd.to_numeric(imdb.apply(lambda d: d.get("rating")), errors="coerce")
    df["imdb_votes"] = pd.to_numeric(imdb.apply(lambda d: d.get("votes")), errors="coerce")
    df["imdb_id"] = imdb.apply(lambda d: d.get("id")).astype("string")

    for col in ["year", "runtime", "num_mflix_comments"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "tomatoes" in df.columns:
        df["tomatoes_json"] = df["tomatoes"].apply(
            lambda v: json.dumps(v, default=str) if isinstance(v, dict) else None
        )

    df = safe_to_datetime(df, ["released"])
    for col in ["genres", "cast", "directors", "writers", "countries", "languages"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: (
                    list(v)
                    if isinstance(v, (list, tuple))
                    else ([] if pd.isna(v) else [v])
                )
            )
    return df


def transform_comments(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizacion de la coleccion comments (hechos/transacciones)."""
    validate_required_columns(
        df, ["_id", "movie_id", "name", "email", "text", "date"], "comments"
    )
    df = df.copy()
    df["interaccion_id"] = df["_id"].astype("string")
    df["movie_id"] = df["movie_id"].astype("string")
    df = to_str_columns(df, ["name", "email", "text"])
    df = safe_to_datetime(df, ["date"])
    return df


def _as_dict(series):
    return series.apply(lambda v: v if isinstance(v, dict) else {})


def transform_theaters(df: pd.DataFrame) -> pd.DataFrame:
    """Aplanamiento de theaters (location.address y location.geo)."""
    validate_required_columns(df, ["_id"], "theaters")
    df = df.copy()
    df["theater_id"] = (
        pd.to_numeric(df["theaterId"], errors="coerce")
        if "theaterId" in df.columns
        else df["_id"].astype("string")
    )
    loc = _as_dict(df["location"]) if "location" in df.columns else pd.Series([{}] * len(df), index=df.index)
    addr = loc.apply(lambda d: d.get("address") if isinstance(d, dict) else {})
    geo = loc.apply(lambda d: (d.get("geo") or {}) if isinstance(d, dict) else {})

    for key in ["street1", "street2", "city", "state", "zipcode"]:
        df[f"address_{key}"] = addr.apply(
            lambda d, k=key: d.get(k) if isinstance(d, dict) else None
        ).astype("string")

    coords = geo.apply(
        lambda d: (d.get("coordinates") or []) if isinstance(d, dict) else []
    )
    df["geo_lon"] = coords.apply(lambda c: c[0] if len(c) > 0 else None)
    df["geo_lat"] = coords.apply(lambda c: c[1] if len(c) > 1 else None)
    df["geo_type"] = geo.apply(
        lambda d: d.get("type") if isinstance(d, dict) else None
    ).astype("string")
    return df


def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte diccionarios residuales a JSON para compatibilidad con Parquet.

    PyArrow no puede serializar dicts vacios ni estructuras inconsistentes.
    Las columnas que deban conservar su semantica (ej. listas de cadenas en
    movies) se procesan antes; aqui solo se normalizan los dicts restantes.
    """
    out = df.copy()
    for col in out.columns:
        has_dict = bool(out[col].apply(lambda v: isinstance(v, dict)).any())
        if has_dict:
            out[col] = out[col].apply(
                lambda v: (
                    json.dumps(v, ensure_ascii=False, default=str)
                    if isinstance(v, dict)
                    else v
                )
            )
    return out


TRANSFORMERS = {
    "users": transform_users,
    "movies": transform_movies,
    "comments": transform_comments,
    "theaters": transform_theaters,
}


def transform_collection(collection: str, df: pd.DataFrame) -> pd.DataFrame:
    """Aplica el transformador asociado a la coleccion o el generico.

    Al final se sanitizan los diccionarios residuales para que el DataFrame
    sea serializable por PyArrow sin perdida de informacion.
    """
    transformer = TRANSFORMERS.get(collection)
    if transformer is None:
        logger.warning(
            "'%s': sin transformador especifico; se usa el generico (solo _id a str).",
            collection,
        )
        df = df.copy()
        if "_id" in df.columns:
            df["_id"] = df["_id"].astype("string")
    else:
        df = transformer(df)
    return sanitize_for_parquet(df)