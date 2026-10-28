"""Genera archivos Parquet individuales de gran tamaño y los sube a S3.

Estrategia:
- Genera un número masivo de registros (2-3 millones) en un solo DataFrame.
- Los sube como UN SOLO archivo Parquet por zona (raw, processed, curated).
- Objetivo: cada archivo individual pese >= 1 GB.

Uso:
    python scripts/generar_datos_s3.py                    # 2 archivos de ~1 GB
    python scripts/generar_datos_s3.py --millones 3       # archivos de ~1.5 GB
    python scripts/generar_datos_s3.py --dry-run          # estimar sin subir
"""

from __future__ import annotations

import argparse
import io
import json
import os
import random
import string
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------ config
TARGET_GB    = 1.0
S3_BUCKET    = os.getenv("S3_BUCKET_NAME", "abp-data-lake-audino")
S3_REGION    = os.getenv("S3_REGION", "us-east-2")
AWS_KEY      = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET   = os.getenv("AWS_SECRET_ACCESS_KEY")

# Registros por archivo — 2M genera ~1 GB en Parquet Snappy
MILLONES_DEFAULT = 2

# Géneros y segmentos reales del dataset
GENEROS = ["Drama", "Comedy", "Thriller", "Action", "Romance", "Horror",
           "Documentary", "Animation", "Crime", "Adventure", "Sci-Fi", "Fantasy"]
SEGMENTOS = ["Registrado", "Anonimo"]
CLASIFICACIONES = ["G", "PG", "PG-13", "R", "NR"]
DIRECTORES = ["Nolan", "Tarantino", "Spielberg", "Coppola", "Scorsese",
              "Lynch", "Kubrick", "Fincher", "Anderson", "Villeneuve"]

# Textos de comentario sintéticos (variados para simular longitud real)
TEXTOS_BASE = [
    "Excelente película, la recomiendo ampliamente.",
    "Una historia muy bien contada con actuaciones sobresalientes.",
    "El guión es brillante y la dirección impecable.",
    "No era lo que esperaba pero me sorprendió gratamente.",
    "Los efectos visuales son increíbles para la época.",
    "Una obra maestra del cine moderno sin duda alguna.",
    "Interesante propuesta aunque algo lenta en el segundo acto.",
    "El final me dejó pensando durante días enteros.",
    "Muy recomendada para los amantes del género.",
    "La banda sonora complementa perfectamente la narrativa.",
    "Un clásico que no envejece con el paso del tiempo.",
    "Actuaciones memorables y fotografía deslumbrante.",
    "Historia predecible pero bien ejecutada en pantalla.",
    "Me mantuvo al borde del asiento durante toda la función.",
    "Cinematografía excepcional y diálogos muy bien escritos.",
]


# ------------------------------------------------------------------ helpers
def _s3_client():
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )


def _bucket_size_gb(s3) -> float:
    """Tamaño actual del bucket en GB."""
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=S3_BUCKET):
        for obj in page.get("Contents", []):
            total += obj["Size"]
    return total / (1024 ** 3)


def _upload_bytes(s3, data: bytes, key: str):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data)


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    return buf.getvalue()


def _df_to_jsonl_bytes(df: pd.DataFrame) -> bytes:
    lines = "\n".join(df.to_json(orient="records", lines=True, force_ascii=False)
                      .splitlines())
    return lines.encode("utf-8")


def _rnd_str(n=8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


# ------------------------------------------------- generadores por tabla
def gen_movies(n: int = 500) -> pd.DataFrame:
    rows = []
    for i in range(n):
        mid = f"syn_movie_{i:06d}"
        anio = random.randint(1970, 2026)
        rows.append({
            "movie_id": mid,
            "_id": mid,
            "title": f"Película Sintética {i:06d}",
            "year": anio,
            "genres": [random.choice(GENEROS), random.choice(GENEROS)],
            "directors": [random.choice(DIRECTORES)],
            "rated": random.choice(CLASIFICACIONES),
            "runtime": random.randint(75, 180),
            "imdb_rating": round(random.uniform(4.0, 9.5), 1),
            "plot": f"Trama sintética generada para pruebas de volumen. Película número {i}.",
        })
    return pd.DataFrame(rows)


def gen_users(n: int = 1000) -> pd.DataFrame:
    rows = []
    for i in range(n):
        uid = f"syn_user_{i:06d}"
        rows.append({
            "user_id": uid,
            "_id": uid,
            "name": f"Usuario Sintético {i:06d}",
            "email": f"usuario_{i:06d}@synth.test",
            "segmento": random.choice(SEGMENTOS),
        })
    return pd.DataFrame(rows)


def gen_comments(
    n: int,
    movie_ids: list,
    user_names: list,
    base_date: datetime,
) -> pd.DataFrame:
    """Genera n comentarios sintéticos usando numpy para máxima velocidad."""
    rng = np.random.default_rng(seed=42)

    # Generar fechas distribuidas en un rango de 365 días desde base_date
    segundos_offset = rng.integers(0, 365 * 24 * 3600, size=n)
    fechas = [
        (base_date + timedelta(seconds=int(s))).strftime("%Y-%m-%dT%H:%M:%S")
        for s in segundos_offset
    ]

    # Índices aleatorios para películas y usuarios
    idx_movies = rng.integers(0, len(movie_ids), size=n)
    idx_users  = rng.integers(0, len(user_names), size=n)

    # Textos de longitud variable (simula distribución real ~100-400 chars)
    longitudes = rng.integers(50, 400, size=n)
    textos = [
        (random.choice(TEXTOS_BASE) + " " +
         " ".join(_rnd_str(rng.integers(3, 8)) for _ in range(int(l) // 10)))[:int(l)]
        for l in longitudes
    ]

    return pd.DataFrame({
        "interaccion_id": [f"syn_{i:010d}" for i in range(n)],
        "_id":            [f"syn_{i:010d}" for i in range(n)],
        "movie_id":       [movie_ids[i] for i in idx_movies],
        "name":           [user_names[i] for i in idx_users],
        "email":          [f"u{i}@synth.test" for i in range(n)],
        "text":           textos,
        "date":           fechas,
    })


def gen_theaters(n: int = 200) -> pd.DataFrame:
    ciudades = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena",
                "Bucaramanga", "Pereira", "Manizales", "Pasto", "Mocoa"]
    rows = []
    for i in range(n):
        rows.append({
            "_id": f"syn_theater_{i:04d}",
            "theaterId": i + 10000,
            "location": {
                "city": random.choice(ciudades),
                "state": "Colombia",
            },
            "num_screens": random.randint(2, 12),
        })
    return pd.DataFrame(rows)


# ------------------------------------------------- curated sintético
def gen_curated(comments: pd.DataFrame, movies: pd.DataFrame) -> dict:
    """Genera las 4 tablas del modelo dimensional para una corrida."""

    movie_map = movies.set_index("movie_id")[
        ["title", "genres", "directors", "imdb_rating"]
    ].to_dict("index")

    # FACT
    fact_rows = []
    for _, c in comments.iterrows():
        mid = c["movie_id"]
        info = movie_map.get(mid, {})
        genres = info.get("genres", ["Sin categoria"])
        fecha_str = c["date"][:10] if isinstance(c["date"], str) else "DESCONOCIDO"
        fact_rows.append({
            "interaccion_id": c["_id"],
            "dim_tiempo_key": fecha_str,
            "dim_usuario_key": "anon:" + str(c["name"]).lower(),
            "dim_pelicula_key": mid,
            "categoria": genres[0] if genres else "Sin categoria",
            "cantidad": 1,
            "rating_pelicula": float(info.get("imdb_rating", 0)),
            "longitud_comentario": len(str(c["text"])),
        })
    fact = pd.DataFrame(fact_rows)

    # DIM_TIEMPO
    fechas = pd.to_datetime(
        comments["date"].str[:10], errors="coerce"
    ).dropna().unique()
    dim_tiempo_rows = []
    for f in fechas:
        dt = pd.Timestamp(f)
        dim_tiempo_rows.append({
            "dim_tiempo_key": dt.strftime("%Y-%m-%d"),
            "anio": dt.year,
            "mes": dt.month,
            "dia": dt.day,
            "trimestre": (dt.month - 1) // 3 + 1,
            "nombre_mes": dt.strftime("%B"),
            "dia_semana_num": dt.weekday(),
            "nombre_dia_semana": dt.strftime("%A"),
        })
    dim_tiempo = pd.DataFrame(dim_tiempo_rows)

    # DIM_PELICULA
    dim_pelicula = pd.DataFrame([{
        "dim_pelicula_key": r["movie_id"],
        "titulo": r["title"],
        "categoria": r["genres"][0] if r["genres"] else "Sin categoria",
        "marca": r["directors"][0] if r["directors"] else "Desconocido",
        "anio_lanzamiento": float(r["year"]),
        "clasificacion": r["rated"],
        "duracion_minutos": float(r["runtime"]),
    } for _, r in movies.iterrows()])

    # DIM_USUARIO
    usuarios_unicos = comments["name"].drop_duplicates().head(500)
    dim_usuario = pd.DataFrame([{
        "dim_usuario_key": "anon:" + str(n).lower(),
        "nombre": str(n),
        "email": None,
        "segmento": "Anonimo",
    } for n in usuarios_unicos])

    return {
        "FACT_INTERACCIONES": fact,
        "DIM_TIEMPO": dim_tiempo,
        "DIM_PELICULA": dim_pelicula,
        "DIM_USUARIO": dim_usuario,
    }


# ----------------------------------------------------- subida a S3
def subir_archivo_grande(
    s3,
    key: str,
    data: bytes,
    dry_run: bool,
) -> int:
    """Sube un archivo grande a S3 usando multipart upload automático de boto3."""
    size_mb = len(data) / 1024 / 1024
    if dry_run:
        print(f"  [DRY-RUN] s3://{S3_BUCKET}/{key}  →  {size_mb:.1f} MB")
        return len(data)

    # boto3 usa multipart automáticamente para objetos > 8 MB
    buf = io.BytesIO(data)
    s3.upload_fileobj(buf, S3_BUCKET, key)
    print(f"  OK  s3://{S3_BUCKET}/{key}  ({size_mb:.1f} MB)")
    return len(data)


# ----------------------------------------------------------------------- main
def main():
    parser = argparse.ArgumentParser(
        description="Genera archivos Parquet individuales >= 1 GB y los sube a S3"
    )
    parser.add_argument("--millones", type=float, default=MILLONES_DEFAULT,
                        help=f"Millones de registros por archivo (default: {MILLONES_DEFAULT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Estimar tamaños sin subir nada")
    args = parser.parse_args()

    n = int(args.millones * 1_000_000)
    s3 = _s3_client()

    print(f"\nGenerando {n:,} registros ({args.millones}M)...")
    print("Esto puede tardar 3-5 minutos — es normal.\n")

    # 1. Datos base
    t0 = time.perf_counter()
    print("  [1/5] Generando películas y usuarios base...")
    movies = gen_movies(1000)
    users  = gen_users(5000)
    movie_ids  = movies["movie_id"].tolist()
    user_names = users["name"].tolist()

    # 2. Generar comentarios masivos
    print(f"  [2/5] Generando {n:,} comentarios sintéticos...")
    base_date = datetime(2026, 1, 1)
    comments = gen_comments(n, movie_ids, user_names, base_date)
    print(f"        DataFrame: {len(comments):,} filas, "
          f"{comments.memory_usage(deep=True).sum()/1024**2:.0f} MB en RAM")

    # 3. Serializar a bytes (Parquet Snappy y JSONL)
    print("  [3/5] Serializando a Parquet Snappy (raw processed curated)...")

    # JSONL para raw/comments — será el archivo más grande
    print("        raw/comments → JSONL...")
    raw_bytes = _df_to_jsonl_bytes(comments)
    print(f"        raw/comments: {len(raw_bytes)/1024**2:.1f} MB")

    # Parquet processed
    print("        processed/comments → Parquet...")
    proc_bytes = _df_to_parquet_bytes(comments)
    print(f"        processed/comments: {len(proc_bytes)/1024**2:.1f} MB")

    # Curated — FACT_INTERACCIONES
    print("        curated/FACT_INTERACCIONES → Parquet...")
    curated = gen_curated(comments, movies)
    fact_bytes = _df_to_parquet_bytes(curated["FACT_INTERACCIONES"])
    print(f"        curated/FACT_INTERACCIONES: {len(fact_bytes)/1024**2:.1f} MB")

    # DIM tables (más pequeñas, una corrida)
    dim_tiempo_bytes  = _df_to_parquet_bytes(curated["DIM_TIEMPO"])
    dim_pelicula_bytes = _df_to_parquet_bytes(curated["DIM_PELICULA"])
    dim_usuario_bytes  = _df_to_parquet_bytes(curated["DIM_USUARIO"])

    run_id   = datetime.now().strftime("%Y%m%d_%H%M%S")
    ing_date = "2026-01-01"
    prefix   = f"ingestion_date={ing_date}/run_{run_id}"

    archivos = [
        (f"raw/comments/{prefix}.jsonl",                         raw_bytes),
        (f"processed/comments/{prefix}.parquet",                 proc_bytes),
        (f"curated/FACT_INTERACCIONES/{prefix}.parquet",         fact_bytes),
        (f"curated/DIM_TIEMPO/{prefix}.parquet",                 dim_tiempo_bytes),
        (f"curated/DIM_PELICULA/{prefix}.parquet",               dim_pelicula_bytes),
        (f"curated/DIM_USUARIO/{prefix}.parquet",                dim_usuario_bytes),
    ]

    print(f"\n  [4/5] Archivos a subir:")
    total_bytes = 0
    for key, data in archivos:
        mb = len(data) / 1024**2
        gb = mb / 1024
        indicador = " ← > 1 GB" if gb >= 1.0 else (f" ← {mb:.0f} MB" if mb > 100 else "")
        print(f"        {key.split('/')[0]+'/'+key.split('/')[1]:<35} "
              f"{mb:>8.1f} MB{indicador}")
        total_bytes += len(data)
    print(f"        {'TOTAL':<35} {total_bytes/1024**2:>8.1f} MB")

    elapsed_gen = time.perf_counter() - t0
    print(f"\n  Generación completada en {elapsed_gen:.0f}s")

    if args.dry_run:
        print("\n[DRY-RUN] No se subió nada. Quita --dry-run para subir.")
        return

    # 4. Subir a S3
    print(f"\n  [5/5] Subiendo a s3://{S3_BUCKET}/ ...")
    t_upload = time.perf_counter()
    for key, data in archivos:
        subir_archivo_grande(s3, key, data, dry_run=False)

    elapsed_upload = time.perf_counter() - t_upload
    elapsed_total  = time.perf_counter() - t0

    print(f"\n{'='*60}")
    print(f"Registros generados : {n:,}")
    print(f"Archivos subidos    : {len(archivos)}")
    print(f"Total subido        : {total_bytes/1024**2:.1f} MB")
    print(f"Tiempo generación   : {elapsed_gen:.0f}s")
    print(f"Tiempo subida       : {elapsed_upload:.0f}s")
    print(f"Tiempo total        : {elapsed_total:.0f}s")
    print(f"\nVerifica:")
    print(f"  python scripts/listar_s3.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
