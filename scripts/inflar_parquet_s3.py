"""Genera archivos JSONL individuales de >= 1 GB y los sube a S3.

Estrategia: replica el dataset real de comments (41k filas) hasta
alcanzar el tamaño objetivo, serializa a JSON Lines y sube como
un solo objeto S3 en la zona raw/.

Uso:
    python scripts/inflar_parquet_s3.py              # 2 archivos de 1 GB
    python scripts/inflar_parquet_s3.py --target-gb 2
    python scripts/inflar_parquet_s3.py --dry-run
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET  = os.getenv("S3_BUCKET_NAME", "abp-data-lake-audino")
S3_REGION  = os.getenv("S3_REGION", "us-east-2")
AWS_KEY    = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")


def s3_client():
    return boto3.client(
        "s3", region_name=S3_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )


def _leer_jsonl_local(coleccion: str) -> list[dict]:
    """Lee el JSONL más reciente de data_lake/raw/<coleccion>."""
    base = Path("data_lake") / "raw" / coleccion
    jsonls = sorted(f for f in base.rglob("*.jsonl")
                    if not f.name.endswith(".meta.json"))
    if not jsonls:
        print(f"  No se encontró JSONL local para '{coleccion}'.")
        sys.exit(1)
    path = jsonls[-1]
    registros = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                registros.append(json.loads(line))
    print(f"  Base  : {path.name}")
    print(f"  Filas : {len(registros):,}")
    size_kb = path.stat().st_size / 1024
    print(f"  Peso  : {size_kb:.1f} KB")
    return registros


def _replicar_hasta_gb(registros: list[dict], target_gb: float) -> bytes:
    """Replica los registros hasta que el JSONL pese >= target_gb."""
    target_bytes = int(target_gb * 1024 ** 3)

    # Estimar tamaño de una réplica
    muestra = "\n".join(json.dumps(r, ensure_ascii=False, default=str)
                        for r in registros[:100]).encode("utf-8")
    bytes_por_registro = len(muestra) / 100
    replicas = max(1, int(target_bytes / (bytes_por_registro * len(registros))) + 1)
    filas_totales = len(registros) * replicas

    print(f"  ~{bytes_por_registro:.0f} bytes/registro")
    print(f"  Réplicas necesarias : {replicas}x")
    print(f"  Filas totales       : {filas_totales:,}")
    print(f"  Tamaño estimado     : ~{bytes_por_registro * filas_totales / 1024**2:.0f} MB")
    print(f"  Generando JSONL en memoria...")

    buf = io.BytesIO()
    for rep in range(replicas):
        for doc in registros:
            # Hacer _id único por réplica para que no haya duplicados
            doc_copia = dict(doc)
            if "_id" in doc_copia:
                doc_copia["_id"] = str(doc_copia["_id"]) + f"_r{rep}"
            linea = json.dumps(doc_copia, ensure_ascii=False, default=str) + "\n"
            buf.write(linea.encode("utf-8"))

        # Mostrar progreso cada 10 réplicas
        if (rep + 1) % 10 == 0 or rep == replicas - 1:
            mb_actual = buf.tell() / 1024**2
            pct = min(100, (buf.tell() / target_bytes) * 100)
            print(f"  Progreso: {rep+1}/{replicas} réplicas — "
                  f"{mb_actual:.0f} MB ({pct:.0f}%)", end="\r")

    print()
    return buf.getvalue()


def _subir_multipart(s3, key: str, data: bytes, dry_run: bool) -> int:
    size_mb = len(data) / 1024**2
    size_gb = size_mb / 1024
    if dry_run:
        print(f"  [DRY-RUN] s3://{S3_BUCKET}/{key}")
        print(f"  Tamaño: {size_mb:.1f} MB ({size_gb:.3f} GB)")
        return len(data)

    print(f"  Subiendo {size_mb:.1f} MB a s3://{S3_BUCKET}/{key}")
    print(f"  (puede tardar varios minutos...)")
    t0 = time.perf_counter()
    s3.upload_fileobj(io.BytesIO(data), S3_BUCKET, key)
    elapsed = time.perf_counter() - t0
    vel = size_mb / elapsed if elapsed > 0 else 0
    print(f"  OK — {elapsed:.0f}s a {vel:.1f} MB/s")
    return len(data)


def main():
    parser = argparse.ArgumentParser(
        description="Genera archivos JSONL individuales >= 1 GB en S3"
    )
    parser.add_argument("--target-gb", type=float, default=1.0,
                        help="Tamaño objetivo por archivo en GB (default: 1.0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostrar plan sin subir nada")
    args = parser.parse_args()

    s3 = s3_client()
    timestamp = time.strftime("%Y%m%d_%H%M%S")

    # Dos archivos JSONL grandes: comments y movies
    tareas = [
        {
            "coleccion": "comments",
            "s3_key": (f"raw/comments/ingestion_date=2026-12-01"
                       f"/run_{timestamp}_massive.jsonl"),
        },
        {
            "coleccion": "movies",
            "s3_key": (f"raw/movies/ingestion_date=2026-12-01"
                       f"/run_{timestamp}_massive.jsonl"),
        },
    ]

    print(f"\nObjetivo  : {args.target_gb} GB por archivo")
    print(f"Archivos  : {len(tareas)}")
    print(f"Bucket    : s3://{S3_BUCKET}\n")

    total_bytes = 0
    t_total = time.perf_counter()

    for i, tarea in enumerate(tareas, 1):
        print(f"[{i}/{len(tareas)}] raw/{tarea['coleccion']}")
        registros = _leer_jsonl_local(tarea["coleccion"])
        data = _replicar_hasta_gb(registros, args.target_gb)
        size_final_gb = len(data) / 1024**3
        print(f"  Tamaño final: {len(data)/1024**2:.1f} MB "
              f"({size_final_gb:.3f} GB)")
        bytes_subidos = _subir_multipart(s3, tarea["s3_key"], data, args.dry_run)
        total_bytes += bytes_subidos
        print()

    elapsed = time.perf_counter() - t_total
    print("=" * 55)
    print(f"Archivos generados : {len(tareas)}")
    print(f"Total subido       : {total_bytes/1024**2:.1f} MB "
          f"({total_bytes/1024**3:.2f} GB)")
    print(f"Tiempo total       : {elapsed:.0f}s")
    if not args.dry_run:
        print(f"\nVerifica con:")
        print(f"  python scripts/listar_s3.py")
        print(f"\nEn consola AWS navega a:")
        print(f"  raw/comments/ingestion_date=2026-12-01/")
        print(f"  raw/movies/ingestion_date=2026-12-01/")
    print("=" * 55)


if __name__ == "__main__":
    main()
