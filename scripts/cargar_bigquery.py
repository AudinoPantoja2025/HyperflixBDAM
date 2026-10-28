"""Carga las tablas del modelo dimensional desde S3 (curated/) a BigQuery.

Flujo:
    S3 curated/*.parquet  ->  DataFrame  ->  BigQuery (WRITE_TRUNCATE)

Tablas cargadas:
    FACT_INTERACCIONES, DIM_TIEMPO, DIM_PELICULA, DIM_USUARIO

Uso:
    python scripts/cargar_bigquery.py              # lee de S3
    python scripts/cargar_bigquery.py --local      # lee de data_lake/curated/ local
    python scripts/cargar_bigquery.py --dry-run    # muestra qué cargaría sin cargar
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from google.oauth2 import service_account

load_dotenv()

# ------------------------------------------------------------------ config
PROJECT_ID   = os.getenv("BIGQUERY_PROJECT_ID", "")
DATASET      = os.getenv("BIGQUERY_DATASET", "movielens_dw")
CREDENTIALS  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials/bigquery_key.json")
S3_BUCKET    = os.getenv("S3_BUCKET_NAME", "")
S3_REGION    = os.getenv("S3_REGION", "us-east-2")
AWS_KEY      = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET   = os.getenv("AWS_SECRET_ACCESS_KEY")

# Tablas a cargar: nombre BigQuery -> prefijo S3 (dentro de curated/)
TABLAS = [
    "FACT_INTERACCIONES",
    "DIM_TIEMPO",
    "DIM_PELICULA",
    "DIM_USUARIO",
]

# Esquemas explícitos para BigQuery (evita inferencias incorrectas de tipos)
SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "FACT_INTERACCIONES": [
        bigquery.SchemaField("interaccion_id",      "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("dim_tiempo_key",       "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("dim_usuario_key",      "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("dim_pelicula_key",     "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("categoria",            "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("cantidad",             "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rating_pelicula",      "FLOAT",   mode="NULLABLE"),
        bigquery.SchemaField("longitud_comentario",  "INTEGER", mode="NULLABLE"),
    ],
    "DIM_TIEMPO": [
        bigquery.SchemaField("dim_tiempo_key",       "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("anio",                 "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mes",                  "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("dia",                  "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("trimestre",            "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("nombre_mes",           "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("dia_semana_num",       "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("nombre_dia_semana",    "STRING",  mode="NULLABLE"),
    ],
    "DIM_PELICULA": [
        bigquery.SchemaField("dim_pelicula_key",     "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("titulo",               "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("categoria",            "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("marca",                "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("anio_lanzamiento",     "FLOAT",   mode="NULLABLE"),
        bigquery.SchemaField("clasificacion",        "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("duracion_minutos",     "FLOAT",   mode="NULLABLE"),
    ],
    "DIM_USUARIO": [
        bigquery.SchemaField("dim_usuario_key",      "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("nombre",               "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("email",                "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("segmento",             "STRING",  mode="NULLABLE"),
    ],
}


# ------------------------------------------------------------------ helpers
def get_bq_client() -> bigquery.Client:
    """Devuelve un cliente BigQuery autenticado con el JSON de servicio."""
    creds_path = Path(CREDENTIALS)
    if not creds_path.exists():
        print(f"✗ No se encontró el archivo de credenciales: {creds_path}")
        sys.exit(1)
    creds = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )


def _ultima_particion_s3(tabla: str) -> str | None:
    """Devuelve la ruta S3 del Parquet más reciente de una tabla curated."""
    s3 = get_s3_client()
    prefijo = f"curated/{tabla}/"
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefijo)
    except Exception as e:
        print(f"  ✗ Error listando S3 '{prefijo}': {e}")
        return None

    parquets = [
        obj["Key"] for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    if not parquets:
        return None
    return f"s3://{S3_BUCKET}/" + sorted(parquets)[-1]


def _ultima_particion_local(tabla: str) -> Path | None:
    """Devuelve el Parquet más reciente en data_lake/curated/<tabla>/."""
    base = Path("data_lake") / "curated" / tabla
    parquets = sorted(base.rglob("*.parquet"))
    return parquets[-1] if parquets else None


def leer_parquet(tabla: str, usar_local: bool) -> pd.DataFrame | None:
    """Lee el Parquet más reciente de S3 o local."""
    if usar_local:
        path = _ultima_particion_local(tabla)
        if not path:
            print(f"  ✗ No se encontró Parquet local para '{tabla}'")
            return None
        print(f"  ← {path}")
        return pd.read_parquet(path, engine="pyarrow")
    else:
        s3_path = _ultima_particion_s3(tabla)
        if not s3_path:
            print(f"  ✗ No se encontró Parquet en S3 para '{tabla}'")
            return None
        print(f"  ← {s3_path}")
        return pd.read_parquet(
            s3_path, engine="pyarrow",
            storage_options={"key": AWS_KEY, "secret": AWS_SECRET},
        )


def cargar_tabla(
    client: bigquery.Client,
    tabla: str,
    df: pd.DataFrame,
    dry_run: bool,
) -> dict:
    """Carga un DataFrame a BigQuery con WRITE_TRUNCATE (reemplaza la tabla)."""
    table_id = f"{PROJECT_ID}.{DATASET}.{tabla}"

    # Limpiar tipos problemáticos antes de subir
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).where(df[col].notna(), other=None)

    size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024

    if dry_run:
        print(f"  [DRY-RUN] → {table_id} | filas={len(df):,} | ~{size_mb:.2f} MB")
        return {"tabla": tabla, "filas": len(df), "estado": "dry-run"}

    job_config = LoadJobConfig(
        schema=SCHEMAS[tabla],
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
    )

    t0 = time.perf_counter()
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # esperar a que termine
    elapsed = round(time.perf_counter() - t0, 2)

    dest = client.get_table(table_id)
    print(f"  ✓ → {table_id} | filas={dest.num_rows:,} | {elapsed}s")
    return {"tabla": tabla, "filas": dest.num_rows, "estado": "ok", "segundos": elapsed}


# -------------------------------------------------------------------- main
def main():
    parser = argparse.ArgumentParser(description="Carga curated/ a BigQuery")
    parser.add_argument("--local",   action="store_true", help="Leer de data_lake/curated/ local en vez de S3")
    parser.add_argument("--dry-run", action="store_true", help="Mostrar qué cargaría sin cargar nada")
    parser.add_argument("--tabla",   default=None, help="Cargar solo una tabla específica")
    args = parser.parse_args()

    # Validaciones
    if not PROJECT_ID:
        print("✗ Falta BIGQUERY_PROJECT_ID en .env"); sys.exit(1)
    if not args.local and not S3_BUCKET:
        print("✗ Falta S3_BUCKET_NAME en .env (o usa --local)"); sys.exit(1)

    tablas = [args.tabla] if args.tabla else TABLAS

    print(f"\n{'[DRY-RUN] ' if args.dry_run else ''}Cargando a BigQuery")
    print(f"  Proyecto : {PROJECT_ID}")
    print(f"  Dataset  : {DATASET}")
    print(f"  Fuente   : {'local (data_lake/curated/)' if args.local else f's3://{S3_BUCKET}/curated/'}")
    print(f"  Tablas   : {tablas}\n")

    client = None if args.dry_run else get_bq_client()

    resultados = []
    total_filas = 0

    for tabla in tablas:
        print(f">> {tabla}")
        df = leer_parquet(tabla, args.local)
        if df is None:
            resultados.append({"tabla": tabla, "estado": "sin_datos"})
            continue

        resultado = cargar_tabla(client, tabla, df, args.dry_run)
        resultados.append(resultado)
        total_filas += resultado.get("filas", 0)

    # Resumen
    print(f"\n{'─'*55}")
    ok  = sum(1 for r in resultados if r["estado"] in ("ok", "dry-run"))
    err = sum(1 for r in resultados if r["estado"] == "sin_datos")
    print(f"Tablas cargadas : {ok}/{len(tablas)}")
    print(f"Total filas     : {total_filas:,}")
    if err:
        print(f"Sin datos       : {err} tabla(s)")
    if not args.dry_run and ok > 0:
        print(f"\n🚀 Data Warehouse disponible en:")
        print(f"   https://console.cloud.google.com/bigquery?project={PROJECT_ID}")
    print(f"{'─'*55}\n")


if __name__ == "__main__":
    main()
