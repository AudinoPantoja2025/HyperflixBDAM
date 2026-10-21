"""Sube los archivos del Data Lake local a Amazon S3.

Sincroniza data_lake/ -> s3://<bucket>/ manteniendo la misma estructura
de carpetas. Útil para hacer la migración inicial de los datos ya generados.

Ejecutar:
    python scripts/migrar_lake_a_s3.py

O con argumentos:
    python scripts/migrar_lake_a_s3.py --bucket abp-data-lake-Audino --dry-run
"""

import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

EXTENSIONES_VALIDAS = {".parquet", ".jsonl", ".json"}


def subir_archivo(s3_client, local_path: Path, bucket: str, s3_key: str, dry_run: bool) -> bool:
    if dry_run:
        print(f"  [DRY-RUN] {local_path} -> s3://{bucket}/{s3_key}")
        return True
    try:
        s3_client.upload_file(str(local_path), bucket, s3_key)
        size_kb = local_path.stat().st_size / 1024
        print(f"  ✓ {s3_key} ({size_kb:.1f} KB)")
        return True
    except Exception as e:
        print(f"  ✗ Error subiendo {local_path}: {e}")
        return False


def migrar(
    lake_root: Path,
    bucket: str,
    region: str,
    profile: str | None,
    dry_run: bool,
) -> None:
    session = boto3.Session(profile_name=profile, region_name=region)
    s3 = session.client("s3")

    archivos = [
        p for p in lake_root.rglob("*")
        if p.is_file() and p.suffix in EXTENSIONES_VALIDAS
    ]

    if not archivos:
        print(f"No se encontraron archivos en {lake_root}")
        return

    print(f"\n📦 Migrando {len(archivos)} archivo(s) de {lake_root} -> s3://{bucket}/\n")

    ok = 0
    fail = 0
    for local_path in sorted(archivos):
        # Convierte ruta local a S3 key relativa al root
        s3_key = local_path.relative_to(lake_root).as_posix()
        if subir_archivo(s3, local_path, bucket, s3_key, dry_run):
            ok += 1
        else:
            fail += 1

    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Resumen: {ok} OK, {fail} errores")
    if not dry_run and ok > 0:
        print(f"\n🚀 Datos disponibles en: s3://{bucket}/")


def main():
    parser = argparse.ArgumentParser(description="Migrar Data Lake local a S3")
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET_NAME", "abp-data-lake-Audino"),
        help="Nombre del bucket S3",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("S3_REGION", "us-east-2"),
        help="Región AWS",
    )
    parser.add_argument(
        "--profile",
        default=os.getenv("AWS_PROFILE"),
        help="Perfil AWS CLI",
    )
    parser.add_argument(
        "--lake-root",
        default=os.getenv("DATA_LAKE_ROOT", "data_lake"),
        help="Carpeta raíz del Data Lake local",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostrar qué se subiría sin subir nada",
    )
    args = parser.parse_args()

    lake_root = Path(args.lake_root)
    if not lake_root.exists():
        print(f"✗ La carpeta {lake_root} no existe.")
        sys.exit(1)

    try:
        migrar(lake_root, args.bucket, args.region, args.profile, args.dry_run)
    except NoCredentialsError:
        print("\n✗ No se encontraron credenciales AWS.")
        print("  Ejecuta primero: aws configure")
        sys.exit(1)


if __name__ == "__main__":
    main()
