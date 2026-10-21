"""Crea el bucket S3 para el Data Lake y configura sus políticas base.

Ejecutar una sola vez:
    python scripts/crear_bucket_s3.py

Requisitos previos:
    - Tener configuradas las credenciales AWS (ver instrucciones al final).
    - La variable S3_BUCKET_NAME debe estar en .env (o pasarse como argumento).

El script:
    1. Crea el bucket en la región indicada.
    2. Bloquea el acceso público (best practice).
    3. Activa el versionado (protección contra sobreescrituras accidentales).
    4. Crea las carpetas lógicas raw/, processed/ y curated/ con un objeto vacío
       (.keep) para que sean visibles en la consola de AWS.
"""

import argparse
import sys

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
import os

load_dotenv()


def crear_bucket(bucket_name: str, region: str, profile: str | None = None) -> None:
    session = boto3.Session(
        profile_name=profile,
        region_name=region,
    )
    s3 = session.client("s3", region_name=region)

    # 1. Crear bucket
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"✓ Bucket creado: s3://{bucket_name}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"ℹ Bucket ya existe: s3://{bucket_name}")
        else:
            print(f"✗ Error creando bucket: {e}")
            sys.exit(1)

    # 2. Bloquear acceso público (seguridad)
    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    print("✓ Acceso público bloqueado")

    # 3. Activar versionado
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    print("✓ Versionado activado")

    # 4. Crear prefijos lógicos (zonas del Data Lake)
    zones = ["raw/", "processed/", "curated/", "cdc/raw/", "rejected/"]
    for zone in zones:
        s3.put_object(Bucket=bucket_name, Key=zone + ".keep", Body=b"")
        print(f"✓ Zona creada: s3://{bucket_name}/{zone}")

    print(f"\n🚀 Data Lake listo en s3://{bucket_name}")
    print(f"   Agrega esto a tu .env:\n   S3_BUCKET_NAME={bucket_name}\n   S3_REGION={region}")


def main():
    parser = argparse.ArgumentParser(description="Crear bucket S3 para el Data Lake")
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET_NAME", "abp-data-lake-Audino"),
        help="Nombre del bucket S3 (default: abp-data-lake-Audino)",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("S3_REGION", "us-east-2"),
        help="Región AWS (default: us-east-2)",
    )
    parser.add_argument(
        "--profile",
        default=os.getenv("AWS_PROFILE"),
        help="Perfil AWS CLI (default: credenciales del entorno)",
    )
    args = parser.parse_args()

    try:
        crear_bucket(args.bucket, args.region, args.profile)
    except NoCredentialsError:
        print("\n✗ No se encontraron credenciales AWS.")
        print("  Ejecuta primero: aws configure")
        sys.exit(1)


if __name__ == "__main__":
    main()
