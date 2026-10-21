"""Lista el contenido del bucket S3 del Data Lake."""
import boto3, os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    region_name=os.getenv("S3_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
bucket = os.getenv("S3_BUCKET_NAME")

paginator = s3.get_paginator("list_objects_v2")
total_bytes = 0
total_files = 0

print(f"\nContenido de s3://{bucket}/\n")
print(f"{'Archivo':<80} {'Tamaño':>10}")
print("-" * 92)

for page in paginator.paginate(Bucket=bucket):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".keep"):
            continue
        kb = obj["Size"] / 1024
        print(f"{obj['Key']:<80} {kb:>9.1f} KB")
        total_bytes += obj["Size"]
        total_files += 1

print("-" * 92)
print(f"Total: {total_files} archivos | {total_bytes / 1024 / 1024:.2f} MB")
