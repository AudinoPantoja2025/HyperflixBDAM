"""Data Lake de tres zonas: RAW, PROCESSED y CURATED.

Soporta dos modos de almacenamiento configurables via variable de entorno:

  LOCAL (por defecto):
      data_lake/
      ├── raw/       <coleccion>/ingestion_date=YYYY-MM-DD/run_<run_id>.jsonl
      ├── processed/ <coleccion>/ingestion_date=YYYY-MM-DD/run_<run_id>.parquet
      └── curated/   <dataset>/ingestion_date=YYYY-MM-DD/run_<run_id>.parquet

  S3 (cuando S3_BUCKET_NAME está definido en .env):
      s3://abp-data-lake-Audino/
      ├── raw/       <coleccion>/ingestion_date=YYYY-MM-DD/run_<run_id>.jsonl
      ├── processed/ <coleccion>/ingestion_date=YYYY-MM-DD/run_<run_id>.parquet
      └── curated/   <dataset>/ingestion_date=YYYY-MM-DD/run_<run_id>.parquet

RAW se escribe en JSON Lines con ``bson.json_util`` (extended JSON), lo que
conserva los tipos originales de MongoDB (ObjectId como ``$oid``, fechas como
``$date``) y garantiza trazabilidad: cada documento mantiene su ``_id``.

Algunos principios aplicados:

- Los archivos de ejecuciones anteriores no se sobrescriben: cada corrida
  usa un ``run_id`` unico.
- Cada archivo generado lleva un sidecar ``run_<run_id>.meta.json`` con el
  conteo y la ruta, para auditoria.
"""

import io
import json
from pathlib import Path
from typing import Dict, List, Optional, Union

import boto3
import s3fs
from bson import json_util

from src.logger import get_logger

logger = get_logger(__name__)

# ------------------------------------------------ helpers S3
def _get_s3fs(region: str = "us-east-2", profile: Optional[str] = None) -> s3fs.S3FileSystem:
    kwargs: dict = {"anon": False}
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
        creds = session.get_credentials().resolve()
        kwargs.update(key=creds.access_key, secret=creds.secret_key, token=creds.token)
    return s3fs.S3FileSystem(**kwargs)


def _is_s3(path: Union[str, Path]) -> bool:
    return str(path).startswith("s3://")


class DataLake:
    """Data Lake con soporte dual: almacenamiento local o Amazon S3.

    Para usar S3, pasa ``s3_bucket`` al constructor o define ``S3_BUCKET_NAME``
    en tu ``.env``:

        lake = DataLake("data_lake", s3_bucket="abp-data-lake-Audino", s3_region="us-east-2")

    Cuando ``s3_bucket`` está definido, **todas** las rutas apuntan a S3 y
    el ``root`` local se usa únicamente como fallback de lectura de checkpoints.
    """

    def __init__(
        self,
        root: Union[str, Path] = "data_lake",
        s3_bucket: str = "",
        s3_region: str = "us-east-2",
        aws_profile: Optional[str] = None,
    ):
        self.root = Path(root)
        self.s3_bucket = s3_bucket.strip()
        self.s3_region = s3_region
        self.aws_profile = aws_profile
        self.zones = ("raw", "processed", "curated")

    @property
    def use_s3(self) -> bool:
        return bool(self.s3_bucket)

    def _s3_prefix(self, *parts: str) -> str:
        """Devuelve una ruta S3 completa: ``s3://bucket/parte1/parte2/...``"""
        return "s3://" + self.s3_bucket + "/" + "/".join(p.strip("/") for p in parts if p)

    def _fs(self) -> s3fs.S3FileSystem:
        return _get_s3fs(self.s3_region, self.aws_profile)

    # ------------------------------------------------------------------ zonas
    def create_zones(self):
        if self.use_s3:
            # En S3 no hay directorios vacíos: los prefijos existen
            # implícitamente al subir el primer objeto. Solo verificamos acceso.
            try:
                fs = self._fs()
                fs.ls(self.s3_bucket)
                logger.info(
                    "Zonas S3 listas en 's3://%s': %s",
                    self.s3_bucket,
                    ", ".join(self.zones),
                )
            except Exception as exc:
                logger.error("No se puede acceder al bucket S3 '%s': %s", self.s3_bucket, exc)
                raise
        else:
            for zone in self.zones:
                (self.root / zone).mkdir(parents=True, exist_ok=True)
            logger.info(
                "Zonas del Data Lake listas en '%s': %s",
                self.root,
                ", ".join(self.zones),
            )

    def zone_path(self, zone: str, *parts) -> str:
        """Devuelve la ruta (local o S3) para una zona y subpartes dadas."""
        if self.use_s3:
            return self._s3_prefix(zone, *[str(p) for p in parts])
        return str(self.root / zone / Path(*parts)) if parts else str(self.root / zone)

    def raw_partition_dir(self, collection: str, ingestion_date: str) -> str:
        return self.zone_path("raw", collection, f"ingestion_date={ingestion_date}")

    def processed_partition_dir(self, collection: str, ingestion_date: str) -> str:
        return self.zone_path("processed", collection, f"ingestion_date={ingestion_date}")

    def curated_partition_dir(self, dataset: str, ingestion_date: str) -> str:
        return self.zone_path("curated", dataset, f"ingestion_date={ingestion_date}")

    # ---------------------------------------------------------------- helpers
    def _write_metadata(self, output_dir: str, run_id: str, metadata: Dict) -> str:
        meta_path = output_dir.rstrip("/") + f"/run_{run_id}.meta.json"
        payload = json.dumps(metadata, indent=2, ensure_ascii=False, default=str)
        if _is_s3(meta_path):
            fs = self._fs()
            with fs.open(meta_path, "w", encoding="utf-8") as fh:
                fh.write(payload)
        else:
            p = Path(meta_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(payload, encoding="utf-8")
        return meta_path

    # ------------------------------------------------------------------- RAW
    def write_raw_jsonl(
        self, collection: str, docs: List[Dict], run_id: str, ingestion_date: str
    ) -> Dict:
        """Escribe los documentos originales como JSON Lines (extended JSON)."""
        out_dir = self.raw_partition_dir(collection, ingestion_date)
        path = out_dir.rstrip("/") + f"/run_{run_id}.jsonl"

        lines = "".join(json_util.dumps(doc) + "\n" for doc in docs)
        count = lines.count("\n")

        if _is_s3(path):
            fs = self._fs()
            with fs.open(path, "w", encoding="utf-8") as fh:
                fh.write(lines)
            size_bytes = fs.info(path).get("size", 0)
        else:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(lines, encoding="utf-8")
            size_bytes = p.stat().st_size

        metadata = {
            "run_id": run_id,
            "collection": collection,
            "zone": "raw",
            "ingestion_date": ingestion_date,
            "format": "jsonl",
            "encoding": "bson-extended-json",
            "count": count,
            "bytes": size_bytes,
            "path": path,
        }
        self._write_metadata(out_dir, run_id, metadata)
        logger.info(
            "RAW: '%s' -> %d documento(s) en %s (%d bytes)",
            collection, count, path, size_bytes,
        )
        return metadata

    def read_raw_jsonl(
        self,
        collection: str,
        ingestion_date: str,
        run_id: Optional[str] = None,
    ) -> List[Dict]:
        """Relee los documentos RAW (extended JSON) como listas de dict."""
        partition = self.raw_partition_dir(collection, ingestion_date)
        docs: List[Dict] = []

        if _is_s3(partition):
            fs = self._fs()
            try:
                all_files = fs.ls(partition, detail=False)
            except FileNotFoundError:
                return docs
            files = sorted(f for f in all_files if f.endswith(".jsonl"))
            if run_id:
                files = [f for f in files if run_id in f]
            for s3_path in files:
                with fs.open(s3_path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        if line.strip():
                            docs.append(json_util.loads(line))
        else:
            partition_path = Path(partition)
            files = sorted(partition_path.glob("run_*.jsonl"))
            if run_id:
                files = [f for f in files if run_id in f.name]
            for path in files:
                with path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        if line.strip():
                            docs.append(json_util.loads(line))
        return docs

    # ------------------------------------------------------------- rejected
    def rejected_partition_dir(self, collection: str, ingestion_date: str) -> str:
        if self.use_s3:
            return self._s3_prefix("rejected", collection, f"ingestion_date={ingestion_date}")
        return str(self.root / "rejected" / collection / f"ingestion_date={ingestion_date}")

    def write_rejected(
        self, collection: str, records, run_id: str, ingestion_date: str
    ) -> str:
        """Persiste registros rechazados (duplicados) como JSON Lines."""
        out_dir = self.rejected_partition_dir(collection, ingestion_date)
        path = out_dir.rstrip("/") + f"/run_{run_id}.jsonl"
        lines = "".join(
            json.dumps(row.to_dict(), ensure_ascii=False, default=str) + "\n"
            for _, row in records.iterrows()
        )
        if _is_s3(path):
            fs = self._fs()
            with fs.open(path, "w", encoding="utf-8") as fh:
                fh.write(lines)
        else:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(lines, encoding="utf-8")

        logger.warning(
            "RECHAZADOS: '%s' -> %d registro(s) en %s",
            collection, len(records), path,
        )
        return path

    # ---------------------------------------------------------- CDC / streaming
    def cdc_partition_dir(self, collection: str, ingestion_date: str) -> str:
        """Ruta inmutable para micro-lotes de Change Streams."""
        if self.use_s3:
            return self._s3_prefix("cdc", "raw", collection, f"ingestion_date={ingestion_date}")
        return str(self.root / "cdc" / "raw" / collection / f"ingestion_date={ingestion_date}")

    def cdc_checkpoint_path(self, collection: str) -> Path:
        """Ubicacion del ultimo token confirmado (siempre local para baja latencia)."""
        return self.root / "checkpoints" / "cdc" / f"{collection}.json"

    def write_cdc_microbatch(
        self,
        collection: str,
        events: List[Dict],
        run_id: str,
        ingestion_date: str,
        batch_number: int,
    ) -> Dict:
        """Guarda un micro-lote CDC antes de confirmar su resume token."""
        if not events:
            raise ValueError("Un micro-lote CDC no puede estar vacio")

        out_dir = self.cdc_partition_dir(collection, ingestion_date)
        batch_suffix = f"{run_id}_batch_{batch_number:06d}"
        path = out_dir.rstrip("/") + f"/run_{batch_suffix}.jsonl"

        lines = "".join(json_util.dumps(event) + "\n" for event in events)

        if _is_s3(path):
            fs = self._fs()
            if fs.exists(path):
                raise FileExistsError(f"El micro-lote CDC ya existe: {path}")
            with fs.open(path, "w", encoding="utf-8") as fh:
                fh.write(lines)
            size = fs.info(path).get("size", 0)
        else:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            if p.exists():
                raise FileExistsError(f"El micro-lote CDC ya existe: {path}")
            p.write_text(lines, encoding="utf-8")
            size = p.stat().st_size

        operations: Dict[str, int] = {}
        for event in events:
            op = event.get("operation_type", "unknown")
            operations[op] = operations.get(op, 0) + 1

        metadata = {
            "run_id": run_id,
            "collection": collection,
            "zone": "cdc/raw",
            "ingestion_date": ingestion_date,
            "format": "jsonl",
            "encoding": "bson-extended-json",
            "batch_number": batch_number,
            "count": len(events),
            "operations": operations,
            "bytes": size,
            "path": path,
        }
        self._write_metadata(out_dir, batch_suffix, metadata)
        logger.info("CDC RAW: '%s' -> %d evento(s) en %s", collection, len(events), path)
        return metadata

    def read_cdc_checkpoint(self, collection: str) -> Optional[Dict]:
        """Obtiene el checkpoint confirmado; ``None`` inicia desde eventos nuevos."""
        path = self.cdc_checkpoint_path(collection)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as fh:
            return json_util.loads(fh.read())

    def write_cdc_checkpoint(self, collection: str, checkpoint: Dict) -> Path:
        """Confirma de forma atómica el token posterior al micro-lote escrito."""
        path = self.cdc_checkpoint_path(collection)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as fh:
            fh.write(json_util.dumps(checkpoint))
        temporary.replace(path)
        return path