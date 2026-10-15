"""Data Lake local de tres zonas: RAW, PROCESSED y CURATED.

Estructura:

    data_lake/
    ├── raw/       <coleccion>/ingestion_date=YYYY-MM-DD/run_<run_id>.jsonl
    ├── processed/ <coleccion>/ingestion_date=YYYY-MM-DD/run_<run_id>.parquet
    └── curated/   <dataset>/ingestion_date=YYYY-MM-DD/run_<run_id>.parquet

RAW se escribe en JSON Lines con ``bson.json_util`` (extended JSON), lo que
conserva los tipos originales de MongoDB (ObjectId como ``$oid``, fechas como
``$date``) y garantiza trazabilidad: cada documento mantiene su ``_id``.

Algunos principios aplicados:

- Los archivos RAW de ejecuciones anteriores no se sobrescriben: cada corrida
  usa un ``run_id`` unico.
- Cada archivo generado lleva un sidecar ``run_<run_id>.meta.json`` con el
  conteo y la ruta, para auditoria.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

from bson import json_util

from src.logger import get_logger

logger = get_logger(__name__)


class DataLake:
    def __init__(self, root):
        self.root = Path(root)
        self.zones = ("raw", "processed", "curated")

    # ------------------------------------------------------------------ zonas
    def create_zones(self):
        for zone in self.zones:
            (self.root / zone).mkdir(parents=True, exist_ok=True)
        logger.info(
            "Zonas del Data Lake listas en '%s': %s",
            self.root,
            ", ".join(self.zones),
        )

    def zone_path(self, zone: str, *parts) -> Path:
        return self.root / zone / Path(*parts)

    def raw_partition_dir(self, collection: str, ingestion_date: str) -> Path:
        return self.zone_path("raw", collection, f"ingestion_date={ingestion_date}")

    def processed_partition_dir(self, collection: str, ingestion_date: str) -> Path:
        return self.zone_path("processed", collection, f"ingestion_date={ingestion_date}")

    def curated_partition_dir(self, dataset: str, ingestion_date: str) -> Path:
        return self.zone_path("curated", dataset, f"ingestion_date={ingestion_date}")

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _write_metadata(output_dir: Path, run_id: str, metadata: Dict) -> Path:
        meta_path = output_dir / f"run_{run_id}.meta.json"
        with meta_path.open("w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2, ensure_ascii=False, default=str)
        return meta_path

    # ------------------------------------------------------------------- RAW
    def write_raw_jsonl(
        self, collection: str, docs: List[Dict], run_id: str, ingestion_date: str
    ) -> Dict:
        """Escribe los documentos originales como JSON Lines (extended JSON)."""
        out_dir = self.raw_partition_dir(collection, ingestion_date)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"run_{run_id}.jsonl"

        count = 0
        with path.open("w", encoding="utf-8") as fh:
            for doc in docs:
                fh.write(json_util.dumps(doc) + "\n")
                count += 1

        size_bytes = path.stat().st_size
        metadata = {
            "run_id": run_id,
            "collection": collection,
            "zone": "raw",
            "ingestion_date": ingestion_date,
            "format": "jsonl",
            "encoding": "bson-extended-json",
            "count": count,
            "bytes": size_bytes,
            "path": str(path),
        }
        self._write_metadata(out_dir, run_id, metadata)
        logger.info(
            "RAW: '%s' -> %d documento(s) en %s (%d bytes)",
            collection,
            count,
            path,
            size_bytes,
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
        files = sorted(partition.glob("run_*.jsonl"))
        if run_id:
            files = [f for f in files if run_id in f.name]
        docs: List[Dict] = []
        for path in files:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        docs.append(json_util.loads(line))
        return docs

    # ------------------------------------------------------------- rejected
    def rejected_partition_dir(self, collection: str, ingestion_date: str) -> Path:
        return self.root / "rejected" / collection / f"ingestion_date={ingestion_date}"

    def write_rejected(
        self, collection: str, records, run_id: str, ingestion_date: str
    ) -> Path:
        """Persiste registros rechazados (duplicados) como JSON Lines.

        No se eliminan silenciosamente: cada corrida deja trazabilidad de los
        registros que no pasaron a PROCESSED.
        """
        out_dir = self.rejected_partition_dir(collection, ingestion_date)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"run_{run_id}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for _, row in records.iterrows():
                fh.write(json.dumps(row.to_dict(), ensure_ascii=False, default=str) + "\n")
        logger.warning(
            "RECHAZADOS: '%s' -> %d registro(s) en %s",
            collection,
            len(records),
            path,
        )
        return path

    # ---------------------------------------------------------- CDC / streaming
    def cdc_partition_dir(self, collection: str, ingestion_date: str) -> Path:
        """Ruta inmutable para micro-lotes de Change Streams.

        Esta zona se mantiene separada de RAW del ETL batch: un evento CDC no
        es un documento fuente completo y, por tanto, no debe mezclarse con la
        fuente de verdad que el proceso batch vuelve a leer.
        """
        return self.root / "cdc" / "raw" / collection / f"ingestion_date={ingestion_date}"

    def cdc_checkpoint_path(self, collection: str) -> Path:
        """Ubicacion del ultimo token confirmado para una coleccion CDC."""
        return self.root / "checkpoints" / "cdc" / f"{collection}.json"

    def write_cdc_microbatch(
        self,
        collection: str,
        events: List[Dict],
        run_id: str,
        ingestion_date: str,
        batch_number: int,
    ) -> Dict:
        """Guarda un micro-lote CDC antes de confirmar su resume token.

        La escritura previa al checkpoint entrega semantica *at-least-once*:
        ante una interrupcion pueden repetirse eventos, pero no se confirma un
        evento que no haya quedado persistido.
        """
        if not events:
            raise ValueError("Un micro-lote CDC no puede estar vacio")
        out_dir = self.cdc_partition_dir(collection, ingestion_date)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"run_{run_id}_batch_{batch_number:06d}.jsonl"
        if path.exists():
            raise FileExistsError(f"El micro-lote CDC ya existe: {path}")

        with path.open("x", encoding="utf-8") as fh:
            for event in events:
                fh.write(json_util.dumps(event) + "\n")

        operations = {}
        for event in events:
            operation = event.get("operation_type", "unknown")
            operations[operation] = operations.get(operation, 0) + 1
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
            "bytes": path.stat().st_size,
            "path": str(path),
        }
        self._write_metadata(out_dir, f"{run_id}_batch_{batch_number:06d}", metadata)
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
