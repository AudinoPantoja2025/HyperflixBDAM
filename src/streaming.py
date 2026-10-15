"""Ingesta CDC en tiempo real basada en MongoDB Change Streams.

El modulo representa de forma local el flujo ``productor -> broker ->
micro-lote``: MongoDB produce cambios, el buffer en memoria hace el papel del
broker y cada micro-lote persistido equivale al trabajo de Spark Streaming.
No modifica ni invoca el ETL batch existente.
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from bson import json_util

from src.data_lake import DataLake
from src.logger import get_logger

logger = get_logger(__name__)


class StreamingError(Exception):
    """Error controlado en la captura o persistencia de eventos CDC."""


@dataclass(frozen=True)
class StreamingResult:
    """Resumen auditable de una ejecucion del consumidor CDC."""

    run_id: str
    events: int
    microbatches: int
    operations: Dict[str, int]
    checkpointed_collections: List[str]


def _serialize_change(change: Dict) -> Dict:
    """Conserva la informacion CDC relevante sin alterar el evento original."""
    namespace = change.get("ns") or {}
    return {
        "event_id": change.get("_id"),
        "operation_type": change.get("operationType"),
        "cluster_time": change.get("clusterTime"),
        "collection": namespace.get("coll"),
        "database": namespace.get("db"),
        "document_key": change.get("documentKey"),
        "full_document": change.get("fullDocument"),
        "update_description": change.get("updateDescription"),
        "wall_time": change.get("wallTime"),
        "captured_at": datetime.now().isoformat(),
    }


class ChangeStreamConsumer:
    """Consume Change Streams y persiste eventos en micro-lotes inmutables."""

    def __init__(
        self,
        db,
        data_lake: DataLake,
        collections: Iterable[str],
        run_id: str,
        ingestion_date: str,
        microbatch_size: int = 100,
        max_await_time_ms: int = 1000,
        resume: bool = True,
    ):
        selected = [name for name in collections if name]
        if not selected:
            raise ValueError("Se requiere al menos una coleccion para CDC")
        if microbatch_size < 1:
            raise ValueError("microbatch_size debe ser mayor que cero")
        self.db = db
        self.data_lake = data_lake
        self.collections = selected
        self.run_id = run_id
        self.ingestion_date = ingestion_date
        self.microbatch_size = microbatch_size
        self.max_await_time_ms = max(1, max_await_time_ms)
        self.resume = resume
        self._batch_number = 0
        self._events = 0
        self._operations: Counter = Counter()
        self._checkpointed = set()

    def _resume_token(self) -> Optional[Dict]:
        """Retorna el checkpoint global del stream a nivel de base.

        El token de ``db.watch`` representa una secuencia de toda la base, no
        de una coleccion. Por ello se guarda un solo checkpoint global y no se
        mezclan tokens parciales de colecciones distintas.
        """
        if not self.resume:
            return None
        checkpoint = self.data_lake.read_cdc_checkpoint("_database")
        return checkpoint.get("resume_token") if checkpoint else None

    def _flush(self, pending: List[Dict]) -> None:
        if not pending:
            return
        last_event = pending[-1]
        grouped: Dict[str, List[Dict]] = {}
        for item in pending:
            grouped.setdefault(item["collection"], []).append(item)
        for collection, events in grouped.items():
            self._batch_number += 1
            self.data_lake.write_cdc_microbatch(
                collection, events, self.run_id, self.ingestion_date, self._batch_number
            )
            self._checkpointed.add(collection)
            self._events += len(events)
            self._operations.update(event["operation_type"] or "unknown" for event in events)
        # Todas las particiones del micro-lote ya estan persistidas: solo ahora
        # se avanza el token global. Si falla antes, MongoDB reentregara eventos.
        self.data_lake.write_cdc_checkpoint(
            "_database",
            {
                "scope": "database",
                "resume_token": last_event["event_id"],
                "run_id": self.run_id,
                "updated_at": datetime.now().isoformat(),
            },
        )
        pending.clear()

    def run(self, duration_seconds: Optional[float] = None) -> StreamingResult:
        """Ejecuta el consumidor; sin duracion se mantiene activo hasta Ctrl+C."""
        pipeline = [{"$match": {"ns.coll": {"$in": self.collections}}}]
        options = {"full_document": "updateLookup", "max_await_time_ms": self.max_await_time_ms}
        token = self._resume_token()
        if token is not None:
            options["resume_after"] = token

        pending: List[Dict] = []
        started = time.monotonic()
        try:
            with self.db.watch(pipeline, **options) as stream:
                logger.info("CDC activo para colecciones: %s", ", ".join(self.collections))
                while duration_seconds is None or time.monotonic() - started < duration_seconds:
                    change = stream.try_next()
                    if change is None:
                        self._flush(pending)
                        continue
                    event = _serialize_change(change)
                    if event["collection"] not in self.collections:
                        continue
                    pending.append(event)
                    if len(pending) >= self.microbatch_size:
                        self._flush(pending)
        except KeyboardInterrupt:
            logger.info("CDC detenido por el usuario")
        except Exception as exc:  # driver errors retain their original message in chained cause
            raise StreamingError(f"Fallo en MongoDB Change Stream: {exc}") from exc
        finally:
            self._flush(pending)

        return StreamingResult(
            run_id=self.run_id,
            events=self._events,
            microbatches=self._batch_number,
            operations=dict(sorted(self._operations.items())),
            checkpointed_collections=sorted(self._checkpointed),
        )
