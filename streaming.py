"""Comando independiente para la ingesta CDC por MongoDB Change Streams.

Ejemplo:
    python streaming.py --duration 60 --microbatch-size 100
"""

import argparse
import sys
from dataclasses import replace
from pathlib import Path

from src.config import ConfigError, load_settings
from src.data_lake import DataLake
from src.logger import get_logger, setup_logging
from src.mongo_client import MongoConnection, MongoConnectionError
from src.streaming import ChangeStreamConsumer, StreamingError
from src.validators import validate_settings

logger = get_logger("streaming")


def _parse_args(args=None):
    parser = argparse.ArgumentParser(description="CDC en tiempo real con MongoDB Change Streams")
    parser.add_argument("--env", default=None, help="Ruta al archivo .env")
    parser.add_argument("--root", default=None, help="Raiz del Data Lake")
    parser.add_argument("--collections", default=None, help="Colecciones separadas por coma")
    parser.add_argument("--duration", type=float, default=None, help="Segundos de escucha")
    parser.add_argument("--microbatch-size", type=int, default=100, help="Eventos por micro-lote")
    parser.add_argument("--max-await-ms", type=int, default=1000, help="Espera maxima por evento")
    parser.add_argument("--no-resume", action="store_true", help="No usar checkpoint anterior")
    return parser.parse_args(args)


def run_streaming(args=None):
    cli = _parse_args(args)
    try:
        settings = load_settings(cli.env)
        if cli.root:
            settings = replace(settings, data_lake_root=Path(cli.root))
        if cli.collections:
            settings = replace(settings, mongodb_collections=[x.strip() for x in cli.collections.split(",") if x.strip()])
        validate_settings(settings)
    except (ConfigError, ValueError) as exc:
        logger.error("Configuracion invalida: %s", exc)
        return None

    setup_logging(log_dir="logs")
    lake = DataLake(
        settings.data_lake_root,
        s3_bucket=settings.s3_bucket,
        s3_region=settings.s3_region,
        aws_profile=settings.aws_profile,
    )
    lake.create_zones()
    connection = MongoConnection(settings.mongodb_uri, settings.mongodb_database, retries=3)
    try:
        _, db = connection.connect()
        consumer = ChangeStreamConsumer(
            db, lake, settings.mongodb_collections, settings.run_id, settings.ingestion_date,
            microbatch_size=cli.microbatch_size, max_await_time_ms=cli.max_await_ms,
            resume=not cli.no_resume,
        )
        result = consumer.run(duration_seconds=cli.duration)
        print(f"CDC finalizado: eventos={result.events}, micro_lotes={result.microbatches}, operaciones={result.operations}")
        return result
    except (MongoConnectionError, StreamingError, ValueError) as exc:
        logger.error("CDC abortado: %s", exc)
        return None
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(0 if run_streaming() is not None else 1)
