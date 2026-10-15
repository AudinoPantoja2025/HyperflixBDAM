"""Comando independiente Fase 3: OLAP streaming -> Data Warehouse estrella.

No toca ``main.py`` (ETL batch), ``streaming.py`` (CDC) ni ``spark_jobs.py``.
Reusa ``src.config`` (.env/MONGO_URI), ``src.mongo_client`` y logging.

Ejemplos (sustentacion):
    python data_warehouse.py --seed --drop        # demo desde cero
    python data_warehouse.py --seed               # respeta DW existente
    python data_warehouse.py --db sample_mflix    # base explicita
    python data_warehouse.py --env .env --no-drop # sin limpiar
"""

import argparse
import sys

from src import data_warehouse as dw
from src.config import ConfigError, load_settings
from src.logger import get_logger, setup_logging
from src.mongo_client import MongoConnection, MongoConnectionError
from src.validators import validate_settings

logger = get_logger("data_warehouse")


def _parse_args(args=None):
    p = argparse.ArgumentParser(description="Fase 3: Data Warehouse estrella en MongoDB")
    p.add_argument("--env", default=None, help="Ruta al archivo .env")
    p.add_argument("--db", default=None, help="Base del DW (default: sample_mflix)")
    p.add_argument("--source", default=dw.SOURCE_COLLECTION, help="Coleccion OLTP fuente")
    p.add_argument("--seed", action="store_true",
                   help="Inserta interacciones demo si la fuente esta vacia")
    p.add_argument("--drop", dest="drop", action="store_true", default=True,
                   help="Limpia dims/fact antes de cargar (default: si)")
    p.add_argument("--no-drop", dest="drop", action="store_false",
                   help="No limpiar; $merge actualiza/inserta")
    p.add_argument("--no-disk-use", dest="disk", action="store_false", default=True,
                   help="Desactiva allowDiskUse (solo para comparar en demo)")
    return p.parse_args(args)


def run_dw(args=None):
    cli = _parse_args(args)
    try:
        settings = load_settings(cli.env)
        validate_settings(settings)
    except (ConfigError, ValueError) as exc:
        logger.error("Configuracion invalida: %s", exc)
        return None

    setup_logging(log_dir="logs")
    dw_db = cli.db or dw.DW_DATABASE_DEFAULT
    conn = MongoConnection(settings.mongodb_uri, dw_db, retries=3)
    try:
        client, _ = conn.connect()
        db = client[dw_db]
        result = dw.run_data_warehouse(
            db, source=cli.source, drop_first=cli.drop,
            allow_disk_use=cli.disk, seed_if_empty=cli.seed,
        )
        print("\nDATA WAREHOUSE CONFIGURADO  |  base='%s' fuente='%s'"
              % (result["database"], result["source"]))
        for name, n in result["counts"].items():
            print(f"  {name:<18} {n:>6} docs (count_documents({{}}))")
        return result
    except (MongoConnectionError, dw.DataWarehouseError) as exc:
        logger.error("DW abortado: %s", exc)
        return None
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(0 if run_dw() is not None else 1)
