"""Configuracion central del logging para el pipeline Big Data.

Usa el logger estandar de Python con formato consistente y, de forma
opcional, un archivo rotativo para la auditoria de ejecuciones.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-18s | %(message)s"


def setup_logging(level=logging.INFO, log_dir=None, log_name="datalake", force=False):
    """Configura los handlers raiz.

    - `log_dir`: si se indica, agrega un archivo rotativo (5 MB por archivo).
    - `force`: si True, limpia handlers existentes (util en notebooks/tests).
    """
    root = logging.getLogger()
    if root.handlers and not force:
        return root

    if root.handlers:
        for h in root.handlers[:]:
            root.removeHandler(h)

    root.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path / f"{log_name}.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    return root


def get_logger(name):
    return logging.getLogger(name)