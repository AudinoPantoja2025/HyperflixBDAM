"""Punto de entrada de los jobs Spark de la Fase 2."""

import argparse
import sys
from pathlib import Path

from src.spark_jobs import SparkJobError, run_interacciones_por_categoria


def _parse_args(args=None):
    parser = argparse.ArgumentParser(description="Job PySpark: interacciones por categoria")
    parser.add_argument("--root", default="data_lake", help="Raiz del Data Lake")
    parser.add_argument("--output-root", default=None, help="Raiz para la capa Spark CURATED")
    parser.add_argument("--master", default=None, help="Spark master; ej. local[*]")
    parser.add_argument(
        "--single-file",
        action="store_true",
        help="Escribe un unico Parquet con Pandas; recomendado en Windows para la demostracion",
    )
    return parser.parse_args(args)


def main(args=None):
    cli = _parse_args(args)
    try:
        result = run_interacciones_por_categoria(
            Path(cli.root),
            output_root=Path(cli.output_root) if cli.output_root else None,
            master=cli.master,
            single_file=cli.single_file,
        )
    except SparkJobError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Spark Job completado: {result['records']} filas -> {result['output']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
