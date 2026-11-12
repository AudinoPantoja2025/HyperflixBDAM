"""Punto de entrada del feature engineering de ML (taller PySpark MLlib)."""

import argparse
import sys
from pathlib import Path

from src.ml_features import (
    DIAS_INACTIVIDAD_DEFAULT,
    SparkJobError,
    run_dataset_ml_churn,
)


def _parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Dataset de ML de la audiencia: RFA + etiqueta churn (sin ventas)"
    )
    parser.add_argument("--root", default="data_lake", help="Raiz del Data Lake")
    parser.add_argument("--output-root", default=None, help="Raiz para la capa Spark CURATED")
    parser.add_argument("--master", default=None, help="Spark master; ej. local[*]")
    parser.add_argument(
        "--single-file",
        action="store_true",
        help="Escribe un unico Parquet con Pandas; recomendado en Windows para la demostracion",
    )
    parser.add_argument(
        "--dias-inactividad",
        type=int,
        default=DIAS_INACTIVIDAD_DEFAULT,
        help="Dias sin interactuar para etiquetar churn (default: 90)",
    )
    parser.add_argument(
        "--fecha-referencia",
        default=None,
        help="Fecha fija de referencia YYYY-MM-DD; por defecto se usa la mas reciente del dataset",
    )
    return parser.parse_args(args)


def main(args=None):
    cli = _parse_args(args)
    try:
        result = run_dataset_ml_churn(
            Path(cli.root),
            output_root=Path(cli.output_root) if cli.output_root else None,
            master=cli.master,
            single_file=cli.single_file,
            dias_inactividad=cli.dias_inactividad,
            fecha_referencia=cli.fecha_referencia,
        )
    except SparkJobError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Dataset de ML creado: {result['records']} usuarios")
    print(f"Usuarios activos: {result['usuarios_activos']}")
    print(f"Usuarios inactivos: {result['usuarios_inactivos']}")
    print(f"Tasa de churn en el dataset: {result['tasa_churn']:.2f}%")
    print(f"Salida: {result['output']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
