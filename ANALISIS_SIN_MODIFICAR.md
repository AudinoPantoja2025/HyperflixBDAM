# ANÁLISIS SIN MODIFICAR ARCHIVOS

## 1) Árbol de directorios (relevante)

Raíz: D:\.UNIPutumayo\2026-2\BD Y ALMACENAMIENTO MASIVO\BD_Parcial
Archivos raíz:
- conexion.py, data_warehouse.py, iac_setup.py, main.py, spark_jobs.py, streaming.py
- spark_jobs_databricks.py (creado anteriormente)
- requirements.txt, README.md, .env(.example), etc.
- EXPLICACION_SIMPLE_DATABRICKS.md, MIGRACION_DATABRICKS.md, INSTRUCCIONES_DATABRICKS_SENCILLAS.md

src/ (módulos usados por spark_jobs):
- __init__.py, config.py, curated.py, data_lake.py, data_warehouse.py, extractor.py
- logger.py, mongo_client.py, parquet_writer.py, spark_jobs.py, streaming.py, transformations.py, validators.py

scripts/: automatizacion.py, benchmarking_consultas.py, crear_estructura.py, dashboard_monitoreo.py, generar_catalogo.py, generar_datos_masivos.py, transformar_oltp_a_olap.py

## 2) Imports de spark_jobs.py (raíz)
Líneas relevantes:
3: import argparse
4: import sys
5: from pathlib import Path
7: from src.spark_jobs import SparkJobError, run_interacciones_por_categoria

Depende de: `src/spark_jobs` (módulo local). No importa pandas/pyarrow directamente.

## 3) ¿Ejecutable, main y argumentos?
- Sí, ejecutable (tiene `if __name__ == "__main__": sys.exit(main())`)
- Tiene `def main(args=None)`
- `_parse_args` acepta:
  - `--root` (default: "data_lake") → raíz del Data Lake
  - `--output-root` (default: None) → raíz para `spark_curated` (si None usa `lake_root/spark_curated`)
  - `--master` (default: None) → ej. `local[*]`
  - `--single-file` (flag) → si presente, escribe **un único** Parquet con Pandas (modo Windows/winutils). Si NO presente, escribe con `result.write.parquet` (Spark nativo)

## 4) ¿Cómo está pensado para ejecutarse?
- Diseñado para ejecutarse como **script CLI** con `python spark_jobs.py [args]`
- Puede importarse, pero su uso habitual es CLI (`%run` en Databricks también ejecuta ese script)
- No es un `.ipynb` nativo; es `.py` ejecutable

## 5) SparkSession y escritura Parquet
- `src/spark_jobs.create_spark_session(app_name="BDParcial-SparkInteracciones", master=None)`:
  - importa `pyspark.sql.SparkSession` bajo demanda
  - `builder = SparkSession.builder.appName(...)`
  - si `master` no es None → `.master(master)` (esto fuerza `local[*]` en local)
  - `getOrCreate()` (evita duplicados)
- Escritura:
  - `result.count()` para auditar
  - si `single_file` es True: usa `pandas` + `collect()` → `pandas_result.to_parquet(target, engine="pyarrow", compression="snappy", index=False)` (**híbrido Windows**)
  - si `single_file` es False: `result.write.mode("errorifexists").parquet(str(target), compression="snappy")` (**Spark nativo**)

## 6) Rutas de salida
- Fuente: `latest_parquet(lake_root / "curated" / "FACT_INTERACCIONES")` → busca `ingestion_date=*/run_*.parquet` más reciente
- Destino: `target_root / "INTERACCIONES_CATEGORIA" / ingestion_date=YYYY-MM-DD / run_YYYYMMDD_HHMMSS.parquet`
  - `target_root = output_root if output_root else lake_root / "spark_curated"`
- **Sin rutas absolutas Windows** (`C:`, `D:`) en este código. Usa `pathlib.Path` relativo/parametrizado.
- En Databricks usar `--root /dbfs/FileStore/data_lake` o `/Volumes/<cat>/<schema>/<vol>/data_lake` (Unity Catalog). NO inventar.

## 7) Archivos a subir a Databricks
**SÍ subir:**
- `spark_jobs.py` (raíz) – script ejecutable con `%run`
- **carpeta `src/` completa** (obligatoria): `src/spark_jobs.py`, `src/logger.py`, etc. (raíz importa `from src.spark_jobs import ...`)

**NO subir (innecesarios para este job):**
- `main.py`, `data_warehouse.py`, `streaming.py`, `conexion.py`, `iac_setup.py`
- `scripts/` completo (salvo que quieras, no requerido)
- `.venv/`, `__pycache__/`, `*.xlsx`, `*.png`, `.pytest_cache/`
- `spark_jobs_databricks.py` (opcional; puedes usarlo, pero NO necesario)
- `.env` (usar Secrets en Databricks)

**Mínimo estricto:** `spark_jobs.py` + `src/`

## 8) Código exacto para celdas del Notebook

### Celda 1 – Verificar entorno (opcional pero útil)
```python
print(spark.version)
print(spark.conf.get("spark.app.name", "n/a"))
```

### Celda 2 – Ejecutar job Spark NATIVE (sin Pandas)
**NO usar `--single-file`. NO forzar `--master`.**
```python
%run ./spark_jobs.py --root /dbfs/FileStore/data_lake
```
Si usas Unity Catalog Volumes:
```python
%run ./spark_jobs.py --root /Volumes/<catalog>/<schema>/<volume>/data_lake
```
(Usa EXACTAMENTE lo que devolvió `spark.sql("SHOW VOLUMES").show()`)

### Celda 3 – Validar salida
```python
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
# Localiza última salida
# (ajusta ruta si cambiaste output-root)
import glob
import os

base = "/dbfs/FileStore/data_lake/spark_curated/INTERACCIONES_CATEGORIA"
# o Volumes equivalente
try:
    paths = [p for p in dbutils.fs.ls(base) if p.path.endswith('/')] if 'dbutils' in globals() else []
    # alternativa con glob si accedes por FS
    pass
except Exception:
    pass

# Más directo: leer lo generado (conoces ruta por resultado impreso)
# Tras ejecución, spark_jobs.py imprime: "Spark Job completado: ... -> <ruta>"
```
Pero más simple: **leer por ruta impresa**. El script imprime `result['output']`. También puedes listar:
```python
base = "/dbfs/FileStore/data_lake/spark_curated/INTERACCIONES_CATEGORIA"
# listar particiones
dbutils.fs.ls(base)  # si dbutils disponible
```
Y leer el `.parquet` con:
```python
out = "<pega_aquí_ruta_completa_impresa>"
df = spark.read.parquet(out)
df.printSchema()
print("Registros:", df.count())
display(df)
```

**IMPORTANTE:** NO uses `--single-file` en Databricks. Así fuerza Spark nativo (`.write.parquet`). No modifiques `spark_jobs.py`.

## 9) Notas
- `master` solo aplica local. En Databricks NO pasar `--master`.
- El "parche" híbrido existe ÚNICAMENTE por `--single-file` (Windows/winutils).
- Con `%run ./spark_jobs.py --root ...` sin `--single-file` → escritura 100% Spark nativo.
