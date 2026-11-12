# Guía del taller PySpark MLlib aplicado a streaming

## 1. Alcance

Esta entrega aplica el taller al dominio `sample_mflix`: películas, comentarios,
usuarios y teatros. No se agregan ventas, precios, ingresos, clientes
comerciales, cupones ni dinero. El ETL, el CDC y el modelo dimensional
existentes permanecen sin cambios.

El flujo es:

```text
MongoDB sample_mflix
        ↓ Change Streams / ETL
RAW → PROCESSED → CURATED/FACT_INTERACCIONES
                                      ↓
                         RFA + etiqueta de inactividad
                                      ↓
                         ML_DATASET_AUDIENCIA
```

La etiqueta `churn` es una regla histórica de negocio, no una predicción:
`churn = 1` cuando `recency > 90`. Esta separación evita afirmar que se entrenó
un modelo predictivo cuando el taller solo exige ingeniería de características.

## 2. Adaptación RFM a streaming

| Taller comercial | Streaming de películas |
|---|---|
| Cliente | Usuario de la audiencia (`dim_usuario_key`) |
| Venta | Comentario/interacción |
| Fecha de venta | Fecha del comentario |
| Recency | Días desde la última interacción |
| Frequency | Número de comentarios/interacciones |
| Monetary | No aplica |
| Engagement | Longitud promedio del comentario |
| Cliente perdido | Usuario inactivo |

El rating promedio de las películas comentadas se conserva como característica
adicional. La columna `cantidad` vale uno por interacción y permite contar
hechos válidos sin introducir conceptos de venta.

## 3. Requisitos

### Local

Desde la raíz del repositorio:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest -q
```

En Windows, Spark puede necesitar `winutils.exe` para escribir Parquet. Para
una demostración local pequeña se puede usar:

```powershell
.\.venv\Scripts\python.exe ml_churn.py `
  --root data_lake `
  --single-file `
  --fecha-referencia 2026-10-21
```

`--single-file` solo debe usarse con resultados pequeños; ejecuta las
transformaciones en Spark y escribe el agregado final con Pandas/PyArrow.

### Databricks

El código debe estar disponible en el Repo:

```text
/Workspace/Repos/HyperflixBDAM
```

Los datos proporcionados están en el Volume:

```text
/Volumes/workspace/default/data-lake/data_lake/processed/comments/ingestion_date=2026-10-21/run_20261021_100032.parquet
/Volumes/workspace/default/data-lake/data_lake/processed/movies/ingestion_date=2026-10-21/run_20261021_100032.parquet
/Volumes/workspace/default/data-lake/data_lake/processed/theaters/ingestion_date=2026-10-21/run_20261021_100032.parquet
/Volumes/workspace/default/data-lake/data_lake/processed/users/ingestion_date=2026-10-14/run_20261014_190228.parquet
```

La función ML consume `CURATED/FACT_INTERACCIONES`, no los cuatro Parquet
PROCESSED directamente. Primero debe ejecutarse el ETL/curated que construye
esa tabla. La salida recomendada es:

```text
/Volumes/workspace/default/data-lake/data_lake/spark_curated/ML_DATASET_AUDIENCIA
```

No usar `dbutils.notebook.getContext()`, rutas Windows ni `--single-file` en
Serverless. El notebook debe usar rutas explícitas y escritura Spark nativa.

## 4. Notebook Databricks: celdas

### Celda 1 — configuración

```python
from pathlib import Path
import sys

REPO_ROOT = "/Workspace/Repos/HyperflixBDAM"
LAKE_ROOT = "/Volumes/workspace/default/data-lake/data_lake"
FACT_ROOT = f"{LAKE_ROOT}/curated/FACT_INTERACCIONES"
OUTPUT_ROOT = f"{LAKE_ROOT}/spark_curated/ML_DATASET_AUDIENCIA"

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.ml_features import dataset_ml_churn, ml_dataset_metrics
print("Código:", REPO_ROOT)
print("Entrada:", FACT_ROOT)
print("Salida:", OUTPUT_ROOT)
```

### Celda 2 — localizar la fotografía CURATED

```python
from src.spark_jobs import latest_parquet

source = latest_parquet(Path(FACT_ROOT))
print(source)
```

Si esta celda informa que no hay Parquet, ejecute antes el ETL que genera
`FACT_INTERACCIONES`. No sustituya la entrada por un archivo PROCESSED, porque
esas columnas todavía no representan el modelo dimensional requerido por el
taller.

### Celda 3 — construir RFA

```python
dataset = dataset_ml_churn(
    spark,
    source,
    dias_inactividad=90,
    fecha_referencia="2026-10-21",
)

display(dataset.orderBy("recency", ascending=False))
```

La fecha fija hace reproducible la evidencia. Para una ejecución automática se
puede omitir `fecha_referencia`; en ese caso se usa la fecha máxima del dataset.

### Celda 4 — métricas del taller

```python
metrics = ml_dataset_metrics(dataset, dias_inactividad=90)
print(metrics)
```

Se deben reportar total de usuarios, usuarios activos, usuarios inactivos y
tasa de churn. La tasa está expresada en porcentaje.

### Celda 5 — persistir la salida

```python
(
    dataset.write
    .mode("overwrite")
    .format("parquet")
    .save(OUTPUT_ROOT)
)
display(spark.read.parquet(OUTPUT_ROOT))
```

En una entrega productiva se recomienda escribir una nueva partición con
`run_YYYYMMDD_HHMMSS` en lugar de sobrescribir. El `run_dataset_ml_churn`
local ya usa esa estrategia; la celda anterior es una demostración controlada
para el notebook.

### Celda 6 — evidencia

```python
print(f"Usuarios procesados: {metrics['total_usuarios']}")
print(f"Usuarios activos: {metrics['usuarios_activos']}")
print(f"Usuarios inactivos: {metrics['usuarios_inactivos']}")
print(f"Tasa de churn: {metrics['tasa_churn']:.2f}%")
display(
    dataset.groupBy("churn")
    .count()
    .withColumnRenamed("count", "usuarios")
)
```

Conserve capturas de las celdas 2, 3, 4 y 6, además de la ruta de salida.

## 5. Qué debe explicarse en la sustentación

1. Cada comentario se convierte en una interacción de la FACT.
2. `frequency` cuenta interacciones por usuario.
3. `recency` usa la diferencia entre la fecha de referencia y la última
   interacción.
4. `engagement` mide longitud promedio del comentario, no dinero.
5. El umbral de 90 días produce una etiqueta auditable.
6. La etiqueta no es una predicción MLlib: para entrenar un clasificador se
   necesitaría una etiqueta histórica independiente y una separación
   temporal train/test.
7. El cálculo es reproducible con una fecha fija y compatible con nuevas
   fotografías generadas por el CDC.

Great Expectations no es necesario para este notebook. El Quality Gate puede
ejecutarse por separado; si el Runtime no tiene GX instalado, debe reportarse
como no disponible y no como una ejecución exitosa.

## 6. Problemas frecuentes

| Error | Solución |
|---|---|
| `NotebookHandler has no attribute getContext` | Eliminar esa API; usar las rutas explícitas de la Celda 1. |
| `No hay Parquet ... FACT_INTERACCIONES` | Ejecutar el ETL/curated antes del notebook. |
| `CAST_INVALID_INPUT` con `theaterId` | No modificar tipos del notebook ML; la simulación del Quality Gate debe usar valores compatibles con el esquema. |
| `imdb` no es STRUCT | Usar `imdb_rating` o el tipo real del esquema; no aplicar `withField` a un `STRING`. |
| Great Expectations no disponible | Continuar con validaciones PySpark y documentar GX como no instalado. |
| Error `HADOOP_HOME` local | Usar `--single-file` o configurar Hadoop local; en Databricks usar escritura Spark nativa. |
| Fecha inválida | Usar formato ISO `YYYY-MM-DD`. |

## 7. Criterios de cumplimiento

- [ ] Se consume `FACT_INTERACCIONES` generado por el flujo existente.
- [ ] Se calculan `recency`, `frequency`, `rating_promedio` y `engagement`.
- [ ] Se genera `churn` con umbral documentado.
- [ ] Se reportan total, activos, inactivos y tasa.
- [ ] Se guarda el dataset Parquet en el Volume.
- [ ] Se muestran evidencias de entrada, transformación, métricas y salida.
- [ ] No aparecen ventas, dinero, precios, compras ni cupones.
- [ ] Se aclara que `churn` es etiqueta heurística y no predicción entrenada.
