# Migración Databricks (Serverless + Unity Catalog Volumes)

## 1) Diagnóstico
- **ETL (local)**: `main.py` usa Pandas + `src/parquet_writer.py` (Pandas→PyArrow) para generar PROCESSED/CURATED. NO tocar.
- **Job Spark**: `src/spark_jobs.py` lee CURATED con Spark. Modo distribuido: `.write.parquet` (nativo). Modo `--single-file`: Pandas (evita winutils.exe en Windows).
- **Conexión MongoDB**: `src/config.py` + `src/mongo_client.py` leen `.env` (`MONGODB_URI`/`MONGODB_DATABASE`). Sin credenciales hardcodeadas.
- **Colecciones**: `users, movies, comments, theaters` (4). CURATED deriva DIM/FACT desde ellas.
- **Zonas**: `raw/ processed/ curated/` + `spark_curated/`.
- **Tests**: 85 passed.

## 2) Objetivo
Ejecutar PySpark **nativo** en Databricks (Serverless). Eliminar escritura vía Pandas. Usar **Unity Catalog Volumes** (`/Volumes/<catalog>/<schema>/<volume>/...`). No destruir implementación local.

## 3) Archivo a importar en Databricks
**NO** importes `main.py`. Crea/importa **`spark_jobs_databricks.py`** (raíz del repo). Subelo a Databricks Repos o Upload al Workspace.

**Recomendado:** Opción A – PyMongo → `spark.createDataFrame` → transformaciones Spark → `.write.parquet` (sin JAR extra, compatible Free Edition). Opción B usa MongoDB Spark Connector (requiere `spark.jars.packages`).

## 4) Archivo creado
`spark_jobs_databricks.py` (raíz) – implementación Spark nativa para Databricks. Usa `SparkSession.getActiveSession()` (aprovecha `spark` de Databricks), lee último Parquet CURATED o desde MongoDB si `read_from_mongo=True`, transforma con `pyspark.sql.functions` y escribe con `.write.mode(write_mode).parquet(..., compression="snappy")`.

## 5) Paso a paso en Databricks (Notebook ya creado)

### Paso 1: Subir archivos
- **Repo Git (recomendado):** Conectar repo GitHub/GitLab → sincronizar (trae `spark_jobs_databricks.py`, `src/`, `requirements.txt`).
- **Upload directo:** Subir `spark_jobs_databricks.py` y carpeta `src/` al mismo nivel del Notebook, o `.zip` → extraer.

### Paso 2: Detectar Unity Catalog Volumes
Celda 1:
```python
spark.sql("SHOW CATALOGS").show()
spark.sql("SHOW SCHEMAS").show()
spark.sql("SHOW VOLUMES").show(truncate=False)
dbutils.fs.ls("/Volumes/")
```
Usa **exactamente** `/Volumes/<catalog>/<schema>/<volume>/...`. Si no existe, créalo: Catalog → tu catálogo → Esquema → Volumes → Create Volume.

### Paso 3: Configurar secretos (MongoDB URI)
Celda 2 (recomendado):
```python
MONGO_URI = dbutils.secrets.get(scope="db-secrets", key="mongo_uri")
MONGO_DB = "sample_mflix"
```
Crear scope: Databricks → Secrets → Create Scope `db-secrets`, key `mongo_uri` = tu URI completa (igual a `.env`). NO pegar contraseña en código.

### Paso 4: Importar módulo
Celda 3:
```python
from spark_jobs_databricks import run_interacciones_por_categoria_spark
```

### Paso 5: Ejecutar (leer último CURATED existente)
Celda 4 – asume que tienes `data_lake/curated/FACT_INTERACCIONES/...` subido o ya generado:
```python
lake_root_vol = "/Volumes/<catalog>/<schema>/<volume>/data_lake"
result = run_interacciones_por_categoria_spark(
    lake_root=lake_root_vol,
    curated_ds="FACT_INTERACCIONES",
    output_ds="INTERACCIONES_CATEGORIA",
    write_mode="overwrite"
)
print(result)
```

### Paso 6: Validación
Celda 5:
```python
out = result["output"]
dfp = spark.read.parquet(out)
dfp.printSchema()
print("Registros recuperados:", dfp.count())
display(dfp.limit(10))
```

### Alternativa: leer directo desde MongoDB (sin CURATED previo)
```python
result = run_interacciones_por_categoria_spark(
    lake_root=lake_root_vol,
    read_from_mongo=True,
    mongo_uri=MONGO_URI,
    mongo_db=MONGO_DB,
    mongo_collection_fact="fact_interacciones",  # o reconstruir desde comments si prefieres
    output_ds="INTERACCIONES_CATEGORIA",
    write_mode="overwrite"
)
```

## 6) Evidencias para informe (capturas mínimas)
1. **Notebook**: secciones Markdown + celdas ejecutadas (OK).
2. **Spark activo**: `spark` disponible (Serverless).
3. **Lectura Mongo/Parquet**: count/schema visibles.
4. **Transformaciones**: `groupBy/agg/orderBy` aplicadas (mostrar resultado).
5. **Escritura Parquet**: ruta `/Volumes/.../spark_curated/INTERACCIONES_CATEGORIA/...` creada.
6. **Lectura posterior**: `spark.read.parquet` devuelve mismo esquema/count.
7. **Comparativa**: mencionar local (local[*]+Pandas puente) vs Databricks (Serverless+Spark nativo).

## 7) Notas
- **NO** tocas implementación local (Windows). `spark_jobs_databricks.py` es nuevo.
- Evitamos `toPandas()` en Databricks (solo lectura Spark). Escritura 100% `.write.parquet`.
- Compatible Free Edition (Opción A). Usa `SparkSession.getActiveSession()` → no fuerza `master("local[*]")`.
- Tests siguen 85 passed.
