TE LO EXPLICO MUY CLARO - SIN GIT

## Opción más fácil: "Upload" (subir archivos directamente al Notebook/Workspace)

NO necesitas Git. En Databricks puedes subir archivos directamente.

### PASO 1: Sube `spark_jobs_databricks.py` al Workspace
1. En Databricks (izquierda) → Workspace → selecciona la carpeta donde está tu Notebook
2. Click `⋯` (tres puntos) → Upload → File
3. Selecciona `D:\.UNIPutumayo\2026-2\BD Y ALMACENAMIENTO MASIVO\BD_Parcial\spark_jobs_databricks.py`
4. Sube

### PASO 2: Sube la carpeta `src/` completa
1. En esa misma carpeta → `⋯` → Upload → Folder (o selecciona la carpeta)
2. Sube la carpeta `src` completa (así Databricks tendrá `src/config.py, src/mongo_client.py, etc.`)
3. Debe quedar: tu Notebook + `spark_jobs_databricks.py` + carpeta `src/` (al mismo nivel)

### PASO 3: Detectar Unity Catalog Volumes (Celda 1)
Crea una celda en tu Notebook y pega esto:
```python
spark.sql("SHOW CATALOGS").show()
spark.sql("SHOW SCHEMAS").show()
spark.sql("SHOW VOLUMES").show(truncate=False)
dbutils.fs.ls("/Volumes/")
```
Ejecuta (▶). Mira lo que aparece. Te mostrará algo tipo `/Volumes/main/default/mi_volumen`. COPIA esa ruta base.

### PASO 4: Guardar MongoDB URI en Secrets (Celda 2) - SEGURO
```python
MONGO_URI = dbutils.secrets.get(scope="db-secrets", key="mongo_uri")
MONGO_DB = "sample_mflix"
```
Si no existe ese scope: Databricks → Settings/Secrets → Create Scope `db-secrets` → Add Secret `mongo_uri` y pega TU URI completa (la de tu `.env`, formato `mongodb+srv://<usuario>:<password>@<cluster>.mongodb.net/...`). Nunca la escribas en el código del Notebook.

### PASO 5: Importar el módulo (Celda 3)
```python
from spark_jobs_databricks import run_interacciones_por_categoria_spark
```

### PASO 6: Ejecutar (Celda 4)
Reemplaza `<catalog>/<schema>/<volume>` con lo que viste en Paso 3:
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
Ejecuta. Si NO tienes CURATED aún en ese Volume, usa `read_from_mongo=True` (explicado en el .md). Pero lo mejor es primero generar CURATED local o subir `data_lake/curated/` a ese Volume.

### PASO 7: Validar (Celda 5)
```python
out = result["output"]
dfp = spark.read.parquet(out)
dfp.printSchema()
print("Registros recuperados:", dfp.count())
display(dfp.limit(10))
```

## IMPORTANTE
- **Qué importar:** SOLO `spark_jobs_databricks.py` (NO main.py)
- **Dónde subir:** al mismo nivel que tu Notebook (Workspace). Sube `src/` como CARPETA.
- **Rutas:** usa SIEMPRE `/Volumes/...` (lo que aparezca, NO inventes).
- **Credenciales:** NUNCA las pegas en código, usa Secrets.
