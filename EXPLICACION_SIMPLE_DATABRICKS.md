## Explicación directa: lo que debes hacer (muy simple)

Tu compañero probablemente hizo **solo 3 cosas**:

1. **Subió `spark_jobs.py` a un Notebook de Databricks** (o lo pegó en celdas)
2. **Cambió las rutas** de `data_lake/...` a `dbfs:/data_lake/...` (o a `/Volumes/...`)
3. **Quitó `--master local[*]` y `--single-file`** para que use Spark nativo (`df.write.parquet`)

**NO** necesitó crear 10 archivos ni seguir 20 pasos.

### Lo más sencillo (sin complicarte)

Tienes `spark_jobs.py` (raíz) que ya llama a `src/spark_jobs.py`. En `src/spark_jobs.py` la línea 136 ya escribe **directamente con Spark** cuando `single_file=False`:

```python
result.write.mode("errorifexists").parquet(str(target), compression="snappy")
```

El `single_file=True` es SOLO para Windows (evita winutils). En Databricks **NO lo uses**.

### PASOS MUY CORTOS

**Paso 1: Sube a Databricks (Upload)**
- Databricks → Workspace → carpeta de tu Notebook → ⋯ → Upload → File → sube `spark_jobs.py`
- ⋯ → Upload → Folder → sube carpeta `src/` (completa)

**Paso 2: Abre un Notebook y ejecuta (Celda 1)**
```python
# Ejecutar job Spark nativo en Databricks
%run ./spark_jobs.py --root /dbfs/FileStore/data_lake --output-root /dbfs/FileStore/data_lake/spark_curated
```
Pero mejor con rutas Volumes (recomendado). O más simple: sin `--single-file`.

**Paso 3: Forma correcta (nativo Spark, sin Pandas)**
En Databricks, ejecuta:
```python
%run ./spark_jobs.py --root /dbfs/FileStore/data_lake
```
**NO** pases `--single-file`. Así usa `result.write.parquet` (línea 136 de src/spark_jobs.py) → Spark nativo, sin Pandas.

**Explicación para informe:**
> "En el entorno local se utilizó PySpark en modo `local[*]` con la opción `--single-file` (patrón híbrido Pandas→Parquet para evitar dependencias de winutils.exe en Windows). En la migración a Databricks (Serverless), se elimina dicho parche didáctico y se ejecuta el mismo job con escritura nativa `df.write.parquet`, aprovechando el procesamiento distribuido en Linux sin dependencias adicionales."

**Conclusión:** No necesitabas el archivo nuevo. Con subir `spark_jobs.py + src/` y NO usar `--single-file`, ya estás usando Spark nativo en Databricks. Eso explica por qué otros compañeros lo hicieron con menos pasos.
