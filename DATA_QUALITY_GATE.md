# Taller Data Quality Gate para `sample_mflix`

La implementación es una extensión aislada del ETL existente. El módulo
[`src/data_quality_gate.py`](src/data_quality_gate.py) no modifica MongoDB
Atlas, no reemplaza `main.py` y no cambia RAW, PROCESSED ni CURATED.

## Ejecución en el Notebook existente de Databricks

Agrega estas celdas al final del Notebook que ya tienes. No borres las celdas
anteriores. Cambia `lake_root` únicamente por un Volume que exista realmente.

### 1. Verificar el entorno

```python
import sys
print("Python:", sys.version)
print("Spark:", spark.version)
spark.sql("SHOW VOLUMES").show(truncate=False)
```

### 2. Importar el módulo y leer PROCESSED

Si trabajas con Databricks Repos, el repositorio ya debe contener `src/`. Si
subiste archivos al Workspace, agrega el directorio real del repositorio:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<usuario>/<repositorio>")
from src.data_quality_gate import (
    simulate_dirty_data,
    validate_all_collections,
    write_quality_outputs,
)
```

Luego lee la última partición real disponible:

```python
lake_root = "/Volumes/<catalog>/<schema>/<volume>/data_lake"
movies = spark.read.parquet(f"{lake_root}/processed/movies")
comments = spark.read.parquet(f"{lake_root}/processed/comments")
users = spark.read.parquet(f"{lake_root}/processed/users")
theaters = spark.read.parquet(f"{lake_root}/processed/theaters")

frames = {
    "movies": movies,
    "comments": comments,
    "users": users,
    "theaters": theaters,
}
for name, frame in frames.items():
    print(name, frame.count())
    frame.printSchema()
```

Si la ruta general no existe, usa la ruta completa de la partición que se
muestre en el Volume. No inventes el catálogo, esquema o volumen.

### 3. Simular datos sucios sin tocar MongoDB

```python
dirty_frames = {
    name: simulate_dirty_data(frame, name)
    for name, frame in frames.items()
}
for name, frame in dirty_frames.items():
    print(name)
    display(frame.filter("_simulated_dirty_record"))
```

La simulación agrega una anomalía y un duplicado sobre una copia de trabajo.
Los documentos originales de MongoDB no se actualizan ni eliminan.

### 4. Ejecutar el Quality Gate en las cuatro colecciones

```python
results = validate_all_collections(dirty_frames)

for collection, result in results.items():
    print(collection, result.metrics)
    display(result.valid_df)
    display(result.rejected_df)
    display(result.rule_counts_df)
```

La referencia `comments.movie_id -> movies._id` se valida con una operación
distribuida de Spark; no se recolectan todos los identificadores al driver.

### 5. Persistir válidos, cuarentena y métricas

```python
quality_root = f"{lake_root}/quality_gate"
paths = write_quality_outputs(results, quality_root)
print(paths)
```

La estructura generada es:

```text
quality_gate/
├── valid/{movies,comments,users,theaters}
├── quarantine/{movies,comments,users,theaters}
└── metrics/{movies_rules,comments_rules,users_rules,theaters_rules,run_summary}
```

Verifica la escritura:

```python
for name, path in paths.items():
    print(name, path)
    check = spark.read.parquet(path)
    check.printSchema()
    display(check.limit(20))
```

### 6. Entrada controlada a Curated

Para demostrar el flujo académico, utiliza solo los DataFrames válidos como
entrada del proceso analítico:

```python
valid_movies = results["movies"].valid_df
valid_comments = results["comments"].valid_df
valid_users = results["users"].valid_df
valid_theaters = results["theaters"].valid_df
```

El ETL productivo existente permanece intacto. Esta ejecución demuestra que
los registros aprobados son los únicos que continuarían hacia Curated.

## Great Expectations

Primero ejecuta esta celda y conserva la salida:

```python
import importlib.util
print("Great Expectations disponible:",
      importlib.util.find_spec("great_expectations") is not None)
```

Si el resultado es `False`, no instales una versión al azar. Configura GX solo
mediante el mecanismo aprobado de librerías del workspace y verifica la
compatibilidad exacta del Runtime Serverless. No agregues GX a
`requirements.txt` local sin haber confirmado esa compatibilidad.

Cuando GX esté disponible, crea una sección separada en el Notebook que:

1. Muestre `great_expectations.__version__`.
2. Cree una Expectation Suite.
3. Valide como mínimo:
   - `movies.title` obligatorio.
   - `movies.imdb.rating` entre 0 y 10.
   - `movies.runtime` positivo.
   - `_id` único.
   - `comments.text` no vacío.
4. Ejecute la validación sobre el DataFrame de trabajo.
5. Muestre expectativas aprobadas y fallidas.
6. Genere Data Docs HTML en una ruta real del Volume.
7. Muestre o descargue la ruta del reporte.

La API de GX cambia entre versiones. La celda debe usar la documentación de
la versión instalada. Si la importación o la validación falla, documenta GX
como incompatibilidad del Runtime y conserva PySpark nativo como resultado
principal; no presentes filtros Spark como si fueran GX.

## Reflexión académica

Permitir películas sin título, con calificaciones fuera del rango esperado o
con duraciones inválidas contaminaría los indicadores de calidad del catálogo.
Además, un comentario asociado a una película inexistente rompería la
integridad de las relaciones utilizadas en la analítica. Estos errores podrían
distorsionar los análisis por género, las métricas de interacción y cualquier
recomendación basada en el catálogo. El Quality Gate evita que esos registros
lleguen a Curated sin trazabilidad y conserva los rechazos para su revisión.

## Cómo ejecutar y guardar el Notebook

1. Abre el Notebook existente en Databricks.
2. Confirma que el repositorio o los archivos contienen `src/data_quality_gate.py`.
3. Agrega las celdas anteriores al final, preservando el trabajo anterior.
4. Ejecuta cada celda con `Shift+Enter` o usa **Run all**.
5. Reemplaza los marcadores de `lake_root` por la ruta real mostrada por
   `SHOW VOLUMES`.
6. Revisa que las cuatro colecciones produzcan válidos, rechazos y métricas.
7. Revisa `quality_gate/metrics/run_summary`.
8. Ejecuta la celda de GX y conserva su resultado.
9. Guarda el Notebook y exporta el HTML o PDF si el docente lo solicita.

## Capturas requeridas

Captura la versión de Python/Spark, los esquemas, la simulación, los cuatro
DataFrames válidos y rechazados, las reglas incumplidas, `run_summary`, las
rutas Parquet, la lectura de cuarentena y Data Docs de GX si fueron generados.
