## ⚙️ PROCESAMIENTO EN BATCH

Para el procesamiento batch se trabja con un conjunto de datos que contiene información sobre los contenidos ofrecidos por Netflix, incluyendo peliculas y series disponibles en la plataforma.

Se realizan las siguientes actividades, a través de una aplicación en PySpark:

- Cargar datos: leer el conjunto de datos almacenado en un sistema distribuido HDFS.
- Limpieza y tranformación: manejar valores faltantes y eliminar registros duplicados.
- Análisis exploratorio de datos (EDA): comprender los contenidos del conjunto de datos.
- Generación de estadísticas descriptivas: obtener conteos, promendios y distribuciones de las variables.
- Amacenamiento de resultados: guardar los datos procesados y estadísticas nuevamente en HDFS.


## **Comandos utilizados en los pasos del proyecto**

---
Codigo para incializar hadoop
```python
start-dfs.sh
start-yarn.sh
```

###  1️⃣ Crear la sesión de Spark

Se inicializa una sesión de Spark que permite ejecutar el procesamiento distribuido.  

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, mean, split, explode, regexp_replace

spark = SparkSession.builder \
    .appName("tarea3_Procesamiento_Batch_Netflix") \
    .getOrCreate()
```


###  2️⃣ Cargar el conjunto de datos desde HDFS


El dataset está almacenado en el sistema HDFS en la siguiente ruta:
```python
hdfs://localhost:9000/tarea3/netflix_titles.csv
```

Código utilizado:
```python
df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://localhost:9000/tarea3/netflix_titles.csv")

print("\n=== Vista previa del dataset ===\n")
df.show(10) 
```
Comando para ejecutar el script:
```python
python3 Tarea3.py
```
###  3️⃣ Limpieza y transformación de datos

Se manejar los valores faltantes, se eliminan los registros duplicadps y se estandarizan los datos.

Código:
```python
print("\n=== Limpieza y transformación de datos ===\n")

#Renombrer columnas
df= df.withColumnRenamed("show_id", "ID")\
      .withColumnRenamed("type", "Tipo")\
      .withColumnRenamed("title", "Título")\
      .withColumnRenamed("director", "Director")\
      .withColumnRenamed("cast", "Elenco")\
      .withColumnRenamed("country", "País")\
      .withColumnRenamed("date_added", "Fecha_Agregación")\
      .withColumnRenamed("release_year", "Año_Lanzamiento")\
      .withColumnRenamed("rating", "Clasificación")\
      .withColumnRenamed("duration", "Duración")\
      .withColumnRenamed("listed_in", "Género")\
      .withColumnRenamed("description", "Descripción")

#Eliminar duplicados
df = df.dropDuplicates()

#Conteo de valores nulo
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

#Reemplazar valores nulos por textos estándar
df = df.fillna({
    "Director": "Desconocido",
    "Elenco": "No especificado",
    "País": "Sin país",
    "Género": "Sin género",
    "Tipo": "Desconocido",
    "Titulo": "Sin título",
    "Año_Lanzamiento": 0,
    "Duración": "O min"
})

#Convertir ID a tipo string
df = df.withColumn("ID", col("ID").cast("string"))

# Filtrar valores válidos en Tipo
df = df.filter((col("Tipo").isin(["Movie", "TV Show"])))

# Filtrar Año_Lanzamiento solo valores numéricos mayores a 1900
df = df.filter((col("Año_Lanzamiento") >= 1900) & (col("Año_Lanzamiento") <= 2025))


print("\n=== Nueva vista del dataset ===\n")
df.show(10) 
```
###  4️⃣ Análisis exploratorio de datos (EDA)

Se realiza un análisis descriptivo utilizando operaciones con DataFrames, comprendiendo las características principales del conjunto de datos:

```python
print("\n=== Análisis exploratorio de datos (EDA) ===\n")

#Conteo de registros y columnas
num_filas = df.count()
num_columnas = len(df.columns)
print(f"\nNúmero de filas: {num_filas}")
print(f"Número de columnas: {num_columnas}")

#Vista general de los datos
print("\n=== Primeras filas del dataset ===")
df.show(5, truncate=False)

#1. Conteo total y proporción de películas vs series
print("=== Conteo por tipo (Película vs Serie) ===")
conteo_tipo = df.groupBy("Tipo").count()
conteo_tipo.show()

#2. Top 10 años con más lanzamientos
print("\n=== Top 10 años con más lanzamientos ===")
df_años = df.filter(col("Año_Lanzamiento").isNotNull() & col("Año_Lanzamiento").cast("int").isNotNull())
top_años = df_años.groupBy("Año_Lanzamiento").count().orderBy(col("count").desc())
top_años.show(10)

#3. Top 10 géneros más comunes
print("\n=== Top 10 géneros más comunes ===")
generos = df.withColumn("Género", explode(split(col("Género"), ",")))
generos.groupBy("Género").count().orderBy(col("count").desc()).show(10)

#4. Top 10 países con más títulos
print("\n=== Top 10 países con más títulos ===")
df.groupBy("País").count().orderBy(col("count").desc()).show(10)

#5. Promedio de duración de películas
print("\n=== Promedio de duración de películas ===")
peliculas = df.filter(col("Tipo") == "Movie")
df_duracion = peliculas.withColumn("Duración_min", regexp_replace("Duración", " min", "").cast("int"))
promedio = df_duracion.select(mean("Duración_min")).collect()[0][0]
print(f"Promedio de duración de películas: {promedio:.2f} minutos")

#6. Top 10 directores con más títulos
print("\n=== Top 10 directores con más títulos ===")
df.filter(col("Director") != "Desconocido") \
  .groupBy("Director").count().orderBy(col("count").desc()).show(10)
```
### 5️⃣ Almacenamiento de resultados

Los datos limpios y transformados se guardan nuevamente en HDFS, en formato CSV llamado resultados.

Código:
```python
print("\n=== Guardando resultados limpios en HDFS ===\n")
df.write.mode("overwrite").option("header", True).csv("hdfs://localhost:9000/tarea3/resultados")

print("\n Proceso completado con éxito. Los resultados se guardaron en: hdfs://localhost:9000/tarea3/resultados\n")

#Cierre de la sesión
spark.stop()
```
