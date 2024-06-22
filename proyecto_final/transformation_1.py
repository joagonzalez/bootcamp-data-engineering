from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import regexp_replace


sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
df_2021 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/vuelos_2021.csv")
df_2022 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/vuelos_2022.csv")


df_2021 = df_2021.drop('Calidad dato')
df_2021_filtered = df_2021.filter(df_2021["Clasificación Vuelo"] == "Domestico")
df_2021_filtered = df_2021_filtered.fillna({"Pasajeros": 0})

df_2022 = df_2022.drop('Calidad dato')
df_2022_filtered = df_2022.filter(df_2022["Clasificación Vuelo"] == "Doméstico")
df_2022_filtered = df_2022_filtered.withColumn("Clasificación Vuelo", regexp_replace("Clasificación Vuelo", "Doméstico", "Domestico"))
df_2022_filtered = df_2022_filtered.fillna({"Pasajeros": 0})

df_vuelos = df_2021_filtered.union(df_2022_filtered)
df_vuelos = df_vuelos.withColumn("fecha", to_date(col("fecha"), "dd/MM/yyyy"))


# Renombrar y normalizar nombres de columnas
df_vuelos = df_vuelos.withColumnRenamed("Fecha", "fecha")
df_vuelos = df_vuelos.withColumnRenamed("Hora UTC", "horaUTC")
df_vuelos = df_vuelos.withColumnRenamed("Clase de Vuelo (todos los vuelos)", "clase_de_vuelo")
df_vuelos = df_vuelos.withColumnRenamed("Clasificación Vuelo", "clasificacion_de_vuelo")
df_vuelos = df_vuelos.withColumnRenamed("Tipo de Movimiento", "tipo_de_movimiento")
df_vuelos = df_vuelos.withColumnRenamed("Aeropuerto", "aeropuerto")
df_vuelos = df_vuelos.withColumnRenamed("Origen / Destino", "origen_destino")
df_vuelos = df_vuelos.withColumnRenamed("Aerolinea Nombre", "aerolinea_nombre")
df_vuelos = df_vuelos.withColumnRenamed("Aeronave", "aeronave")
df_vuelos = df_vuelos.withColumnRenamed("Pasajeros", "pasajeros")

# Castear y normalizar tipos de datos
df_vuelos = df_vuelos.select(
    df_vuelos.fecha.cast("date"), 
    df_vuelos.horaUTC.cast("string"),
    df_vuelos.clase_de_vuelo.cast("string"),
    df_vuelos.clasificacion_de_vuelo.cast("string"),
    df_vuelos.tipo_de_movimiento.cast("string"),
    df_vuelos.aeropuerto.cast("string"),
    df_vuelos.origen_destino.cast("string"),
    df_vuelos.aerolinea_nombre.cast("string"),
    df_vuelos.aeronave.cast("string"),
    df_vuelos.pasajeros.cast("int")
    )

df_vuelos.show(5)
df_vuelos.printSchema()

## creamos una nueva vista filtrada
df_vuelos.createOrReplaceTempView("filtered_vuelos")

## insertamos el DF filtrado en las tablas de Hive
hc.sql("insert into vuelos.aeropuerto_tabla select * from filtered_vuelos;")
