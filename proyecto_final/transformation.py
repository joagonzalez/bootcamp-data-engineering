from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.types import DateType
from pyspark.sql.functions import when, avg
from pyspark.sql.session import SparkSession


sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
df_2021 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/vuelos_2021.csv")
df_2022 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/vuelos_2022.csv")
df_aeropuertos = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/aeropuertos.csv")

df_aeropuertos = df_aeropuertos.drop('inhab', 'fir')
df_2021 = df_2021.drop('Calidad dato')
df_2022 = df_2022.drop('Calidad dato')

df_2021_filtered = df_2021.filter(df_2021["Clasificación Vuelo"] == "Domestico")
df_2022_filtered = df_2022.filter(df_2022["Clasificación Vuelo"] == "Domestico")

df_2021_filtered = df_2021_filtered.fillna({"Pasajeros": 0})
df_2022_filtered = df_2022_filtered.fillna({"Pasajeros": 0})
 
df_aeropuertos_filtered = df_aeropuertos.fillna({"distancia_ref": 0})
df_vuelos = df_2021_filtered.union(df_2022_filtered)

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


df_aeropuertos_filtered = df_aeropuertos_filtered.withColumnRenamed("local", "aeropuerto")
df_aeropuertos_filtered = df_aeropuertos_filtered.withColumnRenamed("oaci", "oac")
df_aeropuertos_filtered = df_aeropuertos_filtered.withColumnRenamed("iata", "iata")


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


df_aeropuertos_filtered = df_aeropuertos_filtered.select(
    df_aeropuertos_filtered.aeropuerto.cast("string"), 
    df_aeropuertos_filtered.oac.cast("string"),
    df_aeropuertos_filtered.iata.cast("string"),
    df_aeropuertos_filtered.tipo.cast("string"),
    df_aeropuertos_filtered.denominacion.cast("string"),
    df_aeropuertos_filtered.coordenadas.cast("string"),
    df_aeropuertos_filtered.latitud.cast("string"),
    df_aeropuertos_filtered.longitud.cast("string"),
    df_aeropuertos_filtered.elev.cast("float"),
    df_aeropuertos_filtered.uom_elev.cast("string"),
    df_aeropuertos_filtered.ref.cast("string"),
    df_aeropuertos_filtered.distancia_ref.cast("float"),
    df_aeropuertos_filtered.direccion_ref.cast("string"),
    df_aeropuertos_filtered.condicion.cast("string"),
    df_aeropuertos_filtered.control.cast("string"),
    df_aeropuertos_filtered.region.cast("string"),
    df_aeropuertos_filtered.uso.cast("string"),
    df_aeropuertos_filtered.trafico.cast("string"),
    df_aeropuertos_filtered.sna.cast("string"),
    df_aeropuertos_filtered.concesionado.cast("string"),
    df_aeropuertos_filtered.provincia.cast("string")
    )


df_vuelos.show(5)
df_vuelos.printSchema()

## creamos una nueva vista filtrada
df_vuelos.createOrReplaceTempView("filtered_vuelos")
df_aeropuertos_filtered.createOrReplaceTempView("filtered_detalles_vuelos")


## insertamos el DF filtrado en la tabla edv.titanic
hc.sql("insert into vuelos.aeropuerto_tabla select * from filtered_vuelos;")
hc.sql("insert into vuelos.aeropuerto_detalles_tabla select * from filtered_detalles_vuelos;")