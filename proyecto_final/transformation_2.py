from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import regexp_replace


sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
df_aeropuertos = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/aeropuertos.csv")
df_aeropuertos = df_aeropuertos.drop('inhab', 'fir')
df_aeropuertos_filtered = df_aeropuertos.fillna({"distancia_ref": 0})


# Renombrar y normalizar nombres de columnas
df_aeropuertos_filtered = df_aeropuertos_filtered.withColumnRenamed("local", "aeropuerto")
df_aeropuertos_filtered = df_aeropuertos_filtered.withColumnRenamed("oaci", "oac")
df_aeropuertos_filtered = df_aeropuertos_filtered.withColumnRenamed("iata", "iata")


# Castear y normalizar tipos de datos
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


df_aeropuertos_filtered.show(5)
df_aeropuertos_filtered.printSchema()

## creamos una nueva vista filtrada
df_aeropuertos_filtered.createOrReplaceTempView("filtered_detalles_vuelos")


## insertamos el DF filtrado en las tablas de Hive
hc.sql("insert into vuelos.aeropuerto_detalles_tabla select * from filtered_detalles_vuelos;")