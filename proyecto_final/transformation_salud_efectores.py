from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import round


sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
efectores = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/efectores.csv")


efectores = efectores.withColumnRenamed("Label", "nombre_efector")
efectores = efectores.withColumnRenamed("Institucion", "institucion")
efectores = efectores.withColumnRenamed("Localidad", "localidad")
efectores = efectores.withColumnRenamed("Partido", "partido")
efectores = efectores.withColumnRenamed("Provincia", "provincia")
efectores = efectores.withColumnRenamed("Region Sanitaria", "region_sanitaria")
efectores = efectores.withColumnRenamed("Ubicacion_relacional", "ubicacion_relacional")
efectores = efectores.withColumnRenamed("Nivel de atencion", "nivel_de_atencion")
efectores = efectores.withColumnRenamed("Gestion", "gestion")
efectores = efectores.withColumnRenamed("x", "longitud")
efectores = efectores.withColumnRenamed("y", "latitud")

efectores.show(5)
efectores.printSchema()

## creamos una nueva vista filtrada
efectores.createOrReplaceTempView("filtered_efectores")

## insertamos el DF filtrado en las tablas de Hive
hc.sql("insert into salud.efectores select * from filtered_efectores;")
