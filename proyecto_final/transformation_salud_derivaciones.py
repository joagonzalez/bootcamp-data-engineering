from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import round
from pyspark.sql.functions import trim


sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
derivaciones = spark.read.option("header", "true").option("delimiter", ",").csv("hdfs://etl:9000/ingest/derivaciones.csv")


derivaciones = derivaciones.withColumnRenamed("Año_evento", "anio_evento")
derivaciones = derivaciones.withColumnRenamed("Efector desde donde se deriva", "efector_derivador")
derivaciones = derivaciones.withColumnRenamed("partido_efector_derivante", "partido_efector_derivador")
derivaciones = derivaciones.withColumnRenamed("provincia_efector_derivante", "provincia_efector_derivador")
derivaciones = derivaciones.withColumnRenamed("region_sanitaria_efector_derivante", "region_sanitaria_efector_derivador")
derivaciones = derivaciones.withColumnRenamed("ID_efector_derivante", "id_efector_derivador")
derivaciones = derivaciones.withColumnRenamed("ID_efector_hacia_donde_derivar", "id_efector_a_derivar")
derivaciones = derivaciones.withColumnRenamed("Ubicacion_relacional", "ubicacion_relacional")
derivaciones = derivaciones.withColumnRenamed("Efector de atencion", "efector_de_atencion")
derivaciones = derivaciones.withColumnRenamed("ID_efector_atencion", "id_efector_de_atencion")
derivaciones = derivaciones.withColumnRenamed("Efector hacia donde se deriva", "efector_a_derivar")
derivaciones = derivaciones.withColumnRenamed("Area / servicio", "area_servicio")
derivaciones = derivaciones.withColumnRenamed("barrio / localidad", "barrio_localidad")
derivaciones = derivaciones.withColumnRenamed("x", "longitud")
derivaciones = derivaciones.withColumnRenamed("y", "latitud")
derivaciones = derivaciones.withColumnRenamed("Fecha de ingreso", "fecha_ingreso")
derivaciones = derivaciones.withColumnRenamed("Fecha de egreso", "fecha_egreso")
derivaciones = derivaciones.withColumnRenamed("CIE11", "cie11")
derivaciones = derivaciones.withColumnRenamed("Clinico o Quirurgico", "clinico_o_quirurgico")
derivaciones = derivaciones.withColumnRenamed("Enf_poco_frecuente", "enfermedad_poco_frecuente")
derivaciones = derivaciones.withColumnRenamed("Congenito", "congenito")
derivaciones = derivaciones.withColumnRenamed("Observaciones", "observaciones")

derivaciones = derivaciones.drop('_c34')

derivaciones = derivaciones.drop('id_efector_a_derivar')
derivaciones = derivaciones.drop('id_efector_derivador')
derivaciones = derivaciones.drop('id_efector_de_atencion')
derivaciones = derivaciones.drop('partido_efector_derivador')
derivaciones = derivaciones.drop('region_sanitaria_efector_derivador')
derivaciones = derivaciones.drop('provincia_efector_derivador')
derivaciones = derivaciones.drop('ubicacion_relacional')


derivaciones = derivaciones.withColumn("fecha_ingreso", to_date(col("fecha_ingreso"), "M/d/yyyy"))
derivaciones = derivaciones.withColumn("fecha_egreso", to_date(col("fecha_egreso"), "M/d/yyyy"))
derivaciones = derivaciones.withColumn("fecha_nacimiento", to_date(col("fecha_nacimiento"), "M/d/yyyy"))


derivaciones = derivaciones.withColumn("anio_evento", col("anio_evento").cast("int"))
derivaciones = derivaciones.withColumn("altura", col("altura").cast("int"))

derivaciones = derivaciones.withColumn("efector_a_derivar", trim(derivaciones.efector_a_derivar))


derivaciones.show(5)
derivaciones.printSchema()

## creamos una nueva vista filtrada
derivaciones.createOrReplaceTempView("filtered_derivaciones")

## insertamos el DF filtrado en las tablas de Hive
hc.sql("insert into salud.derivaciones select * from filtered_derivaciones;")
