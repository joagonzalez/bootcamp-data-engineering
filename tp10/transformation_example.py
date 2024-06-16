from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo el csv desde HDFS y lo cargo en un dataframe
df = spark.read.option("header", "true").csv("hdfs://etl:9000/ingest/yellow_tripdata_2021-01.csv")

##opcional: si queres ver la data que esta en el DF### 
df.show(5)

##creamos una vista del DF
df.createOrReplaceTempView("tripdata_vista")

##Filtramos el DF quedandonos solamente con aquellos viejes que viajaron un solo pasajero y tuvieron una distancia mayor a 5 millas
new_df = spark.sql("select * from tripdata_vista where passenger_count = 1 and trip_distance > 5")

##opcional: si queremos ver la info que quedo filtrada####
new_df.show(5)

##Creamos una vista con la data filtrada###
new_df.createOrReplaceTempView("tripdata_vista_filtrada")

##insertamos el DF filtrado en la tabla tripdata_table2
hc.sql("insert into tripdata.tripdata_table2 select * from tripdata_vista_filtrada;")
