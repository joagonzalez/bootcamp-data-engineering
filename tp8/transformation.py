from pyspark.sql import HiveContext
from pyspark.sql.types import DateType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession



sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
df1 = spark.read.parquet("hdfs://etl:9000/ingest/yellow_tripdata_2021-01.parquet")
df2 = spark.read.parquet("hdfs://etl:9000/ingest/yellow_tripdata_2021-02.parquet")

df_union = df1.union(df2)

df = df_union.select(
    df_union.tpep_pickup_datetime.cast(DateType()), 
    df_union.payment_type.cast("int"), 
    df_union.airport_fee.cast("float"),
    df_union.tolls_amount.cast("float"),
    df_union.total_amount.cast("float")
    )

df.show(5)

## creamos una vista del DF
df.createOrReplaceTempView("airport_trip_vista")

## filtramos el DF quedandonos solamente con aquellos viejes que tienen airport_fee y el pago fue con efectivo
new_df = spark.sql("select * from airport_trip_vista where airport_fee IS NOT NULL and payment_type = 2")

new_df.show(5)

## creamos una nueva vista filtrada
new_df.createOrReplaceTempView("airport_tripdata_vista_filtrada")

## insertamos el DF filtrado en la tabla tripdata_table2
hc.sql("insert into tripdata.airport_trips select * from airport_tripdata_vista_filtrada;")
