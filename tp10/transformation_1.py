from pyspark.sql import HiveContext
from pyspark.sql.types import DateType
from pyspark.context import SparkContext
from pyspark.sql.functions import col, avg
from pyspark.sql.session import SparkSession



sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
df = spark.read.parquet("hdfs://etl:9000/sqoop/ingest/clientes/")
df = df.withColumn("quantity", col("quantity").cast("int"))
avgQuantity = df.agg(avg("quantity")).first()[0]

print(f'Average quantity is: {avgQuantity}')

result = df.filter(col("quantity") > avgQuantity)

result.show(5)

## creamos una nueva vista filtrada
result.createOrReplaceTempView("quantity")

## insertamos el DF filtrado en la tabla tripdata_table2
hc.sql("insert into northwind_analytics.products_sold select * from quantity;")