from pyspark.sql import HiveContext
from pyspark.sql.functions import col
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_unixtime, to_date



sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
envios = spark.read.parquet("hdfs://etl:9000/sqoop/ingest/envios")
order_details = spark.read.parquet("hdfs://etl:9000/sqoop/ingest/order_details")

joined_df = envios.join(order_details, envios.order_id == order_details.order_id)
joined_df = joined_df.drop(order_details.order_id)
joined_df = joined_df.withColumn("unit_price_discount", col("unit_price") - col("discount"))
joined_df = joined_df.withColumn("total_price", col("unit_price_discount") * col("quantity"))

joined_df = joined_df.withColumn("shipped_date", to_date(from_unixtime(col("shipped_date"))))

joined_df = joined_df.select(
    joined_df.order_id.cast("int"), 
    joined_df.shipped_date, 
    joined_df.company_name.cast("string"),
    joined_df.phone.cast("string"),
    joined_df.unit_price_discount.cast("float"),
    joined_df.quantity.cast("int"),
    joined_df.total_price.cast("float"),
    joined_df.discount.cast("float") 
    )

joined_df.show(5)

## creamos una vista del DF
joined_df.createOrReplaceTempView("filter")

new_df = spark.sql("select order_id, shipped_date, company_name, phone, unit_price_discount, quantity, total_price from filter where discount != 0")

new_df.show(5)

## creamos una nueva vista filtrada
new_df.createOrReplaceTempView("filtered_final")

## insertamos el DF filtrado en la tabla tripdata_table2
hc.sql("insert into northwind_analytics.products_sent select * from filtered_final;")