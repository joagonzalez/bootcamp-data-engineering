from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import round


sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
df_rental = spark.read.option("header", "true").option("delimiter", ",").csv("hdfs://etl:9000/ingest/automoviles_rental.csv")
df_georef = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://etl:9000/ingest/automoviles_georef.csv")

df_rental.show(5)
df_rental.printSchema()

df_georef.show(5)
df_georef.printSchema()

# Renombrar y normalizar nombres de columnas
df_rental = df_rental.withColumnRenamed("fuelType", "fueltype")
df_rental = df_rental.withColumnRenamed("renterTripsTaken", "rentertripstaken")
df_rental = df_rental.withColumnRenamed("reviewCount", "reviewcount")
df_rental = df_rental.withColumnRenamed("location.city", "city")
df_rental = df_rental.withColumnRenamed("location.country", "country")
df_rental = df_rental.withColumnRenamed("location.latitude", "location_latitude")
df_rental = df_rental.withColumnRenamed("location.longitude", "location_longitude")
df_rental = df_rental.withColumnRenamed("location.state", "state_name")
df_rental = df_rental.withColumnRenamed("owner.id", "owner_id")
df_rental = df_rental.withColumnRenamed("rate.daily", "rate_daily")
df_rental = df_rental.withColumnRenamed("vehicle.make", "make")
df_rental = df_rental.withColumnRenamed("vehicle.model", "model")
df_rental = df_rental.withColumnRenamed("vehicle.type", "type")
df_rental = df_rental.withColumnRenamed("vehicle.year", "year")

df_georef = df_georef.withColumnRenamed("Geo Point", "geo_point")
df_georef = df_georef.withColumnRenamed("Geo Shape", "geo_shape")
df_georef = df_georef.withColumnRenamed("Year", "year_georef")
df_georef = df_georef.withColumnRenamed("Official Code State", "official_code_state")
df_georef = df_georef.withColumnRenamed("Official Name State", "official_name_state")
df_georef = df_georef.withColumnRenamed("Iso 3166-3 Area Code", "iso_3166_3_area_code")
df_georef = df_georef.withColumnRenamed("Type", "type")
df_georef = df_georef.withColumnRenamed("United States Postal Service state abbreviation", "usps_state_abbreviation")
df_georef = df_georef.withColumnRenamed("State FIPS Code", "state_fips_code")
df_georef = df_georef.withColumnRenamed("State GNIS Code", "state_gnis_code")


# castear y normalizar tipos de datos
df_rental = df_rental.withColumn("rating", col("rating").cast("float"))
df_rental = df_rental.withColumn("rating", round("rating", 1).cast("int"))

df_rental = df_rental.withColumn("rentertripstaken", col("rentertripstaken").cast("int"))
df_rental = df_rental.withColumn("reviewcount", col("reviewcount").cast("int"))
df_rental = df_rental.withColumn("rate_daily", col("rate_daily").cast("int"))
df_rental = df_rental.withColumn("year", col("year").cast("int"))


df_rental.show(5)
df_rental.printSchema()

df_georef.show(5)
df_georef.printSchema()

df_joined = df_rental.join(df_georef, df_rental["state_name"] == df_georef["usps_state_abbreviation"], 'inner')

df_joined.show(5)
df_joined.printSchema()

df_filtered = df_joined.filter(df_joined["rating"] != 0)
df_filtered = df_filtered.filter(df_filtered["state_name"] != "TX")

# dropeamos columnas que no estan en la tabla de hive
df_filtered = df_filtered.drop("country")
df_filtered = df_filtered.drop("location_latitude")
df_filtered = df_filtered.drop("location_longitude")
df_filtered = df_filtered.drop("type")
df_filtered = df_filtered.drop("geo_point")
df_filtered = df_filtered.drop("geo_shape")
df_filtered = df_filtered.drop("official_code_state")
df_filtered = df_filtered.drop("official_name_state")
df_filtered = df_filtered.drop("iso_3166_3_area_code")
df_filtered = df_filtered.drop("usps_state_abbreviation")
df_filtered = df_filtered.drop("state_fips_code")
df_filtered = df_filtered.drop("state_gnis_code")
df_filtered = df_filtered.drop("year_georef")

## creamos una nueva vista filtrada
df_filtered.createOrReplaceTempView("df_rental_filtered")

## insertamos el DF filtrado en las tablas de Hive
hc.sql("insert into car_rental_db.car_rental_analytics select * from df_rental_filtered;")