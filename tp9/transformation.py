from pyspark.sql import HiveContext
from pyspark.sql.types import DateType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession



sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
results = spark.read.option("header", "true").csv("hdfs://etl:9000/ingest/results.csv")
drivers = spark.read.option("header", "true").csv("hdfs://etl:9000/ingest/drivers.csv")
constructors = spark.read.option("header", "true").csv("hdfs://etl:9000/ingest/constructors.csv")
races = spark.read.option("header", "true").csv("hdfs://etl:9000/ingest/races.csv")

df_driver_results = results.join(drivers, results.driverId == drivers.driverId)
df_constructor_results = constructors.join(results, constructors.constructorId == results.constructorId)
races = races.drop('name', 'url')
df_constructor_results = df_constructor_results.withColumnRenamed("name", "constructors_name"). \
    join(races, df_constructor_results.raceId == races.raceId)

df_driver_results = df_driver_results.select(
    df_driver_results.forename.cast("string"), 
    df_driver_results.surname.cast("string"), 
    df_driver_results.nationality.cast("string"),
    df_driver_results.points.cast("int"),
    )

df_constructor_results = df_constructor_results.select(
    df_constructor_results.constructorRef.cast("string"), 
    df_constructor_results.constructors_name.cast("string"), 
    df_constructor_results.nationality.cast("string"),
    df_constructor_results.url.cast("string"),
    df_constructor_results.points.cast("int"),
    df_constructor_results.year.cast("int"),
    )


df_driver_results.show(5)
df_constructor_results.show(5)

## creamos una vista del DF
df_driver_results.createOrReplaceTempView("f1_driver_results_vista")
df_constructor_results.createOrReplaceTempView("f1_driver_constructors_vista")

## filtramos el DF quedandonos solamente con aquellos viejes que tienen airport_fee y el pago fue con efectivo
new_df = spark.sql("""
                   select forename as driver_forename, surname as driver_surname, nationality as driver_nationality, sum(points) as points
                   from f1_driver_results_vista
                   group by forename, surname, nationality
                   ORDER BY points DESC
                   """)

new_df_constructors = spark.sql("""
                   select constructorRef as constructorRef, constructors_name as cons_name, nationality as cons_nationality, url, sum(points) as points
                   from f1_driver_constructors_vista
                   GROUP BY constructorRef, cons_name, cons_nationality, url
                   ORDER BY points DESC
                   LIMIT 5
                   """)

new_df.show(20)
new_df_constructors.show(20)

## creamos una nueva vista filtrada
new_df.createOrReplaceTempView("f1_driver_results_vista_filtrada")
new_df_constructors.createOrReplaceTempView("f1_driver_results_constructors_vista_filtrada")


hc.sql("insert into f1.driver_results select * from f1_driver_results_vista_filtrada;")
hc.sql("insert into f1.constructor_results select * from f1_driver_results_constructors_vista_filtrada;")

