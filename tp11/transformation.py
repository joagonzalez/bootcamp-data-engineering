from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.functions import when, avg
from pyspark.sql.session import SparkSession



sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

## leemos archivos parquet desde HDFS y se cargan en dataframes
df = spark.read.option("header", "true").csv("hdfs://etl:9000/nifi/titanic.csv")

df = df.drop('SibSp', 'Parch')
df = df.fillna({'Cabin': 0})

# calculamos promedios
avg_age_women = df.filter(df['Sex'] == 'female').agg(avg('Age')).first()[0]
avg_age_men = df.filter(df['Sex'] == 'male').agg(avg('Age')).first()[0]

# agregamos nueva columna con el promedio de edad por genero
df = df.withColumn('avg_age_by_gender', when(df['Sex'] == 'female', avg_age_women).otherwise(avg_age_men))

df = df.select(
    df.PassengerId.cast("int"), 
    df.Survived.cast("int"),
    df.Pclass.cast("int"),
    df.Name.cast("string"),
    df.Sex.cast("string"),
    df.Age.cast("int"),
    df.Ticket.cast("string"),
    df.Fare.cast("float"),
    df.Cabin.cast("string"),
    df.Embarked.cast("string"),
    df.avg_age_by_gender.cast("float")
    )

df.show(5)

## creamos una nueva vista filtrada
df.createOrReplaceTempView("filtered_titanic")

## insertamos el DF filtrado en la tabla edv.titanic
hc.sql("insert into edv.titanic select * from filtered_titanic;")