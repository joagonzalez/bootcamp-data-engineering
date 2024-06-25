# Efectores y derivaciones

### Solución

1) Hacer ingest y transofrmación de los siguientes files relacionados con Derivaciones y efectores de saludo de la provincia de Buenos Aires.

```bash
hadoop@fbb56750a21f:~$ ls salud/
derivaciones.csv  efectores.csv
```

2) Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022 (2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle de los aeropuertos (aeropuertos_detalle.csv)

```sql
CREATE DATABASE salud;

drop table salud.efectores;
drop table salud.derivaciones;

CREATE TABLE salud.derivaciones (
    anio_evento INT,
    efector_derivador STRING,
    efector_de_atencion STRING,
    efector_a_derivar STRING,
    area_servicio STRING,
    apellido STRING,
    nombre STRING,
    dni STRING,
    fecha_nacimiento DATE,
    sexo_genero STRING,
    calle STRING,
    altura INT,
    barrio_localidad STRING,
    partido STRING,
    provincia STRING,
    latitud STRING,
    longitud STRING,
    fecha_ingreso DATE,
    fecha_egreso DATE,
    diagnostico STRING,
    cie11 STRING,
    tipo_patologia STRING,
    clinico_o_quirurgico STRING,
    enfermedad_poco_frecuente STRING,
    congenito STRING,
    obra_social STRING,
    observaciones STRING
);

CREATE TABLE IF NOT EXISTS salud.efectores (
  id STRING,
  nombre_efector STRING,
  institucion STRING,
  localidad STRING,
  partido STRING,
  provincia STRING,
  region_sanitaria STRING,
  ubicacion_relacional STRING,
  nivel_de_atencion STRING,
  gestion STRING,
  jurisdiccion STRING,
  longitud STRING,
  latitud STRING
);
```


3) Realizar un proceso automático orquestado por airflow que ingeste los archivos previamente mencionados entre las fechas 01/01/2021 y 30/06/2022 en las dos tablas creadas.


**DAG**
```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup



args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl_salud',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:


    start = DummyOperator(
        task_id='comienza_proceso',
    )
    
    end = DummyOperator(
        task_id='finaliza_proceso',
    )
    
    # ingest = BashOperator(
    #     task_id='ingest',
    #     bash_command="ssh -o StrictHostKeyChecking=no hadoop@etl 'bash /home/hadoop/scripts/ingest.sh'",
    # )
    
    with TaskGroup("transform_load") as transform_load:
        transform_1 = BashOperator(
            task_id='transform_derivaciones',
            bash_command='ssh -o StrictHostKeyChecking=no hadoop@etl /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation_salud_derivaciones.py',
        )
        
        transform_2 = BashOperator(
            task_id='transform_efectores',
            bash_command='ssh -o StrictHostKeyChecking=no hadoop@etl /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation_salud_efectores.py',
        )

    start >> transform_load >> end




if __name__ == "__main__":
    dag.cli()
```

4) Realizar las siguiente transformaciones en los pipelines de datos:

**transformation_salud_derivaciones.py**
```python
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

derivaciones.show(5)
derivaciones.printSchema()

## creamos una nueva vista filtrada
derivaciones.createOrReplaceTempView("filtered_derivaciones")

## insertamos el DF filtrado en las tablas de Hive
hc.sql("insert into salud.derivaciones select * from filtered_derivaciones;")
```

**transformation_salud_efectores.py**
```python
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

```

Llamada al job desde Airflow
```bash
/home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation_salud_efectores.py

/home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation_salud_derivaciones.py
```

5) Queries:

Contador por tipo de patología
```sql
SELECT
  tipo_patologia,
  count(tipo_patologia) AS contador
FROM
  derivaciones
GROUP BY
  tipo_patologia
ORDER BY
  contador
DESC
```

Derivación de segunda instancia
```sql
SELECT
  anio_evento,
  CASE
    WHEN efector_de_atencion IS NULL THEN 'SIN_DATO'
    ELSE efector_de_atencion
  END AS efector_de_atencion,
  CASE
    WHEN efector_a_derivar IS NULL THEN 'SIN_DATO'
    ELSE efector_a_derivar
  END AS efector_a_derivar,
  count(*) AS cantidad
FROM
  derivaciones
GROUP BY
  anio_evento,
  efector_de_atencion,
  efector_a_derivar
ORDER BY
  anio_evento, cantidad DESC
```

Derivaciones derivador atencion por año
```sql
SELECT
  anio_evento,
  CASE
    WHEN efector_derivador IS NULL THEN 'SIN_DATO'
    ELSE efector_derivador
  END AS efector_derivador,
  efector_de_atencion,
  count(*) AS cantidad
FROM
  derivaciones
GROUP BY
  anio_evento,
  efector_derivador,
  efector_de_atencion
ORDER BY
  anio_evento, cantidad DESC
```

Ubicacion relacional por año
```sql
SELECT
  anio_evento,
  ubicacion_relacional,
  count(ubicacion_relacional) as cantidad
FROM
  (
    derivaciones AS d
    INNER JOIN efectores AS e ON d.efector_derivador = e.nombre_efector
  )
GROUP BY
  ubicacion_relacional,
  anio_evento
```
