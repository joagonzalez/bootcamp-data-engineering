# Data Pipeline Orchestration: 
Apache Airflow Spark and Hive


### Infraestructura
Se prepara un stack con docker-compose sobre docker swarm con un red interna para poder referenciar las conexiones entre containers utilizando el nombre de servicio en vez de IPs. El docker-compose utilizado se adjunta a continuación asi como el comando de deploy y servicios.

```bash
---
version: '3'
services:
  postgres:
    image: fedepineyro/edvai_postgres:v1
    environment:
      POSTGRES_PASSWORD: edvai
      POSTGRES_USER: postgres
      POSTGRES_DB: northwind
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    ports:
      - 5432:5432
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "northwind"]
      interval: 5s
      retries: 5
    restart: always
    networks:
      - bootcamp

  etl:
    image: joagonzalez/edvai-etl:v6
    command: tail -f /dev/null  # chmod a+x /home/hadoop/scripts/start-services.sh && ./home/hadoop/scripts/start-services.sh 
    volumes:
      - ./core-site.xml:/home/hadoop/hadoop/etc/hadoop/core-site.xml
      - ./start-services.sh:/home/hadoop/scripts/start-services.sh
      - ./ingest.sh:/home/hadoop/scripts/ingest.sh:rw
      - ./transformation_example.py:/home/hadoop/scripts/transformation_example.py:rw
      - ./transformation.py:/home/hadoop/scripts/transformation.py:rw
      - ./etl_dag.py:/home/hadoop/airflow/dags/etl_dag.py:rw
    ports:
      - 8010:8010 # airflow
      - 8088:8088 # hadoop infra UI
      - 9000:9000 # hadoop
      - 9870:9870 # hadoop file system UI
      - 8080:8080 # spark UI
      - 10000:10000 # hive
      - 10002:10002 # hive UI

    networks:
      - bootcamp

  nifi:
    image: apache/nifi
    networks:
      - bootcamp
    ports:
      - 8443:8443
    volumes:
      # - ./ingest.sh:/home/nifi/ingest/ingest.sh
      - ./nifi:/home/nifi/hadoop:ro
      - ./:/home/nifi/ingest:rw
      - bucket-volume:/home/nifi/bucket
    environment:
      - NIFI_WEB_HTTP_PORT=8443
      - NIFI_WEB_HTTP_HOST=0.0.0.0
      - NIFI_WEB_PROXY_CONTEXT_PATH=/

volumes:
  postgres-db-volume: {}
  bucket-volume: {}
  
networks:
  bootcamp:
```

```bash
# deploy stack 
docker stack deploy -c docker-compose.yml edv

# docker service ls
ID             NAME           MODE         REPLICAS   IMAGE                           PORTS
ru993dyd8cs8   edv_etl        replicated   1/1        joagonzalez/edvai-etl:v6        *:8010->8010/tcp, *:8080->8080/tcp, *:8088->8088/tcp
vied85rfpusz   edv_nifi       replicated   1/1        apache/nifi:latest              *:8443->8443/tcp
1epql2usua91   edv_postgres   replicated   1/1        fedepineyro/edvai_postgres:v1   *:5432->5432/tcp

```

### Apache Hive
1) En Hive, crear la siguiente tabla (externa) en la base de datos tripdata:

airport_trips
- tpep_pickup_datetetime
- airport_fee
- payment_type
- tolls_amount
- total_amount

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS tripdata.airport_trips( 
  tpep_pickup_datetetime TIMESTAMP,
  payment_type integer,
  airport_fee float,
  tolls_amount float,
  total_amount float
)
LOCATION 'hdfs://etl:9000/user/hive/warehouse/tables/airport';
```

2) En Hive, mostrar el esquema de airport_trips

<img src="airport_describe.png" />

3) Crear un archivo .bash que permita descargar los archivos mencionados abajo e ingestarlos en HDFS.

- Yellow_tripdata_2021-01.parquet
- Yellow_tripdata_2021-02.parquet


```bash
## download yellow_tripdata_2021 dataset @landing zone
wget -nc -O /home/hadoop/landing/yellow_tripdata_2021-01.parquet https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet
wget -nc -O /home/hadoop/landing/yellow_tripdata_2021-02.parquet https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-02.parquet

whoami
pwd

# ingest @hadoop
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-02.parquet /ingest

# check
/home/hadoop/hadoop/bin/hdfs  dfs -ls /ingest

```

4) Crear un archivo .py que permita, mediante Spark, crear un data frame uniendo los viajes del mes 01 y mes 02 del año 2021 y luego Insertar en la tabla airport_trips los viajes que tuvieron como inicio o destino aeropuertos, que hayan pagado con dinero.

Adaptamos el script y trabajamos en **transformation.py**

```python
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
```

```bash
/home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation.py
```

<img src="transformation_script.png" />



5) Realizar un proceso automático en Airflow que orqueste los archivos creados en los puntos 3 y 4. Correrlo y mostrar una captura de pantalla (del DAG y del resultado en la base de datos)

```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:


    start = DummyOperator(
        task_id='Start_Pipeline',
    )
    
    end = DummyOperator(
        task_id='End_Pipeline',
    )


    ingest = BashOperator(
        task_id='ingest',
        bash_command="ssh -o StrictHostKeyChecking=no hadoop@etl 'bash /home/hadoop/scripts/ingest.sh'",
    )


    transform = BashOperator(
        task_id='transform',
        bash_command='ssh -o StrictHostKeyChecking=no hadoop@etl /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation.py ',
    )


    start >> ingest >> transform >> end
```

<img src="graph_dag.png" />

<img src="ingest_task_dag.png" />

<img src="transform_task_dag.png" />

<img src="final_result_dag.png" />
