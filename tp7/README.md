# Data Ingestion: 
Scoop and Apache Nifi


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
    ports:
      - 8010:8010
      - 8088:8088 
      - 8080:8080
      - 9000:9000
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

### PySpark ETL

Dentro del contenedro etl

```bash
docker exec -it edv_etl bash
cd /home/hadoop/scripts
./start-services.sh

# una vez que los servicios se encuentran corriendo
pyspark
df = spark.read.option("header", "true").csv("/ingest/yellow_tripdata_2021-01.csv")
df.head()

# crear vista
df.createOrReplaceTempView("vtripdata")

# ahora se pueden realizar queries SQL sobre el DF
df_transform = spark.sql("select * from vtripdata where fare_amount > 10")
 
# select para filtrar columnas
df_2 = df.select(df.forename, df.surname, df.nationality, df.points.cast("int"))
```

### Hive
```bash
su hadoop
hive
show tables;
use tripdata_table;
# change location to supported format by docker-compose
hive> describe formatted tripdata_table;
hive> alter table tripdata_table set location 'hdfs://localhost:9000/user/hive/warehouse/tables';
# qry
hive> select * from tripdata_table limit 10;
OK
Time taken: 0.103 seconds
```
