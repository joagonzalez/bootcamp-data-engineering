# Trabajo Final Bootcamp Data Engineering

Trabajo práctico final de bootcamp de Data Engineering de Escuela de Datos Vivos. A continuación, enlaces a la resolución de cada uno de los ejercicios.

- [Ejercicio 1](./Ejercicio1.md)
- [Ejercicio 2](./Ejercicio2.md)
- [Ejercicio 3](./Ejercicio3.md)

Para realizar los ejercicios se monto la infraestructura sobre un stack de docker swarm utilizando el docker-compose.yml que se adjunta a continuación.

Se agrega el contenedor de la herramienta de Business Intelligence Open-Source **Metabase** que se utilizará para generar visualizaciones pedidas en el **Ejercicio 2** utilizando Apache Hive como datasource.

**docker service ls**
```bash
ID             NAME                    MODE         REPLICAS   IMAGE                           PORTS
nv2p9t17udui   edv_etl                 replicated   1/1        joagonzalez/edvai-etl:v6        *:8010->8010/tcp, *:8080->8080/tcp, *:8088->8088/tcp, *:9000->9000/tcp, *:9870->9870/tcp, *:10000->10000/tcp, *:10002->10002/tcp
gvn5qvxnk47w   edv_metabase            replicated   1/1        metabase/metabase:latest        *:3000->3000/tcp
z338btajc5oq   edv_mongo               replicated   1/1        mongo:latest                    *:27017->27017/tcp
xmgb3lm82ez3   edv_nifi                replicated   1/1        apache/nifi:latest              *:8443->8443/tcp
lk629v8u6thl   edv_postgres            replicated   1/1        fedepineyro/edvai_postgres:v1   *:5432->5432/tcp
6ndkli3mjjcc   edv_postgres_metabase   replicated   1/1        postgres:latest 
```

**docker-compose.yml**
```yml
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
      - ./ingest_automoviles.sh:/home/hadoop/scripts/ingest_automoviles.sh:rw
      - ./transformation_1.py:/home/hadoop/scripts/transformation_1.py:rw
      - ./transformation_2.py:/home/hadoop/scripts/transformation_2.py:rw
      - ./transformation_automoviles.py:/home/hadoop/scripts/transformation_automoviles.py:rw
      - ./etl_dag.py:/home/hadoop/airflow/dags/etl_dag.py:rw
      - ./etl_dag_automoviles_padre.py:/home/hadoop/airflow/dags/etl_dag_automoviles_padre.py:rw
      - ./etl_dag_automoviles_hijo.py:/home/hadoop/airflow/dags/etl_dag_automoviles_hijo.py:rw
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

  mongo:
    image: mongo
    networks:
      - bootcamp
    ports:
      - 27017:27017
    volumes:
      - mongo-volume:/data/db

  metabase:
    image: metabase/metabase:latest
    hostname: metabase
    volumes:
    - /dev/urandom:/dev/random:ro
    ports:
      - 3000:3000
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: metabaseappdb
      MB_DB_PORT: 5432
      MB_DB_USER: metabase
      MB_DB_PASS: mysecretpassword
      MB_DB_HOST: postgres_metabase
    networks:
      - bootcamp
    healthcheck:
      test: curl --fail -I http://metabase:3000/api/health || exit 1
      interval: 15s
      timeout: 5s
      retries: 5

  postgres_metabase:
    image: postgres:latest
    hostname: postgres
    environment:
      POSTGRES_USER: metabase
      POSTGRES_DB: metabaseappdb
      POSTGRES_PASSWORD: mysecretpassword
    networks:
      - bootcamp

volumes:
  postgres-db-volume: {}
  bucket-volume: {}
  mongo-volume: {}
  
networks:
  bootcamp:
```