# ingest.sh
## download flights and airports datasets
wget -nc -O /home/hadoop/landing/vuelos_2021.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv
wget -nc -O /home/hadoop/landing/vuelos_2022.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv
wget -nc -O /home/hadoop/landing/aeropuertos.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv

# ingest @hadoop
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/vuelos_2021.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/vuelos_2022.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/aeropuertos.csv /ingest


# check
/home/hadoop/hadoop/bin/hdfs  dfs -ls /ingest