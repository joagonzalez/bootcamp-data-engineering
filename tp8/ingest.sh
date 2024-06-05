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