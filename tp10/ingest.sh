## download f1 dataset @landing zone


wget -nc -O /home/hadoop/landing/results.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/results.csv
wget -nc -O /home/hadoop/landing/drivers.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/drivers.csv
wget -nc -O /home/hadoop/landing/constructors.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/constructors.csv
wget -nc -O /home/hadoop/landing/races.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/races.csv

# ingest @hadoop
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/results.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/races.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/drivers.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/constructors.csv /ingest

# check
/home/hadoop/hadoop/bin/hdfs  dfs -ls /ingest