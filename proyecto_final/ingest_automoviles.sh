# ingest.sh
## download flights and airports datasets
wget -nc -O /home/hadoop/landing/automoviles_rental.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv
wget -nc -O /home/hadoop/landing/automoviles_georef.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv


# ingest @hadoop
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/automoviles_rental.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/automoviles_georef.csv /ingest

# check
/home/hadoop/hadoop/bin/hdfs  dfs -ls /ingest