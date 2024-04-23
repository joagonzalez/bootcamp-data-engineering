Start container and services 
```bash
docker run -it -p 8010:8010 -p 8088:8088 -p 8080:8080  joagonzalez/edvai-etl:v6 bash
./home/hadoop/scripts/start-services.sh
hdfs dfs -ls /
```

Script
```bash
chmod a+x ingest.sh
nano ingest.sh

----
rm -f /home/hadoop/landing/*.*

wget -P /home/hadoop/landing  https://github.com/fpineyro/homework-0/blob/master/starwars.csv

/home/hadoop/hadoop/bin/hdfs dfs -rm /ingest/*.*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/*.* /ingest

rm -rf /home/hadoop/landing/starwars.csv
```

Script output
```bash
hadoop@e44730aa1746:~/scripts$ ./ingest.sh 
--2024-04-15 22:52:51--  https://github.com/fpineyro/homework-0/blob/master/starwars.csv
Resolving github.com (github.com)... 20.201.28.151
Connecting to github.com (github.com)|20.201.28.151|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: unspecified [text/html]
Saving to: '/home/hadoop/landing/starwars.csv'

starwars.csv                             [ <=>                                                                 ] 151.18K   840KB/s    in 0.2s    

2024-04-15 22:52:52 (840 KB/s) - '/home/hadoop/landing/starwars.csv' saved [154805]

Deleted /ingest/starwars.csv
hadoop@e44730aa1746:~/scripts$ ls ../landing/
hadoop@e44730aa1746:~/scripts$ hdfs dfs -ls /ingest
Found 1 items
-rw-r--r--   1 hadoop supergroup     154805 2024-04-15 22:52 /ingest/starwars.csv

```

sqoop
```bas
sqoop list-databases --connect jdbc:postgresql://postgres/northwind --username postgres -P
```