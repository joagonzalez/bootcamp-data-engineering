# Data Ingestion: 
Scoop and Apache Nifi

### sqoop
```bash
sqoop list-databases --connect jdbc:postgresql://postgres/northwind --username postgres -P
```

### apache nifi
```bash
docker run -it -p 8443:8443 apache/nifi:latest bash
```

There are known issues trying to run apache nifi on swarm single node. Will try to create a single stack with everything running.
