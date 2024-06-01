##Start SSH service
service ssh start

##Start Posgres service
service postgresql start

##Load Hadoop services
runuser -l hadoop -c '/home/hadoop/hadoop/sbin/start-dfs.sh'
runuser -l hadoop -c '/home/hadoop/hadoop/sbin/start-yarn.sh'



##Start Spark services
runuser -l hadoop -c '/home/hadoop/spark/sbin/start-master.sh'
runuser -l hadoop -c '/home/hadoop/spark/sbin/start-worker.sh spark://etl:7077'


#start Hive metastore and server
runuser -l hadoop -c 'nohup /home/hadoop/hive/bin/hiveserver2 &'
runuser -l hadoop -c 'nohup /home/hadoop/hive/bin/hive --service metastore &'


##Kill Airflow pid in case was not closed
#cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill -9
#cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill -9
rm /home/hadoop/airflow/airflow-webserver.pid
rm /home/hadoop/airflow/airflow-scheduler.pid
rm /home/hadoop/airflow/airflow-webserver-monitor.pid


##Start Airflow services
runuser -l hadoop -c '/home/hadoop/.local/bin/airflow webserver -p 8010 -D'
runuser -l hadoop -c '/home/hadoop/.local/bin/airflow scheduler -D'

##Change user
su hadoop
