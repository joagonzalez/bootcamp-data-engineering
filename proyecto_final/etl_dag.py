from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup



args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:


    start = DummyOperator(
        task_id='comienza_proceso',
    )
    
    end = DummyOperator(
        task_id='finaliza_proceso',
    )
    
    ingest = BashOperator(
        task_id='ingest',
        bash_command="ssh -o StrictHostKeyChecking=no hadoop@etl 'bash /home/hadoop/scripts/ingest.sh'",
    )
    
    with TaskGroup("transform_load") as transform_load:
        transform_1 = BashOperator(
            task_id='transform_vuelos',
            bash_command='ssh -o StrictHostKeyChecking=no hadoop@etl /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation_1.py',
        )

        transform_2 = BashOperator(
            task_id='transform_aeropuertos',
            bash_command='ssh -o StrictHostKeyChecking=no hadoop@etl /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation_2.py',
        )
    
    start >> ingest >> transform_load >> end




if __name__ == "__main__":
    dag.cli()
