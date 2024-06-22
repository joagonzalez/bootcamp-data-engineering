from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator



args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl_automoviles_ingest_padre',
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
        bash_command="ssh -o StrictHostKeyChecking=no hadoop@etl 'bash /home/hadoop/scripts/ingest_automoviles.sh'",
    )
    
    with TaskGroup("transform_load") as transform_load:
        etl_automoviles_load_hijo = TriggerDagRunOperator(
            task_id="etl_automoviles_load_hijo",
            trigger_dag_id="etl_automoviles_load_hijo",
    )
    start >> ingest >> transform_load >> end




if __name__ == "__main__":
    dag.cli()
