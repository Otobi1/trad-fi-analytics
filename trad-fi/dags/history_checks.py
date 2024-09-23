from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
}

with DAG(dag_id='dbt_run_dag', default_args=default_args, schedule_interval='@daily') as dag:
    
    # Airflow task to run DBT command
    run_dbt_task = BashOperator(
        task_id='run_dbt',
        bash_command='cd /path/to/dbt/project && dbt run'
    )
    
    run_dbt_task
