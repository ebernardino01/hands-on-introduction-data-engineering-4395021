# One Task DAG
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2023, 8, 2),
}

with DAG(
    dag_id='one_task_dag',
    description='A one task Airflow DAG',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='t1',
        bash_command='echo "Hello, LinkedIn Learning" > /workspaces/hands-on-introduction-data-engineering-4395021/lab/temp/create-this-file.txt',
        dag=dag
    )
