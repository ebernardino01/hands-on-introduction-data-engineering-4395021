''' Extract DAG '''
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='extract_dag',
    description='An extraction Airflow DAG',
    start_date=datetime(2023, 8, 2),
    schedule_interval=None,
    catchup=False
) as dag:
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-extract-data.csv',
        dag=dag
    )
