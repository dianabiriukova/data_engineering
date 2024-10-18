from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

def some_task_function():
    print('Hello from airflow')

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
        dag_id='process_sales',
        start_date=datetime(2024, 10, 17),
        # end_date=datetime(2023, 1, 15),
        schedule_interval="@daily",
        catchup=False,
        default_args=DEFAULT_ARGS,
        # tags=['Sales', 'R_D', 'Test']
) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=some_task_function,
    )

    task1