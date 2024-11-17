import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

BASE_DIR = os.environ.get("BASE_DIR")

if not BASE_DIR:
    raise EnvironmentError("BASE_DIR environment variable must be set")

JOB1_PORT = 8081
JOB2_PORT = 8082

def run_job1(execution_date, raw_dir):
    print("Starting job1:")
    resp = requests.post(
        url=f'http://localhost:{JOB1_PORT}/',
        json={"date": execution_date, "raw_dir": raw_dir},
        timeout=30
    )
    assert resp.status_code in [200, 201]
    print("job1 completed!")


def run_job2(raw_dir, stg_dir):
    print("Starting job2:")
    resp = requests.post(
        url=f'http://localhost:{JOB2_PORT}/',
        json={"raw_dir": raw_dir, "stg_dir": stg_dir}
    )
    assert resp.status_code in [200, 201]
    print("job2 completed!")


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
        dag_id='process_sales',
        start_date=datetime(2024, 10, 17),
        schedule_interval="0 1 * * *",
        catchup=True,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['sales'],
) as dag:
    for i in range(3):
        execution_date = (datetime(2022, 8, 9) + timedelta(days=i)).date().strftime('%Y-%m-%d')

        RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", execution_date)
        STG_DIR = os.path.join(BASE_DIR, "stg", "sales", execution_date)

        with TaskGroup(group_id=f'process_sales_{execution_date}') as group:
            task1 = PythonOperator(
                task_id='extract_data_from_api',
                python_callable=run_job1,
                op_kwargs={'execution_date': execution_date, 'raw_dir': RAW_DIR}
            )

            task2 = PythonOperator(
                task_id='convert_to_avro',
                python_callable=run_job2,
                op_kwargs = {'raw_dir': RAW_DIR, 'stg_dir': STG_DIR}
            )

            task1 >> task2