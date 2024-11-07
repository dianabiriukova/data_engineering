import os
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
AUTH_TOKEN = os.getenv("AUTH_TOKEN")


URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
DATES = ['2022-08-09', '2022-08-11']  # Список нужных дат
PAGE = 1
bucket_name = "news-data-2"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 7),
}

dag = DAG(
    'gcs_sales_upload_dag',
    default_args=default_args,
    schedule_interval=None,  # Установите по расписанию, если нужно
)


def get_sales_data(date, page, auth_token):
    response = requests.get(
        url=URL,
        params={'date': date, 'page': page},
        headers={'Authorization': auth_token},
    )
    if response.status_code == 200:
        return response.text  # Возвращаем данные в формате CSV
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


def save_data_locally(data, file_path):
    with open(file_path, "w") as file:
        file.write(data)


def upload_to_gcs(local_file_path, bucket_name, file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.upload_from_filename(local_file_path)
    print(f"File uploaded to {file_path} in bucket {bucket_name}")


def process_data_for_dates():
    for date in DATES:
        local_file_path = f"/tmp/sales_{date}.csv"
        file_path = f"src1/sales/v1/{date[:4]}/{date[5:7]}/{date[8:]}/sales.csv"

        data = get_sales_data(date, PAGE, AUTH_TOKEN)
        print(f"Data for {date}:", data)

        save_data_locally(data, local_file_path)

        upload_to_gcs(local_file_path, bucket_name, file_path)



process_data_task = PythonOperator(
    task_id='process_data_for_dates',
    python_callable=process_data_for_dates,
    dag=dag
)

process_data_task
