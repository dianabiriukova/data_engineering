from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os


RAW_SALES_PATH = "/opt/airflow/dags/data/raw/sales"
BRONZE_SALES_PATH = "/opt/airflow/dags/data/bronze/sales"
SILVER_SALES_PATH = "/opt/airflow/dags/data/silver/sales"

def process_sales_raw_to_bronze(**kwargs):
    spark = SparkSession.builder.appName("SalesPipeline").master("local[*]").getOrCreate()

    execution_date = kwargs['ds']
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    execution_date = f"{date_obj.year}-{str(date_obj.month).zfill(2)}-{date_obj.day}"

    daily_sales_path = os.path.join(RAW_SALES_PATH, execution_date, f"{execution_date}__sales.csv")

    df = spark.read.csv(daily_sales_path, header=True, inferSchema=True)
    bronze_path = os.path.join(BRONZE_SALES_PATH, f"{execution_date}")
    df.write.format("parquet").mode("overwrite").save(bronze_path)


def process_sales_bronze_to_silver(**kwargs):
    spark = SparkSession.builder.appName("SalesPipeline").master("local[*]").getOrCreate()

    execution_date = kwargs['ds']
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    execution_date = f"{date_obj.year}-{str(date_obj.month).zfill(2)}-{date_obj.day}"

    bronze_path = os.path.join(BRONZE_SALES_PATH, f"{execution_date}")

    df = spark.read.parquet(bronze_path)
    df = df.withColumnRenamed("CustomerId", "client_id") \
        .withColumnRenamed("PurchaseDate", "purchase_date") \
        .withColumnRenamed("Product", "product_name") \
        .withColumnRenamed("Price", "price")

    silver_path = os.path.join(SILVER_SALES_PATH, f"{execution_date}")
    df.write.format("parquet").mode("overwrite").save(silver_path)


default_args = {
    "start_date": datetime(2022, 9, 1),
    "end_date": datetime(2022, 9, 30),
    "catchup": True,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "process_sales",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
) as dag:
    task1 = PythonOperator(
        task_id="raw_to_bronze",
        python_callable=process_sales_raw_to_bronze,
        provide_context=True,
    )
    task2 = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=process_sales_bronze_to_silver,
        provide_context=True,
    )

task1 >> task2