from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


RAW_SALES_PATH = "/opt/airflow/dags/data/raw/customers"
BRONZE_SALES_PATH = "/opt/airflow/dags/data/bronze/customers"
SILVER_SALES_PATH = "/opt/airflow/dags/data/silver/customers"

def process_customers_raw_to_bronze(**kwargs):
    spark = SparkSession.builder.appName("CustomersPipeline").master("local[*]").getOrCreate()

    latest_folder = "2022-08-5"
    folder_path = f"{RAW_SALES_PATH}/{latest_folder}"

    df = spark.read.csv(f"{folder_path}/*.csv", header=True, inferSchema=True)

    bronze_path = f"{BRONZE_SALES_PATH}/{latest_folder}"
    df.write.format("parquet").mode("overwrite").save(bronze_path)
    print(f"File writed in {bronze_path} successfully!")

def process_customers_bronze_to_silver(**kwargs):
    spark = SparkSession.builder.appName("CustomersPipeline").master("local[*]").getOrCreate()

    latest_folder = "2022-08-5"
    bronze_path = f"{BRONZE_SALES_PATH}/{latest_folder}"
    df = spark.read.parquet(bronze_path)

    silver_df = (
        df.select(
            col("Id").alias("client_id"),
            col("FirstName").alias("first_name"),
            col("LastName").alias("last_name"),
            col("Email").alias("email"),
            col("RegistrationDate").alias("registration_date"),
            col("State").alias("state")
        )
    )

    silver_path = f"{SILVER_SALES_PATH}/{latest_folder}"
    silver_df.write.format("parquet").mode("overwrite").save(silver_path)
    print(f"File writed in {silver_path} successfully!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "process_customers",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task_raw_to_bronze = PythonOperator(
        task_id="raw_to_bronze",
        python_callable=process_customers_raw_to_bronze,
    )

    task_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=process_customers_bronze_to_silver,
    )

    task_raw_to_bronze >> task_bronze_to_silver