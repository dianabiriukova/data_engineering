from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


RAW_USER_PROFILES_PATH = "/opt/airflow/dags/data/raw/user_profiles/user_profiles.json"
SILVER_USER_PROFILES_PATH = "/opt/airflow/dags/data/silver/user_profiles"

profile_schema = StructType([
    StructField("email", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("phone_number", StringType(), True)
])

def process_user_profiles(**kwargs):
    spark = SparkSession.builder.appName("ProcessUserPipeline").master("local[*]").getOrCreate()

    profiles_df = spark.read.schema(profile_schema).json(RAW_USER_PROFILES_PATH)
    print(f"Dataframe count: {profiles_df.count()}")

    profiles_df.write.format("parquet").mode("overwrite").save(SILVER_USER_PROFILES_PATH)
    print(f"File writed in {SILVER_USER_PROFILES_PATH} successfully!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    'process_user_profiles',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    process_user_profiles_task = PythonOperator(
        task_id='process_user_profiles_task',
        python_callable=process_user_profiles,
    )

process_user_profiles_task
