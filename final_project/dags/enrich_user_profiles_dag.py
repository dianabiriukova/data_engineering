from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType


SILVER_CUSTOMERS_PATH = "/opt/airflow/dags/data/silver/customers/2022-08-5"
SILVER_USER_PROFILES_PATH = "/opt/airflow/dags/data/silver/user_profiles"
GOLD_USER_PROFILES_ENRICHED_PATH = "/opt/airflow/dags/data/gold/user_profiles_enriched"

customers_schema = StructType([
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("registration_date", TimestampType(), True)
])

user_profiles_schema = StructType([
    StructField("email", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("birth_date", DateType(), True),
    StructField("phone_number", StringType(), True)
])


def enrich_user_profiles(**kwargs):
    spark = SparkSession.builder \
        .appName("EnrichUserProfiles") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()


    customers_df = spark.read.schema(customers_schema).parquet(SILVER_CUSTOMERS_PATH)
    user_profiles_df = spark.read.schema(user_profiles_schema).parquet(SILVER_USER_PROFILES_PATH)
    customers_df.printSchema()

    print(f"Customers count: {customers_df.count()}")
    print(f"User profiles count: {user_profiles_df.count()}")

    enriched_df = customers_df.join(
        user_profiles_df,
        customers_df.email == user_profiles_df.email,
        "left"
    ).select(
        customers_df["*"],
        user_profiles_df["full_name"],
        user_profiles_df["state"],
        user_profiles_df["phone_number"]
    )

    if enriched_df.count() > 0:
        enriched_df.write.format("parquet").mode("overwrite").save(GOLD_USER_PROFILES_ENRICHED_PATH)
        print(f"Enriched data written to {GOLD_USER_PROFILES_ENRICHED_PATH} successfully!")
    else:
        print("No data to write to gold level.")

    spark.stop()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
        'enrich_user_profiles',
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:

    enrich_user_profiles_task = PythonOperator(
        task_id='enrich_user_profiles_task',
        python_callable=enrich_user_profiles,
    )

enrich_user_profiles_task