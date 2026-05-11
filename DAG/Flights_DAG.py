from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# --------------------------------------
# Default Arguments
default_args = {
    "owner": "ahmed-refat",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------------------------
# DAG Definition
with DAG(
    dag_id="nyc_flights_data_warehouse",
    default_args=default_args,
    description="NYC Flights Data Warehouse Pipeline",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["nyc", "flights", "data-warehouse"],
) as dag:

    # Task 1: رفع الداتا على S3 Bronze لو مش موجودة
    upload_bronze = BashOperator(
        task_id="upload_bronze",
        bash_command="python /home/ahmed-refat/dags/scripts/01_upload_bronze.py",
    )

    # Task 2 و 3 و 4: تنظيف الـ 3 datasets بالتوازي
    process_flights = BashOperator(
        task_id="process_flights",
        bash_command="python /home/ahmed-refat/dags/scripts/02_process_flights.py",
    )

    process_weather = BashOperator(
        task_id="process_weather",
        bash_command="python /home/ahmed-refat/dags/scripts/03_process_weather.py",
    )

    process_airports = BashOperator(
        task_id="process_airports",
        bash_command="python /home/ahmed-refat/dags/scripts/04_process_airports.py",
    )

    # Task 5: بناء Gold Layer بعد ما الـ 3 tasks خلصوا
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="python /home/ahmed-refat/dags/scripts/05_build_gold.py",
    )

    # Task 6: رفع الداتا على Snowflake
    upload_snowflake = BashOperator(
        task_id="upload_snowflake",
        bash_command="python /home/ahmed-refat/dags/scripts/06_upload_snowflake.py",
    )

    # ==============================
    # Task Dependencies
    # التوازي: الـ 3 processing tasks بيشتغلوا مع بعض بعد الـ upload
    upload_bronze >> [process_flights, process_weather, process_airports] >> build_gold >> upload_snowflake