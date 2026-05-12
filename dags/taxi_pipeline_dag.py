from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore


with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    description="Pipeline batch Bronze to Silver to Gold for NYC Taxi Case",
    start_date=datetime(2024, 1, 1),
    schedule_interval= "@daily",
    catchup=False,
    tags=["nyc-taxi", "datalake", "parquet"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="cd /opt/airflow && python -m src.processing.bronze_to_silver",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="cd /opt/airflow && python -m src.processing.silver_to_gold",
    )

    bronze_to_silver >> silver_to_gold