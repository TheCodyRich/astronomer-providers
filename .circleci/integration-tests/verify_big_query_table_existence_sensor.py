# dags/verify_big_query_table_existence_sensor.py
#     simplified version of https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_bigquery_sensors.py


"""Example Airflow DAG for Google BigQuery Sensors."""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "astronomer-airflow-providers")
DATASET_NAME = os.getenv("GCP_BIGQUERY_DATASET_NAME", "astro_dataset")
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

TABLE_NAME = os.getenv("TABLE_NAME", "partitioned_table")


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "gcp_conn_id": GCP_CONN_ID,
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_test_bigquery_sensors",
    schedule_interval=None,  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "bigquery", "sensors"],
) as dag:
    # [START howto_sensor_bigquery_table_existence_async]
    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_table_exists", project_id=PROJECT_ID, dataset_id=DATASET_NAME, table_id=TABLE_NAME
    )
    # [END howto_sensor_bigquery_table_existence_async]

    check_table_exists
