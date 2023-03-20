# dags/verify_dbt_cloud_job_run_sensor.py
#     simplified version of https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/dbt/cloud/example_dags/example_dbt_cloud.py


import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime
from astronomer.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor

DBT_CLOUD_CONN_ID = os.getenv("ASTRO_DBT_CLOUD_CONN", "dbt_cloud_default")
DBT_CLOUD_ACCOUNT_ID = os.getenv("ASTRO_DBT_CLOUD_ACCOUNT_ID", 12345)
DBT_CLOUD_JOB_ID = int(os.getenv("ASTRO_DBT_CLOUD_JOB_ID", 12345))
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "dbt_cloud_conn_id": DBT_CLOUD_CONN_ID,
    "account_id": DBT_CLOUD_ACCOUNT_ID,
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 1)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 0))),
}


with DAG(
    dag_id="example_test_dbt_cloud",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    tags=["example", "async", "dbt-cloud"],
    catchup=False,
) as dag:
    # [START howto_operator_dbt_cloud_run_job_sensor]
    job_run_sensor = DbtCloudJobRunSensor(task_id="job_run_sensor", run_id="some_run_id", timeout=5)
    # [END howto_operator_dbt_cloud_run_job_sensor]
