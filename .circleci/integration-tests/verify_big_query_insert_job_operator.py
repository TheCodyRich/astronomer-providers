# dags/verify_big_query_insert_job_operator.py
#     simplified version of https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DATASET_NAME = os.getenv("GCP_BIGQUERY_DATASET_NAME", "astro_dataset")
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
LOCATION = os.getenv("GCP_LOCATION", "us")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

TABLE_1 = "table1"
DATASET = DATASET_NAME
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET}.{TABLE_1} VALUES "
    f"(42, 'monthy python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)
#
default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_test_async_bigquery_queries",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "bigquery"],
    user_defined_macros={"DATASET": DATASET, "TABLE": TABLE_1},
) as dag:

    # [START howto_operator_bigquery_insert_job_async]
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_operator_bigquery_insert_job_async]

    insert_query_job
