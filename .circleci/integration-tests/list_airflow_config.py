from datetime import datetime

from airflow import DAG
from airflow.configuration import conf
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def show_config_content():
    import pprint

    from config import airflow_local_settings

    pp = pprint.PrettyPrinter(indent=4)

    print(airflow_local_settings)
    pp.pprint(dir(airflow_local_settings))
    print(conf)
    pp.pprint(dir(conf))


with DAG(
    dag_id="list_airflow_config",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    list_config_and_display_content = BashOperator(
        task_id="list_config_and_display_content",
        bash_command="ls ${AIRFLOW_HOME}/config && cat ${AIRFLOW_HOME}/config/airflow_local_settings.py",
    )

    import_config = PythonOperator(task_id="import_config", python_callable=show_config_content)
