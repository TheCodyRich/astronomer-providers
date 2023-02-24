# config/airflow_local_settings.py

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import task


def task_policy(task: BaseOperator) -> None:
    # check whether an operator has the deferrable attribute
    # if not, it means the operator does not have a Async version
    try:
        task.deferrable
    except AttributeError:
        return
    else:
        # it's can be loaded from db, env var and elsewhere if desired
        # but using Airflow Variable enables user to toggle it easily
        # note that the variable here is a string, we'll need to add a logic
        # to turn it into boolean
        # but here is just a demonstration on how we can change the "deferrable"
        # attribute globally
        task.deferrable = Variable.get("ENABLE_DEFERRED_EXECUTION", None)
        print("task __dict__", task.__dict__)


def case_string_to_bool(bool_str: str) -> bool:
    if bool_str.lower() == "true":
        return True
    elif bool_str.lower() == "false":
        return False

    raise ValueError(f"bool_str {bool_str} should be a string that contains true of false")


def set_task_deferrable(enable_deferred_execution: bool) -> None:
    task.deferrable = enable_deferred_execution
