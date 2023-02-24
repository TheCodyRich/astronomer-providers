# config/airflow_local_settings.py

from typing import Optional

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import task


def task_has_deferrable_attribute(task: BaseOperator) -> bool:
    # check whether an operator has the deferrable attribute
    # if not, it means the operator does not have a Async version
    try:
        task.deferrable
    except AttributeError:
        return False
    return True


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.


    modified from distutils.util.strtobool as distutils is depcrecating
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError(f"invalid truth value {val}")


def task_policy(task: BaseOperator) -> None:
    # it's can be loaded from db, env var and elsewhere if desired
    # but using Airflow Variable enables user to toggle it easily
    try:
        enable_deferred_execution_var = Variable.get("ENABLE_DEFERRED_EXECUTION", None)
        if isinstance(enable_deferred_execution_var, str):
            enable_deferred_execution = strtobool(enable_deferred_execution_var)
        else:
            enable_deferred_execution = None
    except ValueError:
        enable_deferred_execution = None
    task.deferrable = enable_deferred_execution

    print("wei-testing task __dict__", task.__dict__)
