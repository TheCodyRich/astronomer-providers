# config/airflow_local_settings.py

import pprint
from importlib import import_module
from typing import Optional

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.sensors.base import BaseSensorOperator


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


def get_enable_deferred_execution() -> Optional[bool]:
    # it's can be loaded from db, env var and elsewhere if desired
    # but using Airflow Variable enables user to toggle it easily
    try:
        enable_deferred_execution_var = Variable.get("ENABLE_DEFERRED_EXECUTION", None)
        if isinstance(enable_deferred_execution_var, str):
            return strtobool(enable_deferred_execution_var)
    except ValueError:
        return None
    return None


def task_policy(task: BaseOperator) -> None:
    enable_deferred_execution = get_enable_deferred_execution()

    # pp = pprint.PrettyPrinter(indent=4)

    if isinstance(task, BaseSensorOperator):
        # currently, always use deferable
        print(f"wei-testing sensor operator {task}")

        task_cls = task.__class__
        task_cls_name = task_cls.__name__

        if "Async" in task_cls_name:
            print(f"task {task} is an Async Sensor")
        else:
            async_cls_name = f"{task_cls_name}Async"
            module_path = task.__module__.replace("airflow", "astronomer")
            try:
                async_cls = getattr(import_module(module_path), async_cls_name)
            except AttributeError as exc:
                print(f"Cannot import Async Sensor {async_cls_name} due to {exc}")
                pass
            else:
                print(f"yes, {async_cls} imported")

        # pp.pprint(dir())
    else:
        task.deferrable = enable_deferred_execution
        print("wei-testing task __dict__", task.__dict__)
