# config/airflow_local_settings.py
from __future__ import annotations

import logging
from contextlib import redirect_stdout
from functools import wraps
from importlib import import_module
from io import StringIO
from typing import Callable

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.sensors.base import BaseSensorOperator


def task_policy(task: BaseOperator) -> None:
    enable_auto_deferred_execution = get_enable_auto_deferred_execution()
    logging.info(f"'enable_auto_deferred_execution' is set to {enable_auto_deferred_execution}")

    with redirect_stdout(StringIO()) as f:
        if enable_auto_deferred_execution:
            print(
                f"OperatorAutoSwitch: dag_id={task.dag_id}, task_id={task.task_id}, operator={task.operator_name}"
            )

            if isinstance(task, BaseSensorOperator):
                enable_sensor_deferred_execution(task)
            else:
                enable_operator_deferred_execution(task)

            task.pre_execute = decorate_task_pre_execute(func=task.pre_execute, log_message=f.getvalue())


def get_enable_auto_deferred_execution() -> bool | None:
    # it's can be loaded from db, env var and elsewhere if desired
    # but using Airflow Variable enables user to toggle it easily
    try:
        enable_auto_deferred_execution_var = Variable.get("enable_auto_deferred_execution", None)
        if isinstance(enable_auto_deferred_execution_var, str):
            return strtobool(enable_auto_deferred_execution_var)
    except ValueError:
        return None
    return None


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


def enable_sensor_deferred_execution(task: BaseSensorOperator) -> None:
    original_cls_name = task.__class__.__name__
    if "Async" not in original_cls_name:
        async_cls = get_sensor_async_version_cls(task)
        if not async_cls:
            print(f"Sensor {task} has no deferrable version available")
        elif async_cls:
            async_cls_name = async_cls.__name__
            print(f"{async_cls} ---debug")
            print(f"{async_cls_name} ---debug")
            if async_cls_name == "BigQueryTableExistencePartitionAsyncSensor":
                task.poke_interval = getattr(task, "poke_interval", 5)

            elif async_cls_name == "BigQueryTableExistenceAsyncSensor":
                task.polling_interval = getattr(task, "polling_interval", 5.0)

            elif async_cls_name == "DbtCloudJobRunAsyncSensor":
                task.poll_interval = getattr(task, "poll_interval", 5)
                task.timeout = getattr(task, "timeout", 60 * 60 * 24 * 7)
            elif async_cls_name == "GCSObjectExistenceAsyncSensor":
                pass
            else:
                print(
                    f"{async_cls_name} is not expected. "
                    "Without extra handling, it might cause errors hard to trace. "
                    f"Skip transforming {task.__class__.__name__} to {async_cls_name}"
                )
                return

            task.__class__ = async_cls
            print(
                f"Sensor {task} has been switched from {original_cls_name} " f"to {task.__class__.__name__}"
            )
    else:
        print(f"Sensor {task} is an async sensor")


def get_sensor_async_version_cls(task: BaseSensorOperator) -> type | None:
    task_cls = task.__class__
    task_cls_name = task_cls.__name__

    if "Async" in task_cls_name:
        return task_cls
    else:
        # search in airflow provider
        async_cls_name = f"{task_cls_name[:-6]}AsyncSensor"
        module_name = task.__module__.replace("astronomer", "airflow")
        async_cls = import_async_sensor(module_name, async_cls_name)
        return async_cls


def import_async_sensor(module_name: str, async_cls_name: str) -> type | None:
    try:
        async_cls = getattr(import_module(module_name), async_cls_name)
    except (ImportError, AttributeError) as exc:
        print(f"Cannot import Async Sensor {async_cls_name} due to {exc}")
    else:
        print(f"{async_cls} imported")
        return async_cls


def enable_operator_deferred_execution(task: BaseOperator) -> None:
    if task_has_deferrable_attribute(task):
        task.deferrable = True

        print(f"Operator {task} has been switched to deferred execution")
        print(f"task.deferrable = {task.deferrable}")
    else:
        print(f"Operator {task} has no deferrable version\n")


def task_has_deferrable_attribute(task: BaseOperator) -> bool:
    """check whether an operator has the deferrable attribute"""
    try:
        task.deferrable
    except AttributeError:
        return False
    return True


def decorate_task_pre_execute(func: Callable, log_message: str) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        logging.info(log_message)
        return func(*args, **kwargs)

    return wrapper
