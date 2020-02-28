from functools import wraps

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.state import State


class AsyncOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        on_success_callback = keep_task_running(
            kwargs.pop("on_success_callback", _dummy_callback)
        )
        super().__init__(
            *args, **kwargs, on_success_callback=on_success_callback,
        )


def keep_task_running(func) -> callable(dict):
    """
    Decorator to ensure task state is running upon successful execution
    Will return a callback to be passed into "on_success_callback"

    :param func: callable(dict)
    :return: callable(dict)
    """

    @wraps
    def callback(context: dict):
        _update_task_instance_state(context["ti"], State.RUNNING)
        func(context)

    return callback


def _update_task_instance_state(task_instance: TaskInstance, state: str) -> None:
    """
    Helper to modify TaskInstance state to the desired state
    provided by user. Useful in manually marking task states
    after synchronous task executions

    :param task_instance:TaskInstance task instance from execution
    :param state: str desired state to change task instance
    :return:
    """
    task_instance.state = state
    if state == State.UP_FOR_RETRY:
        task_instance.try_number += 1


def _dummy_callback(context: dict) -> None:
    """
    Placeholder callback if none is defined by user

    :param context:dict TaskInstance context
    :return: None
    """
    pass
