import multiprocessing
import subprocess
from datetime import datetime
from multiprocessing import Queue
from threading import Thread

from airflow import LoggingMixin
from airflow.api.common.experimental import check_and_get_dag
from airflow.executors.debug_executor import DebugExecutor
from airflow.executors.local_executor import LocalExecutor, LocalWorker
from airflow.utils.dag_processing import SimpleTaskInstance
from airflow.utils.db import create_session
from airflow.utils.state import State
from typing import NewType, Tuple

TaskKey = NewType("TaskKey", Tuple[str, str, datetime, int])
# TODO: move into a cache or something
RUNNING_TIS = {}
LOCKED_TASK_IDS = set()


class WebhookDebugExecutor(DebugExecutor):
    def _run_task(self, ti):
        self.log.debug("Executing task: %s", ti)
        key = ti.key
        try:
            with create_session() as session:
                full_ti = ti.construct_task_instance(session, lock_for_update=True)
                params = self.tasks_params.pop(ti.key, {})
                full_ti._run_raw_task(  # pylint: disable=protected-access
                    job_id=full_ti.job_id, **params
                )

            self.change_state(key, State.SUCCESS)
            return True
        except Exception as e:  # pylint: disable=broad-except
            self.change_state(key, State.FAILED)
            self.log.exception("Failed to execute task: %s.", str(e))
            return False

    def change_state(self, key, state):
        self.log.debug("Popping %s from executor task queue.", key)
        del self.running[key]
        self.event_buffer[key] = state


class WebhookWorker(Thread, LoggingMixin):
    def __init__(
        self, result_queue: Queue, locked_task_ids: set, simple_ti: SimpleTaskInstance
    ):
        super().__init__()
        self.key = None
        self.command = None
        self.result_queue = (result_queue,)
        self.simple_ti = simple_ti
        self.locked_task_ids = locked_task_ids

    def run(self):
        self.execute_work(self.key, self.command)

    def execute_work(self, key: TaskKey, command: str):
        """
        Executes command received and stores result state in queue.
        Will not mark the state as SUCCESS. It is up to the task
        to relay that information to Airflow

        :param key: the key to identify the TI
        :type key: tuple(dag_id, task_id, execution_date)
        :param command: the command to execute
        :type command: str
        """
        if key is None:
            return

        (dag_id, task_id, execution_date, _) = key

        self.log.info(f"{self.__class__.__name__} running {command}")
        dag = check_and_get_dag(dag_id)
        task = dag.get_task(task_id)
        for downstream_task_id in task.downstream_task_ids:
            downstream_key = (dag_id, downstream_task_id, execution_date, 0)
            dowstream_seralized_key = _serialized_key(downstream_key)
            self.log.info(f"Locking {downstream_key} from running ..")
            self.locked_task_ids.add(dowstream_seralized_key)

        try:
            # TODO: check if we want synchronous behavior
            # ti.task
            # Update ti state to running using database

            # Launch the execution of the task, but do not change
            # the task state, it should still be considered RUNNING
            # state = State.SUCCESS
            # TODO: move into SQS? at least the success events, could be done by worker

            subprocess.check_call(command, close_fds=True)
            # self.result_queue.put((key, State.RUNNING))

            # with create_session() as session:
            #     ti = self.simple_ti.construct_task_instance(session, lock_for_update=True)
            #     ti.state = State.RUNNING

            self.log.info(
                f"Completed running {command}. Waiting for webhook to mark task as success .."
            )

        except subprocess.CalledProcessError as e:
            # Handle any errors as usual
            state = State.FAILED
            self.log.error(f"Failed to execute task: {e}.")
            self.result_queue.put((key, state))

            for downstream_task_id in task.downstream_task_ids:
                (dag_id, _, execution_date, _) = key
                downstream_key = (dag_id, downstream_task_id, execution_date, 0)
                downstream_seralized_key = _serialized_key(downstream_key)

                self.log.info(f"Removing lock on {downstream_seralized_key} ..")
                # Check if the requested task_id is in the set,
                # could have changed if a new version of DAG was deployed
                if downstream_seralized_key in self.locked_task_ids:
                    self.locked_task_ids.remove(downstream_seralized_key)


class WebhookExecutor(LocalExecutor):
    class _UnlimitedParallelism(LocalExecutor._UnlimitedParallelism):
        """
        Subclasses LocalExecutor with unlimited parallelism,
        starting one process per each command to execute.
        Will use the WebhookWorker instead of LocalWorker
        to manage task state and asynchronous DAG processing
        """

        def execute_async(self, key: TaskKey, command: str):
            """
            :param key: the key to identify the TI
            :type key: tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: str
            """
            simple_ti = self.executor.running_tis[key]
            local_worker = WebhookWorker(
                self.executor.result_queue, self.executor.locked_task_ids, simple_ti
            )
            local_worker.key = key
            local_worker.command = command
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

        def sync(self):
            # TODO: fetch completed state from webhook via SQS?
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 1

    def start(self):
        self.manager = multiprocessing.Manager()
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = self._UnlimitedParallelism(self)
        self.impl.start()
        self.running_tis = RUNNING_TIS

        # keep track of downstream tasks for any ongoing task execution
        # these tasks should not be enqueued until the worker exits the current task
        # the set acts as a guard against mistmatched timing of task queues and task runs
        # the worker will mark the current task as running, so that subsequent scheduler runs
        # do not enqueue the tasks
        self.locked_task_ids = LOCKED_TASK_IDS

    def trigger_tasks(self, open_slots: int):
        """
        Queue task instances to execute, patched from BaseExecutor:
        https://github.com/apache/airflow/blob/a943d6beab473b8ec87bb8c6e43f93cc64fa1d23/airflow/executors/base_executor.py#L136-L154

        :param open_slots: Number of open slots
        :return:
        """
        self.log.info(self.locked_task_ids)
        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True,
        )
        for i in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, simple_ti) = sorted_queue.pop(0)
            self.log.info(f"Attempting to enqueue: {key}")

            # Prevent running downstream tasks before upstream tasks finish
            if _serialized_key(key) in self.locked_task_ids:
                self.log.info(
                    f"""
                    Parent task is still running, Could not enqueue: {key}
                    Will try again during next scheduler heartbeat.
                    """,
                )
                continue

            self.queued_tasks.pop(key)
            self.running[key] = command
            self.running_tis[key] = simple_ti
            self.execute_async(
                key=key,
                command=command,
                queue=queue,
                executor_config=simple_ti.executor_config,
            )


def _serialized_key(key: TaskKey) -> str:
    (dag_id, task_id, execution_date, _) = key
    return f"{dag_id}:{task_id}:{execution_date.isoformat()}"
