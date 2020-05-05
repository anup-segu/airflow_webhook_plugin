import multiprocessing
import subprocess

from airflow.executors.local_executor import LocalExecutor, LocalWorker
from airflow.utils.db import create_session
from airflow.utils.state import State


class WebhookWorker(LocalWorker):
    def execute_work(self, key, command):
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
        self.log.info("%s running %s", self.__class__.__name__, command)
        try:
            # Launch the execution of the task, but do not change
            # the task state, it should still be considered RUNNING
            # state = State.SUCCESS
            subprocess.check_call(command, close_fds=True)
            self.log.info(
                f"Completed running {command}. Waiting for webhook to mark task as success .."
            )

        except subprocess.CalledProcessError as e:
            state = State.FAILED
            self.log.error("Failed to execute task %s.", str(e))
            self.result_queue.put((key, state))


class WebhookExecutor(LocalExecutor):
    class _UnlimitedParallelism(LocalExecutor._UnlimitedParallelism):
        """
        Subclasses LocalExecutor with unlimited parallelism,
        starting one process per each command to execute.
        Will use the WebhookWorker instead of LocalWorker
        to manage task state and DAG processing
        """

        def execute_async(self, key, command):
            """
            :param key: the key to identify the TI
            :type key: tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: str
            """
            # TODO: move into SQS?
            local_worker = WebhookWorker(self.executor.result_queue)
            local_worker.key = key
            local_worker.command = command
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

            # Update ti state to running using database
            simple_ti = self.executor.running_tis[key]
            with create_session() as session:
                ti = simple_ti.construct_task_instance(session, lock_for_update=True)
                ti.set_state(State.RUNNING, session=session)

        def sync(self):
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 1

            # TODO: fetch completed state from webhook via SQS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running_tis = {}

    def start(self):
        self.manager = multiprocessing.Manager()
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = self._UnlimitedParallelism(self)
        self.impl.start()

    def trigger_tasks(self, open_slots):
        """
        Queue task instances to execute, patched from BaseExecutor:
        https://github.com/apache/airflow/blob/a943d6beab473b8ec87bb8c6e43f93cc64fa1d23/airflow/executors/base_executor.py#L136-L154

        :param open_slots: Number of open slots
        :return:
        """
        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True,
        )
        for i in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, simple_ti) = sorted_queue.pop(0)
            self.queued_tasks.pop(key)
            self.running[key] = command
            self.running_tis[key] = simple_ti
            self.execute_async(
                key=key,
                command=command,
                queue=queue,
                executor_config=simple_ti.executor_config,
            )
