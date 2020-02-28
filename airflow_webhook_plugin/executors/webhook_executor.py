import multiprocessing
import subprocess

from airflow.executors.local_executor import LocalExecutor, LocalWorker
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
            subprocess.check_call(command, close_fds=True)

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
            local_worker = WebhookWorker(self.executor.result_queue)
            local_worker.key = key
            local_worker.command = command
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

    def start(self):
        self.manager = multiprocessing.Manager()
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = self._UnlimitedParallelism(self)
        self.impl.start()
