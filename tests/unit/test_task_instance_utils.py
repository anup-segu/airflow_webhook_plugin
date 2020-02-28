from datetime import datetime

import pytz
from airflow.models import TaskInstance
from airflow.utils.db import create_session

from airflow_webhook_plugin.task_instance_utils import fetch_task_instance
from tests.utils import DatabaseTestCase


class TestTaskInstanceUtils(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Setup DAG, task, and task_instance
        self.dag_id = "example_dag"
        self.task_id = "example_task"
        self.execution_date = datetime(2020, 1, 1, tzinfo=pytz.UTC)
        self.create_dag_artifacts(self.dag_id, self.task_id, self.execution_date)

    def test_fetch_task_instance(self):
        # Should be able to return task instance
        # originally created in setUp of test case
        # dag_id, task_id, and execution_date are primary keys
        # for TaskInstance model so use to fetch the same instance
        with create_session() as session:
            task_instance = fetch_task_instance(
                session, self.dag_id, self.task_id, self.execution_date
            )
            self.assertTrue(task_instance, TaskInstance)
