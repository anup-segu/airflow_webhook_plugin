from datetime import datetime
from unittest import TestCase

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import create_session
from airflow.utils.state import State


class DatabaseTestCase(TestCase):
    """
    Helper base TestCase to handle database cleanup
    """

    def setUp(self) -> None:
        self.purge_database()

    def tearDown(self) -> None:
        self.purge_database()

    def purge_database(self) -> None:
        # Clean up test artifacts in database
        with create_session() as session:
            session.query(TaskInstance).delete()
            session.commit()

    def create_dag_artifacts(
        self,
        dag_id: str,
        task_id: str,
        execution_date: datetime,
    ) -> (DAG, DummyOperator, TaskInstance):
        """
        Seed a DAG, task, and task instance

        :param dag_id: str
        :param task_id: str
        :param execution_date: datetime
        :return:
        """
        # Seed a DAG, task, and task instance
        self.dag = DAG(
            dag_id=dag_id,
            start_date=execution_date,
            max_active_runs=1,
            concurrency=2
        )
        self.task = DummyOperator(
            task_id=task_id,
            dag=self.dag,
            task_concurrency=0
        )
        self.task_instance = TaskInstance(
            task=self.task,
            execution_date=execution_date,
            state=State.QUEUED
        )
        with create_session() as session:
            session.add(self.task_instance)
            session.commit()

            return self.dag, self.task, self.task_instance
