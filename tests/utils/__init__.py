from datetime import datetime
from unittest import TestCase

import pytz
from airflow import DAG
from airflow.models import TaskInstance, DagRun, DagModel, DagTag, DagBag
from airflow.utils.db import create_session
from airflow.utils.state import State

TEST_DAG_FOLDER = "tests/fixtures/dags/"


class DatabaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(dag_folder=TEST_DAG_FOLDER)

    def setUp(self) -> None:
        self.purge_database()

    def tearDown(self) -> None:
        self.purge_database()

    @staticmethod
    def purge_database() -> None:
        # Clean up test artifacts in database
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()
            session.query(DagTag).delete()
            session.query(DagModel).delete()

    def create_dag(
        self,
        dag_id: str,
        start_date: datetime,
        is_paused_upon_creation=False,
        **dag_kwargs,
    ) -> DAG:
        """
        Create a DAG python instance to be used

        :param dag_id: str
        :param start_date: datetime
        :param is_paused_upon_creation: bools

        :return: DAG
        """
        return DAG(
            dag_id=dag_id,
            start_date=start_date.replace(tzinfo=pytz.UTC),
            default_args={"owner": "test@yipitdata.com"},
            is_paused_upon_creation=is_paused_upon_creation,
            full_filepath=f"{TEST_DAG_FOLDER}/{dag_id}.py",
            **dag_kwargs,
        )

    def create_dag_db_resources(
        self, dag: DAG, execution_date: datetime, state: State = State.SUCCESS
    ) -> None:
        """
        Seed a DAG, DAG run, and related task instances in the Database
        By default will create artifacts for a completed run

        :param dag: DAG
        :param execution_date: datetime execution date for DAG run
        :param state: Starting state for DAG runs
        :return:
        """
        with create_session() as session:
            # Save the dag to the DB
            dag.sync_to_db(session=session)
            orm_dag = DagModel.get_current(dag.dag_id)
            # Also update the DAG file location so that DagBag instances can fetch it
            orm_dag.fileloc = f"{TEST_DAG_FOLDER}/{dag.dag_id}.py"
            session.add(orm_dag)

            # Create a DAG run in the DB
            dag.create_dagrun(
                f"{dag.dag_id}_test_run",
                state,
                execution_date=execution_date,
                session=session,
            )
            session.query(TaskInstance).all()

            # Create task instances for the DAG in the DB
            for task in dag.tasks:
                ti = TaskInstance(task=task, execution_date=execution_date, state=state)
                ti.set_state(state, session=session)
