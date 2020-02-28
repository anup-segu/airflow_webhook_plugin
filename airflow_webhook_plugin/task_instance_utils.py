import datetime

from airflow.models import TaskInstance
from airflow.settings import Session


def fetch_task_instance(
    session: Session, dag_id: str, task_id: str, execution_date: datetime,
) -> TaskInstance:
    """
    Query airflow database for a specific TaskInstance object

    :param session: Session Airflow DB Session object
    :param dag_id: str  ID for DAG
    :param task_id: str Task id for DAG's task
    :param execution_date: datetime Execution date for DAG run
    :return: TaskInstance (maybe None if not found)
    """
    return (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id == task_id,
            TaskInstance.execution_date == execution_date,
        )
        .first()
    )
