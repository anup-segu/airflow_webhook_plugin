from airflow.utils.db import create_session
from airflow.utils.state import State
from airflow.www.app import csrf
from flask import Blueprint, request, jsonify, Response
import pendulum
from pendulum.parsing.exceptions import ParserError

from airflow_webhook_plugin.exceptions import InvalidStateException
from airflow_webhook_plugin.task_instance_utils import fetch_task_instance

webhook_blueprint = Blueprint("webhook", __name__, url_prefix="/webhook",)


@webhook_blueprint.route(
    "/dag/<dag_id>/task/<task_id>/", methods=["PATCH"],
)
@csrf.exempt
def update_task_state(dag_id: str, task_id: str) -> (Response, int):
    """
    For a given task instance, update the state in the Database.
    This endpoint should be used by external processes triggered by
    the WebhookExecutor so that task state can be updated upon
    completion or failure.

    :param dag_id: str
    :param task_id: str
    :return: Response
    """
    try:
        data = request.get_json()
        execution_date_iso = data["execution_date"]
        state = data["state"]

        # Parse parameters and validate their values
        execution_date = pendulum.parse(execution_date_iso)
        if state not in State.task_states:
            raise InvalidStateException("Invalid State provided")

    except KeyError as exception:
        return jsonify({"error": f"Missing parameter(s): {exception}",}), 400

    except ParserError:
        error = f'Invalid timestamp for execution_date: "{execution_date_iso}"'
        return jsonify({"error": error}), 400

    except InvalidStateException:
        return jsonify({"error": f'Invalid state provided: "{state}"'}), 400

    # Query the database for the related task instance
    # then update the state accordingly
    with create_session() as session:
        task_instance = fetch_task_instance(session, dag_id, task_id, execution_date,)

        # Return an error if we cannot find a
        # matching task instance in the database
        if task_instance is None:
            return jsonify({"error": "Matching task instance does not exist"}), 400

        # Update task state
        previous_state = task_instance.state
        task_instance.state = state
        session.merge(task_instance)

        # Return task state to client
        message = f"Task instance state was updated to '{task_instance.state}'."  # NOQA
        return (
            jsonify(
                {
                    "dag_id": task_instance.dag_id,
                    "task_id": task_instance.task_id,
                    "previous_state": previous_state,
                    "state": task_instance.state,
                    "try_number": task_instance.try_number,
                    "execution_date": task_instance.execution_date.isoformat(),
                    "message": message,
                }
            ),
            200,
        )
