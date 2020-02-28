from datetime import datetime

import pytz
from airflow.utils.state import State
from airflow.www import app

from airflow_webhook_plugin import webhook_blueprint
from tests.utils import DatabaseTestCase


class TestWebhookBlueprint(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Setup DAG, task, and task_instance
        self.dag_id = "example_dag"
        self.task_id = "example_task"
        self.execution_date = datetime(2020, 1, 1, tzinfo=pytz.UTC)
        self.create_dag_artifacts(
            self.dag_id, self.task_id, self.execution_date,
        )

        # Initialize a flask test client and register the blueprint
        # so we can test the blueprint's routes
        self.app = app.create_app(config=None, testing=True)
        # Disable CSRF checks for testing
        self.app.config["WTF_CSRF_ENABLED"] = False
        with self.app.app_context():
            self.webhook_bluerint = webhook_blueprint
            self.app.register_blueprint(self.webhook_bluerint)
            self.client = self.app.test_client()

    def test_update_task_state_endpoint_verifies_parameters(self):
        # Should fail if no parameters passed
        response = self.client.patch(
            f"webhook/dag/{self.dag_id}/task/{self.task_id}/", json={}
        )
        self.assertEqual(response.status_code, 400)
        data = response.get_json()
        self.assertIn("Missing parameter", data["error"])

        # Should fail if not all parameters passed
        response = self.client.patch(
            f"webhook/dag/{self.dag_id}/task/{self.task_id}/",
            json={"execution_date": "2020-01-01T00:00:00+00:00",},
        )
        self.assertEqual(response.status_code, 400)
        data = response.get_json()
        # Should reference missing field in error message
        self.assertIn("Missing parameter", data["error"])
        self.assertIn("state", data["error"])

    def test_update_task_state_endpoint_handles_invalid_timestamps(self):
        # Should fail if no parameters passed
        response = self.client.patch(
            f"webhook/dag/{self.dag_id}/task/{self.task_id}/",
            json={"execution_date": "fake timestamp", "state": State.SUCCESS,},
        )
        self.assertEqual(response.status_code, 400)
        data = response.get_json()
        # Should reference missing field in error message
        self.assertIn("Invalid timestamp", data["error"])

    def test_update_task_state_endpoint_handles_invalid_state(self):
        # Should fail if no parameters passed
        response = self.client.patch(
            f"webhook/dag/{self.dag_id}/task/{self.task_id}/",
            json={
                "execution_date": "2020-01-01T00:00:00+00:00",
                "state": "invalid_state",
            },
        )
        self.assertEqual(response.status_code, 400)
        data = response.get_json()
        # Should reference missing field in error message
        self.assertIn("Invalid state", data["error"])

    def test_update_task_state_returns_error_for_no_instance(self):
        # Request a state change for a non-existent task on the DAG
        response = self.client.patch(
            f"webhook/dag/{self.dag_id}/task/non_existent_task/",
            json={
                "execution_date": "2020-01-01T00:00:00+00:00",
                "state": State.SUCCESS,
            },
        )
        self.assertEqual(response.status_code, 400)
        data = response.get_json()
        # Should indicate in error message object does not exist
        self.assertIn("does not exist", data["error"])

    def test_update_task_state_updates_task_instance_state(self):
        response = self.client.patch(
            f"webhook/dag/{self.dag_id}/task/{self.task_id}/",
            json={
                "execution_date": "2020-01-01T00:00:00+00:00",
                "state": State.SUCCESS,
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()

        # Should have updated state on TaskInstance object
        self.task_instance.refresh_from_db()
        self.assertEqual(self.task_instance.state, State.SUCCESS)

        # Should return task state changes
        self.assertEqual(
            data,
            {
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "try_number": 1,
                "state": State.SUCCESS,
                "previous_state": State.QUEUED,
                "execution_date": "2020-01-01T00:00:00+00:00",
                "message": "Task instance state was updated to 'success'.",
            },
        )
