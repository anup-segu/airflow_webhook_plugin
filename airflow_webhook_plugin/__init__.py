from airflow.plugins_manager import AirflowPlugin

from airflow_webhook_plugin.executors.webhook_executor import WebhookExecutor
from airflow_webhook_plugin.flask_blueprints.webhook_blueprint import webhook_blueprint
from airflow_webhook_plugin.operators.async_operator import AsyncOperator


class AirflowWebhookPlugin(AirflowPlugin):
    name = "airflow_webhook_plugin"
    operators = [AsyncOperator]
    hooks = []
    executors = [WebhookExecutor]
    macros = []
    admin_views = []
    flask_blueprints = [webhook_blueprint]
    menu_links = []
