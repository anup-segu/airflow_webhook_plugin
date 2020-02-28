from airflow.plugins_manager import AirflowPlugin

from airflow_webhook_plugin.executors.webhook_executor import WebhookExecutor
from airflow_webhook_plugin.flask_blueprints.webhook_blueprint import webhook_blueprint


class AirflowWebhookPlugin(AirflowPlugin):
    name = "airflow_webhook_plugin"
    operators = []
    hooks = []
    executors = [WebhookExecutor]
    macros = []
    admin_views = []
    flask_blueprints = [webhook_blueprint]
    menu_links = []
