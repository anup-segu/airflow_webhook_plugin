# airflow_webhook_plugin
---------------------------------------------------------
Airflow plugin to support asynchronous task execution. This can be useful when running tasks with long-duration where the task can instead notify Airflow upon completion.

The executor will launch Airflow tasks but will not mark the task as complete. It is up to the task, or the resources spawned by the task to notify Airflow that the task is complete or failed. This can be done via the API exposed by this plugin.

This plugin is composed of the following components:
* `WebhookExecutor`: A custom executor that will launch task executions and not manage task state
* `WebhookBlueprint`: A custom flask blueprint that exposes a REST API to airflow to manage state
* `WebhookOperatorMixin`: A mixin for the `BaseOperator` class that will signal that a task should be executed asynchronously

### Requirements
---------------------------------------------------------
* `python3.6` or above
* `apache-airflow>=1.10.8`

### Getting Started
---------------------------------------------------------
```bash
pip install airflow_webhook_plugin
```

Then add the following environment variables:

```bash
export AIRFLOW__CORE__EXECUTOR=airflow_webhook_plugin.WebhookExecutor
export AIRFLOW__WEBHOOK__AUTH_TOKEN=<some seceret token>
```

#### Example Usage
---------------------------------------------------------
Some operators will launch workloads asynchronously. These can be implemented by subclassing the `AsyncOperator`
```python
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.plugins.operators.airflow_webhook_plugin.async_operator import AsyncOperator

def launch_some_resource():
    # Launch a remote workload. Rather than wait for its completion.
    # The workload will need to make an API request (see REST API)
    # to airflow, notifying it terminal state
    pass

class PythonAyncOperator(PythonOperator, AsyncOperator):
    """
    Subclass existing PythonOperator, the SyncOperator will make sure
    to update task state at the end of its execution
    """
    pass

with DAG('async_dag') as dag:
    operator = PythonOperator(callable=launch_some_resource)
```

Some tasks may still be synchronous and will need to be marked done right away.
These operators will continue to work as before.
```python
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator


def do_some_work():
    print('Completed some work. Now task will be marked as a "success"')


with DAG('sync_dag') as dag:
    operator = PythonOperator(callable=do_some_work)
```

#### REST API Routes
---------------------------------------------------------
To update a task instance state from an external service use this:
* Request: `PATCH <AIRFLOW__WEBSERVER__BASE_URL>/webhook/dag/<dag_id>/task/<task_id>/`
* Parameters (**Required**):
    * `execution_date`: ISO 8601 string of the related execution date for the task instance
    * `state`: Desired state to change. Should be one of `airflow.utils.state.State`
* Returns a `400` status if parameters are invalid/missing, and a `200` for valid requests
* Example response:
```json
{
    "dag_id": "<dag_id>",
    "task_id": "<task_id>",
    "execution_date": "<execution_date>",
    "try_number": "<try_number>",
    "previous_state": "<previous_state>",  
    "state": "<state>",
    "message": "Task instance state was updated to '<state>'."
}
```
