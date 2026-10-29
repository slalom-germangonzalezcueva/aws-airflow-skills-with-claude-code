import json
import logging
import os
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from utils_uni.mexai_app.uda_mexai_connection_util import (
    set_env_snowflake_conn_details,
)
from utils_uni.mexai_app.uda_mexai_dag_util import set_path

local_tz = pendulum.timezone("America/New_York")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
DBT_BASE_DIR = AIRFLOW_HOME + "/dags/"
DBT_PROJECT = "mexai_app"
DBT_PROJECT_DIR = DBT_BASE_DIR + DBT_PROJECT + "/dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
default_dag_id = os.path.basename(__file__).replace(".py", "")

SNOWFLAKE_CONN_ID = "mexai_snowflake"
schedule = None
max_active_runs = 1
DBT_PROJECT_VARS = {}

try:
    dag_vars = Variable.get(default_dag_id, default_var=None, deserialize_json=True)
    dbt_global_vars = Variable.get("uda_dbt_global_vars", default_var={}, deserialize_json=True)
    DBT_PROJECT_VARS = dbt_global_vars.get("dbt_project_vars", {})
    if dag_vars:
        schedule = dag_vars.get("schedule", None)
        max_active_runs = dag_vars.get("max_active_runs", 1)
        SNOWFLAKE_CONN_ID = dag_vars.get("snowflake_conn_id", SNOWFLAKE_CONN_ID)
        DBT_PROJECT_VARS.update(dag_vars.get("dbt_project_vars", {}))
except Exception as err:
    logging.error(f"Error while reading the environment variables: {err}")

try:
    connection = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra_dejson = connection.extra_dejson
    set_path(DBT_PROJECT)
    set_env_snowflake_conn_details(extra_dejson, SNOWFLAKE_CONN_ID)
except Exception as err:
    logging.error(f"Error setting up Snowflake connection: {err}")
    extra_dejson = {}


def get_dbt_cli_args(command, dag):
    return ""


def set_env_vars():
    return os.environ.copy()


default_args = {
    "owner": "4217-mexai_app",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

dag = DAG(
    dag_id=default_dag_id,
    description=default_dag_id,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    end_date=None,
    schedule=schedule,
    catchup=False,
    tags=["4217", "mexai_app"],
    default_args=default_args,
    max_active_runs=max_active_runs,
    is_paused_upon_creation=False,
    user_defined_macros={"get_dbt_cli_args": get_dbt_cli_args, "set_env_vars": set_env_vars},
    render_template_as_native_obj=True,
)

with dag:
    start_task = EmptyOperator(task_id="start")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"/usr/local/airflow/.local/bin/dbt --log-format json run "
            f"{{{{ get_dbt_cli_args('run', dag) }}}} "
            f"--vars '{json.dumps(DBT_PROJECT_VARS)}' "
            f"--profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
        env="{{ set_env_vars() }}",
        do_xcom_push=True,
    )

    success = EmptyOperator(task_id="success_task")

    failure_log = BashOperator(
        task_id=default_dag_id + "_failed_log",
        trigger_rule=TriggerRule.ONE_FAILED,
        bash_command=f"cat /tmp/logs/{DBT_PROJECT}/dbt.log || true",
        env="{{ set_env_vars() }}",
    )

    start_task >> dbt_run >> [success, failure_log]
