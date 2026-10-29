import os
from datetime import datetime, timedelta


def set_path(dbt_project):
    airflow__logging__log_filename_template = os.getenv("AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE")
    if airflow__logging__log_filename_template:
        os.environ["AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE"] = ""

    log_path = os.getenv("DBT_LOG_PATH")
    if log_path is None:
        os.makedirs("/tmp/logs", exist_ok=True)
        os.environ["DBT_LOG_PATH"] = f"/tmp/logs/{dbt_project}"

    target_path = os.getenv("DBT_TARGET_PATH")
    if target_path is None:
        os.makedirs(f"/tmp/target/{dbt_project}", exist_ok=True)
        os.environ["DBT_TARGET_PATH"] = "/tmp/target"


def get_time_window_expression(dag):
    start_time = dag.latest_execution_date
    end_time = datetime.now() + timedelta(hours=1)
    if start_time is None:
        start_time = end_time + timedelta(hours=-2)
    return "/" + start_time.strftime("%Y-%m-%d %H:") + "/ , /" + end_time.strftime("%Y-%m-%d %H:") + "/p"
