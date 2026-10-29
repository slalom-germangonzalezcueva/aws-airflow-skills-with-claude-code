import base64
import json
import logging
import os

import boto3
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.settings import Session
from botocore.exceptions import ClientError

CONN_ID = "mexai_snowflake"


def get_secret_config(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    if "SecretString" in response:
        return json.loads(response["SecretString"])
    return json.loads(base64.b64decode(response["SecretBinary"]))


def disable_use_alternative_wh_connection_value(kwargs=None):
    session = Session()
    try:
        existing_connection = session.query(Connection).filter(Connection.conn_id == CONN_ID).first()
        if existing_connection:
            existing_extra = existing_connection.extra_dejson
            existing_extra.update({"use_alternative_wh": False})
            existing_connection.set_extra(json.dumps(existing_extra))
            session.commit()
    except Exception as error:
        logging.error(f"Error updating use_alternative_wh on Snowflake connection: {error}")
    finally:
        session.close()


def set_env_snowflake_conn_details(extra_dejson, connection_id=CONN_ID):
    alternative_wh_error_flag = False
    try:
        connection = BaseHook.get_connection(connection_id)
        if connection is not None:
            os.environ["SNOWFLAKE_ACCOUNT"] = extra_dejson.get("account", "")
            os.environ["SNOWFLAKE_USER"] = connection.login
            os.environ["SNOWFLAKE_ACCESSKEY"] = connection.password
            os.environ["SNOWFLAKE_DATABASE"] = extra_dejson.pop("database", None)
            os.environ["SNOWFLAKE_SCHEMA"] = connection.schema
            os.environ["SNOWFLAKE_THREAD"] = str(extra_dejson.pop("thread", 10))
            if extra_dejson.pop("use_alternative_wh", False):
                if extra_dejson.get("extra__snowflake__warehouse", False):
                    os.environ["SNOWFLAKE_WAREHOUSE"] = extra_dejson.pop("extra__snowflake__warehouse")
                else:
                    alternative_wh_error_flag = True
                    raise Exception("Warehouse field of Airflow connection is empty")
            else:
                os.environ["SNOWFLAKE_WAREHOUSE"] = extra_dejson.pop("warehouse", None)
    except Exception as error:
        disable_use_alternative_wh_connection_value()
        if alternative_wh_error_flag:
            logging.error("Provide warehouse name under Warehouse field of Airflow connections")
        else:
            logging.error(f"Snowflake connection details not available in Airflow: {error}")
