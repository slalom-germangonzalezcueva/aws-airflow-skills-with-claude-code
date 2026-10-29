#!/bin/bash
# Rebuild offline wheels for MWAA (no-egress) in CI. Wheels are never committed.
set -euo pipefail

AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.0.6}"
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-$3.0.6/constraints-${PYTHON_VERSION}.txt"

mkdir -p plugins
rm -f plugins/*.whl

pip download --dest plugins --only-binary=:all: --constraint "$CONSTRAINTS" \
  apache-airflow-providers-snowflake==6.4.0 \
  snowflake-connector-python==3.17.2 \
  pandas numpy pyarrow python-dateutil pytz tzdata six \
  cryptography cffi pyopenssl requests charset-normalizer idna certifi \
  typing_extensions filelock sortedcontainers platformdirs tomlkit urllib3 pycparser

python3 scripts/mwaa_requirements_preflight.py --airflow-version "$AIRFLOW_VERSION" --python-version "$PYTHON_VERSION" --skip-resolver
