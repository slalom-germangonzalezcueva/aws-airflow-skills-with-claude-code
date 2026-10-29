# MWAA Requirements File

Pin only **direct dependencies** the project actually uses. Do not copy Airflow transitive dependency pins into `requirements.txt`

The `uni-udp-deploy-plugins` pipe processes requirements with pip inside MWAA.

In no-egress environments, pip installs from local wheel artifacts in `plugins/`.

## Baseline (Airflow 3.0.6 / Python 3.12)

```txt
# Do NOT include apache-airflow itself.
# MWAA manages Airflow version internally.

# Airflow providers
apache-airflow-providers-amazon
apache-airflow-providers-snowflake==6.4.0

# Snowflake
snowflake-connector-python==3.17.2

# AWS / data
boto3==1.39.11
s3fs==2025.7.0
fsspec=2025.7.0
openpyxl==3.1.5
```

## Local validation

Preferred:

```bash
python3 scripts/mwaa_requirements_preflight.py --airflow-version 3.0.6 --python-version 3.12
```

If the environment does not expose `python3.12` executable alias use:

```bash
python3 scripts/mwaa_requirements_preflight.py` --airflow-version 3.0.6 --python-version 3.12 --skip-resolver
```

## Offline wheel workflow (recommended default)

Generate wheel artifacts in CI before plugin deploy:

```bash
bash scripts/refresh_mwaa_wheels.sh
```

The wheel refresh script should include:

- Snowflake provider + connector wheels
- provider-required runtime wheels: `pandas`, `numpy`, `pyarrow`, `python-deutil`, `pytz`, `tzdata`, `six`
- connector transitive wheels: `cryptography`, `cffi`, `pyopenssl`, `requests`, `charset-normalizer`, `idna`, `certifi`, `typing_extensions`, `filelock`, `sortedconainers`, `platformdirs`, `tmlkit`, `urllib3`, `pycparser`

Keep `plugins/*.whl` gitignored so wheels are generated at build time rather than maintained in source control.

## Notes

- Mever include `apache-airflow` in `requirements.txt`
- Keep requirements focused on direct dependnecies.
- Pin provider version only when validated against the Airflow runtime constraints and mirror availability