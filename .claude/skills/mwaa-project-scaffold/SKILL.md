----
name: mwaa-project-scaffold
description: Scafold a complete Unicorn Inc MWAA (Amazon Managed Workflows for Apache Airflow) project from an empty repo. Collects project variables via questionnaire, then creates the full repo structure: GitHub Actions workflows, dbt project scaffold, Airflow DAG template, utility modules, per-environment Airflow Variables, Flyway configuration, and session memory - all following best practices and domain code conventions. Triggers: scaffold mawaa, new airflow project, mwaa scafild, create airflow project, scaffold mwaa project, new mwaa repo.
----

# MWAA Project Scaffold

Scaffold a production-ready Unicorn Inc MWAA Airflow project folowing the stabished domain code pattern (reference: `uni-uda-airflow`, domain `0042`). All naming, pipeline steps, and IAM patters are derived from the domain code.

## When to use

- Creating a new MWAA Airflow project from scratch.
- AN MWAA environment already exists (provisioned via terraform or other IaC tools) and the DAG repo needs to be populated.
- Onboarding a new domain in the Unicorn Inc MWAA Airflow project.

## Phase 0: Variable Gathering

Before writing any files, collect ALL required variables. Present them as a checklist and ask for each one. Do NOT proceed untill all variables are confirmed.

### Required Variables

| Variable | Description | Example |
|---|---|---|
| `DOMAIN_CODE` | 4-digit Unicorn Inc domain code | `4217` |
| `APPLICATION_CI` | ServiceNow CMDB application name (or equvalente) | `MexAI Application` |
| `PROJECT_FOLDER` | dbt project + DAG folder name under `dags/` (snake_case) | `mexai_app` |
| `DAG_PREFIX` | Short prefix for all DAG filenames (no underscores at end) | `mexai` |
| `SNOWFLAKE_CONN_ID` | Airflow Snowflake connection ID | `mexai_snowflake` |
| `DBT_PROFILE_NAME` | dbt profile name used in `profiles.yml` | `4217-mexai-app` |
| `SUPPORT_EMAIL` | Support contact email for the project | `support@mexai.com` |

### Optional Variables (defaults shown)

| Variable | Description | Example |
|---|---|---|
| `SNOWFLAKE_SCHEMA` | Target Snowflake schema name | `<PROJECT_FOLDER upper-cased>` |
| `DEV_DB_NAME` | DEV Snowflake database placeholder | `DEV_<PROJECT_FOLDER upper-cased>` |
| `UAT_DB_NAME` | UAT Snowflake database placeholder | `UAT_<PROJECT_FOLDER upper-cased>` |
| `PRD_DB_NAME` | PRD Snowflake database placeholder | `PRD_<PROJECT_FOLDER upper-cased>` |
| `ENVIRONMENTS` | Comma-separated environment list | `dev,uat,prd` |
| `AIRFLOW_VERSION` | MWAA Airflow version | `3.0.6` |
| `PRIMARY_REGION` | Primary AWS region for the MWAA environment | `us-east-1` |
| `INCLUDE_FLYWAY` | Scaffold Flyway Snowflake migrations | `yes` |
| `INCLUDE_DBT` | Scaffold dbt project inside DATG folder | `yes` |

## Always-Derived Values (show to user for confirmation)

```
Deployment IAM role:   UNI-AR-Github-{DOMAIN_CODE}-deployment
MWAA env name:         uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1
S3 DAG bucket:         uni-<env>-{DOMAIN_CODE}-managed-airflow-code-ue1
Snowflake secret path: /{DOMAIN_CODE}/<env>/snowflake/snowflake
GitHub Environments:   dev, uat, prd (one per ENVIRONMENTS entry)
Flyway history table:  uni-{DOMAIN_CODE}-airflow-schema-history
```

**After Phase 0:** Show a confirmation summary of all variables and derive values. Ask the user to confirm before writting any files.

---

## Phase 1: Core Infrastructure Files

Create the root-level files and `uda_core` helper scripts.

#### 1.1 `.gitignore`

```
# Pipeline runtime artifacts
**/.pipeline/phase_*_evidence.json
**/.pipeline/schema_override.yml

/plugin.zip
/variables_*.zip

# Python
*.py[cod]
*.pyc
__pycache__/
venv/
myvenv
.venv/

# IDEs
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db

# Build artifacts
*.class
*.jar
*.war
dist/
target/
node_modules/

# Logs
*.log

# Deploy artifacts (root-level generated files only)
/plugins*.zip
/scripts/plugins*.zip
/requirements*.txt
/variables*.zip

# dbt generated
/dags/{PROJECT_FOLDER}/models/edr_target/
/dags/{PROJECT_FOLDER}/target/
/dags/{PROJECT_FOLDER}/dbt_packages/

# Local
/Dockerfile
.memory/

# Offline wheels (generated in CI by scripts/refresh_mwaa_wheels.sh)
plugins/*.whl
.venv-mwaa-preflight/
```

### 1.2 `__init__.py`

Empty file.

### 1.3 `uda_core/.gitignore`

```
.idea
venv/
*.pyc
.vscode

# Files made by deployment script
plugins*.zip
scripts/plugins*.zip
requirements*.txt
variables*.zip
```

### 1.4 `uda_core/config/awscli-install.sh`

```bash
#!/bin/bash
zip_file="awscliv2.zip"
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$zip_file"
unzip "$zip_file"
./aws/install
rm $zip_file
aws --version
```

### 1.5 `uda_core/config/updateawscredentials.sh`

```bash
#!/bin/bash
access_key_var=AWS_ACCESS_KEY_ID_$AppEnv
secret_key_var=AWS_SECRET_ACCESS_KEY_$AppEnv
default_region_var=AWS_DEFAULT_REGION_$AppEnv
backup_region_var=AWS_BACKUP_REGION_$AppEnv
export AWS_ACCESS_KEY_ID=${!access_key_var}
export AWS_SECRET_ACCESS_KEY=${!secret_key_var}
if [[ $region == ${!backup_region_var} ]]; then
  export AWS_DEFAULT_REGION=$region
else
  export AWS_DEFAULT_REGION=${!default_region_var}
fi
```

# 1.6 `requirements/requirements.txt`

See `references/requirements.md` for the full pinned package list. Do **not** include `apache-airflow` itself - MWAA manages its own version and pinning it causes the requirements apply step to fail silently.

### 1.7 `local/__init__.py`

Empty file.


### 1.8 GitHub Actions workflows (`.github/`)

See `references/pipeline.md` for the full workflow templates (5 workflow files plus a composite action).

Files to create:

- `.github/actions/mwaa-context/action.yml` - composite action deriving region code, S3 bucket and MWAA env name
- `.github/workflows/01-deploy-dags.yml`
- `.github/workflows/02-deploy-dags-and-db-migration.yml` _(Flyway only)_
- `.github/workflows/03-database-migration-only.yml` _(Flyway only)_
- `.github/workflows/04-deploy-plugins-and-restart.yml`
- `.github/workflows/05-restart-environment.yml`

Key substitutions:

- `{DOMAIN_CODE}` -> the actual domain code for the project
- `{PROJECT_FOLDER}` -> the folder name of the project within the `dags` directory
- `{PRIMARY_REGION}` -> the primary AWS region for the deployment
- `{AIRFLOW_VERSION}` -> the MWAA Airflow version
- `options` of the `deployment_environment` input -> the values of `ENVIRONMENTS` (e.g. dev, uat, prd)
- workflows 02 and 03, and the `workflow_call` trigger on 01 -> omit if `INCLUDE_FLYWAY=no`

The result is always **5 workflows** (or 3 without Flyway):

- `01 Deploy DAGs`
- `02 Deploy DAGs and DB Migration` _(Flyway only)_
- `03 Database Migration Only` _(Flyway only)_
- `04 Deploy Plugins and Restart`
- `05 Restart Environment`

All workflows are `workflow_dispatch` (manual) and take a `deployment_environment` choice input. Each job sets `environment: ${{ inputs.deployment_environment }}` and `AppEnv` from it. `AWS_ACCOUNT_ID` is read from the GitHub Environment variable `vars.AWS_ACCOUNT_ID`, and AWS access uses OIDC through `aws-actions/configure-aws-credentials` (workflow 05 uses environment secrets, as the original pipeline did). Tell the user to create the GitHub Environments, the `AWS_ACCOUNT_ID` variable per environment, and the `ARTIFACTORY_USERNAME` / `ARTIFACTORY_PASSWORD` repository secrets.

### 1.9 `plugins/.gitkeep`

Empty file. Plugins live in a **top-level** `plugins/` directory, never under `dags/`. See `references/plugins.md`. Also create `plugins/README.md` documenting the Airflow 3.x plugin rules from that file.

### 1.10 `scripts/`

Create `scripts/refresh_mwaa_wheels.sh` and `scripts/mwaa_requirements_preflight.py` from `references/scripts.md`.

---

## Phase 2: DAGs Folder

### 2.1 `dags/{PROJECT_FOLDER}/__init__.py`

Empty file.

### 2.2 `dags/sql/.gitkeep`

Empty placeholder. SQL scripts executed by `UNISnowflakeToSnowflakeTransfer` go here as `.sql` files. The operator resolves scripts at `{AIRFLOW_HOME}/dags/sql/{sql_script_name}`. so files in this folder are available on MWAA after the DAGs deployment syncs them.

Naming conventions: `{DAG_PREFIX}_{description}.sql` (e.g. `mexai_truncate_stg_entries.sql`)

SQL files should use **unqualified table names** - the operator passes `database` and `schema` to the Snowflake connection so the files works across environments without modification.

### 2.3 Airflow Configuration JSONs

For each environment in `ENVIRONMENTS`, create `dags/configurations/airflow_config_{env}.json`:

**dev** (mw1.micro - 2 workers max):

```json
{
    "core.max_active_runs_per_dag": "1",
    "core.default_task_retries": "1",
    "celery.worker_autoscale": "2,2",
    "core.fallback_to_env_variables": "True",
    "smtp.smtp_host": "smtp.unicor-inc.example",
    "smtp.smtp_mail_from": "{SUPPORT_EMAIL}",
    "smtp.smtp_ssl": "False",
    "smtp.smtp_starttls": "True",
    "celery.task_adoption_timeout": "1800",
    "scheduler.min_file_process_interval": "300",
    "core.enable_xcom_pickling": "True",
    "webserver.show_trigger_form_if_no_params": "True"
}
```

**uat** (mw1.small - 4 workers max):

Same as above but `"celery.worker_autoscale": "4,4"`

**prd** (mw1.small - 6 workers max):

Same as above but `"celery.worker_autoscale": "6,2"`

### 2.4 Environment Class Config JSONs

For each environment in `ENVIRONMENTS`, create `dags/configurations/env_class_config_{env}.json`:

| Environment | class     | min_workers | max_workers | webservers |
|-------------|-----------|-------------|-------------|------------|
| dev         | mw1.micro | 1           | 2           | 1          |
| uat         | mw1.small | 1           | 4           | 1          |
| prd         | mw1.small | 2           | 6           | 1          |

**mw1.micro enforces ALL five values to 1. The pipe validates them in order and fails on the first violation - so you see one error per run untill all are correct.**

Format (micro):

```json
{
    "class": "mw1.micro",
    "min_workers": 1,
    "max_workers": 1,
    "schedulers": 1,
    "min_webservers": 1,
    "max_webservers": 1
}
```

Format (small/medium):

```json
{
    "class": "mw1.small",
    "min_workers": 2,
    "max_workers": 6,
    "schedulers": 2,
    "min_webservers": 2,
    "max_webservers": 2
}
```

**Important:** If any environment uses a non default class, ask the user to confirm before using the default. The `restart_environment` pipeline epplies these configs via the `uni-udp-deploy-restart` pipe.

### 2.5 dbt Project Scaffold (if `INCLUDE_DBT=yes`)

Create the following under `dags/{PROJECT_FOLDER}/dbt`:

**`dbt_project.yml`:**

```yaml
name: '{PROJECT_FOLDER}'
version: '1.0.0'
config-version: 2

profile: '{DBT_PROFILE_NAME}'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
    - "{{ env_var('DBT_TARGET_PATH', 'target') }}/{PROJECT_FOLDER}"
    - "dbt_modules"

models:
    +transient: false
    +copy_grants: true
    +query_tag: dbt

tests:
    +store_failures: true
    +schema: {SNOWFLAKE_SCHEMA}_DBT_TEST_RESULTS

vars:
    default_hist_load_days: -30
    run_only_with_param_dates: False
    {PROJECT_FOLDER}_db_name: "{DEV_DB_NAME}"
    {PROJECT_FOLDER}_schema_name: "{SNOWFLAKE_SCHEMA}"

require-dbt-version: ">=1.9.0"
```

**`profiles.yml`:**

```yaml
{DBT_PROFILE_NAME}:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
      threads: "{{ env_var('SNOWFLAKE_THREADS') | int }}"
      client_session_keep_alive: True
elementary:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
      threads: "{{ env_var('SNOWFLAKE_THREADS') | int }}"
```

**`packages.yml`:**

```yaml
packages:
  - package: elementary-data/elementary
    version: 0.16.0
```

**`.dbtignore`:**

```
target/
dbt_packages/
logs/
```

**`__init__.py`:**

**Empty stub directories** (with `.gitkeep`):

`models/`, `macros/`, `snapshots/`, `seeds/`, `tests/`, `analysis/`

### 2.6 Stub DAG File

**Important Airflow 3.x notes:**

- Use `EmptyOperator` (NOT `DummyOperator` - removed in 3.x)
- Use `schedule=` (NOT `schedule_interval` - removed in 3.x)
- Import `datetime` from `datetime` direclty, not from `airflow.utils.dates`

Create `dags/{PROJECT_FOLDER}/dags/__init__.py` (empty) and `dags/{PROJECT_FOLDER}/dags/{DAG_PREFIX}_basic_dag.py`:

See `references/dag-template.md` for the full stub DAG template. Key substitutions:

- `{PROJECT_FOLDER}` -> Replace with your project folder name.
- `{DAG_PREFIX}` -> Replace with the desired prefix for your DAG file.
- `{SNOWFLAKE_CONN_ID}` -> Replace with the Snowflake connection ID used in Airflow.
- `{DOMAIN_CODE}` -> Replace with the domain code used in your project.
- Tags: `["{DOMAIN_CODE}", "{PROJECT_FOLDER}"]`

---

## Phase 3: Utility Modules

### 3.1 `dags/utils_uni/__init__.py`

Empty file.

### 3.2 `dags/utils_uni/common/__init__.py`

Empty file.

### 3.3 `dags/utils_uni/{PROJECT_FOLDER}/__init__.py`

Empty file.

### 3.4 `dags/utils_uni/{PROJECT_FOLDER}/uda_{DAG_PREFIX}_dag_util.py`

```python
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
        os.environ["DBT_TARGET_PATH"] = f"/tmp/target"

def get_time_window_expression(dag):
    start_time = dag.latest_execution_date
    end_time = datetime.now() + timedelta(hours=1)
    if start_time is None:
        start_time = end_time + timedelta(hours=-2)
    return "/" + start_time.strftime("%Y-%m-%d %H:") + "/ , /" + end_time.strftime("%Y-%m-%d %H:") + "/p"
```

### 3.5 `dags/utils_uni/{PROJECT_FOLDER}/uda_{DAG_PREFIX}_connection_util.py`

See `references/connection-utils.md` for the full template. Key substitutions:

- `{SNOWFLAKE_CONN_ID}` -> Replace with the Snowflake connection ID used in Airflow.

### 3.6 `dags/utils_uni/{PROJECT_FOLDER}/default_config.py`

```json
{
  "payload": {
    "category": "software",
    "impact": 6,
    "cmdb_ci": "{APPLICATION_CI}",
    "contact_type": "System Generated",
    "state": 1,
    "short_description": "",
    "description": "",
    "assignment_group": "TBD-SUPPORT-GROUP",
    "urgency_dict": {"C": 4, "H": 5, "M": 6, "L": 7},
    "urgency_name_mapping": {"C": "Critical", "H": "High", "M": "Medium", "L": "Low"}
  }
}
```

After creating this file, remind, the user to update `assignment_group` with ther ServiceNow assigment group.

## Phase 4: Airflow Variables

For each environment in `ENVIRONMENTS`, create `variables/{env}`:

### 4.1 `uda_global_vars.json`

```json
{
  "uda_global_vars": {
    "dag_schedule": "0 12 * * *",
    "send_failures_email_to": "{failure_email}",
    "airflow_env": "{Env}",
    "email_support": "{SUPPORT_EMAIL}"
  }
}
```

Where `{failure_email}` is `""` for non-prd and `"{SUPPORT_EMAIL}"` for prd. 

`{Env}` is titled-cased (Dev, Uat, Prd).

### 4.2 `uda_dbt_global_vars.json`

```json
{
  "uda_dbt_global_vars": {
    "dbt_project_vars": {
      "{PROJECT_FOLDER}_db_name": "{env_db_name}",
      "{PROJECT_FOLDER}_schema_name": "{SNOWFLAKE_SCHEMA}",
      "default_hist_load_days": -30,
      "run_only_with_param_dates": "False"
    }
  }
}
```

Where `{env_db_name}` is `DEV_DB_NAME` for dev, `UAT_DB_NAME` for uat, and `PRD_DB_NAME` for prd.

### 4.3 `uda_{PROJECT_FOLDER}_vars.json`

```json
{
  "uda_{PROJECT_FOLDER}_vars": {
    "dags_loaded": false,
    "dag_schedule": "0 12 * * *",
    "snowflake_conn_id": "{SNOWFLAKE_CONN_ID}"
  }
}
```

### 4.4 `{DAG_PREFIX}_basic_dag.json`

```json
{
  "{DAG_PREFIX}_basic_dag": {
    "max_active_runs": 1,
    "uda_env": "{env}",
    "send_failures_email_to": "{failure_email}",
    "email_support": "{SUPPORT_EMAIL}",
    "region": "{PRIMARY_REGION}",
    "snowflake_conn_id": "{SNOWFLAKE_CONN_ID}",
    "dbt_run_cmd": "",
    "dbt_base_cmd": "--select ",
    "servicenow_config": {
      "env": "{env}",
      "raise_incident": {raise_incident}
    }
  }
}
```

Where `{raise_incident}` is `false` for non-prd, `true` for prd.

## Phase 5: Flyway (if `INCLUDE_FLYWAY=yes`)

### 5.1 `flyway/config.yaml`

```yaml
dev:
  db.secrets: "/{DOMAIN_CODE}/dev/snowflake/snowflake"
  db.account: "unicorn_dev.us-east-1.privatelink"
  db.database: "{DEV_DB_NAME}"
  db.user: "{DEV_DB_NAME}_SYSADMIN_USER"
  db.warehouse: "{DEV_DB_NAME}_SYSADMIN_WH"
  db.schema: "{SNOWFLAKE_SCHEMA}"
  db.thread: 10
  db.role: "UNI-G-APP-SF-{DEV_DB_NAME}-SYSADMIN"
uat:
  db.secrets: "/{DOMAIN_CODE}/uat/snowflake/snowflake"
  db.account: "unicorn_uat.us-east-1.privatelink"
  db.database: "{UAT_DB_NAME}"
  db.user: "{UAT_DB_NAME}_SYSADMIN_USER"
  db.warehouse: "{UAT_DB_NAME}_SYSADMIN_WH"
  db.schema: "{SNOWFLAKE_SCHEMA}"
  db.thread: 10
  db.role: "UNI-G-APP-SF-{UAT_DB_NAME}-SYSADMIN"
prd:
  db.secrets: "/{DOMAIN_CODE}/prd/snowflake/snowflake"
  db.account: "unicorn_prd.us-east-1.privatelink"
  db.database: "{PRD_DB_NAME}"
  db.user: "{PRD_DB_NAME}_SYSADMIN_USER"
  db.warehouse: "{PRD_DB_NAME}_SYSADMIN_WH"
  db.schema: "{SNOWFLAKE_SCHEMA}"
  db.thread: 10
  db.role: "UNI-G-APP-SF-{PRD_DB_NAME}-SYSADMIN"
```

### 5.2 `flyway/snowflake_flyway_{env}.conf` (dev, uat, prd)

```
flyway.schemas=
flyway.table=uni-{DOMAIN_CODE}-airflow-schema-history
flyway.locations=flyway/sql
flyway.placeholders.env={env}
flyway.baselineOnMigrate=true
flyway.outOfOrder=true
```

### 5.3 `flyway/get_credentials.py`

```python
from udacore.config_reader import load_config
import argparse

parser = argparse.ArgumentParser(description="flyway deployment credetnaisl")
parser.add_argument("--f", required=True, help="This is the file path for config location")
args = parser.parse_args()
filepath = args.f
response = load_config(filepath)
with open("./export_vars", "w") as f:
    for key, value in response.items():
        if "SNOWFLAKE" in key.upper():
            f.writelines(f'export {key.upper().replace(".", "_")}={value}\n')
```

### 5.4 `flyway/flyway-requirements.txt`

```
uda-core-dataload==1.2.0
```

### 5.5 `flyway/sql/.gitkeep`

Empty placeholder - migrations go here as `V<YYYY.MM.DD.NN>__<description>.sql`

## Phase 6: Documentation & Memory

### 6.1 `.github/pull_request_template.md`

```markdown
## Jira Ticket

## Summary

## Type of Change

- [ ] New DAG
- [ ] DAG modification
- [ ] Bug fix
- [ ] Flyway migration
- [ ] Plugin / utility update
- [ ] Requierements update
- [ ] Configuration changes

## Acceptance Criteria

## Checklist

- [ ] DAG files follow naming convention (`{DAG_PREFIX}_*_dag.py`)
- [ ] Aiflow variable JSOn added/updated in `variables/<ev>/` for all environments
- [ ] dbt models compiled successfully (`dbt parse`) - if applicable
- [ ] No secrets or credentials commited
- [ ] No secret or credentials committed
- [ ] `requirements/requirements.txt` pinned versions updated if new package added
```

### 6.2 `README.md`

Create a README documenting:

- Project identiy table (MWAA env, S3 buckets, Snowflake secret, IAM role, Airflow version)
- Environment table (class + DR status for each env)
- Repository structure (tree diagram)
- Naming conventions table (DAG files, connection, Variables JSON, Flyway)
- Deploying section (list of GitHub Actions workflow names and what they do)
- Required GitHub configuration (Environments, `AWS_ACCOUNT_ID` variable, Artifactory secrets, IAM trust policy)
- Placeholder replacement section (database names that need updating before first deploy

### 6.3 Memory Files

Create `.memgory/` directory (gitignored) and the following files:

**`project_{DOMAIN_CODE}_mwaa.md`** (type: project):

- Project identity: domain, APPLICATION_CI, MWAA env anme, S3 bucket, Airflow version, environments
- Snowflake: connection ID, secret path, scheme, placeholder DB names
- Deployment role, Flyway table name

**`feedback_airflow3x.md`** (type: feedback):

- Use `EmptyOperator ` not `DummyOperator`
- Use `schedule=` not `schedule_interval=`
- Import `datetime` from `datetime` directly

**`.memory/MEMORY.md`**: One-line index of both memgory files

---

## Phase 7: Pos-Deploy Validation Runbook (Airflow 3,0,6 / Python 3.12)

Use this sequence after every requirements or DAG deployment

### 7.1 Validate MWAA update completed

```bash
aws mwaa get-environment \
    -name uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1 \
    --profile {DOMAIN_CODE}-<env> \
    --region {PRIMARY_REGION} \
    --no-cli-pager \
    --query 'Environment.{Status:Status,AirflowVersion:AirflowVersion,RequirementsS3Path:RequirementsS3Path,LastUpdateStatus:LastUpdate.Status,LastUpdateError:LastUpdate.Error,LastUpdateCreatedAt:LastUpdate.CreatedAt}' \
    --output json
```

Expected:

- `Status=AVAILABLE`
- `LastUpdateStatus=SUCCESS`

### 7.2 Verify the active requirements payload in S3

```bash
aws s3 cp s3://uni-<env>-{DOMAIN_CODE}-managed-airflow-code-ue1/requirements/requirements.txt - \
    --profile {DOMAIN_CODE}-<env> \
    --region {PRIMARY_REGION} \
    --no-cli-pager
```

This prevents debugging stale assumptions from local files.

### 7.3 Use the correct CloudWatch log groups

Do NOT use `/aws/mwaa/...` paths in this platform. Use:

- `airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-DAGProcessing`
- `airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-Scheduler`
- `airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-Task`
- `airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-WebServer`
- `airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-Worker`

Discovery command:

```bash
aws logs describe-log-groups \
    --region {PRIMARY_REGION} \
    --profile {DOMAIN_CODE}-<env> \
    --no-cli-pager \
    --log-group-name-prefix "airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1" \
    --query 'logGroups[*].logGroupName' \
    --output json
```

### 7.4 Pull latest scheduler/worker stream errors

```bash
for grp in Scheduler Worker; do
  stream=$(aws logs describe-log-streams \
    --region {PRIMARY_REGION} \
    --profile {DOMAIN_CODE}-<env> \
    --no-cli-pager \
    --log-group-name "airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-${grp}" \
    --order-by "LastEventTime" --descending  --max-items 1 \
    --query 'logStreams[0].logStreamName' --output text | head -n1)
  
  echo "===== ${grp}: ${stream} ====="
  aws logs get-log-events \
    --region {PRIMARY_REGION} \
    --profile {DOMAIN_CODE}-<env> \
    --no-cli-pager \
    --log-group-name "airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-${grp}" \
    --log-stream-name "$stream" \
    --limit 300 \
    --query 'events[].message' \
    --output text | tr '\t' '\n' | grep -iE 'error|importerror|modulenotfound|resolutionimpossible|connection not found'
done
```

Notes:

- CloudWatch `--filter-pattern` does not accept `||`. Use simple terms, or fetch events and filter with `grep`.
- Always add `--no-cli-pager` to avoid blocked terminal sessions.

### 7.5 Validate Snowflake connection wiring

If task fails with `Connection with ID <conn_id> not found`:

- Create/fix the Airflow connection in MWAA UI (`Admin -> Connections`).
- Ensure the `conn_id` matches `SNOWFLAKE_CONN_ID`.
- If using per-DAG overrides, set the DAG Variables JSON key `snowflake_conn_id` to the existing connection ID.

Important:

- If this environment, MWAA CLI subcommand `connections get` can be blocked (`sub command not allowed: get`).
- Use Ariflow UI and workger logs as source of truth.

### 7.6 Snowflake provider class compatibility (Ariflow 3.0.6)

`apache-airflow-providers-snowflake==6.4.0` does not include `SnowflakeOperator`.

Use one of:

- `SnowflakeSqlApiOperator`
- `SQLExecuteQueryOperator`

For legacy DAG compatibility, alias import pattern is acceptable:

```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator as SnowflakeOperator
```

### 7.7 Constrains-first dependency strategy

Always align to runtime constraints:

`https://raw.githubusercontent.com/apache/airflow/constraints-<AIRFLOW_VERSION>/constraints-<PYTHON_VERSION>.txt`

For Airflow 3.0.6 on Python 3.12:

`https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt`

Use local preflight before deploy:

- `scripts/mwaa_requirements_preflight.py --skip-resolver`
- `scripts/mwaa_requirements_preflight.py`

---

## Common Mistakes

| Mistake | Fix |
|---|---|
| Using `DummyOperator` | Use `EmptyOperator` — `DummyOperator` was removed in Airflow 3.x |
| Using `schedule_interval=` | Use `schedule=` — `schedule_interval` was removed in Airflow 3.x |
| `from airflow.utils.dates import datetime` | Use `from datetime import datetime` directly |
| Wrong env class in config JSON | dev = `mw1.micro`, uat/prd = `mw1.small` (default Terraform sizing) |
| Wrong Snowflake secret path | Path is `/{DOMAIN_CODE}/<env>/snowflake/snowflake`, NOT the old `/0042/<env>/LEGACYWH/`
| Adding `apache-airflow-providers-airbyte` | Only include if the project actually uses Airbyte |
| DB name placeholders in prd variables | Placeholders (`DEV_MEXAI_LEDGER` etc.) must be replaced with real Snowflake DB names before first deploy |
| One workflow per environment | Do NOT create `deploy-dev.yml`, `deploy-uat.yml` etc. Use the 5-workflow pattern; `AppEnv` and `environment:` come from the `deployment_environment` input |
| `domain` as a pipeline variable | `domain` is hardcoded to `"{DOMAIN_CODE}"` in the pipe variables, not a pipeline variable |
| Bare `requirements.txt` in .gitignore | Use `/requirements.txt` (root-anchored) — bare pattern matches `requirements/requirements.txt` and prevents it from being committed |
| `ARTIFACTORY_USER` in docker login | The secret is `ARTIFACTORY_USERNAME` (matches infra pipeline). `secrets.ARTIFACTORY_USER` is always empty → Bad Credentials |
| Using selective `domain_dags_folder_path` | Always use `/dags` as the only allowed value. Deploying just `/dags/{PROJECT_FOLDER}` leaves `utils_uni/` out of S3 → `ModuleNotFoundError` at runtime. One path, one sync, no partial deploys. |
| UDP core files fail on Airflow 3.x | The `uni-udp-deploy-dags` pipe auto-deploys UDP core utilities (e.g. `udp_notification_manager.py`, `udp_servicenow_integration_manager.py`) that import `from airflow.hooks.base_hook import BaseHook` — removed in Airflow 3.x. These are not in your repo; raise a compatibility ticket with the DSO/UDP team to update the pipe for Airflow 3.x. |
| Using UDP plugin operators (`UNIS3ToSnowflakeTransfer`, `UNISnowflakeToSnowflakeTransfer`) | These UDP plugin classes are not available in Airflow 3.x MWAA. Replace with `SnowflakeOperator` from `apache-airflow-providers-snowflake`. For S3-to-Snowflake loads, use `SnowflakeOperator` with a `COPY INTO` SQL statement (pass `storage_integration`, `file_format`, `s3_keys` as SQL params). |
| `dag=dag` on operators | Removed in Airflow 3.x. Use `with dag:` context manager instead. |
| Wiz scan failing with no credentials | If a Wiz scan step is added, it scans the deployment pipe image (`uni-udp-deploy-dags:3.1`) for CVEs — it does NOT scan project code. Requires `WIZ_CLIENT_ID`, `WIZ_CLIENT_SECRET`, `WIZ_PROJECT_ID`, `WIZ_IMAGE_VUL_SCAN_POLICY` repository secrets. If the scan fails on policy violations, the pipe image itself has CVEs — contact the UDP team for an updated image or a Wiz policy exception. |
| Pinning `SQLAlchemy` or `werkzeug` in requirements.txt | For Airflow 3.x MWAA, `werkzeug==2.2.3` conflicts with Flask 3.x (requires >=3.1), `SQLAlchemy==1.4.54` conflicts with Airflow 3.x (uses 2.x). Remove both.| 
| pip backtracking 30+ min on Airflow 3.x deps | Pin the key transitive packages that pip backtracks through: `fastapi==0.115.12`, `fastapi-cli==0.0.31`, `typing-inspection==0.4.3`. The `uni-udp-deploy-plugins` pipe processes requirements.txt line-by-line — do NOT use `--constraint "URL"` directives, they get passed directly to pip as arguments and produce `Usage: pip3 [options]`. |
| Using wrong Airflow constraints URL (e.g. `constraints-2.7.2/constraints-3.11.txt`) | Match both runtime versions: use `https://raw.githubusercontent.com/apache/airflow/constraints-<AIRFLOW_VERSION>/constraints-<PYTHON_VERSION>.txt` (for MWAA Airflow 3.0.6/Python 3.12: `constraints-3.0.6/constraints-3.12.txt`). Align Snowflake/Amazon pins to that file before deploy. |
| Plugins under `dags/plugins/` | Plugins live in a top-level `plugins/` directory (create `plugins/.gitkeep` even with no custom plugins). Under `dags/` the DAG sync pushes them to the DAGs S3 path where Airflow imports them as DAGs and poisons `sys.modules`. See `references/plugins.md`. |
| Untrusted `${{ inputs.* }}` inside `run:` scripts | Free-text inputs interpolated into shell are an injection risk. Pass them through `env:` and reference the shell variable. Use `type: choice` wherever the values are known |
| Missing `permissions: id-token: write` | OIDC role assumption fails with `Credentials could not be loaded`. Add it at workflow or job level (workflow 05 uses keys and does not need it) |
| Per-environment role ARN variables | Not needed. Construct the ARN from the Environment variable: `arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/platform/workloads/UNI-AR-Github-{DOMAIN_CODE}-deployment` |
| Missing `dbt deps` before `dbt parse` | `dbt parse` fails if `dbt_packages/` is absent. Always run `dbt deps` first in the pipeline script |
| Repo not in IAM trust policy | The deployment role's trust policy in `terraform-aws-{DOMAIN_CODE}-app-init/configurations/global.tfvars` must trust GitHub's OIDC provider for `repo:<org>/<repo>:environment:<env>` (the `sub` claim). Add it and re-apply `configurations/10_iam_roles` or every OIDC call returns AccessDenied |
| Pipes are Bitbucket Pipes | `uni-udp-deploy-*` and `uni-flyway-pipe` are run with `docker run` and plain AWS credentials. Confirm with the UDP/DSO team that the pipe versions work outside Bitbucket |
| `ImportError: cannot import name 'SnowflakeOperator'` with provider 6.4.0 | `SnowflakeOperator` was removed. Import `SnowflakeSqlApiOperator` (or `SQLExecuteQueryOperator`) from `airflow.providers.snowflake.operators.snowflake`. |
| MWAA requirements update is `SUCCESS` but DAG still fails with `missing_snowflake_provider`/connection errors | Re-run from a fresh DAG run and inspect current worker stream. This usually indicates runtime config mismatch, not unresolved requirements. |
| `Connection with ID mexai_snowflake not found` | Create/update the Airflow Connection in MWAA UI, or override `snowflake_conn_id` via DAG Variable JSON to match an existing conn ID. |
| Using `/aws/mwaa/<env>/worker` CloudWatch paths | In this platform, log groups are named `airflow-uni-<env>-{DOMAIN_CODE}-managed-airflow-ue1-`. Use `describe-log-groups` to discover exact names. |
| `Invalid character(s) in term '|'` from `aws logs filter-log-events` | CloudWatch filter syntax does not support `||`. Filter by one term, or fetch recent events then filter locally with `grep -E`. |
| `sub command not allowed: get` from MWAA CLI API | Some Airflow CLI subcommands are restricted. Validate connections from Airflow UI and task/worker logs instead of relying on CLI `connections get`. |
| dbt packages pinned in MWAA `requirements.txt` keep failing on Airflow 3.0.6/Python 3.12 | `dbt-core`/`dbt-common` can conflict with `protobuf`/`isodate` in Airflow constraints. Keep MWAA runtime requirements minimal (Airflow/providers/runtime libs) and run dbt outside MWAA if constraints cannot be satisfied. |

---

## Invocation

Trigger this skill with:

- `scaffold mwaa project`
- `new airflow project`
- `mwaa scaffold`
- `/mwaa-project-scaffold`

### 4217 Quick Start

Use this when triaging Airflow 3.0.6 / Python 3.12 deployment and runtime issues for domain  4217.

1. Confirm environment  update status:

```bash
aws mwaa get-environment \
  --name uni-dev-4217-managed-airflow-ue1 \
  --profile 4217-dev \
  --region us-east-1 \
  --no-cli-pager \
  --query 'Environment.{Status:Status,LastUpdateStatus:LastUpdate.Status,LastUpdateError:LastUpdate.Error,LastUpdateCreatedAt:LastUpdate.CreatedAt}' \
  --output json
```

2. Confirm active requirements in MWAA S3 bucket:

```bash
aws s3 cp s3://uni-dev-4217-managed-airflow-code-ue1/requirements/requirements.txt requirements.txt \
  --profile 4217-dev \
  --region us-east-1 \
  --no-cli-pager
```

3. Discover correct CLoudWatch log groups:

```bash
aws logs describe-log-groups \
  --region us-east-1 \
  --profile 4217-dev \
  --no-cli-pager \
  --log-group-name-prefix airflow-uni-dev-4217-managed-airflow-ue1 \
  --query 'logGroups[].logGroupName' \
  --output text
```

4. Pull latest scheduler and worker errors:

```bash
for grp in Scheduler Worker; do
  stream=$(aws logs describe-log-streams \
    --region us-east-1 \
    --profile 4217-dev \
    --no-cli-pager \
    --log-group-name "airflow-uni-4217-managed-airflow-ue1-${grp}" \
    --order-by "LastEventTime" --descending  --max-items 1 \
    --query 'logStreams[0].logStreamName' --output text | head -n1)
  
  echo "===== ${grp}: ${stream} ====="
  aws logs get-log-events \
    --region us-east-1 \
    --profile 4217-dev \
    --no-cli-pager \
    --log-group-name "airflow-uni-4217-managed-airflow-ue1-${grp}" \
    --log-stream-name "$stream" \
    --limit 300 \
    --query 'events[].message' \
    --output text | tr '\t' '\n' | grep -iE 'error|importerror|modulenotfound|resolutionimpossible|connection not found'
done
```

5. If runtime fails with connection not found

- Create or update Airflow Connection mexai_snowflake in MWAA UI (`Admin -> Connections`)
- If your env uses another conn ID, set the DAG Variable JSON key `snowflake_conn_id` to that existing conn ID.

Expected progression:

- Requirementsupdate show SUCCESS.
- DAG import succeeds (no snowflakeOperator import error).
- Task move from missing_snowflake_provider failure to actual Snowflake task execution.
- Remaining failures are connection, auth, role, or SQL-level issues.



### Examples

```
User: scaffold a new mwaa project for domain 4217

Skill asks:
  DOMAIN_CODE: 4217
  APPLICATION_CI: MexAI App
  PROJECT_FOLDER: mexai_app
  DAG_PREFIX: mexai
  SNOWFLAKE_CONN_ID: mexai_snowflake
  DBT_PROFILE_NAME: 4217-mexai-app
  SUPPORT_EMAIL: german.gonzalezcueva@slalom.com

Derived values shown for confirmation:
  IAM role: UNI-AR-Github-4217-deployment
  MWAA env: uni-<env>-4217-managed-airflow-ue1
  S3 bucket: uni-<env>-4217-managed-airflow-code-ue1
  Secret: /4217/<env>/snowflake/snowflake
  Flyway table: uni-4217-airflow-schema-history

Phases:
  Phase 0: Gather variables + confirmation
  Phase 1: Core infrastructure (workflows, gitignore, uda_core, requirements, plugins, scripts)
           -> Use domain_dags_folder_path=/dags on first deployment to sync
             both mexai_app/ and utils_uni/ in one shot.
           -> .github/workflows has 5 workflows:
             01 Deploy DAGs
             02 Deploy DAGs and DB Migration
             03 Database Migration Only
             04 Deploy Plugins and Restart
             05 Restart Environment
             All use the deployment_environment input (dev,uat,prd)
             mapped to a GitHub Environment of the same name.
  Phase 2: DAGs folder (configuration, dbg scaffold, stub DAG)
  Phase 3: Utility modules (dag_util, connection_util, default_config)
  Phase 4: Airflow Variables (per-env JSON files)
  Phase 5: Flyway (config.yaml, .conf files, sql/)
  Phase 6: Documentation + memory files (README.md, memory.md)
```

### Prerequisites

- An empty git repo cloned locally
- Claude Code with Bash, Read, Write, Edit tools
- Infrastructure already provisioned (MWAA environment, S3 bucket, IAM roles)

---

## 2026 Hardening Addendum (MWAA 3.x)

Apply these defaults for new scaffolds unless the user explicitly opts out.

### Dependency and packaging defaults

- Treat MWAA environments as **no-egress by default** for pip install at runtime.
- Generate wheels in CI during plugin deploy (do not store wheel binaries in the repo).
- Add `plugins/*.whl` and `.venv-mwaa-preflight/` to `.gitignore` (already in 1.1).
- Keep `requirements/requirements.txt` limited to direct deps only.
- For Airflow 3.0.6 / Python 3.12 baseline use:
  - `apache-airflow-providers-amazon`
  - `apache-airflow-providers-snowflake==6.4.0`
  - `snowflake-connector-python==3.17.2`
  - `boto3==1.39.11`, `s3fs=2025.7.0`, `fsspec==2025.7.0`, `openpyxl==3.1.5`

### Required scaffold file

Always create (source in `references/scripts.md`):

- `scripts/refresh_mwaa_wheels.sh`
- `scripts/mwaa_requirements_preflight.py`

This script should:

1. Clear existing `plugins/*.whl` files.
2. Download Snowflake provider + connector wheels into the top-level `plugins/` directory.
3. Download provider-required runtime wheels for offline install (e.g. `pandas`, `numpy`, `pyarrow`, `python-dateutil`, `pytz`, `tzdata`, `six`).
4. Download connector transitive wheels (`cryptography`, `cffi`, `pyopenssl`, `requests`, etc.)
5. Run `scripts/mwaa_requirements_preflight.py --skip-resolver` for static constraint checks.

### Pipeline defaults

In `.github/workflows/04-deploy-plugins-and-restart.yml`, the plugin deploy job must:

- use `actions/setup-python` with `python-version: "3.12"`.
- run `bash scripts/refresh_mwaa_wheels.sh` before the `uni-udp-deploy-plugins` container.

### Deployment sequencing guardrail

After successful `04 Deploy Plugins and Restart`, run `01 Deploy DAGs` (or combined deploy) to ensure latest DAG code synced. Otherwise may display stale DAG import errors from older files.
