# Plugins

Plugins live in a top-level `plugins/` directory, not inside `dags/`. Placing them inside `dags/` means the DAGs deployment syncs them to the MWAA DAGs S3 path, where Airflow's scanner imports them as potential DAGs and poisons `sys.modules` — causing `ImportError` in every DAG that uses the plugin even when `plugins.zip` is correct.

## Required folder structure

```
plugins/
    .gitkeep
    README.md ← document Airflow 3.x rules for the team
    my_operator.py
    my_hook.py
dags/
    ... ← no plugins/ subfolder here
```

## Plugin file rules

### 1. Every plugin that should be visible in Airflow must register AirflowPlugin

```python
from airflow.plugins_manager import AirflowPlugin

class MyPlugin(AirflowPlugin):
    name = "my_plugin" # unique across all plugins
    operators = [MyOperator]
    hooks = [MyHook]
```

Files without an `AirflowPlugin` subclass are support modules (importable by other plugins but invisible to the plugin manager).

### 2. Provider imports must be deferred — never at module level

Airflow loads plugins at startup before all providers are fully initialised. A module-level `from airflow.providers.X import Y` fails silently, leaving classes undefined and causing `ImportError: cannot import name` in every DAG that uses them.

```python
# WRONG — module-level provider import crashes plugin loading
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class MyOperator(BaseOperator):
    def execute(self, context):
        hook = SnowflakeHook(...)

# CORRECT — defer to the method that needs it
class MyOperator(BaseOperator):
    def execute(self, context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(...)
```

`airflow.models`, `airflow.plugins_manager`, and other base Airflow packages are safe at module level. Only `airflow.providers.` needs to be deferred.

### 3. Airflow 3.x breaking changes — common Airflow 2.x patterns that break silently


| Airflow 2.x (broken) | Airflow 3.x (correct) |
|---|---|
| `from airflow.hooks.dbapi_hook import DbApiHook` | `from airflow.providers.common.sql.hooks.sql import DbApiHook` |
| `from airflow.hooks.dbapi import DbApiHook` | `from airflow.providers.common.sql.hooks.sql import DbApiHook` |
| `from airflow.contrib.hooks.snowflake_hook import SnowflakeHook` | `from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook` |
| `from airflow.utils.decorators import apply_defaults` + `@apply_defaults` | Remove entirely — `BaseOperator` applies defaults automatically |

Avoid writing custom hook classes that extend `DbApiHook` or replicate provider functionality. Use `apache-airflow-providers-snowflake` (already in `requirements.txt`) directly.

### 4. Variable.get() at DAG parse time raises an error in Airflow 3.x

```python
# WRONG — raises AirflowRuntimeError if variable missing, and Airflow 3.x
# SDK enforces that variables are only readable during task execution
dag_metadata = Variable.get("my_dag", deserialize_json=True)

# CORRECT — use default_var or move into task body
dag_metadata = Variable.get("my_dag", default_var={}, deserialize_json=True)
```

If a variable is required and has no sensible default, ensure it is deployed via `variables/{env}/` and the DAG deploy workflow runs with `DOMAIN_VARIABLES_FOLDER_PATH` set.

## Preflight validation

The `04 Deploy Plugins and Restart` workflow runs a `validate-plugins` job before deploying. It:

1. Installs from `requirements/requirements.txt` (matching the MWAA environment)
2. Runs `python -m py_compile` on every plugin file (syntax check)
3. Import-tests each file with `importlib` (catches removed APIs, missing packages)

A plugin that fails the preflight blocks deployment. Fix the import error before pushing.

---

## Operational Lessons (No-egress MWAA)

### Symptom

`No matching distribution found` for packages that should exist (for example `pandas` via `apache-airflow-providers-snowflake`).

### Cause

MWAA pip install is resolving from local plugin wheel links only; transitive dependency wheels are missing.

### Fix pattern

- Keep requirements focused on direct dependencies.
- Build full offline wheel set in CI with `scripts/refresh_mwaa_wheels.sh`.
- Deploy plugins/requirements again.
- Then deploy DAGs to ensure latest DAG code is in sync.

### DAG import noise after dependency fix

If plugin install succeeds but UI still shows old import errors, run DAG deploy (`01 Deploy DAGs`) to replace stale DAG objects in MWAA code S3 path.
