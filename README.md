# MexAI App - MWAA Airflow project (domain 4217)

Airflow DAGs, dbt project, Airflow Variables and Flyway migrations for the `MexAI App` application on Amazon MWAA.

## Project identity

| Item | Value |
|---|---|
| Domain code | `4217` |
| Application CI | `MexAI App` |
| MWAA environment | `uni-<env>-4217-managed-airflow-ue1` |
| S3 DAG bucket | `uni-<env>-4217-managed-airflow-code-ue1` |
| Snowflake secret | `/4217/<env>/snowflake/snowflake` |
| Snowflake connection ID | `mexai_snowflake` |
| Deployment IAM role | `UNI-AR-Github-4217-deployment` |
| Airflow version | `3.0.6` (Python 3.12) |
| Region | `us-east-1` |
| Support | germanngc+claudeai@gmail.com |

## Environments

| Env | MWAA class | Workers (min-max) | DR |
|---|---|---|---|
| dev | mw1.micro | 1-1 | none |
| uat | mw1.small | 1-4 | none |
| prd | mw1.small | 2-6 | none (confirm with platform team) |

## Repository structure

```
.github/
  actions/mwaa-context/    composite action (region code, bucket, MWAA name)
  workflows/               01-05 deployment workflows
  pull_request_template.md
dags/
  configurations/          airflow_config_<env>.json, env_class_config_<env>.json
  sql/                     SQL scripts (mexai_<description>.sql)
  utils_uni/               shared utilities (+ mexai_app/ helpers)
  mexai_app/
    dags/                  DAG files (mexai_*_dag.py)
    dbt/                   dbt project
flyway/                    Snowflake migrations (config, .conf per env, sql/)
plugins/                   top-level plugins (never under dags/)
requirements/requirements.txt
scripts/                   wheel refresh + requirements preflight
uda_core/                  helper scripts
variables/<env>/           Airflow Variables JSON per environment
```

## Naming conventions

| Item | Convention |
|---|---|
| DAG files | `mexai_<name>_dag.py` |
| Snowflake connection | `mexai_snowflake` |
| Variables JSON | `variables/<env>/mexai_<dag>.json`, `uda_*_vars.json` |
| SQL scripts | `dags/sql/mexai_<description>.sql` |
| Flyway migrations | `flyway/sql/V<YYYY.MM.DD.NN>__<description>.sql` |
| Flyway history table | `uni-4217-airflow-schema-history` |

## Deploying

All workflows are manual (`workflow_dispatch`) and take a `deployment_environment` input (dev, uat, prd).

| Workflow | What it does |
|---|---|
| 01 Deploy DAGs | Syncs `/dags` and `variables/<env>` to the MWAA S3 bucket |
| 02 Deploy DAGs and DB Migration | Runs Flyway, then deploys DAGs |
| 03 Database Migration Only | Runs Flyway only |
| 04 Deploy Plugins and Restart | Builds offline wheels, uploads plugins/requirements, restarts MWAA |
| 05 Restart Environment | Applies `airflow_config` / `env_class_config` and restarts MWAA |

After `04`, run `01` so the latest DAG code is synced.

## Required GitHub configuration

- GitHub Environments: `dev`, `uat`, `prd` (add required reviewers on `prd`).
- Environment variable `AWS_ACCOUNT_ID` on each Environment.
- Environment secrets `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (workflow 05 only).
- Repository secrets `ARTIFACTORY_USERNAME` and `ARTIFACTORY_PASSWORD`.
- IAM trust policy: the role `UNI-AR-Github-4217-deployment` must trust GitHub's OIDC provider for `repo:<org>/<repo>:environment:<env>` (set in `terraform-aws-4217-app-init/configurations/global.tfvars`, re-apply `configurations/10_iam_roles`).
- The `uni-udp-deploy-*` and `uni-flyway-pipe` images were built as Bitbucket Pipes. Confirm with the UDP/DSO team that they work with plain AWS credentials.

## Placeholders to replace before first deploy

| Placeholder | Where |
|---|---|
| `DEV_MEXAI_APP`, `UAT_MEXAI_APP`, `PRD_MEXAI_APP` | `flyway/config.yaml`, `variables/<env>/uda_dbt_global_vars.json`, `dbt_project.yml` |
| `MEXAI_APP` | schema in the same files |
| `TBD-SUPPORT-GROUP` | `dags/utils_uni/mexai_app/default_config.py` (ServiceNow assignment group) |
| `unicorn_<env>.us-east-1.privatelink` | `flyway/config.yaml` Snowflake account |
