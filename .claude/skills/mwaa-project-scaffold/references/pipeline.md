# GitHub Actions Pipeline Template

Full GitHub Actions template for a Unicorn Inc MWAA DAG repo. Translated from the Bitbucket template (kept as `pipeline.bitbucket.md.bak`).

Apply substitutions:

- `{DOMAIN_CODE}` - 4-digit domain code
- `{PROJECT_FOLDER}` - dbt/DAG folder name (e.g. `mexai_app`)
- `{APPLICATION_CI}` - ServiceNow app name
- `{PRIMARY_REGION}` - primary AWS region (default `us-east-1`)
- `{AIRFLOW_VERSION}` - MWAA Airflow version (default `3.0.6`)

## Workflow structure (5 workflows + 1 composite action)

| # | File | Name | Purpose |
|---|---|---|---|
| 01 | `.github/workflows/01-deploy-dags.yml` | 01 Deploy DAGs | Sync DAGs + variables to MWAA S3 |
| 02 | `.github/workflows/02-deploy-dags-and-db-migration.yml` | 02 Deploy DAGs and DB Migration | Flyway first, then DAG deploy |
| 03 | `.github/workflows/03-database-migration-only.yml` | 03 Database Migration Only | Flyway only |
| 04 | `.github/workflows/04-deploy-plugins-and-restart.yml` | 04 Deploy Plugins and Restart | Upload plugins/requirements, restart MWAA |
| 05 | `.github/workflows/05-restart-environment.yml` | 05 Restart Environment | Apply airflow_config + env_class changes |
| - | `.github/actions/mwaa-context/action.yml` | (composite action) | Derives region code, bucket and MWAA names |

Omit workflows 02 and 03 if `INCLUDE_FLYWAY=no`. All workflows are `workflow_dispatch` only (manual, like Bitbucket custom pipelines).

## Bitbucket -> GitHub Actions mapping

| Bitbucket | GitHub Actions |
|---|---|
| `pipelines: custom:` | `on: workflow_dispatch` |
| pipeline `variables` / `allowed-values` | `workflow_dispatch.inputs` with `type: choice` |
| Workspace variables `awsaccountid_<env>` | **Environment variable** `AWS_ACCOUNT_ID` on each GitHub Environment (`dev`, `uat`, `prd`) |
| `AWS_ACCESS_KEY_ID_<ENV>` workspace vars | **Environment secrets** `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` per GitHub Environment |
| `oidc: true` + `BITBUCKET_STEP_OIDC_TOKEN` | `permissions: id-token: write` + `aws-actions/configure-aws-credentials` |
| `runs-on: [self.hosted, ...]` | `runs-on: [self-hosted, linux, dso.prd.runner]` |
| `pipe: docker://image` | `docker run` of the same image, env vars passed with `-e` |
| `caches: pip/docker` | `actions/setup-python` with `cache: pip` |
| `$BITBUCKET_CLONE_DIR` | `$GITHUB_WORKSPACE` |
| IAM role `UNI-AR-Bitbucket-{DOMAIN_CODE}-deployment` | IAM role `UNI-AR-Github-{DOMAIN_CODE}-deployment` |
| Trust policy keyed on `BITBUCKET_REPO_UUID` | Trust policy keyed on `token.actions.githubusercontent.com:sub` (`repo:<org>/<repo>:environment:<env>`) |

## Required GitHub configuration

Create three GitHub Environments: `dev`, `uat`, `prd` (add required reviewers on `prd`).

| Where | Name | Example |
|---|---|---|
| Environment variable (each env) | `AWS_ACCOUNT_ID` | `222222222222` (dev), `333333333333` (uat), `444444444444` (prd) |
| Environment secret (each env, workflow 05 only) | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | key-based auth |
| Repository secret | `ARTIFACTORY_USERNAME`, `ARTIFACTORY_PASSWORD` | Artifactory credentials (note: USERNAME, not USER) |

**Important - pipe images.** The `uni-udp-deploy-*` and `uni-flyway-pipe` images were built as Bitbucket Pipes. Bitbucket injects `BITBUCKET_*` variables and the OIDC token itself. These templates give the containers AWS credentials through the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` variables obtained from the OIDC role. Verify with the UDP/DSO team that the pipe versions accept plain AWS credentials (or ask for GitHub-compatible versions) before the first run.

## Composite action: `.github/actions/mwaa-context/action.yml`

```yaml
name: MWAA context
description: Derive region code, S3 bucket and MWAA environment name
inputs:
  env:
    description: Deployment environment (dev, uat, prd)
    required: true
  region:
    description: AWS region
    required: true
outputs:
  short_region:
    value: ${{ steps.derive.outputs.short_region }}
  mwaa_instance_name:
    value: ${{ steps.derive.outputs.mwaa_instance_name }}
  s3_bucket:
    value: ${{ steps.derive.outputs.s3_bucket }}
runs:
  using: composite
  steps:
    - id: derive
      shell: bash
      run: |
        IFS=- read -r a b c <<< "${{ inputs.region }}"
        short_region="${a:0:1}${b:0:1}${c:0:1}"
        echo "short_region=$short_region" >> "$GITHUB_OUTPUT"
        echo "mwaa_instance_name=uni-${{ inputs.env }}-{DOMAIN_CODE}-managed-airflow-${short_region}" >> "$GITHUB_OUTPUT"
        echo "s3_bucket=uni-${{ inputs.env }}-{DOMAIN_CODE}-managed-airflow-code-${short_region}" >> "$GITHUB_OUTPUT"
```

## 01 Deploy DAGs - `.github/workflows/01-deploy-dags.yml`

```yaml
name: 01 Deploy DAGs

on:
  workflow_dispatch:
    inputs:
      deployment_environment:
        description: Target environment
        type: choice
        required: true
        options: [dev, uat, prd]   # list all environments here
      region:
        type: choice
        default: "{PRIMARY_REGION}"
        options: ["{PRIMARY_REGION}", "us-west-2"]
      deployment_type:
        type: choice
        default: OVERWRITE
        options: [UPDATE, OVERWRITE]
      domain_dags_folder_path:
        type: choice
        default: /dags
        options: [/dags]
      domain_variables_folder_path:
        description: Leave empty to use /variables/<env>
        type: string
        default: ""
  workflow_call:
    inputs:
      deployment_environment: { type: string, required: true }
      region: { type: string, default: "{PRIMARY_REGION}" }
      deployment_type: { type: string, default: OVERWRITE }
      domain_dags_folder_path: { type: string, default: /dags }
      domain_variables_folder_path: { type: string, default: "" }

concurrency:
  group: mwaa-{DOMAIN_CODE}-${{ inputs.deployment_environment }}
  cancel-in-progress: false

permissions:
  id-token: write
  contents: read

jobs:
  deploy-dags:
    name: Dbt Dags Deployment
    runs-on: [self-hosted, linux, dso.prd.runner]
    environment: ${{ inputs.deployment_environment }}
    env:
      AppEnv: ${{ inputs.deployment_environment }}
      region: ${{ inputs.region }}
    steps:
      - uses: actions/checkout@v4

      - id: ctx
        uses: ./.github/actions/mwaa-context
        with:
          env: ${{ inputs.deployment_environment }}
          region: ${{ inputs.region }}

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/platform/workloads/UNI-AR-Github-{DOMAIN_CODE}-deployment
          aws-region: ${{ inputs.region }}

      - name: Load Snowflake settings from Secrets Manager
        run: |
          SECRET_JSON=$(aws secretsmanager get-secret-value \
            --secret-id /{DOMAIN_CODE}/$AppEnv/snowflake/snowflake \
            --region {PRIMARY_REGION} --query SecretString --output text)
          echo "::add-mask::$(echo "$SECRET_JSON" | jq -r '.accesskey')"
          {
            echo "SNOWFLAKE_ACCOUNT=$(echo "$SECRET_JSON" | jq -r '.account')"
            echo "SNOWFLAKE_USER=$(echo "$SECRET_JSON" | jq -r '.user')"
            echo "SNOWFLAKE_ACCESSKEY=$(echo "$SECRET_JSON" | jq -r '.accesskey')"
            echo "SNOWFLAKE_DATABASE=$(echo "$SECRET_JSON" | jq -r '.database')"
            echo "SNOWFLAKE_SCHEMA=$(echo "$SECRET_JSON" | jq -r '.schema')"
            echo "SNOWFLAKE_WAREHOUSE=$(echo "$SECRET_JSON" | jq -r '.warehouse')"
            echo "SNOWFLAKE_THREADS=10"
          } >> "$GITHUB_ENV"

      - name: dbt deps and parse
        run: |
          pip install dbt-core==1.9.0 dbt-snowflake==1.9.0
          cd dags/{PROJECT_FOLDER}/dbt
          dbt deps
          dbt parse

      - name: Docker login (Artifactory)
        run: echo "${{ secrets.ARTIFACTORY_PASSWORD }}" | docker login docker-prod.artifactory.unicorn-inc.example -u "${{ secrets.ARTIFACTORY_USERNAME }}" --password-stdin

      - name: Deploy DAGs (uni-udp-deploy-dags)
        env:
          VARIABLES_PATH_INPUT: ${{ inputs.domain_variables_folder_path }}
        run: |
          variables_path="${VARIABLES_PATH_INPUT:-/variables/$AppEnv}"
          docker run --rm \
            -v "$GITHUB_WORKSPACE":/workspace -w /workspace \
            -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
            -e AWS_DEFAULT_REGION="${{ inputs.region }}" \
            -e ENVIRONMENT="$AppEnv" \
            -e DOMAIN="{DOMAIN_CODE}" \
            -e DEPLOYMENT_TYPE="${{ inputs.deployment_type }}" \
            -e DOMAINS_DAGS_FOLDER_PATH="${{ inputs.domain_dags_folder_path }}" \
            -e DOMAIN_VARIABLES_FOLDER_PATH="$variables_path" \
            -e S3_BUCKET="${{ steps.ctx.outputs.s3_bucket }}" \
            -e MWAA_NAME="${{ steps.ctx.outputs.mwaa_instance_name }}" \
            docker-prod.artifactory.unicorn-inc.example/uni-udp-deploy-dags:3.1
```

## 02 Deploy DAGs and DB Migration - `.github/workflows/02-deploy-dags-and-db-migration.yml`

Same inputs and permissions as 01. Runs the Flyway job, then the DAG deploy. To avoid duplicating the deploy job, make 01 and 03 reusable (`on: workflow_call` with the same inputs) and call them:

```yaml
name: 02 Deploy DAGs and DB Migration

on:
  workflow_dispatch:
    inputs:
      deployment_environment:
        type: choice
        required: true
        options: [dev, uat, prd]
      region:
        type: choice
        default: "{PRIMARY_REGION}"
        options: ["{PRIMARY_REGION}", "us-west-2"]
      deployment_type:
        type: choice
        default: OVERWRITE
        options: [UPDATE, OVERWRITE]
      domain_dags_folder_path:
        type: choice
        default: /dags
        options: [/dags]
      domain_variables_folder_path:
        type: string
        default: ""

permissions:
  id-token: write
  contents: read

jobs:
  migrate:
    uses: ./.github/workflows/03-database-migration-only.yml
    with:
      deployment_environment: ${{ inputs.deployment_environment }}
      region: ${{ inputs.region }}
    secrets: inherit

  deploy:
    needs: migrate
    uses: ./.github/workflows/01-deploy-dags.yml
    with:
      deployment_environment: ${{ inputs.deployment_environment }}
      region: ${{ inputs.region }}
      deployment_type: ${{ inputs.deployment_type }}
      domain_dags_folder_path: ${{ inputs.domain_dags_folder_path }}
      domain_variables_folder_path: ${{ inputs.domain_variables_folder_path }}
    secrets: inherit
```

01 and 03 both declare `workflow_call` with the same inputs as their `workflow_dispatch` trigger (already shown in their sections below), and their jobs read `inputs.*`, which works for both triggers.

## 03 Database Migration Only - `.github/workflows/03-database-migration-only.yml`

```yaml
name: 03 Database Migration Only

on:
  workflow_dispatch:
    inputs:
      deployment_environment:
        type: choice
        required: true
        options: [dev, uat, prd]
      region:
        type: choice
        default: "{PRIMARY_REGION}"
        options: ["{PRIMARY_REGION}", "us-west-2"]
  workflow_call:
    inputs:
      deployment_environment: { type: string, required: true }
      region: { type: string, default: "{PRIMARY_REGION}" }

concurrency:
  group: mwaa-{DOMAIN_CODE}-${{ inputs.deployment_environment }}
  cancel-in-progress: false

permissions:
  id-token: write
  contents: read

jobs:
  database-migration:
    name: Database Migration
    runs-on: [self-hosted, linux, dso.prd.runner]
    environment: ${{ inputs.deployment_environment }}
    env:
      AppEnv: ${{ inputs.deployment_environment }}
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/platform/workloads/UNI-AR-Github-{DOMAIN_CODE}-deployment
          aws-region: ${{ inputs.region }}

      - name: Docker login (Artifactory)
        run: echo "${{ secrets.ARTIFACTORY_PASSWORD }}" | docker login docker.artifactory.unicorn-inc.example -u "${{ secrets.ARTIFACTORY_USERNAME }}" --password-stdin

      - name: Flyway (uni-flyway-pipe)
        run: |
          docker run --rm \
            -v "$GITHUB_WORKSPACE":/workspace -w /workspace \
            -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
            -e env="$AppEnv" \
            -e flyway_actions="info,repair,migrate" \
            -e aws_region="${{ inputs.region }}" \
            -e aws_role_arn="arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/platform/workloads/UNI-AR-Github-{DOMAIN_CODE}-deployment" \
            -e database_config_file="flyway/config.yaml" \
            -e flyway_config_file="flyway/snowflake_flyway_${AppEnv}.conf" \
            -e dbms_type="snowflake" \
            -e auth_type="pat" \
            unicorndso/uni-flyway-pipe:1.2.0
```

## 04 Deploy Plugins and Restart - `.github/workflows/04-deploy-plugins-and-restart.yml`

```yaml
name: 04 Deploy Plugins and Restart

on:
  workflow_dispatch:
    inputs:
      deployment_environment:
        type: choice
        required: true
        options: [dev, uat, prd]
      region:
        type: choice
        default: "{PRIMARY_REGION}"
        options: ["{PRIMARY_REGION}", "us-west-2"]

concurrency:
  group: mwaa-{DOMAIN_CODE}-${{ inputs.deployment_environment }}
  cancel-in-progress: false

permissions:
  id-token: write
  contents: read

jobs:
  validate-plugins:
    name: "Preflight: Validate Plugins"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - name: Install dependencies
        run: |
          pip install "apache-airflow=={AIRFLOW_VERSION}"
          pip install -r requirements/requirements.txt
      - name: Syntax check
        run: |
          shopt -s nullglob
          files=(plugins/*.py)
          if [ ${#files[@]} -gt 0 ]; then python -m py_compile "${files[@]}"; fi
          echo "Syntax check passed"
      - name: Import check
        run: |
          cat > /tmp/validate_plugins.py << 'PYEOF'
          import sys, importlib.util, pathlib, os
          os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")
          sys.path.insert(0, "plugins")
          errors = []
          for f in sorted(pathlib.Path("plugins").glob("*.py")):
              if f.name.startswith("_"):
                  continue
              try:
                  spec = importlib.util.spec_from_file_location(f.stem, str(f))
                  mod = importlib.util.module_from_spec(spec)
                  spec.loader.exec_module(mod)
                  print(f"  PASS  {f.name}")
              except Exception as e:
                  print(f"  FAIL  {f.name}: {e}")
                  errors.append(f.name)
          if errors:
              print(f"\nPlugin validation failed for: {errors}")
              sys.exit(1)
          print("All plugins validated successfully")
          PYEOF
          python /tmp/validate_plugins.py

  deploy-plugins:
    name: Deploy Plugins and Restart
    needs: validate-plugins
    runs-on: [self-hosted, linux, dso.prd.runner]
    environment: ${{ inputs.deployment_environment }}
    env:
      AppEnv: ${{ inputs.deployment_environment }}
    steps:
      - uses: actions/checkout@v4

      - id: ctx
        uses: ./.github/actions/mwaa-context
        with:
          env: ${{ inputs.deployment_environment }}
          region: ${{ inputs.region }}

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - name: Build wheels for offline install
        run: bash scripts/refresh_mwaa_wheels.sh

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ vars.AWS_ACCOUNT_ID }}:role/platform/workloads/UNI-AR-Github-{DOMAIN_CODE}-deployment
          aws-region: ${{ inputs.region }}

      - name: Remove stale dags/plugins from S3
        run: |
          echo "Plugins live at top-level plugins/, not dags/plugins/"
          aws s3 rm "s3://${{ steps.ctx.outputs.s3_bucket }}/{DOMAIN_CODE}/dags/plugins/" --recursive --region "${{ inputs.region }}" || true

      - name: Docker login (Artifactory)
        run: echo "${{ secrets.ARTIFACTORY_PASSWORD }}" | docker login docker-prod.artifactory.unicorn-inc.example -u "${{ secrets.ARTIFACTORY_USERNAME }}" --password-stdin

      - name: Deploy plugins (uni-udp-deploy-plugins)
        run: |
          docker run --rm \
            -v "$GITHUB_WORKSPACE":/workspace -w /workspace \
            -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
            -e AWS_DEFAULT_REGION="${{ inputs.region }}" \
            -e ENVIRONMENT="$AppEnv" \
            -e DOMAIN="{DOMAIN_CODE}" \
            -e S3_BUCKET="${{ steps.ctx.outputs.s3_bucket }}" \
            -e DOMAINS_PLUGINS_FOLDER_PATH="/plugins" \
            -e DEPLOY_UDP_CORE_PLUGINS="False" \
            -e DOMAINS_REQUIREMENTS_FILE_PATH="/requirements" \
            docker-prod.artifactory.unicorn-inc.example/uni-udp-deploy-plugins:2.2

      - name: Restart MWAA (uni-udp-deploy-restart)
        run: |
          echo "Restarting the mwaa env ${{ steps.ctx.outputs.mwaa_instance_name }}"
          docker run --rm \
            -v "$GITHUB_WORKSPACE":/workspace -w /workspace \
            -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
            -e AWS_DEFAULT_REGION="${{ inputs.region }}" \
            -e DEBUG="true" \
            -e MWAA_INSTANCE_NAME="${{ steps.ctx.outputs.mwaa_instance_name }}" \
            -e ENVIRONMENT="$AppEnv" \
            -e DOMAIN="{DOMAIN_CODE}" \
            -e REFRESH_PLUGINS_REQUIREMENTS="true" \
            -e DOMAIN_CONFIG_FILE_PATH="/dags/configurations/airflow_config_${AppEnv}.json" \
            -e DOMAIN_ENV_CLASS_CONFIG_FILE_PATH="/dags/configurations/env_class_config_${AppEnv}.json" \
            docker-prod.artifactory.unicorn-inc.example/uni-udp-deploy-restart:1.3
```

## 05 Restart Environment - `.github/workflows/05-restart-environment.yml`

Key-based auth, as in the Bitbucket original (credentials come from GitHub Environment secrets). Switch to OIDC like the others if the restart pipe accepts it.

```yaml
name: 05 Restart Environment

on:
  workflow_dispatch:
    inputs:
      deployment_environment:
        type: choice
        required: true
        options: [dev, uat, prd]
      region:
        type: choice
        default: "{PRIMARY_REGION}"
        options: ["{PRIMARY_REGION}", "us-west-2"]
      refresh_plugins_requirements:
        type: choice
        default: "true"
        options: ["true", "false"]

concurrency:
  group: mwaa-{DOMAIN_CODE}-${{ inputs.deployment_environment }}
  cancel-in-progress: false

permissions:
  contents: read

jobs:
  restart-environment:
    name: Restart Environment
    runs-on: ubuntu-latest
    environment: ${{ inputs.deployment_environment }}
    env:
      AppEnv: ${{ inputs.deployment_environment }}
    steps:
      - uses: actions/checkout@v4

      - id: ctx
        uses: ./.github/actions/mwaa-context
        with:
          env: ${{ inputs.deployment_environment }}
          region: ${{ inputs.region }}

      - name: Docker login (Artifactory)
        run: echo "${{ secrets.ARTIFACTORY_PASSWORD }}" | docker login docker-prod.artifactory.unicorn-inc.example -u "${{ secrets.ARTIFACTORY_USERNAME }}" --password-stdin

      - name: Restart MWAA (uni-udp-deploy-restart)
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        run: |
          docker run --rm \
            -v "$GITHUB_WORKSPACE":/workspace -w /workspace \
            -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
            -e AWS_DEFAULT_REGION="${{ inputs.region }}" \
            -e DEBUG="true" \
            -e MWAA_INSTANCE_NAME="${{ steps.ctx.outputs.mwaa_instance_name }}" \
            -e ENVIRONMENT="$AppEnv" \
            -e DOMAIN="{DOMAIN_CODE}" \
            -e REFRESH_PLUGIN_REQUIREMENTS="${{ inputs.refresh_plugins_requirements }}" \
            -e DOMAIN_CONFIG_FILE_PATH="/dags/configurations/airflow_config_${AppEnv}.json" \
            -e DOMAIN_ENV_CLASS_CONFIG_FILE_PATH="/dags/configurations/env_class_config_${AppEnv}.json" \
            docker-prod.artifactory.unicorn-inc.example/uni-udp-deploy-restart:1.3
```

## Design notes

- `concurrency` serializes runs per environment so a DAG deploy cannot race a plugin restart.
- `environment:` gives per-env variables/secrets and optional approval gates on `prd`; the OIDC `sub` claim becomes `repo:<org>/<repo>:environment:<env>`.
- `short_region` is derived from the region (`us-east-1` -> `ue1`, `us-west-2` -> `uw2`).
- `aws-actions/configure-aws-credentials` replaces the manual web-identity-token file and `aws configure set` steps.
- The plugin syntax check skips cleanly when `plugins/` has no `.py` files, and free-text inputs are passed through `env:` instead of being interpolated into scripts.
- Helper script sources are in `scripts.md`.
- The `domain_dags_folder_path` input only allows `/dags`: deploying a single project folder leaves `utils_uni/` out of S3 and causes `ModuleNotFoundError`.
- Bitbucket variable name typos (`DEPLOYMENT_ENVRIONMENT`, `allwoed_values`, `$deployment_envrionment`) are gone: inputs are named `deployment_environment`.

## Hardening Addendum (Airflow 3.0.6 / Python 3.12)

### Plugin deploy steps must build wheels in CI

Before calling `uni-udp-deploy-plugins`, run (already included in workflow 04):

```bash
bash scripts/refresh_mwaa_wheels.sh
```

and use Python 3.12 in the job (`actions/setup-python` with `python-version: "3.12"`). Apply this to every `Deploy Plugins and Restart` job.

### Why

- Some MWAA environments resolve pip from local plugins links only.
- Missing wheel artifacts (for provider transitive deps like pandas) cause provider install failures.
- Generating wheels in CI avoids maintaining binaries in git and keeps dependencies reproducible.

### Preflight CI

If the runner lacks a `python3.12` alias, run preflight with:

```bash
python3 scripts/mwaa_requirements_preflight.py --airflow-version 3.0.6 --python-version 3.12 --skip-resolver
```

This keeps constraints validation while avoiding venv creation failures.
