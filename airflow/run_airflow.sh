#!/usr/bin/env bash
# Local Airflow for SmokeStream, from its own environment (.venv-airflow).
#
#   airflow/run_airflow.sh                                 UI at http://localhost:8080
#   airflow/run_airflow.sh dags test ingest_weather        run one DAG once, now
#   airflow/run_airflow.sh dags list                       any other airflow command
#
# The UI has no login: everyone on this machine is an admin. Local use only.
set -euo pipefail
cd "$(dirname "$0")/.."
export AIRFLOW_HOME="$PWD/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
if [ $# -eq 0 ]; then
  set -- standalone
fi
exec .venv-airflow/bin/airflow "$@"
