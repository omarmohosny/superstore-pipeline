#!/usr/bin/env bash
# Run a command inside the airflow image with the project mounted.
# Quote the command in single quotes so $PIPELINE_BIN expands in the container, not here.
export MSYS_NO_PATHCONV=1
docker compose -f "$(dirname "$0")/../airflow/docker-compose.yaml" run --rm --no-deps --entrypoint bash airflow-scheduler -c "$*"
