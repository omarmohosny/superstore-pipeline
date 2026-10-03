FROM apache/airflow:3.1.0

# dbt and the loader live in their own venv so their dependencies
# cannot clash with Airflow's pinned ones
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/airflow/pipeline_venv \
    && /opt/airflow/pipeline_venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

ENV PIPELINE_BIN=/opt/airflow/pipeline_venv/bin
