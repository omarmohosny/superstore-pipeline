import os

import pytest

pytest.importorskip("airflow.models")
from airflow.models import DagBag  # noqa: E402

EXPECTED_ORDER = [
    "check_source_file", "dbt_debug", "load_bronze", "test_bronze", "build_silver",
    "snapshot", "test_silver", "build_gold", "test_gold", "log_run",
]


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=os.environ.get("DAGS_DIR", "/opt/airflow/dags"), include_examples=False)
    assert not bag.import_errors
    return bag.get_dag("superstore_pipeline")


def test_every_task_keeps_its_own_id(dag):
    assert sorted(dag.task_ids) == sorted(EXPECTED_ORDER)


def test_tasks_run_in_pipeline_order(dag):
    for upstream, downstream in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_bronze_tests_do_not_pull_in_silver_checks(dag):
    # the reconcile test depends on the source, so without "cautious" it runs before silver exists
    command = dag.get_task("test_bronze").python_callable()
    assert "--indirect-selection cautious" in command
