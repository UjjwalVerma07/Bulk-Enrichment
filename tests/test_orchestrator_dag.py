import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder="orchestrator/dags",include_examples=False)

def test_dag_import(dag_bag):
    assert dag_bag.import_errors=={},f"DAG import errors: {dag_bag.import_errors}"

def test_dag_exists(dag_bag):
    dag=dag_bag.get_dag(dag_id="orchestrator_dag")
    assert dag is not None,"orchestrator_dag not found"

def test_tasks_exist(dag_bag):
    dag=dag_bag.get_dag(dag_id="orchestrator_dag")
    task_ids=[task.task_id for task in dag.tasks]
    expected_task=["start","fetch_pipeline_config","choose_mode","batch_start","extract","run_all_pipelines","realtime_start","extract_real_pipelines","wait_for_data","run_stream","end"]
    for t in expected_task:
        assert t in task_ids, f"Task {t} is not found"  