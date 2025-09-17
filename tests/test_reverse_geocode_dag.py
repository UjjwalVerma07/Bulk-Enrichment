import pytest
from airflow.models import DagBag
from unittest.mock import MagicMock,patch
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.docker.operators.docker import DockerOperator 
from reverse_geocode.dags.reverse_geocode_dag import choose_mode
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from datetime import datetime
import pendulum
import uuid
@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder="reverse_geocode/dags",include_examples=False)

def test_dag_import(dag_bag):
    assert dag_bag.import_errors == {},f"DAG import errors: {dag_bag.import_errors}"

def test_dag_exists(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    assert dag is not None, "reverse_geocode_dag not found"

def test_tasks_exist(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    task_ids=[task.task_id for task in dag.tasks]
    expected_task=["start", "branch_mode", "reverse_geocode_task", "realtime_reverse_geocode", "end"]
    for t in expected_task:
        assert t in task_ids, f"Task {t} is not found"


def test_dag_metadata(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    assert dag.schedule is None,"schedule should be None"
    assert dag.catchup is False,"catchup should be False"
    assert "Test-enrichment" in dag.tags,"Tags should be Test-enrichment"

def test_task_dependencies(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")

    assert "branch_mode" in dag.get_task("start").downstream_task_ids,"branch_mode should be downstream of start"
    downstream_tasks=dag.get_task("branch_mode").downstream_task_ids;
    assert {"reverse_geocode_task","realtime_reverse_geocode"}.issubset(downstream_tasks),"reverse_geocode_task or realtime_reverse_geocode should be downstream of branch mode"

    assert "end" in dag.get_task("reverse_geocode_task").downstream_task_ids,"end should be downstream of reverse_geocode_task"
    assert "end" in dag.get_task("realtime_reverse_geocode").downstream_task_ids,"end should be downstream of realtime_reverse_geocode"

def test_task_operator_types(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    assert isinstance(dag.get_task("start"),EmptyOperator),"start should be an EmptyOperator"
    assert isinstance(dag.get_task("branch_mode"),BranchPythonOperator),"branch_mode should be an BranchPythonOperator"
    assert isinstance(dag.get_task("reverse_geocode_task"),DockerOperator),"reverse_geocode_task should be an DockerOperator"
    assert isinstance(dag.get_task("realtime_reverse_geocode"),DockerOperator),"reverse_geocode_task should be an DockerOperator"
    assert isinstance(dag.get_task("end"),EmptyOperator),"end should be and EmptyOperator"

def  test_dag_params(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag");
    assert dag.params is not None,"Params should be not None"
    params=dag.params
    assert "mode" in params,"mode should be in params"
    assert "input_bucket" in params,"input bucket should be in params"
    assert "input_key" in params,"input key should be in params"
    assert "out_bucket" in params,"out bucket should be in params"
    assert "out_key" in params,"out key should be in params"
    assert "input_topic" in params,"input topic should be in params"
    assert "output_topic" in params,"output topic should be in params"



def test_choose_mode_batch():
    context={"params":{"mode":"batch"}}
    result=choose_mode(**context)
    assert result=="reverse_geocode_task"

def test_choose_mode_stream():
    context={"params":{"mode":"stream"}}
    result=choose_mode(**context)
    assert result=="realtime_reverse_geocode"

def test_reverse_geocode_docker_config(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    reverse_geocode_task=dag.get_task("reverse_geocode_task")
    assert "reverse_geocode_image" in reverse_geocode_task.image,"reverse_geocode_image is required"
    assert reverse_geocode_task.api_version=="auto","api version should be auto"
    assert reverse_geocode_task.auto_remove=="never","auto remove should be never"
    assert reverse_geocode_task.command=="python /opt/airflow/dags/reverse_geocode/reverse_geocode_run.py","command should be python command"
    assert reverse_geocode_task.docker_url=="unix://var/run/docker.sock","docker url should be unix://var/run/docker.sock"
    assert reverse_geocode_task.network_mode=="final-bulk-enrichment-v2_airflow_network","network mode should be final-bulk-enrichment-v2-airflow_network"
    assert reverse_geocode_task.environment is not None,"environment should be not None"
    assert reverse_geocode_task.mounts is not None,"mounts should not be None"
    assert reverse_geocode_task.mount_tmp_dir==False,"mount tmp dir should be False"

def test_realtime_reverse_geocode_docker_config(dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    reverse_geocode_task=dag.get_task("realtime_reverse_geocode")
    assert "reverse_geocode_image" in reverse_geocode_task.image,"reverse_geocode_image is required"
    assert reverse_geocode_task.api_version=="auto","api version should be auto"
    assert reverse_geocode_task.auto_remove=="never","auto remove should be never"
    assert reverse_geocode_task.command=="python /opt/airflow/dags/reverse_geocode/reverse_geocode_run.py","command should be python command"
    assert reverse_geocode_task.docker_url=="unix://var/run/docker.sock","docker url should be unix://var/run/docker.sock"
    assert reverse_geocode_task.network_mode=="final-bulk-enrichment-v2_airflow_network","network mode should be final-bulk-enrichment-v2-airflow_network"
    assert reverse_geocode_task.environment is not None,"environment should be not None"
    assert reverse_geocode_task.mounts is not None,"mounts should not be None"
    assert reverse_geocode_task.mount_tmp_dir==False,"mount tmp dir should be False"



@patch("airflow.providers.docker.operators.docker.DockerHook")
def test_reverse_geocode_docker_executed(mock_docker_hook,dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    task:DockerOperator=dag.get_task("reverse_geocode_task")

    mock_hook_instance=MagicMock()
    mock_docker_hook.return_value=mock_hook_instance
    

    mock_client=MagicMock()
    mock_hook_instance.api_client=mock_client

    mock_client.create_container.return_value={"Id":"test123"}
    mock_client.start.return_value=None
    mock_client.wait.return_value={"StatusCode":0}
    mock_client.logs.return_value=[b"Task completed successfully"]
    mock_client.remove_container.return_value=None 
  
    mock_hook_instance.get_conn.return_value=mock_client

    result=task.execute(context={"task_instance":MagicMock()})
    mock_docker_hook.assert_called_once()
    assert result is None or isinstance(result,str)

@patch("airflow.providers.docker.operators.docker.DockerHook")
def test_realtime_reverse_geocode_docker_executed(mock_docker_hook,dag_bag):
    dag=dag_bag.get_dag(dag_id="reverse_geocode_dag")
    task:DockerOperator=dag.get_task("realtime_reverse_geocode")

    mock_hook_instance=MagicMock()
    mock_docker_hook.return_value=mock_hook_instance
 
    mock_client=MagicMock()
    mock_hook_instance.api_client=mock_client

    mock_client.create_container.return_value={"Id":"test456"}
    mock_client.start.return_value=None
    mock_client.wait.return_value={"StatusCode":0}
    mock_client.logs.return_value=[b"Realtime Task completed successfully"]
    mock_client.remove_container.return_value=None

    mock_hook_instance.get_conn.return_value=mock_client

    result=task.execute(context={"task_instance":MagicMock()})
    mock_docker_hook.assert_called_once()
    assert result is None or isinstance(result,str)



