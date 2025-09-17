import pytest
from airflow.models import DagBag 
from unittest.mock import MagicMock,patch
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.docker.operators.docker import DockerOperator 
from gender_enrichment.dags.gender_enrichment_dag import choose_mode
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from datetime import datetime
import pendulum
import uuid

@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder="gender_enrichment/dags",include_examples=False)

def test_dag_import(dag_bag):
    assert dag_bag.import_errors == {},f"DAG import errors: {dag_bag.import_errors}"

def test_dag_exists(dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    assert dag is not None,"gender_enrichment_dag not found"

def test_tasks_exist(dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    task_ids=[task.task_id for task in dag.tasks]
    expected_task=["start","branch_mode","gender_enrich_task","realtime_gender_enrichment_task","end"]
    for t in expected_task:
        assert t in task_ids, f"Task {t} is not found"

def test_dag_metadata(dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    assert dag.schedule is None,"schedule should be None"
    assert dag.catchup is False,"catchup should be False"
    assert "gender_enrichment" in dag.tags,"Tags should be gender_enrichment"

def test_task_dependencies(dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")

    assert "branch_mode" in dag.get_task("start").downstream_task_ids,"branch_mode should be downstream of start"
    downstream_tasks=dag.get_task("branch_mode").downstream_task_ids;
    assert {"gender_enrich_task","realtime_gender_enrichment_task"}.issubset(downstream_tasks),"gender_enrich_task or realtime_gender_enrichment_task should be downstream of branch mode"

    assert "end" in dag.get_task("gender_enrich_task").downstream_task_ids,"end should be downstream of gender_enrich_task"
    assert "end" in dag.get_task("realtime_gender_enrichment_task").downstream_task_ids,"end should be downstream of realtime_gender_enrichment_task"

def test_task_operator_types(dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    assert isinstance(dag.get_task("start"),EmptyOperator),"start should be an EmptyOperator"
    assert isinstance(dag.get_task("branch_mode"),BranchPythonOperator),"branch_mode should be an BranchPythonOperator"
    assert isinstance(dag.get_task("gender_enrich_task"),DockerOperator),"gender_enrich_task should be an DockerOperator"
    assert isinstance(dag.get_task("realtime_gender_enrichment_task"),DockerOperator),"realtime_gender_enrichment_task should be an DockerOperator"
    assert isinstance(dag.get_task("end"),EmptyOperator),"end should be and EmptyOperator"


def  test_dag_params(dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag");
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
    assert result=="gender_enrich_task"

def test_choose_mode_stream():
    context={"params":{"mode":"stream"}}
    result=choose_mode(**context)
    assert result=="realtime_gender_enrichment_task"

def test_gender_enrichment_docker_config(dag_bag): 
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    gender_enrichment_task=dag.get_task("gender_enrich_task")
    assert "gender_enrichment_image" in gender_enrichment_task.image,"gender_enrichment_image is required"
    assert gender_enrichment_task.api_version=="auto","api version should be auto"
    assert gender_enrichment_task.auto_remove=="never","auto remove should be never"
    assert gender_enrichment_task.command=="python /opt/airflow/dags/gender_enrichment/enrich_gender_run.py","command should be python command"
    assert gender_enrichment_task.docker_url=="unix://var/run/docker.sock","docker url should be unix://var/run/docker.sock"
    assert gender_enrichment_task.network_mode=="final-bulk-enrichment-v2_airflow_network","network mode should be final-bulk-enrichment-v2-airflow_network"
    assert gender_enrichment_task.environment is not None,"environment should be not None"
    assert gender_enrichment_task.mounts is not None,"mounts should not be None"
    assert gender_enrichment_task.mount_tmp_dir==False,"mount tmp dir should be False"

def test_realtime_gender_enrichment_docker_config(dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    realtime_gender_enrichment_task=dag.get_task("realtime_gender_enrichment_task")
    assert "gender_enrichment_image" in realtime_gender_enrichment_task.image,"gender_enrichment_image is required"
    assert realtime_gender_enrichment_task.api_version=="auto","api version should be auto"
    assert realtime_gender_enrichment_task.auto_remove=="never","auto remove should be never"
    assert realtime_gender_enrichment_task.command=="python /opt/airflow/dags/gender_enrichment/enrich_gender_run.py","command should be python command"
    assert realtime_gender_enrichment_task.docker_url=="unix://var/run/docker.sock","docker url should be unix://var/run/docker.sock"
    assert realtime_gender_enrichment_task.network_mode=="final-bulk-enrichment-v2_airflow_network","network mode should be final-bulk-enrichment-v2-airflow_network"
    assert realtime_gender_enrichment_task.environment is not None,"environment should be not None"
    assert realtime_gender_enrichment_task.mounts is not None,"mounts should not be None"
    assert realtime_gender_enrichment_task.mount_tmp_dir==False,"mount tmp dir should be False"



@patch("airflow.providers.docker.operators.docker.DockerHook")
def test_gender_enrichment_docker_executed(mock_docker_hook, dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    task:DockerOperator=dag.get_task("gender_enrich_task")

    mock_hook_instance = MagicMock()
    mock_docker_hook.return_value = mock_hook_instance
    
    mock_client = MagicMock()
    mock_hook_instance.api_client = mock_client

    mock_client.create_container.return_value = {"Id": "test123"}
    mock_client.start.return_value = None
    mock_client.wait.return_value = {"StatusCode": 0}
    mock_client.logs.return_value = [b"Task completed successfully"]
    mock_client.remove_container.return_value = None
    
 
    mock_hook_instance.get_conn.return_value = mock_client
    

    result = task.execute(context={"task_instance": MagicMock()})

    mock_docker_hook.assert_called_once()
    assert result is None or isinstance(result, str) 

@patch("airflow.providers.docker.operators.docker.DockerHook")
def test_realtime_gender_enrichment_docker_executed(mock_docker_hook,dag_bag):
    dag=dag_bag.get_dag(dag_id="gender_enrichment_dag")
    task:DockerOperator=dag.get_task("realtime_gender_enrichment_task")

    mock_hook_instance=MagicMock()
    mock_docker_hook.return_value=mock_hook_instance
    
    mock_client=MagicMock()
    mock_hook_instance.api_client=mock_client
    
    mock_client.create_container.return_value={"Id": "test456"}
    mock_client.start.return_value=None
    mock_client.wait.return_value={"StatusCode":0}
    mock_client.logs.return_value=[b"Realtime Task completed successfully"]
    mock_client.remove_container.return_value=None

    mock_hook_instance.get_conn.return_value=mock_client
    result=task.execute(context={"task_instance":MagicMock()})
    mock_docker_hook.assert_called_once()
    assert result is None or isinstance(result,str)



