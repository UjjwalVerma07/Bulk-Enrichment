import pytest
from airflow.models import DagBag, TaskInstance, DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.docker.operators.docker import DockerOperator 
from email_validation.dags.email_validation_dag import choose_mode
from unittest.mock import MagicMock,patch
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from datetime import datetime
import uuid


#Currently we are doing the DAG Validation Test(Static Test) and doing the unit testing
#in DAG Valiadtion Test we are checking the DAG Structure and Task Dependencies
#1. Dag Import Validation
#2. Dag Exists Validation
#3. Task Exists Validation
#4. Task Metadata Validation
#5. DAG Params Validation
#6. Task Dependencies Validation
#7. Task Operator Types Validation
#8. Docker Operator Configuration Validation

@pytest.fixture(scope="session")
def dag_bag():
    #DagBag is used to load the DAGs from the specific folder or directory;
    return DagBag(dag_folder="email_validation/dags",include_examples=False)

#Dag Import Validation
def test_dag_import(dag_bag):
    #import errors are used to check if the DAGs are imported correctly;
    assert dag_bag.import_errors=={},f"DAG import errors: {dag_bag.import_errors}"

#Dag Exists Validation
def test_dag_exists(dag_bag):
    #get_dag is used to get the DAG Object from the DAG Bag;
    dag=dag_bag.get_dag(dag_id="email_validation_dag");
    #assert is used to check if the DAG is not None;
    assert dag is not None,"email_vaildation_dag not found"

#Task Exists Validation
def test_tasks_exist(dag_bag):
    #get_dag is used to get the DAG Object from the DAG Bag;
    dag=dag_bag.get_dag(dag_id="email_validation_dag");
    task_ids=[task.task_id for task in dag.tasks]
    expected_task=["start","branch_mode","validate_emails","realtime_validate_emails","end"]
    for t in expected_task:
        assert t in task_ids, f"Task {t} is not found"

#Task Metadata Validation
def test_dag_metadata(dag_bag):
    dag=dag_bag.get_dag(dag_id="email_validation_dag");
    assert dag.schedule is None,"schedule should be None"
    assert dag.catchup is False,"Catchup should be False"
    assert "enrichment" in dag.tags,"Tags should be enrichment"


#Task Dependencies Validation
def test_task_dependencies(dag_bag):
    dag=dag_bag.get_dag(dag_id="email_validation_dag");
    #start -> branch_mode
    #upstream_task_ids is used to check if the start task is upstream of branch_mode
    #downstream_task_ids is used to check if the branch_mode is the down stream task of the start task or not
    assert "start" in dag.get_task("branch_mode").upstream_task_ids,"start should be upstream of branch_mode"
    assert "branch_mode" in dag.get_task("start").downstream_task_ids,"branch_mode should be downstream of start"

    #branch_mode -> validate_emails or realtime_validate_emails;
    branch_downstreams=dag.get_task("branch_mode").downstream_task_ids
    assert {"validate_emails","realtime_validate_emails"}.issubset(branch_downstreams),"validate_emails or realtime_validate_emails shoudl be donwsteram of branch_mode"
    #Both path ->end
    assert "end" in dag.get_task("validate_emails").downstream_task_ids,"end should be downstream of validate_emails"
    assert "end" in dag.get_task("realtime_validate_emails").downstream_task_ids,"end should be downstream of realtime_validate_emails"


#Task Operator Types Validation
def test_task_operator_types(dag_bag):
    dag=dag_bag.get_dag(dag_id="email_validation_dag");
    assert isinstance(dag.get_task("start"),EmptyOperator),"start should be EmptyOperator"
    assert isinstance(dag.get_task("branch_mode"),BranchPythonOperator),"branch_mode should be BranchPythonOperator"
    assert isinstance(dag.get_task("validate_emails"),DockerOperator),"validate_emails should be DockerOperator"
    assert isinstance(dag.get_task("realtime_validate_emails"),DockerOperator),"realtime_validate_emails should be DockerOperator"
    assert isinstance(dag.get_task("end"),EmptyOperator),"end should be EmptyOperator"

#Dag Params Validation
def test_dag_params(dag_bag):
    
    dag=dag_bag.get_dag(dag_id="email_validation_dag");
    assert dag.params is not None,"Params should be not None"
    params=dag.params
    assert "mode" in params,"mode should be in params"
    assert "input_bucket" in params,"input bucket should be in params"
    assert "input_key" in params,"input key should be in params"
    assert "out_bucket" in params,"out bucket should be in params"
    assert "out_key" in params,"out key should be in params"
    assert "input_topic" in params,"input topic should be in params"
    assert "output_topic" in params,"output topic should be in params"

          

#-------------Unit Testing------------------
#unit testing is used to test the each task in the DAG Seperately and in an isolated manner
#We pick each task and test it seperately and in an isolated manner
#We are testing the each choose_mode function for the batch and stream mode
def test_choose_mode_batch():
    context={"params":{"mode":"batch"}}
    result=choose_mode(**context)
    assert result=="validate_emails"

def test_choose_mode_stream():
    context={"params":{"mode":"stream"}}
    result=choose_mode(**context)
    assert result=="realtime_validate_emails"


#DockerOperator Configuration Validation
#Next we are testing the Docker Operator Configuration for the batch and stream mode
def test_email_validation_docker_config(dag_bag): 
    dag=dag_bag.get_dag(dag_id="email_validation_dag")
    email_validation_task=dag.get_task("validate_emails")
    assert email_validation_task.image=="email_validation_image","email_validation_image is required"
    assert email_validation_task.api_version=="auto","api version should be auto"
    assert email_validation_task.auto_remove=="never","auto remove should be never"
    assert email_validation_task.command=="python /opt/airflow/dags/email_validation/enrich_email_run.py","command should be python command"
    assert email_validation_task.docker_url=="unix://var/run/docker.sock","docker url should be unix://var/run/docker.sock"
    assert email_validation_task.network_mode=="final-bulk-enrichment-v2_airflow_network","network mode should be final-bulk-enrichment-v2-airflow_network"
    assert email_validation_task.environment is not None,"environment should be not None"
    assert email_validation_task.mounts is not None,"mounts should not be None"
    assert email_validation_task.mount_tmp_dir==False,"mount tmp dir should be False"

def test_realtime_reverse_geocode_docker_config(dag_bag):
    dag=dag_bag.get_dag(dag_id="email_validation_dag")
    realtime_email_validation_task=dag.get_task("realtime_validate_emails")
    assert "email_validation_image" in realtime_email_validation_task.image,"email_validation_image is required"
    assert realtime_email_validation_task.api_version=="auto","api version should be auto"
    assert realtime_email_validation_task.auto_remove=="never","auto remove should be never"
    assert realtime_email_validation_task.command=="python /opt/airflow/dags/email_validation/enrich_email_run.py","command should be python command"
    assert realtime_email_validation_task.docker_url=="unix://var/run/docker.sock","docker url should be unix://var/run/docker.sock"
    assert realtime_email_validation_task.network_mode=="final-bulk-enrichment-v2_airflow_network","network mode should be final-bulk-enrichment-v2-airflow_network"
    assert realtime_email_validation_task.environment is not None,"environment should be not None"
    assert realtime_email_validation_task.mounts is not None,"mounts should not be None"
    assert realtime_email_validation_task.mount_tmp_dir==False,"mount tmp dir should be False"


#Now Here we are mocking the DockerOperator to check if we are able to make the call to the Opearator or not 
#Patch is used to mock the DockerHook and API Client and we are mocking the DockerHook and API Client and we are mocking the container operations and we are mocking the other required hook methods
@patch("airflow.providers.docker.operators.docker.DockerHook")
def test_email_validation_docker_executed(mock_docker_hook,dag_bag):
    dag=dag_bag.get_dag(dag_id="email_validation_dag")
    task:DockerOperator=dag.get_task("validate_emails")
    #Mock Docker Hook and API Client
    mock_hook_instance=MagicMock()
    mock_docker_hook.return_value=mock_hook_instance

    #Mock API Client
    mock_client=MagicMock()
    mock_hook_instance.api_client=mock_client

    #Mock container operations
    mock_client.create_container.return_value={"Id":"test123"}
    mock_client.start.return_value=None
    mock_client.wait.return_value={"StatusCode":0}
    mock_client.logs.return_value=[b"Task completed successfully"]
    mock_client.remove_container.return_value=None

    #Mock other require hook methods
    mock_hook_instance.get_conn.return_value=mock_client
    #Test Execution
    result=task.execute(context={"task_instance":MagicMock()})
    mock_docker_hook.assert_called_once()
    assert result is None or isinstance(result,str)

@patch("airflow.providers.docker.operators.docker.DockerHook")
def test_realtime_email_validation_docker_executed(mock_docker_hook,dag_bag):
    dag=dag_bag.get_dag(dag_id="email_validation_dag")
    task:DockerOperator=dag.get_task("realtime_validate_emails")

    #MOck Docker Hook and API Client
    mock_hook_instance=MagicMock()
    mock_docker_hook.return_value=mock_hook_instance

    #Mock API Client
    mock_client=MagicMock()
    mock_hook_instance.api_client=mock_client

    #Mock container operations
    mock_client.create_container.return_value={"Id":"test456"}
    mock_client.start.return_value=None
    mock_client.wait.return_value={"StatusCode":0}
    mock_client.logs.return_value=[b"Realtime Task completed successfully"]
    mock_client.remove_container.return_value=None

    #Mock other require hook Methods
    mock_hook_instance.get_conn.return_value=mock_client

    #Test Execution
    result=task.execute(context={"task_instance":MagicMock()})
    mock_docker_hook.assert_called_once()
    assert result is None or isinstance(result,str)
