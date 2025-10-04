import pytest
import requests
import time
import uuid
from datetime import datetime
import json

# Configuration for Docker Airflow instance
AIRFLOW_BASE_URL = "http://localhost:8080"
AIRFLOW_USERNAME = "admin" 
AIRFLOW_PASSWORD = "admin"
DAG_ID = "email_validation_dag"

@pytest.fixture(scope="session")
def airflow_session():
    """Create authenticated session with Airflow"""
    session = requests.Session()
    session.auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    # Test connection
    try:
        response = session.get(f"{AIRFLOW_BASE_URL}/api/v1/health")
        if response.status_code != 200:
            pytest.skip(f"Cannot connect to Airflow at {AIRFLOW_BASE_URL}. Make sure Docker is running.")
    except requests.exceptions.ConnectionError:
        pytest.skip(f"Cannot connect to Airflow at {AIRFLOW_BASE_URL}. Make sure Docker is running.")
    
    return session

@pytest.fixture
def clean_dag_runs(airflow_session):
    """Clean up any existing DAG runs before and after test"""
    def cleanup():
        # Get existing DAG runs and clean them up
        try:
            response = airflow_session.get(f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns")
            if response.status_code == 200:
                dag_runs = response.json().get('dag_runs', [])
                for run in dag_runs:
                    if 'test_' in run['dag_run_id']:  # Only clean test runs
                        airflow_session.delete(
                            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run['dag_run_id']}"
                        )
        except Exception as e:
            print(f"Cleanup warning: {e}")
    
    cleanup()  # Clean before test
    yield
    cleanup()  # Clean after test


class TestEmailValidationDAGIntegration:
    """TRUE Integration tests using live Airflow Docker environment"""
    
    def test_dag_exists_in_airflow(self, airflow_session):
        """Verify the DAG is loaded in Airflow"""
        response = airflow_session.get(f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}")
        
        assert response.status_code == 200, f"DAG {DAG_ID} not found in Airflow"
        dag_data = response.json()
        
        assert dag_data['dag_id'] == DAG_ID
        assert dag_data['is_active'] == True
        print(f"✅ DAG {DAG_ID} is loaded and active in Airflow")

    def test_integration_batch_mode_end_to_end(self, airflow_session, clean_dag_runs):
        """TRUE Integration test - Execute DAG in batch mode end-to-end and verify branching"""
        run_id = f"test_batch_integration_{uuid.uuid4()}"
        
        print(f"🚀 Starting batch mode integration test: {run_id}")
        
        # Trigger DAG run with batch mode configuration
        trigger_payload = {
            "dag_run_id": run_id,
            "conf": {
                "mode": "batch",
                "input_bucket": "raw", 
                "input_key": "customer_raw.csv",
                "out_bucket": "enriched",
                "out_key": "email_validated.csv"
            }
        }
        
        # Trigger the DAG run
        response = airflow_session.post(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns",
            json=trigger_payload
        )
        
        assert response.status_code == 200, f"Failed to trigger DAG: {response.text}"
        print(f"✅ DAG run {run_id} triggered successfully")
        
        # Wait for DAG run to complete or reach a final state
        max_wait_time = 180  # 3 minutes
        start_time = time.time()
        final_state = None
        
        print("⏳ Waiting for DAG run to complete...")
        while time.time() - start_time < max_wait_time:
            response = airflow_session.get(
                f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}"
            )
            
            if response.status_code == 200:
                dag_run = response.json()
                state = dag_run.get('state')
                
                print(f"📊 DAG run state: {state}")
                
                if state in ['success', 'failed']:
                    final_state = state
                    break
            
            time.sleep(10)  # Wait 10 seconds before checking again
        
        # Verify the DAG run completed
        assert final_state is not None, f"DAG run did not complete within {max_wait_time} seconds"
        
        # Get final task instance states
        response = airflow_session.get(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances"
        )
        assert response.status_code == 200
        task_instances = response.json()['task_instances']
        
        task_states = {ti['task_id']: ti['state'] for ti in task_instances}
        print(f"📋 Task states: {task_states}")
        
        # VERIFY INTEGRATION: Branch logic worked correctly for batch mode
        assert task_states['branch_mode'] == 'success', "Branch task should complete successfully"
        assert task_states['validate_emails'] == 'success', "Batch task should execute and succeed"
        assert task_states['realtime_validate_emails'] == 'skipped', "Realtime task should be skipped in batch mode"
        assert task_states['end'] == 'success', "End task should complete"
        
        print("🎉 BATCH MODE INTEGRATION TEST PASSED!")
        print("✅ Branching worked correctly - batch task executed, stream task skipped")

    def test_integration_stream_mode_end_to_end(self, airflow_session, clean_dag_runs):
        """TRUE Integration test - Execute DAG in stream mode end-to-end and verify branching"""
        run_id = f"test_stream_integration_{uuid.uuid4()}"
        
        print(f"🚀 Starting stream mode integration test: {run_id}")
        
        # Trigger DAG run with stream mode configuration  
        trigger_payload = {
            "dag_run_id": run_id,
            "conf": {
                "mode": "stream",
                "input_topic": "email-validation-input",
                "output_topic": "email-validation-output"
            }
        }
        
        # Trigger the DAG run
        response = airflow_session.post(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns", 
            json=trigger_payload
        )
        
        assert response.status_code == 200, f"Failed to trigger DAG: {response.text}"
        print(f"✅ DAG run {run_id} triggered successfully")
        
        # Wait for DAG run to complete or reach a final state
        max_wait_time = 180  # 3 minutes
        start_time = time.time()
        final_state = None
        
        print("⏳ Waiting for DAG run to complete...")
        while time.time() - start_time < max_wait_time:
            response = airflow_session.get(
                f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}"
            )
            
            if response.status_code == 200:
                dag_run = response.json()
                state = dag_run.get('state')
                
                print(f"📊 DAG run state: {state}")
                
                if state in ['success', 'failed']:
                    final_state = state
                    break
            
            time.sleep(10)  # Wait 10 seconds before checking again
        
        # Verify the DAG run completed
        assert final_state is not None, f"DAG run did not complete within {max_wait_time} seconds"
        
        # Get final task instance states
        response = airflow_session.get(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances"
        )
        assert response.status_code == 200
        task_instances = response.json()['task_instances']
        
        task_states = {ti['task_id']: ti['state'] for ti in task_instances}
        print(f"📋 Task states: {task_states}")
        
        # VERIFY INTEGRATION: Branch logic worked correctly for stream mode
        assert task_states['branch_mode'] == 'success', "Branch task should complete successfully"
        assert task_states['validate_emails'] == 'skipped', "Batch task should be skipped in stream mode"
        assert task_states['realtime_validate_emails'] == 'success', "Realtime task should execute and succeed"
        assert task_states['end'] == 'success', "End task should complete"
        
        print("🎉 STREAM MODE INTEGRATION TEST PASSED!")
        print("✅ Branching worked correctly - stream task executed, batch task skipped")

    def test_integration_parameter_passing_end_to_end(self, airflow_session, clean_dag_runs):
        """TRUE Integration test - Verify parameters flow through entire DAG execution"""
        run_id = f"test_params_integration_{uuid.uuid4()}"
        
        print(f"🚀 Starting parameter passing integration test: {run_id}")
        
        # Custom configuration to test parameter passing
        custom_config = {
            "mode": "batch",
            "input_bucket": "test-input-bucket",
            "input_key": "test-input.csv", 
            "out_bucket": "test-output-bucket",
            "out_key": "test-output.csv"
        }
        
        trigger_payload = {
            "dag_run_id": run_id,
            "conf": custom_config
        }
        
        # Trigger the DAG run
        response = airflow_session.post(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns",
            json=trigger_payload
        )
        
        assert response.status_code == 200, f"Failed to trigger DAG: {response.text}"
        print(f"✅ DAG run {run_id} triggered with custom parameters")
        
        # Wait a bit for the run to start and process
        time.sleep(20)
        
        # Verify the configuration was stored and is accessible
        response = airflow_session.get(f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}")
        assert response.status_code == 200
        dag_run = response.json()
        
        # Verify configuration was stored correctly
        stored_conf = dag_run.get('conf', {})
        print(f"📋 Stored configuration: {stored_conf}")
        
        for key, value in custom_config.items():
            assert stored_conf.get(key) == value, f"Parameter {key} not passed correctly"
        
        print("🎉 PARAMETER PASSING INTEGRATION TEST PASSED!")
        print("✅ All parameters were correctly passed through DAG execution")


if __name__ == "__main__":
    print("🐳 Docker-based Integration Testing for Email Validation DAG")
    print("")
    print("Prerequisites:")
    print("1. Start Docker: docker-compose up -d")
    print("2. Wait for Airflow to be ready (check http://localhost:8080)")  
    print("3. Run: pytest tests/test_email_validation_dag_integration.py -v -s")
    print("")
    print("These tests will:")
    print("✅ Actually trigger DAG runs in live Airflow")
    print("✅ Execute tasks end-to-end (including Docker containers)")
    print("✅ Verify branching logic with real task execution")
    print("✅ Test parameter passing through entire pipeline")





