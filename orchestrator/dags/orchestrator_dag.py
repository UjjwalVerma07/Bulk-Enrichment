# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import os

# from common.libs import s3_utils

# TEST_BUCKET = "test-bucket"
# TEST_FILE = "/tmp/test_file.csv"

# def create_test_csv():
#     os.makedirs("/tmp", exist_ok=True)
#     with open(TEST_FILE, "w") as f:
#         f.write("id,name\n1,John Doe\n2,Jane Smith\n")
#     print(f"Created file {TEST_FILE}")

# def upload_to_s3():
#     s3_utils.upload_file(TEST_BUCKET, "test_file.csv", TEST_FILE)

# def download_from_s3():
#     download_path = "/tmp/test_file_downloaded.csv"
#     s3_utils.download_file(TEST_BUCKET, "test_file.csv", download_path)
#     print(f"Downloaded file saved to {download_path}")

# with DAG(
#     dag_id="test_s3_dag",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     create_csv_task = PythonOperator(
#         task_id="create_csv",
#         python_callable=create_test_csv
#     )

#     upload_task = PythonOperator(
#         task_id="upload_to_s3",
#         python_callable=upload_to_s3
#     )

#     download_task = PythonOperator(
#         task_id="download_from_s3",
#         python_callable=download_from_s3
#     )

#     create_csv_task >> upload_task >> download_task

from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from common.libs import s3_utils,kafka_utils

def send_event(stage,status):
    kafka_utils.send_event("pipeline-progress",{"stage":stage,"status":status,"time":datetime.now().isoformat()})
    print(f"Event sent: stage={stage},status={status}")

def log_process(stage,status):
    progress_data={
        "stage":stage,
        "status":status,
        "time":datetime.now().isoformat()
    }
    s3_utils.upload_progress_file("test-bucket","progress",progress_data)

def log_email_validation_start():
    log_process("email_validation","started")

def log_email_validation_end():
    log_process("email_validation","completed")


with DAG(
    dag_id="orchestrator_dag",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["orchestrator"],
)as dag:
    start=EmptyOperator(task_id="start")

    log_start=PythonOperator(
        task_id="log_email_validation_start",
        python_callable=log_email_validation_start
    )

    trigger_email_validation=TriggerDagRunOperator(
        task_id="trigger_email_validation",
        trigger_dag_id="email_validation_dag",

    )
    
    log_end=PythonOperator(
        task_id="log_email_validation_end",
        python_callable=log_email_validation_end
    )

    end=EmptyOperator(task_id="end")
    start >> trigger_email_validation >> end





