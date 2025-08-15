from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.libs import s3_utils,kafka_utils
from common.libs.kafka_utils import send_event
from common.libs import s3_utils
from datetime import datetime


BUCKET_NAME="test-bucket"

def send_kafka_message():
    event={"status":"test-event","timestamp":datetime.isoformat(datetime.now())}
    kafka_utils.send_event("pipline-progress",event)

import json, tempfile

def upload_progress_to_minio():
    progress_data = {
        "step": "test_upload",
        "status": "completed",
        "time": datetime.now().isoformat()
    }
    tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".json").name
    with open(tmp_path, "w") as f:
        json.dump(progress_data, f)
    s3_utils.upload_file(BUCKET_NAME, "progress.json", tmp_path)


with DAG(
    dag_id="test_kafka_minio_dag",
    start_date=datetime(2025, 8, 14),
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:

    kafka_task = PythonOperator(
        task_id="send_kafka_event",
        python_callable=send_kafka_message
    )

    minio_task = PythonOperator(
        task_id="upload_progress_to_minio",
        python_callable=upload_progress_to_minio
    )

    kafka_task >> minio_task


