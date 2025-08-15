from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from common.libs import s3_utils

TEST_BUCKET = "test-bucket"
TEST_FILE = "/tmp/test_file.csv"

def create_test_csv():
    os.makedirs("/tmp", exist_ok=True)
    with open(TEST_FILE, "w") as f:
        f.write("id,name\n1,John Doe\n2,Jane Smith\n")
    print(f"Created file {TEST_FILE}")

def upload_to_s3():
    s3_utils.upload_file(TEST_BUCKET, "test_file.csv", TEST_FILE)

def download_from_s3():
    download_path = "/tmp/test_file_downloaded.csv"
    s3_utils.download_file(TEST_BUCKET, "test_file.csv", download_path)
    print(f"Downloaded file saved to {download_path}")

with DAG(
    dag_id="test_s3_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_csv_task = PythonOperator(
        task_id="create_csv",
        python_callable=create_test_csv
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    download_task = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3
    )

    create_csv_task >> upload_task >> download_task
