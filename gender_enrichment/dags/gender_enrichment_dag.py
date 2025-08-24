from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

BUCKET="raw"
LOCAL_INPUT="/samples/input/gender_enrichment.csv"
LOCAL_OUTPUT="/samples/output/final_enriched.csv"
LOCAL_GENDER_MASTER_CSV="/gender_enrichment/data/gender_master.csv"
GENDER_MASTER_CSV="/reference/gender_master.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="gender_enriched.csv"
 
with DAG(
    dag_id="gender_enrichment_dag",
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["gender_enrichment"],
    params={
        "input_bucket": DEFAULT_INPUT_BUCKET,
        "input_key": DEFAULT_INPUT_KEY,
        "out_bucket": DEFAULT_OUT_BUCKET,
        "out_key": DEFAULT_OUT_KEY
    }
) as dag:
    gender_enrichment_task=DockerOperator(
        task_id="gender_enrich_task",
        image="gender_enrichment_image",
        api_version="auto",
        auto_remove=False,
        command="python /opt/airflow/dags/gender_enrichment/enrich_gender_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bulk-enrichment_airflow_network",
        environment={
            "KAFKA_BROKER": "kafka:9092",
            "S3_ENDPOINT": "http://minio:9000",
            "INPUT_BUCKET": "{{ dag_run.conf.get('input_bucket', params.input_bucket) }}",
            "INPUT_KEY": "{{ dag_run.conf.get('input_key', params.input_key) }}",
            "OUT_BUCKET": "{{ dag_run.conf.get('out_bucket', params.out_bucket) }}",
            "OUT_KEY": "{{ dag_run.conf.get('out_key', params.out_key) }}"
        },
        mounts=[
        Mount(source="/Users/uverma/bulk-enrichment/gender_enrichment/data", target="/gender_enrichment/data", type="bind"),
        Mount(source="/Users/uverma/bulk-enrichment/samples/input", target="/samples/input", type="bind"),
        Mount(source="/Users/uverma/bulk-enrichment/samples/output", target="/samples/output", type="bind"),
        ],
        mount_tmp_dir=False
    )

