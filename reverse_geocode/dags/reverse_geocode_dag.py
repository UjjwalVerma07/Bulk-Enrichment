from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json

BUCKET = "raw"
LOCAL_INPUT="/samples/input/reverse_geocode.csv"
LOCAL_OUTPUT="/samples/output/reverse_geocode_enriched.csv"
LOCAL_GEO_MASTER_CSV="/reverse_geocode/data/geo_master.csv"
GEO_MASTER_CSV="/reference/geo_master.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="reverse_geocode_enriched.csv"

with DAG(
    dag_id="reverse_geocode_dag",
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["Test-enrichment"],
    params={
        "mode":"batch",
        "input_bucket": DEFAULT_INPUT_BUCKET,
        "input_key": DEFAULT_INPUT_KEY,
        "out_bucket": DEFAULT_OUT_BUCKET,
        "out_key": DEFAULT_OUT_KEY
    }
) as dag:
    reverse_geocode_task=DockerOperator(
        task_id="reverse_geocode_task",
        image="reverse_geocode_image",
        api_version="auto",
        auto_remove=False,
        command="python /opt/airflow/dags/reverse_geocode/reverse_geocode_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bulk-enrichment_airflow_network",
        environment={
            "MODE": "{{ dag_run.conf.get('mode', params.mode) }}",
            "KAFKA_BROKER": "kafka:9092",
            "S3_ENDPOINT": "http://minio:9000",
            "INPUT_BUCKET": "{{ dag_run.conf.get('input_bucket', params.input_bucket) }}",
            "INPUT_KEY": "{{ dag_run.conf.get('input_key', params.input_key) }}",
            "OUT_BUCKET": "{{ dag_run.conf.get('out_bucket', params.out_bucket) }}",
            "OUT_KEY": "{{ dag_run.conf.get('out_key', params.out_key) }}"
        },
           mounts=[
        # Mount(source="/Users/uverma/bulk-enrichment/reverse_geocode", target="/opt/airflow/dags/reverse_geocode", type="bind"),
        Mount(source="/Users/uverma/bulk-enrichment/reverse_geocode/data", target="/reverse_geocode/data", type="bind"),
        Mount(source="/Users/uverma/bulk-enrichment/samples/input", target="/samples/input", type="bind"),
        Mount(source="/Users/uverma/bulk-enrichment/samples/output", target="/samples/output", type="bind"),
    ],

        mount_tmp_dir=False
    )

