from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import BranchPythonOperator
from docker.types import Mount
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json

BUCKET="raw"
LOCAL_INPUT="/samples/input/customer_raw.csv"
LOCAL_OUTPUT="/samples/output/email_validated.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="email_validated.csv"
   

with DAG(
    dag_id="email_validation_dag",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["enrichment"],
    params={
        "mode":"batch",
        "input_bucket": DEFAULT_INPUT_BUCKET,
        "input_key": DEFAULT_INPUT_KEY,
        "out_bucket": DEFAULT_OUT_BUCKET,
        "out_key": DEFAULT_OUT_KEY
    }
)as dag:
    start=EmptyOperator(task_id="start")
    email_validation_task=DockerOperator(
        task_id="validate_emails", 
        image="email_validation_image",
        api_version="auto",
        auto_remove=True,
        command="python /opt/airflow/dags/email_validation/enrich_email_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bulk-enrichment_airflow_network",
        environment={
            "KAFKA_BROKER": "kafka:9092",
            "S3_ENDPOINT": "http://minio:9000",
            "INPUT_BUCKET":"{{dag_run.conf.get('input_bucket',params.input_bucket)}}",
            "INPUT_KEY":"{{dag_run.conf.get('input_key',params.input_key)}}",
            "OUT_BUCKET":"{{dag_run.conf.get('out_bucket',params.out_bucket)}}",
            "OUT_KEY":"{{dag_run.conf.get('out_key',params.out_key)}}",
        },
        mounts=[
        # Mount(source="/Users/uverma/bulk-enrichment/reverse_geocode", target="/opt/airflow/dags/reverse_geocode", type="bind"),
        # Mount(source="/Users/uverma/bulk-enrichment/reverse_geocode/data", target="/reverse_geocode/data", type="bind"),
        Mount(source="/Users/uverma/bulk-enrichment/samples/input", target="/samples/input", type="bind"),
        Mount(source="/Users/uverma/bulk-enrichment/samples/output", target="/samples/output", type="bind"),
    ],

    )

    realtime_email_validation_task=DockerOperator(
        task_id="realtime_validate_emails",
        image="email_validation_image",
        api_version="auto",
        auto_remove=True,
        command="python /opt/airflow/dags/email_validation/enrich_email_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bulk-enrichment_airflow_network",
        environment={
            "MODE": "realtime",
            "KAFKA_BROKER": "kafka:9092",
            "S3_ENDPOINT": "http://minio:9000",
            # In realtime mode, INPUT/OUTPUT may not be used, Kafka messages are consumed directly
        },
        mounts=[
            Mount(source="/Users/uverma/bulk-enrichment/reverse_geocode/data", target="/reverse_geocode/data", type="bind"),
            Mount(source="/Users/uverma/bulk-enrichment/samples/input", target="/samples/input", type="bind"),
            Mount(source="/Users/uverma/bulk-enrichment/samples/output", target="/samples/output", type="bind"),
        ],
        mount_tmp_dir=False
    )


    def choose_mode(**context):
        mode=context["params"].get("mode","batch")
        if mode=="batch":
            return "validate_emails"
        else:
            return "realtime_validate_emails"

    
    branch=BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_mode,
        provide_context=True
    )

    end=EmptyOperator(task_id="end")

    start >> branch
    branch >> email_validation_task >> end
    branch >> realtime_email_validation_task >> end




