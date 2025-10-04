from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from common.libs import s3_utils, kafka_utils, progress
from common.libs.notification_utils import send_failure_email, send_success_email, send_slack_failure, send_slack_success
from datetime import datetime
import subprocess
import pandas as pd, os, tempfile, json
from airflow.models import Variable

BUCKET = "raw"
LOCAL_INPUT="/samples/input/reverse_geocode.csv"
LOCAL_OUTPUT="/samples/output/reverse_geocode_enriched.csv"
LOCAL_GEO_MASTER_CSV="/reverse_geocode/data/geo_master.csv"
GEO_MASTER_CSV="/reference/geo_master.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="reverse_geocode_enriched.csv"
DEFAULT_INPUT_TOPIC="reverse-geocode-input"
DEFAULT_OUTPUT_TOPIC="reverse-geocode-output"

# Wrapper functions to call both email and Slack notifications
def notify_failure(context):
    """Send both email and Slack notifications on failure"""
    send_failure_email(context)
    send_slack_failure(context)

def notify_success(context):
    """Send both email and Slack notifications on success"""
    send_success_email(context)
    send_slack_success(context)

# Default args with both Email and Slack notifications
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True,
    # 'retries': 1,
    'on_failure_callback': notify_failure,  # Calls both email and Slack
    'on_success_callback': notify_success   # Calls both email and Slack
}

with DAG(
    dag_id="reverse_geocode_dag",
    schedule=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["Test-enrichment"],
    default_args=default_args,
    params={
        "mode":"batch",
        "input_bucket": DEFAULT_INPUT_BUCKET,
        "input_key": DEFAULT_INPUT_KEY,
        "out_bucket": DEFAULT_OUT_BUCKET,
        "out_key": DEFAULT_OUT_KEY,
        "input_topic":DEFAULT_INPUT_TOPIC,
        "output_topic":DEFAULT_OUTPUT_TOPIC
    },
    max_active_runs=5
) as dag:
    
    start=EmptyOperator(task_id="start")

    reverse_geocode_task=DockerOperator(
        task_id="reverse_geocode_task",
        image="reverse_geocode_image",
        api_version="auto",
        auto_remove="never",
        command="python /opt/airflow/dags/reverse_geocode/reverse_geocode_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="final-bulk-enrichment-v2_airflow_network",
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
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/reverse_geocode/data", target="/reverse_geocode/data", type="bind"),
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/input", target="/samples/input", type="bind"),
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/output", target="/samples/output", type="bind"),
    ],
        mount_tmp_dir=False 
    )


    realtime_task=DockerOperator(
        task_id="realtime_reverse_geocode",
        image="reverse_geocode_image",
        api_version="auto",
        auto_remove="never",
        command="python /opt/airflow/dags/reverse_geocode/reverse_geocode_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="final-bulk-enrichment-v2_airflow_network",
        environment={
            "MODE": "{{ dag_run.conf.get('mode', params.mode) }}",
            "KAFKA_BROKER": "kafka:9092",
            "S3_ENDPOINT": "http://minio:9000",
            # In realtime mode, INPUT/OUTPUT may not be used, Kafka messages are consumed directly
            "INPUT_TOPIC": "{{ dag_run.conf.get('input_topic', params.input_topic) }}",
            "OUTPUT_TOPIC": "{{ dag_run.conf.get('out_topic', params.output_topic) }}"
        },
        mounts=[
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/reverse_geocode/data", target="/reverse_geocode/data", type="bind"),
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/input", target="/samples/input", type="bind"),
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/output", target="/samples/output", type="bind"),
        ],
        mount_tmp_dir=False
    )


    # def choose_mode(**context):
    #     mode=context["params"].get("mode","batch")
    #     if mode=="batch":
    #         return "reverse_geocode_task"
    #     else:
    #         return "realtime_reverse_geocode"

    def choose_mode(**context):
        # mode=context["params"].get("mode","batch")
        dag_run=context.get("dag_run")
        mode="batch"
        if dag_run and dag_run.conf and "mode" in dag_run.conf:
            mode=dag_run.conf.get("mode")
        else:
            mode=context["params"].get("mode","batch")
        if mode=="batch":
            return "reverse_geocode_task"
        else:
            return "realtime_reverse_geocode"

    
    branch=BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_mode
    )

    end=EmptyOperator(task_id="end",trigger_rule="none_failed_min_one_success")

    start >> branch
    branch >> reverse_geocode_task >> end
    branch >> realtime_task >> end
