from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess 
import pandas as pd,os,tempfile,json
from common.libs.notification_utils import notify_success, send_failure_email,send_success_email,send_slack_success,send_slack_failure
from airflow.lineage.entities import File
from common.libs.openlineage_utils import OpenLineageClient,create_output_statistics_facet,add_output_statistics_to_dataset

BUCKET="raw"
LOCAL_INPUT="/samples/input/customer_raw.csv"
LOCAL_OUTPUT="/samples/output/email_validated.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="email_validated.csv"
DEFAULT_INPUT_TOPIC="email-validation-input"
DEFAULT_OUTPUT_TOPIC="email-validation-output"

def create_s3_lineage_entity(bucket:str,key:str) -> File:
    s3_endpoint=os.getenv("S3_ENDPOINT","http://minio:9000")
    return File(url=f"s3://{bucket}/{key}")

def create_kafka_lineage_entity(topic:str) -> File:
    kafka_broker=os.getenv("KAFKA_BROKER","kafka:9092")
    return File(url=f"kafka://{kafka_broker}/{topic}")


#Wrapper functions to call both email and Slack notifications
def notify_failure(context):
    """Send both email and Slack notification on failure"""
    send_failure_email(context)
    send_slack_failure(context)

def notify_success(context):
    """Send both email and Slack notification on success"""
    send_success_email(context)
    send_slack_success(context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True,
    'retries': 1,
    'on_failure_callback': notify_failure,
    'on_success_callback': notify_success
}

with DAG(
    dag_id="email_validation_dag",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=["enrichment"],
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
)as dag:
    start=EmptyOperator(task_id="start")
    email_validation_task=DockerOperator(
        task_id="validate_emails", 
        image="email_validation_image",
        api_version="auto",
        auto_remove="never",
        command="python /opt/airflow/dags/email_validation/enrich_email_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="final-bulk-enrichment-v2_airflow_network",
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
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/input", target="/samples/input", type="bind"),
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/output", target="/samples/output", type="bind"),
    ],
    mount_tmp_dir=False,
    inlets=[
        create_s3_lineage_entity(DEFAULT_INPUT_BUCKET,DEFAULT_INPUT_KEY),
    ],
    outlets=[
        create_s3_lineage_entity(DEFAULT_OUT_BUCKET,DEFAULT_OUT_KEY)
    ],
    doc_md="""
    ### Email Validation Batch Task
    
    This task validates email addresses in the input data.
    
    **Inputs:**
    - S3 Input: `{{ params.input_bucket }}/{{ params.input_key }}`
    **Output:**
    - S3 Output: `{{ params.out_bucket }}/{{ params.out_key }}`
    """

    )

    realtime_email_validation_task=DockerOperator(
        task_id="realtime_validate_emails",
        image="email_validation_image",
        api_version="auto",
        auto_remove="never",
        command="python /opt/airflow/dags/email_validation/enrich_email_run.py",
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
            # Mount(source="/Users/uverma/bulk-enrichment/reverse_geocode/data", target="/reverse_geocode/data", type="bind"),
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/input", target="/samples/input", type="bind"),
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/output", target="/samples/output", type="bind"),
        ],
        mount_tmp_dir=False,
        inlets=[
            create_kafka_lineage_entity(DEFAULT_INPUT_TOPIC)
        ],
        outlets=[
            create_kafka_lineage_entity(DEFAULT_OUTPUT_TOPIC)
        ]
    )


    # def choose_mode(**context):
    #     mode=context["params"].get("mode","batch")
    #     if mode=="batch":
    #         return "validate_emails"
    #     else:
    #         return "realtime_validate_emails"

    def choose_mode(**context):
        # mode=context["params"].get("mode","batch")
        dag_run=context.get("dag_run")
        mode="batch"
        if dag_run and dag_run.conf and "mode" in dag_run.conf:
            mode=dag_run.conf.get("mode")
        else:
            mode=context["params"].get("mode","batch")

        if mode=="batch":
            return "validate_emails"
        else:
            return "realtime_validate_emails"

    
    branch=BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_mode,
    )

    end=EmptyOperator(task_id="end",trigger_rule="none_failed_min_one_success")

    start >> branch
    branch >> email_validation_task >> end
    branch >> realtime_email_validation_task >> end




