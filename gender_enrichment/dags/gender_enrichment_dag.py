from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
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
DEFAULT_INPUT_TOPIC="gender-enrich-input"
DEFAULT_OUTPUT_TOPIC="gender-enrich-output"

with DAG(
    dag_id="gender_enrichment_dag",
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False, 
    tags=["gender_enrichment"],
    params={
        "mode":"batch",
        "input_bucket": DEFAULT_INPUT_BUCKET,
        "input_key": DEFAULT_INPUT_KEY,
        "out_bucket": DEFAULT_OUT_BUCKET,
        "out_key": DEFAULT_OUT_KEY,
        "input_topic":DEFAULT_INPUT_TOPIC,
        "output_topic":DEFAULT_OUTPUT_TOPIC,
    },
    max_active_runs=5
) as dag:
    start=EmptyOperator(task_id="start")
    gender_enrichment_task=DockerOperator(
        task_id="gender_enrich_task",
        image="gender_enrichment_image",
        api_version="auto",
        auto_remove=False,
        command="python /opt/airflow/dags/gender_enrichment/enrich_gender_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="final-bulk-enrichment-v2_airflow_network",
        environment={
            "KAFKA_BROKER": "kafka:9092",
            "S3_ENDPOINT": "http://minio:9000",
            "INPUT_BUCKET": "{{ dag_run.conf.get('input_bucket', params.input_bucket) }}",
            "INPUT_KEY": "{{ dag_run.conf.get('input_key', params.input_key) }}",
            "OUT_BUCKET": "{{ dag_run.conf.get('out_bucket', params.out_bucket) }}",
            "OUT_KEY": "{{ dag_run.conf.get('out_key', params.out_key) }}"
        },
        mounts=[
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/gender_enrichment/data", target="/gender_enrichment/data", type="bind"),
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/input", target="/samples/input", type="bind"),
        Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/output", target="/samples/output", type="bind"),
        ],
        mount_tmp_dir=False
    )

    realtime_gender_enrichment_task=DockerOperator(
        task_id="realtime_gender_enrichment_task",
        image="gender_enrichment_image",
        api_version="auto",
        auto_remove=False,
        command="python /opt/airflow/dags/gender_enrichment/enrich_gender_run.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="final-bulk-enrichment-v2_airflow_network",
        environment={
            "KAFKA_BROKER": "kafka:9092",
            "S3_ENDPOINT": "http://minio:9000",
            "MODE": "{{ dag_run.conf.get('mode', params.mode) }}",
            "INPUT_TOPIC": "{{ dag_run.conf.get('input_topic', params.input_topic) }}",
            "OUTPUT_TOPIC": "{{ dag_run.conf.get('out_topic', params.output_topic) }}"
        },
        mounts=[
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/gender_enrichment/data", target="/gender_enrichment/data", type="bind"),
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/input", target="/samples/input", type="bind"),
            Mount(source="/Users/uverma/Documents/ETL Project/final-bulk-enrichment-v2/samples/output", target="/samples/output", type="bind"),
        ],
        mount_tmp_dir=False
    )


    # def choose_mode(**context):
    #     mode=context["params"].get("mode","batch")
    #     if mode=="batch":
    #         return "gender_enrich_task"
    #     else:
    #         return "realtime_gender_enrichment_task"

    def choose_mode(**context):
        # mode=context["params"].get("mode","batch")
        # mode=dag_run.conf.get('mode',context["params"].get("mode","batch"))
        dag_run=context.get("dag_run")
        mode="batch"
        if dag_run and dag_run.conf and "mode" in dag_run.conf:
            mode=dag_run.conf.get("mode")
        else:
            mode=context["params"].get("mode","batch")
        if mode=="batch":
            return "gender_enrich_task"
        else:
            return "realtime_gender_enrichment_task"

    
    branch=BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_mode,
        provide_context=True
    )

    end=EmptyOperator(task_id="end",trigger_rule="none_failed_min_one_success")

    start >> branch
    branch >> gender_enrichment_task >> end
    branch >> realtime_gender_enrichment_task >> end

 