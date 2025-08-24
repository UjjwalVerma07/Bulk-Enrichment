
from datetime import datetime
from airflow import DAG
import yaml
import json
from kafka import KafkaConsumer
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def load_pipeline_config():
    with open("/opt/airflow/dags/config/pipeline.yml","r") as f:
        return yaml.safe_load(f)

def load_pipeline_config_from_kafka():
    # Implement Kafka consumer logic to fetch the pipeline config
    consumer=KafkaConsumer(
        "pipeline-config",
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    config=None
    for message in consumer:
        config=message.value
        break
    consumer.close()

    if config is None:
        config=load_pipeline_config();
        # raise ValueError("No pipeline configuration received from Kafka")
    return config

with DAG(
    dag_id="orchestrator_dag", 
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["orchestrator"],
) as dag:
    start=EmptyOperator(task_id="start")
    end=EmptyOperator(task_id="end")

    # config=load_pipline_config()
    config=load_pipeline_config_from_kafka()
    sequence=config["sequence"]

    input_bucket=config["input_bucket"]
    input_key=config["input_key"]

    prev_task=start

    for step in sequence:
        dag_id=step.get("dag_id") + "_dag"
        out_bucket=step.get("out_bucket")
        out_key=step.get("out_key")

        trigger=TriggerDagRunOperator(
            task_id=f"trigger_{step.get('dag_id')}",
            trigger_dag_id=dag_id,
            conf={
                "input_bucket": input_bucket,
                "input_key": input_key,
                "out_bucket": out_bucket,
                "out_key": out_key
            },
            wait_for_completion=True
        )
        prev_task >> trigger
        prev_task=trigger

        input_bucket=out_bucket
        input_key=out_key
    prev_task >> end



   