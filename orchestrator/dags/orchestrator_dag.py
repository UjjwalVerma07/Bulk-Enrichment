from datetime import datetime
import os
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from kafka import KafkaConsumer
import yaml, json, time
from common.libs import kafka_utils,s3_utils,progress
from airflow.decorators import task


def fetch_pipeline_config(ti):
    try:
        consumer = KafkaConsumer(
            "pipeline-config",
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="pipeline_fetcher"
        )
        
        config = None
        print("Waiting for pipeline config message from Kafka...")

        # Keep polling until message received
        while config is None:
            for message in consumer:
                try:
                    config = json.loads(message.value.decode("utf-8"))
                    print("Received pipeline config:", config)
                    break
                except json.JSONDecodeError:
                    print("Invalid JSON received, skipping...")
                    continue
            if config is None:
                time.sleep(1)

        consumer.close()

    except Exception as e:
        print("Kafka unavailable, falling back to YAML:", str(e))
        with open("/opt/airflow/dags/config/pipeline.yml", "r") as f:
            config = yaml.safe_load(f)

    ti.xcom_push(key="pipeline_config", value=config)
    return config



def choose_mode(ti):
    config = ti.xcom_pull(key="pipeline_config", task_ids="fetch_pipeline_config")
    pipelines=config.get("pipelines",[])
    if not pipelines:
        pipelines=[config]
        # return "end"
    mode=pipelines[0].get("mode","batch").lower()
    print(f"Orchestrator mode chosen: {mode}")
    if mode == "batch":
        return "batch_start"
    elif mode == "stream":
        return "realtime_start"
    else:
        return "end"

import uuid;
from kafka import KafkaConsumer
import time
import json

@task
def wait_for_kafka_data(pipeline: dict):
    input_topic = pipeline.get("main_input_topic", "enrich-input-topic")
    # Stable group_id derived from pipeline_id
    group_id = f"orchestrator_waiter_{pipeline.get('pipeline_id', 'default')}"

    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset="latest",  # "earliest" if you want old messages
        enable_auto_commit=True,
        group_id=group_id,
        consumer_timeout_ms=5000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print(f"Waiting for data in topic '{input_topic}' with group_id '{group_id}'...")

    data_found = False
    while not data_found:
        records = consumer.poll(timeout_ms=5000)
        if records:
            data_found = True
            print(f"Data found in topic '{input_topic}'!")
            break
        print(f"No data yet in topic '{input_topic}', sleeping 5s...")
        time.sleep(5)

    consumer.close()
    return pipeline


from airflow.operators.dagrun_operator import TriggerDagRunOperator

def trigger_and_wait(dag_id, conf, context):
    """Trigger child DAG using TriggerDagRunOperator and wait for completion"""
    operator = TriggerDagRunOperator(
        task_id=f"trigger_{dag_id}",
        trigger_dag_id=dag_id,
        conf=conf,
        wait_for_completion=True,
        poke_interval=10,
    )
    return operator.execute(context=context)



CAP_SIZE=5
import uuid
def count_kafka_message(topic):
    from kafka import KafkaConsumer
    consumer=KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset="earliest",
        # enable_auto_commit=True,
        enable_auto_commit=False,
        group_id="enrich-consumer",
        consumer_timeout_ms=10000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    count=0
    record=0
    for message in consumer:
        record=message.value
        break
    consumer.close()
    if isinstance(record,dict):
        record=[record]
    count=len(record)
    return count,record


@task
def run_stream_pipeline(pipeline:dict, **context):

    stream_sequence = pipeline.get("sequence", [])

    if not stream_sequence:
        # fallback to default stream sequence if not in config
        stream_sequence = [
            {"dag_id": "email_validation", "input_topic": "enrich-input-topic", "out_topic": "email-validation-output"},
            {"dag_id": "reverse_geocode", "input_topic": "email-validation-output", "out_topic": "reverse-geocode-output"},
            {"dag_id": "gender_enrichment", "input_topic": "reverse-geocode-output", "out_topic": "enrich-output-topic"},
        ]
    

    #Here will write the logic for the switchin between the real_time and Batch
    main_topic=stream_sequence[0].get("input_topic","enrich-input-topic")
    message_count,records=count_kafka_message(main_topic)
    print(f"Message count in topic '{main_topic}:{message_count} ")

    if message_count>CAP_SIZE:
        print(f"Message count {message_count} exceeds {CAP_SIZE}, switching to batch processing.")
        #Here we will load the defaul pipeline.yaml file
        with open("/opt/airflow/dags/config/pipeline.yml","r") as f:
            batch_config=yaml.safe_load(f)
        
        batch_sequence=batch_config.get("sequence",[])
        prev_bucket=batch_config.get("input_bucket","raw")
        prev_key=batch_config.get("input_key","customer_raw.csv")
        import pandas as pd

        #we have to create a extra file for the data we are getting from kafka topic and passed it as config
        df=pd.DataFrame(records)
        temp_file=tempfile.NamedTemporaryFile(suffix=".csv",delete=False)
        local_input_path=temp_file.name
        temp_file.close()
        df.to_csv(local_input_path,index=False)
        s3_utils.upload_file("raw","kafka_data.csv",local_input_path)
        os.remove(local_input_path)
        prev_key="kafka_data.csv"
        
        

        for step in batch_sequence:
            dag_id=f"{step['dag_id']}_dag"
            print(f"[BATCH-FALLBACK] TRIGGERING DAG: {dag_id}")
            trigger_and_wait(
                dag_id=dag_id,
                conf={
                    "input_bucket":prev_bucket,
                    "input_key":prev_key,
                    "out_bucket":step["out_bucket"],
                    "out_key":step["out_key"]
                },
                context=context
            )
            prev_bucket,prev_key=step["out_bucket"],step["out_key"]
        print("Batch processing [FALLBACK] completed.")
    
    else:
        for step in stream_sequence:
            dag_id = f"{step['dag_id']}_dag"
            print(f"[STREAM] Triggering DAG: {dag_id}")

            trigger_and_wait(
                dag_id=dag_id,
                conf={
                    "mode": "stream",
                    "input_topic": step["input_topic"],
                    "out_topic": step["out_topic"]
                },
                context=context
            )


@task
def run_single_pipeline(pipeline: dict, **context):
    prev_bucket = pipeline.get("input_bucket")
    prev_key = pipeline.get("input_key")
    sequence = pipeline.get("sequence", [])

    for step in sequence:
        dag_id = f"{step['dag_id']}_dag"
        print(f"[BATCH] Triggering DAG: {dag_id}")
        trigger_and_wait(
            dag_id=dag_id,
            conf={
                "input_bucket": prev_bucket,
                "input_key": prev_key,
                "out_bucket": step['out_bucket'],
                "out_key": step['out_key']
            },
            context=context
        )
        prev_bucket, prev_key = step['out_bucket'], step['out_key']

def extract_real_pipelines(ti):
    config=ti.xcom_pull(task_ids="fetch_pipeline_config",key="pipeline_config")

    if config is None:
        raise ValueError("No config foudn in XCom from fetch_pipeline_config")
    pipelines=config.get("pipelines",[])
    if not pipelines:
        pipelines=[config]
        # raise ValueError("No pipelines found in config!")
    extracted=[]
    for idx,pipeline in enumerate(pipelines,start=1):
        extracted.append({
            "pipeline_id":f"pipeline_{idx}",
            "mode":pipeline.get("mode","stream"),
            "main_input_topic":pipeline.get("main_input_topic","enrich-input-topic"),
            "main_out_topic":pipeline.get("main_out_topic","enrich-output-topic"),
            "sequence":pipeline.get("sequence",[])
        })
    print("Extracted Pipelines:",json.dumps(extracted,indent=2))
    return extracted

#This is for Extracting the Batch Pipelines
def extract_pipelines(ti):
    config = ti.xcom_pull(task_ids="fetch_pipeline_config", key="pipeline_config")

    if config is None:
        raise ValueError("No config found in XCom from fetch_pipeline_config")

    pipelines = config.get("pipelines", [])
    if not pipelines:
        pipelines=[config]
        # raise ValueError("No pipelines found in config!")

    extracted = []
    for idx, pipeline in enumerate(pipelines, start=1):
        extracted.append({
            "pipeline_id": f"pipeline_{idx}",
            "mode":pipeline.get("mode","batch"), #Made Changes Here Last Time
            "input_bucket": pipeline.get("input_bucket", "raw"),
            "input_key": pipeline.get("input_key", "customer_raw.csv"),
            "sequence": pipeline.get("sequence", [])
        })

    print("Extracted Pipelines:", json.dumps(extracted, indent=2))
    return extracted

with DAG(
    dag_id="orchestrator_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestrator"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    fetch_config = PythonOperator(
        task_id="fetch_pipeline_config",
        python_callable=fetch_pipeline_config,
    )

    branch = BranchPythonOperator(
        task_id="choose_mode",
        python_callable=choose_mode
    )


    batch_start = EmptyOperator(task_id="batch_start")

    extract = PythonOperator(
        task_id="extract_pipelines",
        python_callable=extract_pipelines,
    )

    run_all_pipelines = run_single_pipeline.expand(pipeline=extract.output)


    realtime_start = EmptyOperator(task_id="realtime_start")

    extract_real=PythonOperator(
        task_id="extract_real_pipelines",
        python_callable=extract_real_pipelines
    )
    wait_for_data=wait_for_kafka_data.expand(pipeline=extract_real.output)

    run_stream=run_stream_pipeline.expand(pipeline=wait_for_data)


    start >> fetch_config >> branch
    branch >> batch_start >>extract>> run_all_pipelines >> end
    branch >> realtime_start >>extract_real >>  wait_for_data >> run_stream >> end


