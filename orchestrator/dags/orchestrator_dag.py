

from datetime import datetime
import os
import tempfile
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from kafka import KafkaConsumer
import yaml, json, time
from common.libs import kafka_utils,s3_utils,progress


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
    mode = (config.get("mode") or "batch").lower()
    print(f"Orchestrator mode chosen: {mode}")
    if mode == "batch":
        return "batch_start"
    elif mode == "stream":
        return "realtime_start"
    else:
        return "end"

import uuid;
def wait_for_kafka_data(ti):
    config = ti.xcom_pull(task_ids='fetch_pipeline_config', key='pipeline_config')
    input_topic = config.get("main_input_topic", "enrich-input-topic")
    # group_id = f"orchestrator_waiter_{uuid.uuid4()}"
    while True:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=['kafka:9092'],
            # auto_offset_reset="earliest",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="orchestrator_waiter",
            # group_id=group_id,
            consumer_timeout_ms=5000
        )
        data_found = False
        for _ in consumer:
            data_found = True
            break
        consumer.close()
        if data_found:
            print(f"Data found in Kafka topic '{input_topic}', proceeding...")
            break
        print(f"No data yet in Kafka topic '{input_topic}', sleeping 5 seconds...")
        time.sleep(5)



from airflow.models import DagRun
from airflow.utils.state import State
import time

from airflow.api.common.experimental.trigger_dag import trigger_dag
from datetime import datetime

def trigger_and_wait(dag_id, conf):
    run_id = f"manual__{datetime.now().isoformat()}"
    dr = trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=conf,
        replace_microseconds=False
    )

    while True:
        dag_run = DagRun.find(dag_id=dag_id, run_id=run_id)
        if dag_run and dag_run[0].state in [State.SUCCESS, State.FAILED]:
            print(f"DAG {dag_id} finished with state {dag_run[0].state}")
            if dag_run[0].state == State.FAILED:
                raise Exception(f"{dag_id} failed")
            break
        time.sleep(10)


def run_batch_pipeline(ti):
    """Trigger batch DAGs sequentially based on Kafka config."""
    config = ti.xcom_pull(task_ids="fetch_pipeline_config", key="pipeline_config")
    sequence = config.get("sequence", [])
    prev_bucket = config.get("input_bucket", "raw")
    prev_key = config.get("input_key", "customer_raw.csv")

    from airflow.api.common.experimental.trigger_dag import trigger_dag
    from datetime import datetime

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
        )

        prev_bucket, prev_key = step['out_bucket'], step['out_key']

CAP_SIZE=5
import uuid
def count_kafka_message(topic):
    from kafka import KafkaConsumer
    consumer=KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="enrich-consumer",
        consumer_timeout_ms=5000,
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



def run_stream_pipeline(ti):
    """Trigger stream DAGs sequentially based on Kafka config."""
    config = ti.xcom_pull(task_ids="fetch_pipeline_config", key="pipeline_config")
    stream_sequence = config.get("stream_sequence", [])
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
        from airflow.api.common.experimental.trigger_dag import trigger_dag
        from datetime import datetime
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
                }
            )
            prev_bucket,prev_key=step["out_bucket"],step["out_key"]
        print("Batch processing [FALLBACK] completed.")
    
    else:
        from airflow.api.common.experimental.trigger_dag import trigger_dag
        from datetime import datetime

        for step in stream_sequence:
            dag_id = f"{step['dag_id']}_dag"
            print(f"[STREAM] Triggering DAG: {dag_id}")

            trigger_and_wait(
                dag_id=dag_id,
                conf={
                "mode": "stream",
                "input_topic": step["input_topic"],
                "out_topic": step["out_topic"]
                }
            )


with DAG(
    dag_id="orchestrator_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
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
    run_batch = PythonOperator(
        task_id="run_batch_pipeline",
        python_callable=run_batch_pipeline
    )


    realtime_start = EmptyOperator(task_id="realtime_start")
    wait_for_data = PythonOperator(
        task_id="wait_for_kafka_data",
        python_callable=wait_for_kafka_data
    )
    run_stream = PythonOperator(
        task_id="run_stream_pipeline",
        python_callable=run_stream_pipeline
    )


    start >> fetch_config >> branch
    branch >> batch_start >> run_batch >> end
    branch >> realtime_start >> wait_for_data >> run_stream >> end
