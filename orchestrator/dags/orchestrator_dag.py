# from datetime import datetime
# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from kafka import KafkaConsumer
# import yaml, json, time

# # ----------------------
# # Python Callables
# # ----------------------
# # config=None
# def fetch_pipeline_config(ti, **kwargs):
#     """Fetch pipeline config from Kafka or fallback to local YAML."""
#     try:
#         consumer = KafkaConsumer(
#             "pipeline-config",
#             bootstrap_servers=['kafka:9092'],
#             auto_offset_reset="latest",
#             enable_auto_commit=True,
#             group_id="pipeline_fetcher",
#             consumer_timeout_ms=6000
#         )
#         config = None
#         for message in consumer:
#             try:
#                 config = json.loads(message.value.decode('utf-8'))
#                 break
#             except json.JSONDecodeError:
#                 continue
#         consumer.close()

#         if config is None:
#             raise Exception("No Message From Kafka")
#     except Exception:
#         with open("/opt/airflow/dags/config/pipeline.yml", "r") as f:
#             config = yaml.safe_load(f)

#     ti.xcom_push(key="pipeline_config", value=config)
#     print("Fetched Pipeline:", config)
#     return config


# def choose_mode(ti, **kwargs):
#     config = ti.xcom_pull(key="pipeline_config", task_ids="fetch_pipeline_config")
#     mode = (config.get("mode") or "batch").lower()
#     # mode="stream"
#     print(f"Orchestrator mode chosen: {mode}")
#     if mode == "batch":
#         return "batch_start"
#     elif mode == "stream":
#         return "realtime_start"
#     else:
#         return "end"


# def wait_for_kafka_data(ti, **kwargs):
#     config = ti.xcom_pull(task_ids='fetch_pipeline_config', key='pipeline_config')
#     input_topic = config.get("main_input_topic", "enrich-input-topic")

#     while True:
#         consumer = KafkaConsumer(
#             input_topic,
#             bootstrap_servers=['kafka:9092'],
#             auto_offset_reset="earliest",
#             enable_auto_commit=True,
#             group_id="orchestrator_waiter",
#             consumer_timeout_ms=5000
#         )
#         data_found = False
#         for _ in consumer:
#             data_found = True
#             break
#         consumer.close()
#         if data_found:
#             print(f"Data found in Kafka topic '{input_topic}', proceeding...")
#             break
#         print(f"No data yet in Kafka topic '{input_topic}', sleeping 5 seconds...")
#         time.sleep(5)


# # ----------------------
# # DAG Definition
# # ----------------------
# with DAG(
#     dag_id="orchestrator_dag",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["orchestrator"]
# ) as dag:

#     start = EmptyOperator(task_id="start")
#     end = EmptyOperator(task_id="end")

#     fetch_config = PythonOperator(
#         task_id="fetch_pipeline_config",
#         python_callable=fetch_pipeline_config,
#     )

#     branch = BranchPythonOperator(
#         task_id="choose_mode",
#         python_callable=choose_mode
#     )

#     # ----------------------
#     # Batch Path
#     # ----------------------
#     batch_start = EmptyOperator(task_id="batch_start")
#     batch_sequence = [
#         {"dag_id": "email_validation", "out_bucket": "staging", "out_key": "email_validated.csv"},
#         {"dag_id": "reverse_geocode", "out_bucket": "staging", "out_key": "geo_enriched.csv"},
#         {"dag_id": "gender_enrichment", "out_bucket": "enriched", "out_key": "final_enriched.csv"}
#     ]

#     prev_batch_task = batch_start
#     for idx, step in enumerate(batch_sequence):
#         trigger = TriggerDagRunOperator(
#             task_id=f"trigger_{step['dag_id']}",
#             trigger_dag_id=f"{step['dag_id']}_dag",
#             conf={
#                 "input_bucket": "raw" if idx == 0 else batch_sequence[idx-1]['out_bucket'],
#                 "input_key": "customer_raw.csv" if idx == 0 else batch_sequence[idx-1]['out_key'],
#                 "out_bucket": step['out_bucket'],
#                 "out_key": step['out_key']
#             },
#             wait_for_completion=True
#         )
#         # Log when DAG is triggered
#         def log_trigger(dag_id):
#             print(f"[BATCH] Triggering DAG: {dag_id}")
#         log_task = PythonOperator(
#             task_id=f"log_{step['dag_id']}",
#             python_callable=log_trigger,
#             op_kwargs={"dag_id": step['dag_id']}
#         )
#         prev_batch_task >> log_task >> trigger
#         prev_batch_task = trigger

#     # ----------------------
#     # Stream Path
#     # ----------------------
#     realtime_start = EmptyOperator(task_id="realtime_start")
#     wait_for_data = PythonOperator(
#         task_id="wait_for_kafka_data",
#         python_callable=wait_for_kafka_data
#     )
    
#     stream_sequence = [
#         {"dag_id": "email_validation", "input_topic": "enrich-input-topic", "out_topic": "email-validation-output"},
#         {"dag_id": "reverse_geocode", "input_topic": "email-validation-output", "out_topic": "reverse-geocode-output"},
#         {"dag_id": "gender_enrichment", "input_topic": "reverse-geocode-output", "out_topic": "enrich-output-topic"},
#     ]
#     # stream_sequence = config.get("sequence", [])

#     # prev_stream_task = realtime_start
#     prev_stream_task=wait_for_data
#     # input_topic="enrich-input-topic"
#     for step in stream_sequence:
#         trigger = TriggerDagRunOperator(
#             task_id=f"realtime_trigger_{step['dag_id']}",
#             trigger_dag_id=f"{step['dag_id']}_dag",
#             conf={
#                 "mode": "stream",
#                 "input_topic": step["input_topic"],
#                 "out_topic": step["out_topic"]
#             },
#             wait_for_completion=True
#         )
#         # Log stream triggers
#         def log_stream_trigger(dag_id):
#             print(f"[STREAM] Triggering DAG: {dag_id}")
#         log_task = PythonOperator(
#             task_id=f"log_stream_{step['dag_id']}",
#             python_callable=log_stream_trigger,
#             op_kwargs={"dag_id": step['dag_id']}
#         )
#         prev_stream_task >> log_task >> trigger
#         prev_stream_task = trigger

#     # ----------------------
#     # DAG Flow
#     # ----------------------
#     start >> fetch_config >> branch
#     branch >> batch_start >> prev_batch_task >> end
#     branch >> realtime_start >> wait_for_data >> prev_stream_task >> end




from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from kafka import KafkaConsumer
import yaml, json, time

# ----------------------
# Python Callables
# ----------------------

# def fetch_pipeline_config(ti, **kwargs):
#     """Fetch pipeline config from Kafka or fallback to local YAML."""
#     try:
#         consumer = KafkaConsumer(
#             "pipeline-config",
#             bootstrap_servers=['kafka:9092'],
#             auto_offset_reset="latest",
#             enable_auto_commit=True,
#             group_id="pipeline_fetcher",
#             consumer_timeout_ms=6000
#         )
#         config = None
#         for message in consumer:
#             try:
#                 config = json.loads(message.value.decode('utf-8'))
#                 break
#             except json.JSONDecodeError:
#                 continue
#         consumer.close()

#         if config is None:
#             raise Exception("No Message From Kafka")
#     except Exception:
#         # fallback to YAML
#         with open("/opt/airflow/dags/config/pipeline.yml", "r") as f:
#             config = yaml.safe_load(f)

#     ti.xcom_push(key="pipeline_config", value=config)
#     print("Fetched Pipeline:", config)
#     return config



def fetch_pipeline_config(ti, **kwargs):
    """Fetch pipeline config from Kafka and wait until a message arrives.
    If Kafka is unavailable, fallback to YAML.
    """
    try:
        consumer = KafkaConsumer(
            "pipeline-config",
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="pipeline_fetcher"
            # No consumer_timeout_ms here → keeps listening forever
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
            # Optional: avoid tight loop if no messages
            if config is None:
                time.sleep(1)

        consumer.close()

    except Exception as e:
        print("Kafka unavailable, falling back to YAML:", str(e))
        with open("/opt/airflow/dags/config/pipeline.yml", "r") as f:
            config = yaml.safe_load(f)

    ti.xcom_push(key="pipeline_config", value=config)
    return config



def choose_mode(ti, **kwargs):
    config = ti.xcom_pull(key="pipeline_config", task_ids="fetch_pipeline_config")
    mode = (config.get("mode") or "batch").lower()
    print(f"Orchestrator mode chosen: {mode}")
    if mode == "batch":
        return "batch_start"
    elif mode == "stream":
        return "realtime_start"
    else:
        return "end"


def wait_for_kafka_data(ti, **kwargs):
    config = ti.xcom_pull(task_ids='fetch_pipeline_config', key='pipeline_config')
    input_topic = config.get("main_input_topic", "enrich-input-topic")

    while True:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="orchestrator_waiter",
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
    # Poll for completion
    while True:
        dag_run = DagRun.find(dag_id=dag_id, run_id=run_id)
        if dag_run and dag_run[0].state in [State.SUCCESS, State.FAILED]:
            print(f"DAG {dag_id} finished with state {dag_run[0].state}")
            if dag_run[0].state == State.FAILED:
                raise Exception(f"{dag_id} failed")
            break
        time.sleep(10)


def run_batch_pipeline(ti, **kwargs):
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


def run_stream_pipeline(ti, **kwargs):
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


# ----------------------
# DAG Definition
# ----------------------
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

    # ----------------------
    # Batch Path
    # ----------------------
    batch_start = EmptyOperator(task_id="batch_start")
    run_batch = PythonOperator(
        task_id="run_batch_pipeline",
        python_callable=run_batch_pipeline
    )

    # ----------------------
    # Stream Path
    # ----------------------
    realtime_start = EmptyOperator(task_id="realtime_start")
    wait_for_data = PythonOperator(
        task_id="wait_for_kafka_data",
        python_callable=wait_for_kafka_data
    )
    run_stream = PythonOperator(
        task_id="run_stream_pipeline",
        python_callable=run_stream_pipeline
    )

    # ----------------------
    # DAG Flow
    # ----------------------
    start >> fetch_config >> branch
    branch >> batch_start >> run_batch >> end
    branch >> realtime_start >> wait_for_data >> run_stream >> end
