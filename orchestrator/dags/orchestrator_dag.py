
# from datetime import datetime
# from airflow import DAG
# import yaml
# import json
# from kafka import KafkaConsumer
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# def load_pipeline_config():
#     with open("/opt/airflow/dags/config/pipeline.yml","r") as f:
#         return yaml.safe_load(f)

# def load_pipeline_config_from_kafka():
#     # Implement Kafka consumer logic to fetch the pipeline config
#     consumer=KafkaConsumer(
#         "pipeline-config",
#         bootstrap_servers=['kafka:9092'],
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#     )
#     config=None
#     for message in consumer:
#         config=message.value
#         break
#     consumer.close()

#     if config is None:
#         config=load_pipeline_config()
#         # raise ValueError("No pipeline configuration received from Kafka")
#     return config

# with DAG(
#     dag_id="orchestrator_dag", 
#     start_date=datetime(2025,1,1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["orchestrator"],
# ) as dag:
#     start=EmptyOperator(task_id="start")
#     end=EmptyOperator(task_id="end")
#     #Because of this orchestrator dag is not visible in the UI
#     # config=load_pipeline_config()
#     config=load_pipeline_config_from_kafka()
#     sequence=config["sequence"]

#     input_bucket=config["input_bucket"]
#     input_key=config["input_key"]

#     prev_task=start

#     for step in sequence:
#         dag_id=step.get("dag_id") + "_dag"
#         out_bucket=step.get("out_bucket")
#         out_key=step.get("out_key")

#         trigger=TriggerDagRunOperator(
#             task_id=f"trigger_{step.get('dag_id')}",
#             trigger_dag_id=dag_id,
#             conf={
#                 "input_bucket": input_bucket,
#                 "input_key": input_key,
#                 "out_bucket": out_bucket,
#                 "out_key": out_key
#             },
#             wait_for_completion=True
#         )
#         prev_task >> trigger
#         prev_task=trigger

#         input_bucket=out_bucket
#         input_key=out_key
#     prev_task >> end

from datetime import datetime
from airflow import DAG
import yaml
import json
from kafka import KafkaConsumer
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def fetch_pipeline_config(**context):
    """
    Fetch pipeline config from Kafka at runtime.
    Pushes config into XCom for downstream tasks.
    """
    try:
        consumer = KafkaConsumer(
            "pipeline-config",
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset="latest",   # only new messages
            enable_auto_commit=True
        )

        config = None
        for message in consumer:
            try:
                config = json.loads(message.value.decode('utf-8'))
                break
            except json.JSONDecodeError:
                continue
        consumer.close()

        if config is None:
            # fallback: read local yaml
            with open("/opt/airflow/dags/config/pipeline.yml", "r") as f:
                config = yaml.safe_load(f)

        # save to XCom for later tasks
        context['ti'].xcom_push(key="pipeline_config", value=config)

    except Exception:
        # fallback if Kafka unreachable
        with open("/opt/airflow/dags/config/pipeline.yml", "r") as f:
            config = yaml.safe_load(f)
        context['ti'].xcom_push(key="pipeline_config", value=config)






with DAG(
    dag_id="orchestrator_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["orchestrator"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    fetch = PythonOperator(
        task_id="fetch_pipeline_config",
        python_callable=fetch_pipeline_config,
        provide_context=True
    )

    try:
        with open("/opt/airflow/dags/config/pipeline.yml", "r") as f:
            default_config = yaml.safe_load(f)
    except Exception:
        default_config = {
            "input_bucket": "raw",
            "input_key": "customer_raw.csv",
            "sequence": []
        }

    input_bucket = default_config.get("input_bucket", "raw")
    input_key = default_config.get("input_key", "customer_raw.csv")

    prev_task = fetch

    for step in default_config.get("sequence", []):
        dag_id = step.get("dag_id") + "_dag"
        out_bucket = step.get("out_bucket")
        out_key = step.get("out_key")

        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{step.get('dag_id')}",
            trigger_dag_id=dag_id,
            conf={
                "input_bucket": input_bucket,
                "input_key": input_key,
                "out_bucket": out_bucket,
                "out_key": out_key
            },
            wait_for_completion=True,
        )

        prev_task >> trigger
        prev_task = trigger
        input_bucket, input_key = out_bucket, out_key

    prev_task >> end
