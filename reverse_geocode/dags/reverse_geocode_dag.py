from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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

def run_reverse_geocode(input_bucket,input_key,out_bucket,out_key):
    kafka_utils.send_event("pipeline-progress",{
        "stage":"reverse_geocode",
        "status":"Started",
        "time":datetime.now().isoformat()
    })
    progress.upload_progress_file(BUCKET,"progress",{
        "stage":"reverse_geocode",
        "status":"Started",
        "time":datetime.now().isoformat()
    })
    
    try:
    #Step 1-Download the input file 
        local_input=LOCAL_INPUT
        s3_utils.download_file(input_bucket,input_key,local_input)
    #Step 2-Download the geo_master file from S3
        local_geo_master=LOCAL_GEO_MASTER_CSV
        s3_utils.download_file(BUCKET,GEO_MASTER_CSV,local_geo_master)

    #Step 3 - Read both the files
    
        df=pd.read_csv(local_input)
        geo_master_df=pd.read_csv(local_geo_master)

    #Merge on latitude and longitute
        merged_df=pd.merge(df,geo_master_df,how="left",on=["latitude","longitude"])
        merged_df.fillna({"city":"Unknown"},inplace=True)
        local_output=LOCAL_OUTPUT
        merged_df.to_csv(local_output,index=False)
        s3_utils.upload_file(out_bucket,out_key,local_output)

    #send the messsage via kakfa util

        kafka_utils.send_event("pipeline-progress",{
            "stage":"reverse_geocode",
            "status":"Succeeded",
            "time":datetime.now().isoformat()
            })

    #Upload progress.json file to S3

        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"reverse_geocode",
            "status":"Succeeded",
            "time":datetime.now().isoformat()
            })
    except Exception as e:
        kafka_utils.send_event("pipeline-progress",{
            "stage":"reverse_geocode",
            "status":"Failed",
            "time":datetime.now().isoformat()
        })

        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"reverse_geocode",
            "status":"Failed",
            "time":datetime.now().isoformat()
        })


with DAG(
    dag_id="reverse_geocode_dag",
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["enrichment"],
    params={
        "input_bucket": DEFAULT_INPUT_BUCKET,
        "input_key": DEFAULT_INPUT_KEY,
        "out_bucket": DEFAULT_OUT_BUCKET,
        "out_key": DEFAULT_OUT_KEY
    }
) as dag:
    reverse_geocode_task=PythonOperator(
        task_id="reverse_geocode_task",
        python_callable=run_reverse_geocode,
        op_kwargs={
            "input_bucket":"{{dag_run.conf.get('input_bucket',params.input_bucket)}}",
            "input_key":"{{dag_run.conf.get('input_key',params.input_key)}}",
            "out_bucket":"{{dag_run.conf.get('out_bucket',params.out_bucket)}}",
            "out_key":"{{dag_run.conf.get('out_key',params.out_key)}}",
        },
    )

