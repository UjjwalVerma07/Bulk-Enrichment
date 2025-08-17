from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common.libs import s3_utils,kafka_utils
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json

# BUCKET="test-bucket"
# INPUT_FILE="input/input.csv"
# OUTPUT_FILE="output.csv"
# GEO_MASTER_CSV="reference/geo_master.csv"
BUCKET = "test-bucket"
INPUT_FILE = "input/location_data.csv"   # matches upload
OUTPUT_FILE = "output/output.csv"        # (better to keep it under output/)
GEO_MASTER_CSV = "reference/geo_master.csv"

def run_reverse_geocode(input_bucket,input_key,out_bucket,out_key):

    #started event
    kafka_utils.send_event("pipeline-progress",{
        "stage":"reverse_geocode",
        "status":"Started",
        "time":datetime.now().isoformat()
    })
    try:
    #Step 1-Download the input file 
        local_input=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
    # s3_utils.download_file(BUCKET,INPUT_FILE,local_input)
        s3_utils.download_file(input_bucket,input_key,local_input)
    #For Pipelinging Purpose (kind of working)
    # s3_utils.download_file(BUCKET,OUTPUT_FILE,local_input)

    #Step 2-Download the geo_master file from S3

        local_geo_master=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
        s3_utils.download_file(BUCKET,GEO_MASTER_CSV,local_geo_master)

    #Step 3 - Read both the files
    
        df=pd.read_csv(local_input)
        geo_master_df=pd.read_csv(local_geo_master)

    #Merge on latitude and longitute
        merged_df=pd.merge(df,geo_master_df,how="left",on=["latitude","longitude"])
        merged_df.fillna({"city":"Unknown"},inplace=True)

    #city column comes from geo_master_df
        local_output=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
        merged_df.to_csv(local_output,index=False)


    #Upload the enriched file back to S3
    # s3_utils.upload_file(BUCKET,OUTPUT_FILE,local_output)
    # s3_utils.upload_file(BUCKET,"input/location_data.csv",local_output)
        s3_utils.upload_file(out_bucket,out_key,local_output)

    #send the messsage via kakfa util

        kafka_utils.send_event("pipeline-progress",{
            "stage":"reverse_geocode",
            "status":"Succeeded",
            "time":datetime.now().isoformat()
            })

    #Upload progress.json file to S3

        s3_utils.upload_progress_file(BUCKET,"progress",{
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

        s3_utils.upload_progress_file(BUCKET,"progress",{
            "stage":"reverse_geocode",
            "status":"Failed",
            "time":datetime.now().isoformat()
        })


with DAG(
    dag_id="reverse_geocode_dag",
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["enrichment"]
) as dag:
    reverse_geocode_task=PythonOperator(
        task_id="reverse_geocode_task",
        python_callable=run_reverse_geocode,
        op_kwargs={
            "input_bucket":"{{dag_run.conf.get('input_bucket','staging')}}",
            "input_key":"{{dag_run.conf.get('input_key','email_validated.csv')}}",
            "out_bucket":"{{dag_run.conf.get('out_bucket','staging')}}",
            "out_key":"{{dag_run.conf.get('out_key','geo_enriched.csv')}}",
        },
    )

