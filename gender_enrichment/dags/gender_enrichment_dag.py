from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json

BUCKET="raw"
LOCAL_INPUT="/samples/input/gender_enrichment.csv"
LOCAL_OUTPUT="/samples/output/final_enriched.csv"
LOCAL_GENDER_MASTER_CSV="/gender_enrichment/data/gender_master.csv"
GENDER_MASTER_CSV="/reference/gender_master.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="gender_enriched.csv"

def enrich_gender(input_bucket,input_key,out_bucket,out_key):
    
    kafka_utils.send_event("pipeline-progress",{
        "stage":"gender_enrichment",
        "status":"Started",
        "time":datetime.now().isoformat()
    })
    progress.upload_progress_file(BUCKET,"progress",{
        "stage":"gender_enrichment",
        "status":"Started",
        "time":datetime.now().isoformat()
    })


    try:
    #Step1 - download the inputfile
        local_input=LOCAL_INPUT
        s3_utils.download_file(input_bucket,input_key,local_input)
        local_gender_master=LOCAL_GENDER_MASTER_CSV
        s3_utils.download_file(BUCKET,GENDER_MASTER_CSV,local_gender_master)
        df=pd.read_csv(local_input)
        gender_master_df=pd.read_csv(local_gender_master)

    #Step4-Merge the dataframe
        merged_df=pd.merge(df,gender_master_df,on="name",how="left")
        merged_df.fillna({"gender":"UNKNOWN"},inplace=True) #handle the unknow gender

    #Step5-Save the output file
        local_output=LOCAL_OUTPUT
        merged_df.to_csv(local_output,index=False)

    #Step6-Upload the output file
        s3_utils.upload_file(out_bucket,out_key,local_output)
    #Step7- Send Message to kafka and update the progress file

        kafka_utils.send_event("pipeline-progress",{
            "stage":"gender_enrichment",
            "status":"Succeeded",
            "date":datetime.now().isoformat()
            })

        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"gender_enrichment",
            "status":"Succeeded",
            "date":datetime.now().isoformat()
            })
    except Exception as e:
        kafka_utils.send_event("pipeline-progress",{
            "stage":"gender_enrichment",
            "status":"Failed",
            "date":datetime.now().isoformat()
        })
        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"gender_enrichment",
            "status":"Failed",
            "date":datetime.now().isoformat()
        })


with DAG(
    dag_id="gender_enrichment_dag",
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["gender_enrichment"],
    params={
        "input_bucket": DEFAULT_INPUT_BUCKET,
        "input_key": DEFAULT_INPUT_KEY,
        "out_bucket": DEFAULT_OUT_BUCKET,
        "out_key": DEFAULT_OUT_KEY
    }
) as dag:
    gender_enrichment_task=PythonOperator(
        task_id="gender_enrichment_task",
        python_callable=enrich_gender,
        op_kwargs={
            "input_bucket":"{{dag_run.conf.get('input_bucket',params.input_bucket)}}",
            "input_key":"{{dag_run.conf.get('input_key',params.input_key)}}",
            "out_bucket":"{{dag_run.conf.get('out_bucket',params.out_bucket)}}",
            "out_key":"{{dag_run.conf.get('out_key',params.out_key)}}",
        },
    )

