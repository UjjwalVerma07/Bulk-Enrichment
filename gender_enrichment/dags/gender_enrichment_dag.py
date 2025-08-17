from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common.libs import s3_utils,kafka_utils
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json

BUCKET="test-bucket"
INPUT_FILE="input/location_data.csv"
OUTPUT_FILE="output/output.csv"
GENDER_MASTER_CSV="reference/gender_master.csv"

def enrich_gender(input_bucket,input_key,out_bucket,out_key):
    #Started Event
    kafka_utils.send_event("pipeline-progress",{
        "stage":"gender_enrichment",
        "status":"Started",
        "time":datetime.now().isoformat()
    })


    try:
    #Step1 - download the inputfile
        local_input=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
    # s3_utils.download_file(BUCKET,INPUT_FILE,local_input)
        s3_utils.download_file(input_bucket,input_key,local_input)
   #FOR PIPELINIGN PURPOSE. (kind of working)
    # s3_utils.download_file(BUCKET,OUTPUT_FILE,local_input)

    #Step2-download the gender reference file
        local_gender_master=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
        s3_utils.download_file(BUCKET,GENDER_MASTER_CSV,local_gender_master)

    #Step3 - Read both the files
        df=pd.read_csv(local_input)
        gender_master_df=pd.read_csv(local_gender_master)

    #Step4-Merge the dataframe
        merged_df=pd.merge(df,gender_master_df,on="name",how="left")
        merged_df.fillna({"gender":"UNKNOWN"},inplace=True) #handle the unknow gender

    #Step5-Save the output file
        local_output=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
        merged_df.to_csv(local_output,index=False)

    #Step6-Upload the output file
    # s3_utils.upload_file(BUCKET,OUTPUT_FILE,local_output)
        s3_utils.upload_file(out_bucket,out_key,local_output)
    # s3_utils.upload_file(BUCKET,"input/location_data.csv",local_output)



    #Step7- Send Message to kafka and update the progress file

        kafka_utils.send_event("pipeline-progress",{
            "stage":"gender_enrichment",
            "status":"Succeeded",
            "date":datetime.now().isoformat()
            })

        s3_utils.upload_progress_file(BUCKET,"progress",{
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
        s3_utils.upload_progress_file(BUCKET,"progress",{
            "stage":"gender_enrichment",
            "status":"Failed",
            "date":datetime.now().isoformat()
        })


with DAG(
    dag_id="gender_enrichment_dag",
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["gender_enrichment"]
) as dag:
    gender_enrichment_task=PythonOperator(
        task_id="gender_enrichment_task",
        python_callable=enrich_gender,
        op_kwargs={
            "input_bucket":"{{dag_run.conf.get('input_bucket','staging')}}",
            "input_key":"{{dag_run.conf.get('input_key','geo_enriched.csv')}}",
            "out_bucket":"{{dag_run.conf.get('out_bucket','enriched')}}",
            "out_key":"{{dag_run.conf.get('out_key','final_enriched.csv')}}",
        },
    )

