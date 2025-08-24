from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json

BUCKET="raw"
LOCAL_INPUT="/samples/input/customer_raw.csv"
LOCAL_OUTPUT="/samples/output/email_validated.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="email_validated.csv"

def validate_emails(input_bucket,input_key,out_bucket,out_key):
    kafka_utils.send_event("pipeline-progress",{
        "stage":"email_validation",
        "status":"Started",
        "time":datetime.now().isoformat(),
    })
    progress.upload_progress_file(BUCKET,"progress",{
        "stage":"email_validation",
        "status":"Started",
        "time":datetime.now().isoformat()
    })
    
    try:

        local_input=LOCAL_INPUT
        s3_utils.download_file(input_bucket,input_key,local_input)
        df=pd.read_csv(local_input)
        local_output=LOCAL_OUTPUT


    #Step3:- Run the validation Script
        try:
            subprocess.run(
                ["/opt/airflow/dags/email_validation/scripts/run_validator.sh", local_input, local_output],
                check=True 
                )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Validator script failed: {e}")
    
    #Step4:- Upload the output file backe to s3    
        s3_utils.upload_file(out_bucket,out_key,local_output)
        kafka_utils.send_event("pipeline-progress",{
            "stage":"email_validation",
            "status":"Succeeded",
            "time":datetime.now().isoformat()
        })
        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"email_validation",
            "status":"Succeeded",
            "time":datetime.now().isoformat()
        })
    except Exception as e:
        kafka_utils.send_event("pipeline-progress",{
            "stage":"email_validation",
            "status":"Failed",
            "time":datetime.now().isoformat()
        })
        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"email_validation",
            "status":"Failed",
            "time":datetime.now().isoformat()
        })

if __name__=="__main__":
    validate_emails(
        input_bucket=DEFAULT_INPUT_BUCKET,
        input_key=DEFAULT_INPUT_KEY,
        out_bucket=DEFAULT_OUT_BUCKET,
        out_key=DEFAULT_OUT_KEY
    )