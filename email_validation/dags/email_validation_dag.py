from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json

BUCKET="raw"
INPUT_FILE="input/location_data.csv"
OUTPUT_FILE="output/output.csv"

def validate_emails(input_bucket,input_key,out_bucket,out_key):
    #Started event-----------------
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
    #Step1:-Download the input file
        # local_input=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
        local_input="/samples/input/customer_raw.csv"
    # s3_utils.download_file(BUCKET,INPUT_FILE,local_input)


        s3_utils.download_file(input_bucket,input_key,local_input)


        df=pd.read_csv(local_input)
    #Fake Validation
    #Here we have to write the validation for the c++ cli email Validation
    #df["is_valid"]=True
    
    #Step2:- Create the output file path
        # local_output=tempfile.NamedTemporaryFile(delete=False,suffix=".csv").name
        local_output="/samples/output/email_validated.csv"
    #df.to_csv(local_output,index=False)


    #Step3:- Run the validation Script
        try:
            subprocess.run(
                ["/opt/airflow/dags/email_validation/scripts/run_validator.sh", local_input, local_output],
                check=True
                )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Validator script failed: {e}")
    
    #Step4:- Upload the output file backe to s3
    # s3_utils.upload_file(BUCKET,OUTPUT_FILE,local_output)

    #s3_utils.upload_file(BUCKET,"input/location_data.csv",local_output)
    
        s3_utils.upload_file(out_bucket,out_key,local_output)

    #Step4:- Send the Kafka Message and Update the Progress File
     #Succeed event
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
        #Failed Case handle;
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

with DAG(
    dag_id="email_validation_dag",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["enrichment"],
)as dag:
    task=PythonOperator(
        task_id="validate_emails",
        python_callable=validate_emails,
        op_kwargs={
     "input_bucket":"{{dag_run.conf.get('input_bucket','raw')}}",
     "input_key":"{{dag_run.conf.get('input_key','customer_raw.csv')}}",
     "out_bucket":"{{dag_run.conf.get('out_bucket','staging')}}",
     "out_key":"{{dag_run.conf.get('out_key','email_validated.csv')}}",
        },
        # provide_context=True,
    )

