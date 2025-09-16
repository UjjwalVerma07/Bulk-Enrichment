import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
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
CAP_SIZE=5


def send_status(stage, status, error=None):

    event = {
        "stage": stage,
        "status": status,
        "time": datetime.now().isoformat()
    }
    if error:
        event["error"] = error

    kafka_utils.send_event("pipeline-progress", event)
    progress.upload_progress_file(BUCKET, "progress", event)

def run_email_validation_stream(topic="email-validation-input", out_topic="email-validation-output"):
    send_status("email_validation", "Started")
    try:
        for records in kafka_utils.consume_records(topic):
            if not records:
                continue
            if isinstance(records, dict):
                records = [records]
            print(f"Received {len(records)} records from {topic}")
            
            if len(records)>CAP_SIZE:
                print(f"CAP Size exceeded switching back to batch mode")
                df=pd.DataFrame(records)
                df.to_csv(LOCAL_INPUT,index=False)
                try:
                    subprocess.run(
                        ["/opt/airflow/dags/email_validation/scripts/run_validator.sh", LOCAL_INPUT, LOCAL_OUTPUT],
                        check=True
                    )
                except subprocess.CalledProcessError as e:
                    send_status("email_validation", "Failed", error=str(e))
                    print(f"C++ Validator failed: {str(e)}")
                    continue
                s3_utils.upload_file(DEFAULT_OUT_BUCKET,DEFAULT_OUT_KEY,LOCAL_OUTPUT)
                send_status("email_validation","Succeeded(batch_fallback)")
                return

            df = pd.DataFrame(records)
            if df.empty:
                continue

            with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_input, \
                 tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_output:

                input_path = tmp_input.name
                output_path = tmp_output.name

                df.to_csv(input_path, index=False)

                try:
                    subprocess.run(
                        ["/opt/airflow/dags/email_validation/scripts/run_validator.sh", input_path, output_path],
                        check=True
                    )
                except subprocess.CalledProcessError as e:
                    send_status("email_validation", "Failed", error=str(e))
                    print(f"C++ Validator failed: {str(e)}")
                    continue

                validated_df = pd.read_csv(output_path)
                validated_records = validated_df.to_dict(orient="records")

                if validated_records:
                    # kafka_utils.send_event(out_topic, validated_records)
                    for record in validated_df.to_dict(orient="records"):
                        kafka_utils.send_event(out_topic, record)
                    send_status("email_validation", "Succeeded")

                # cleanup
                os.remove(input_path)
                os.remove(output_path)

    except Exception as e:
        send_status("email_validation", "Failed", error=str(e))
        print(f"Error in Processing Stream: {str(e)}")
        raise  




def validate_emails(input_bucket=DEFAULT_INPUT_BUCKET,input_key=DEFAULT_INPUT_KEY,out_bucket=DEFAULT_OUT_BUCKET,out_key=DEFAULT_OUT_KEY):
    
    send_status("email_validation","Started")
    print(f"InputBucket:{input_bucket} , Input_key:{input_key} , OutputBucket:{out_bucket} , OutputKey:{out_key}")
    
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
        send_status("email_validation","Succeeded")
    except Exception as e:
        error_msg=str(e)
        send_status("email_validation","Failed",error=error_msg)
        raise

if __name__=="__main__":
    mode=os.environ.get("MODE","batch").lower()
    if mode=="batch":
        validate_emails(
        os.environ.get("INPUT_BUCKET"),
        os.environ.get("INPUT_KEY"),
        os.environ.get("OUT_BUCKET"),
        os.environ.get("OUT_KEY")
        )
    else:
        run_email_validation_stream(
            os.environ.get("INPUT_TOPIC"),
            os.environ.get("OUTPUT_TOPIC")
        )
