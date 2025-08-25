import sys
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


def send_status(stage, status, error=None):
    """Send status to Kafka + S3 progress."""
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
                    kafka_utils.send_event(out_topic, validated_records)
                    send_status("email_validation", "Succeeded")

                # cleanup
                os.remove(input_path)
                os.remove(output_path)

    except Exception as e:
        send_status("email_validation", "Failed", error=str(e))
        print(f"Error in Processing Stream: {str(e)}")
        raise  # don't use sys.exit(1)



# def run_email_validation_stream(topic="email-validation-input", out_topic="email-validation-output"):
#     send_status("email_validation", "Started")
#     try:
#         for records in kafka_utils.consume_records(topic):
#             print(f"[DEBUG] Raw records from Kafka: {records}")

#             if not records:
#                 print("[DEBUG] No records found, skipping...")
#                 continue

#             if isinstance(records, dict):
#                 records = [records]
#                 print(f"[DEBUG] Converted single dict to list: {records}")

#             try:
#                 df = pd.DataFrame(records)
#                 print(f"[DEBUG] DataFrame created with shape {df.shape}")
#             except Exception as e:
#                 send_status("email_validation", "Failed", error=f"DataFrame creation failed: {str(e)}")
#                 print(f"[ERROR] DataFrame creation failed: {str(e)} | records={records}")
#                 continue

#             if df.empty:
#                 print("[DEBUG] Empty DataFrame, skipping...")
#                 continue

#             try:
#                 with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_input, \
#                      tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_output:

#                     input_path = tmp_input.name
#                     output_path = tmp_output.name

#                     print(f"[DEBUG] Writing DataFrame to temp input file: {input_path}")
#                     df.to_csv(input_path, index=False)

#                     try:
#                         print(f"[DEBUG] Running validator script with input={input_path}, output={output_path}")
#                         subprocess.run(
#                             ["/opt/airflow/dags/email_validation/scripts/run_validator.sh", input_path, output_path],
#                             check=True
#                         )
#                     except subprocess.CalledProcessError as e:
#                         send_status("email_validation", "Failed", error=f"C++ Validator failed: {str(e)}")
#                         print(f"[ERROR] C++ Validator failed: {str(e)}")
#                         continue

#                     try:
#                         print(f"[DEBUG] Reading validated output file: {output_path}")
#                         validated_df = pd.read_csv(output_path)
#                         print(f"[DEBUG] Validated DataFrame shape: {validated_df.shape}")
#                     except Exception as e:
#                         send_status("email_validation", "Failed", error=f"Output read failed: {str(e)}")
#                         print(f"[ERROR] Failed reading validator output: {str(e)}")
    #                     continue

    #                 try:
    #                     validated_records = validated_df.to_dict(orient="records")
    #                     print(f"[DEBUG] Validated records count: {len(validated_records)}")

    #                     if validated_records:
    #                         kafka_utils.send_event(out_topic, validated_records)
    #                         send_status("email_validation", "Succeeded")
    #                         print("[DEBUG] Records sent to Kafka successfully.")
    #                 except Exception as e:
    #                     send_status("email_validation", "Failed", error=f"Kafka send failed: {str(e)}")
    #                     print(f"[ERROR] Kafka send failed: {str(e)}")
    #                     continue

    #                 # cleanup
    #                 try:
    #                     os.remove(input_path)
    #                     os.remove(output_path)
    #                     print(f"[DEBUG] Cleaned up temp files: {input_path}, {output_path}")
    #                 except Exception as e:
    #                     print(f"[WARNING] Temp file cleanup failed: {str(e)}")

    #         except Exception as e:
    #             send_status("email_validation", "Failed", error=f"File handling failed: {str(e)}")
    #             print(f"[ERROR] File handling failed: {str(e)}")
    #             continue

    # except Exception as e:
    #     send_status("email_validation", "Failed", error=f"Stream processing failed: {str(e)}")
    #     print(f"[FATAL] Error in Processing Stream: {str(e)}", exc_info=True)
    #     raise




    

def validate_emails(input_bucket=DEFAULT_INPUT_BUCKET,input_key=DEFAULT_INPUT_KEY,out_bucket=DEFAULT_OUT_BUCKET,out_key=DEFAULT_OUT_KEY):
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
    mode=os.environ.get("MODE","batch").lower()
    if mode=="batch":
        validate_emails(
        os.environ.get("INPUT_BUCKET"),
        os.environ.get("INPUT_KEY"),
        os.environ.get("OUT_BUCKET"),
        os.environ.get("OUT_KEY")
        )
    else:
        run_email_validation_stream()
