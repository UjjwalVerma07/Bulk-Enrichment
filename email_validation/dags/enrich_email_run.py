import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd,os,tempfile,json
from common.libs.openlineage_utils import OpenLineageClient,create_processing_facet,add_output_statistics_to_dataset
from uuid import uuid4

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
    ol_client=OpenLineageClient()
    run_id=str(uuid4())
    job_name="email-validation-stream"
    email_schema=[
        {"name":"email","type":"STRING","description":"Email Addresss"},
        {"name":"is_valid","type":"BOOLEAN","description":"Email Validation Result"},
    ]
    input_dataset=ol_client.create_kafka_dataset(
        topic=topic,
        schema_fields=email_schema,
        description=f"Input stream for email validation from topic {topic}"
    ),
    output_dataset=ol_client.create_kafka_dataset(
        topic=out_topic,
        schema_fields=email_schema,
        description=f"Output stream for email validation to topic {out_topic}"
    )
    job_facets={
        "jobType":{
            "_producer":ol_client.producer,
            "_schemaURL":"https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
            "processingType":"STREAMING",
            "integration":"KAFKA",
            "jobType":"VALIDATION"
        }
    }
    run_facets=create_processing_facet(name="email-validation-stream",mode="stream")
    ol_client.emit_start_event(
        job_name=job_name,
        run_id=run_id,
        inputs=[input_dataset],
        job_facets=job_facets,
        run_facets=run_facets,
    )
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
                total_records_processed += len(records)
                fallback_output=ol_client.create_s3_dataset(
                    bucket=DEFAULT_OUT_BUCKET,
                    key=DEFAULT_OUT_KEY,
                    schema_fields=email_schema,
                    description="Batch fallback output due to CAP_SIZE exceeded"
                )
                add_output_statistics_to_dataset(fallback_output,len(df))
                run_facets_complete=create_processing_facet(mode="stream",record_count=total_records_processed)
                ol_client.emit_complete_event(
                    job_name=job_name,
                    run_id=run_id,
                    inputs=[input_dataset],
                    outputs=[fallback_output],
                    job_facets=job_facets,
                    run_facets=run_facets_complete
                )
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
                
                total_records_processed += len(validated_records)
                output_dataset_with_stats=add_output_statistics_to_dataset(output_dataset.copy(),total_records_processed)
                run_facets_complete=create_processing_facet(name="email-validation-stream",mode="stream",record_count=total_records_processed)
                ol_client.emit_complete_event(
                    job_name=job_name,
                    run_id=run_id,
                    inputs=[input_dataset],
                    outputs=[output_dataset_with_stats],
                    job_facets=job_facets,
                    run_facets=run_facets_complete
                )
                # cleanup
                os.remove(input_path)
                os.remove(output_path)

    except Exception as e:
        ol_client.emit_fail_event(
            job_name=job_name,
            run_id=run_id,
            error_message=str(e),
            inputs=[input_dataset],
            job_facets=job_facets
        )
        send_status("email_validation", "Failed", error=str(e))
        print(f"Error in Processing Stream: {str(e)}")
        raise  




def validate_emails(input_bucket=DEFAULT_INPUT_BUCKET,input_key=DEFAULT_INPUT_KEY,out_bucket=DEFAULT_OUT_BUCKET,out_key=DEFAULT_OUT_KEY):
    
    send_status("email_validation","Started")
    print(f"InputBucket:{input_bucket} , Input_key:{input_key} , OutputBucket:{out_bucket} , OutputKey:{out_key}")

    ol_client=OpenLineageClient()
    run_id=str(uuid4())
    job_name="email-validation-batch"
    email_schema=[
        {"name":"email",'type':"STRING","description":"Email Addresss"},
        {"name":"is_valid","type":"BOOLEAN","description":"Email Validation Result"},
    ]
    input_dataset=ol_client.create_s3_dataset(
        bucket=input_bucket,
        key=input_key,
        schema_fields=email_schema[:2],
        description=f"Input data for email validation from {input_bucket}/{input_key}"
    )
    output_dataset=ol_client.create_s3_dataset(
        bucket=out_bucket,
        key=out_key,
        schema_fields=email_schema,
        description=f"Validated output data to {out_bucket}/{out_key}"
    )
    job_facets={
        "jobType":{
            "_producer":ol_client.producer,
            "_schemaURL":"https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
            "processingType":"BATCH",
            "integration":"S3",
            "jobType":"VALIDATION"
        }
    }
    run_facets=create_processing_facet(name="email-validation-batch",mode="batch")
    ol_client.emit_start_event(
        job_name=job_name,
        run_id=run_id,
        inputs=[input_dataset],
        job_facets=job_facets,
        run_facets=run_facets,
    )
    
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
        output_file_size = os.path.getsize(LOCAL_OUTPUT) if os.path.exists(LOCAL_OUTPUT) else None
        output_dataset_with_stats=add_output_statistics_to_dataset(output_dataset.copy(),len(df),output_file_size)
        run_facets_complete=create_processing_facet(name="email-validation-batch",mode="batch",record_count=len(df))
        ol_client.emit_complete_event(
            job_name=job_name,
            run_id=run_id,
            inputs=[input_dataset],
            outputs=[output_dataset_with_stats],
            job_facets=job_facets,
            run_facets=run_facets_complete
        )
        send_status("email_validation","Succeeded")
    except Exception as e:
        error_msg=str(e)
        ol_client.emit_fail_event(
            job_name=job_name,
            run_id=run_id,
            error_message=error_msg,
            inputs=[input_dataset],
            job_facets=job_facets
        )
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
