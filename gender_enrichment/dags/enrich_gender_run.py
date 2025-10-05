import sys
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd 
import os,tempfile,json
from uuid import uuid4
from common.libs.openlineage_utils import OpenLineageClient, create_processing_facet, add_output_statistics_to_dataset
BUCKET="raw"
LOCAL_INPUT="/samples/input/gender_enrichment.csv"
LOCAL_OUTPUT="/samples/output/final_enriched.csv"
LOCAL_GENDER_MASTER_CSV="/gender_enrichment/data/gender_master.csv"
GENDER_MASTER_CSV="/reference/gender_master.csv"
DEFAULT_INPUT_BUCKET="raw"
DEFAULT_INPUT_KEY="customer_raw.csv"
DEFAULT_OUT_BUCKET="enriched"
DEFAULT_OUT_KEY="gender_enriched.csv"
CAP_SIZE=5

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


def real_enrich_gender(topic="gender-enrich-input",out_topic="gender-enrich-output"):
    send_status("gender-enrichment","Started")
    ol_client=OpenLineageClient()  #Initialize OpenLineageClient
    run_id=str(uuid4())  #Generate a unique run ID
    job_name="gender-enrichment-stream" #Define the job name
    gender_schema=[
        {"name":"name","type":"STRING","description":"Person's Name"},
        {"name":"gender","type":"STRING","description":"Inferred Gender"},
        {"name":"email","type":"STRING","description":"Email Address"},
        {"name":"latitude","type":"DOUBLE","description":"Latitude"},
        {"name":"longitude","type":"DOUBLE","description":"Longitude"},
    ]  #Define the schema for the input and output datasets
    input_dataset=ol_client.create_kafka_dataset(
        topic=topic,
        schema_fields=gender_schema[:2],
        description=f"Input stream for gender enrichment from topic {topic}"
    )  #Create the input dataset
    output_dataset=ol_client.create_kafka_dataset(
        topic=out_topic,
        schema_fields=gender_schema,
        description=f"Enriched output stream with gender information to topic {out_topic}"
    )  #Create the output dataset
    reference_dataset=ol_client.create_s3_dataset(
        bucket=BUCKET,
        key=GENDER_MASTER_CSV,
        schema_fields=gender_schema,
        description="Reference dataset for gender enrichment lookup"
    )  #Create the reference dataset
    job_facets={
        "jobType":{
            "_producer":ol_client.producer,
            "_schemaURL":"https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
            "processingType":"STREAMING",
            "integration":"KAFKA",
            "jobType":"ENRICHMENT"
        },
    }   #Define the job facets for streaming 
    run_facets=create_processing_facet(name="gender-enrichment-stream",mode="stream") # Create the run facets for stremaing
    ol_client.emit_start_event(
        job_name=job_name,
        run_id=run_id,
        inputs=[input_dataset,reference_dataset],
        job_facets=job_facets,
        run_facets=run_facets
    )  #Emit the start event  This will be consumed by the OpenLineage UI

    try:
        s3_utils.download_file(BUCKET,GENDER_MASTER_CSV,LOCAL_GENDER_MASTER_CSV)
        gender_master_df=pd.read_csv(LOCAL_GENDER_MASTER_CSV)
        
        total_records_processed = 0
        
        for records in kafka_utils.consume_records(topic):
            if not records:
                continue
            if isinstance(records,dict): 
                records=[records]
            print(f"Received {len(records)} records from {topic}")
            if len(records)>CAP_SIZE:
                print(f"CAP Size exceeded switching back to batch mode")
                df=pd.DataFrame(records)
                merged_df=pd.merge(df,gender_master_df,on="name",how="left")
                merged_df.fillna({"gender":"UNKNOWN"},inplace=True)
                merged_df.to_csv(LOCAL_OUTPUT,index=False)
                s3_utils.upload_file(DEFAULT_OUT_BUCKET,DEFAULT_OUT_KEY,LOCAL_OUTPUT)

                total_records_processed += len(records) #Increment the total records processed
                fallback_output=ol_client.create_s3_dataset(
                    bucket=DEFAULT_OUT_BUCKET,
                    key=DEFAULT_OUT_KEY,
                    schema_fields=gender_schema,
                    description="Batch fallback output due to CAP_SIZE exceeded"
                ) # Create the fallback output dataset 
                add_output_statistics_to_dataset(fallback_output,len(merged_df)) # Add the output statistics to the fallback output 
                # Create the Complete run facets for the fallback output
                run_facets_complete=create_processing_facet(mode="stream_to_batch_fallback",record_count=total_records_processed)
                 
                #Emit the complete event for the fallback output
                ol_client.emit_complete_event(
                    job_name=job_name,
                    run_id=run_id,
                    inputs=[input_dataset,reference_dataset],
                    outputs=[fallback_output],
                    job_facets=job_facets,
                    run_facets=run_facets_complete
                )
                
                send_status("gender-enrichment","Succeeded(batch_fallback)")
                return
            
            else:
                df=pd.DataFrame(records)
                merged_df=pd.merge(df,gender_master_df,on="name",how="left")
                merged_df.fillna({"gender":"UNKNOWN"},inplace=True)
                enriched_records=merged_df.to_dict(orient="records")
            # kafka_utils.send_event(out_topic,enriched_records)
                for record in merged_df.to_dict(orient="records"):
                        kafka_utils.send_event(out_topic, record)
                
                total_records_processed += len(enriched_records)
            
        output_dataset_with_stats=add_output_statistics_to_dataset(output_dataset.copy(),total_records_processed)
        run_facets_complete=create_processing_facet(name="gender-enrichment-stream",mode="stream",record_count=total_records_processed)
        ol_client.emit_complete_event(
            job_name=job_name,
            run_id=run_id,
            inputs=[input_dataset,reference_dataset],
            outputs=[output_dataset_with_stats],
            job_facets=job_facets,
            run_facets=run_facets_complete
        )
        send_status("gender-enrichment","Succeeded")
    except Exception as e:
        #Emit the fail evenet for the stream;; this will be consumed by the OpenLineage UI and Marquez UI
        ol_client.emit_fail_event(
            job_name=job_name,
            run_id=run_id,
            error_message=str(e),
            inputs=[input_dataset,reference_dataset],
            job_facets=job_facets
        )
        send_status("gender-enrichment","Failed",error=str(e))
        print(f"Error in Processing Stream:{str(e)}")
        sys.exit(1)



def enrich_gender(input_bucket=DEFAULT_INPUT_BUCKET,input_key=DEFAULT_INPUT_KEY,out_bucket=DEFAULT_OUT_BUCKET,out_key=DEFAULT_OUT_KEY):  

    send_status("gender-enrichment","Started")
    print(f"InputBucket:{input_bucket} , Input_key:{input_key} , OutputBucket:{out_bucket} , OutputKey:{out_key}")

    ol_client=OpenLineageClient()
    run_id=str(uuid4())
    job_name="gender-enrichment-batch"
    gender_schema=[
        {"name":"name","type":"STRING","description":"Person's Name"},
        {"name":"email","type":"STRING","description":"Email Address"},
        {"name":"latitude","type":"DOUBLE","description":"Latitude"},
        {"name":"longitude","type":"DOUBLE","description":"Longitude"},
           {"name":"gender","type":"STRING","description":"Inferred Gender"},
    ]
    input_dataset=ol_client.create_s3_dataset(
        bucket=input_bucket,
        key=input_key,
        schema_fields=gender_schema[:2],
        description=f"Input data for gender enrichment from {input_bucket}/{input_key}"
    )
    reference_dataset=ol_client.create_s3_dataset(
        bucket=BUCKET,
        key=GENDER_MASTER_CSV,
        schema_fields=gender_schema,
        description="Reference dataset for gender enrichment lookup"
    )
    output_dataset=ol_client.create_s3_dataset(
        bucket=out_bucket,
        key=out_key,
        schema_fields=gender_schema,
        description=f"Enriched output with gender information to {out_bucket}/{out_key}"
    )
    job_facets={
        "jobType":{
            "_producer":ol_client.producer,
            "_schemaURL":"https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
            "processingType":"BATCH",
            "integration":"S3",
            "jobType":"ENRICHMENT"
        }
    }
    run_facets=create_processing_facet(name="gender-enrichment-batch",mode="batch")
    ol_client.emit_start_event(
        job_name=job_name,
        run_id=run_id,
        inputs=[input_dataset,reference_dataset],
        job_facets=job_facets,
        run_facets=run_facets
    )
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
        output_file_size = os.path.getsize(LOCAL_OUTPUT) if os.path.exists(LOCAL_OUTPUT) else None
        output_dataset_with_stats=add_output_statistics_to_dataset(output_dataset.copy(),len(merged_df),output_file_size)
        run_facets_complete=create_processing_facet(name="gender-enrichment-batch",mode="batch",record_count=len(merged_df))
        ol_client.emit_complete_event(
            job_name=job_name,
            run_id=run_id,
            inputs=[input_dataset,reference_dataset],
            outputs=[output_dataset_with_stats],
            job_facets=job_facets,
            run_facets=run_facets_complete
        )


        send_status("gender-enrichment","Succeeded")
    except Exception as e:
        error_msg=str(e)
        ol_client.emit_fail_event(
            job_name=job_name,
            run_id=run_id,
            error_message=error_msg,
            inputs=[input_dataset,reference_dataset],
            job_facets=job_facets
        )
        send_status("gender-enrichment","Failed",error=error_msg)
        raise

if __name__=="__main__":
    mode=os.getenv("MODE","batch").lower()
    if mode=="batch":
        enrich_gender(
            input_bucket=os.environ.get("INPUT_BUCKET"),
            input_key=os.environ.get("INPUT_KEY"),
            out_bucket=os.environ.get("OUT_BUCKET"),
            out_key=os.environ.get("OUT_KEY")
        )
    else:
        real_enrich_gender(
            os.environ.get("INPUT_TOPIC"),
            os.environ.get("OUTPUT_TOPIC")
        )
