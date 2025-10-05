
import os
import sys
import pandas as pd
from datetime import datetime
from uuid import uuid4
from common.libs import s3_utils, progress, kafka_utils
from common.libs.openlineage_utils import OpenLineageClient, create_processing_facet, add_output_statistics_to_dataset
BUCKET = "raw"
LOCAL_INPUT = "/samples/input/reverse_geocode.csv"
LOCAL_OUTPUT = "/samples/output/reverse_geocode_enriched.csv"
LOCAL_GEO_MASTER_CSV = "/reverse_geocode/data/geo_master.csv"
GEO_MASTER_CSV = "/reference/geo_master.csv"
DEFAULT_INPUT_BUCKET = "raw"
DEFAULT_INPUT_KEY = "customer_raw.csv"
DEFAULT_OUT_BUCKET = "enriched"
DEFAULT_OUT_KEY = "reverse_geocode_enriched.csv"
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

def run_reverse_geocode_stream(topic="reverse-geocode-input",out_topic="reverse-geocode-output"):
    send_status("reverse_geocode","Started")
    
    # Initialize OpenLineage client
    ol_client = OpenLineageClient()
    run_id = str(uuid4())
    job_name = "reverse_geocode_stream"
    
    # Define schema for geo data
    geo_schema = [
        {"name": "latitude", "type": "DOUBLE", "description": "Latitude coordinate"},
        {"name": "longitude", "type": "DOUBLE", "description": "Longitude coordinate"},
        {"name": "city", "type": "STRING", "description": "City name from reverse geocoding"}
    ]
    
    # Create input/output dataset definitions
    input_dataset = ol_client.create_kafka_dataset(
        topic=topic,
        schema_fields=geo_schema[:2],  # Input has lat/lon only
        description=f"Input stream for reverse geocoding from topic {topic}"
    )
    
    output_dataset = ol_client.create_kafka_dataset(
        topic=out_topic,
        schema_fields=geo_schema,  # Output has lat/lon/city
        description=f"Enriched output stream with city information to topic {out_topic}"
    )
    
    # Reference dataset (geo master)
    reference_dataset = ol_client.create_s3_dataset(
        bucket=BUCKET,
        key=GEO_MASTER_CSV,
        schema_fields=geo_schema,
        description="Reference dataset for reverse geocoding lookup"
    )
    
    # Emit START event
    job_facets = {
        "jobType": {
            "_producer": ol_client.producer,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
            "processingType": "STREAMING",
            "integration": "KAFKA",
            "jobType": "ENRICHMENT"
        }
    }
    
    run_facets = create_processing_facet(name="reverse-geocode-stream",mode="stream")
    ol_client.emit_start_event(
        job_name=job_name,
        run_id=run_id,
        inputs=[input_dataset, reference_dataset],
        job_facets=job_facets,
        run_facets=run_facets
    )

    
    try:
        s3_utils.download_file(BUCKET,GEO_MASTER_CSV,LOCAL_GEO_MASTER_CSV)
        geo_master_df=pd.read_csv(LOCAL_GEO_MASTER_CSV)
        
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
                merged_df=pd.merge(df,geo_master_df,how="left",on=["latitude","longitude"])
                merged_df.fillna({"city":"Unknown"},inplace=True)
                merged_df.to_csv(LOCAL_OUTPUT,index=False)
                s3_utils.upload_file(DEFAULT_OUT_BUCKET,DEFAULT_OUT_KEY,LOCAL_OUTPUT)
                
                total_records_processed += len(records)
                
                # Emit COMPLETE event with batch fallback info
                fallback_output = ol_client.create_s3_dataset(
                    bucket=DEFAULT_OUT_BUCKET,
                    key=DEFAULT_OUT_KEY,
                    schema_fields=geo_schema,
                    description="Batch fallback output due to CAP_SIZE exceeded"
                )
                add_output_statistics_to_dataset(fallback_output, len(merged_df))
                
                run_facets_complete = create_processing_facet(mode="stream_to_batch_fallback", record_count=total_records_processed)
                ol_client.emit_complete_event(
                    job_name=job_name,
                    run_id=run_id,
                    inputs=[input_dataset, reference_dataset],
                    outputs=[fallback_output],
                    job_facets=job_facets,
                    run_facets=run_facets_complete
                )

                
                send_status("reverse_geocode","Succeeded(batch_fallback)")
                return
            else:
                df=pd.DataFrame(records)
                merged_df=pd.merge(df,geo_master_df,how="left",on=["latitude","longitude"])
                merged_df.fillna({"city":"Unknown"},inplace=True)

                enriched_records=merged_df.to_dict(orient="records")
                total_records_processed += len(enriched_records)
                
            # kafka_utils.send_event(out_topic,enriched_records)
                for record in merged_df.to_dict(orient="records"):
                        kafka_utils.send_event(out_topic, record)

        # Emit COMPLETE event
        output_dataset_with_stats = add_output_statistics_to_dataset(
            output_dataset.copy(), 
            total_records_processed
        )
        
        run_facets_complete = create_processing_facet(name="reverse-geocode-stream",mode="stream", record_count=total_records_processed)
        ol_client.emit_complete_event(
            job_name=job_name,
            run_id=run_id,
            inputs=[input_dataset, reference_dataset],
            outputs=[output_dataset_with_stats],
            job_facets=job_facets,
            run_facets=run_facets_complete
        )
        
        send_status("reverse_geocode","Succeeded")
    except Exception as e:
        # Emit FAIL event
        ol_client.emit_fail_event(
            job_name=job_name,
            run_id=run_id,
            error_message=str(e),
            inputs=[input_dataset, reference_dataset],
            job_facets=job_facets
        )
        
        send_status("reverse_geocode","Failed",error=str(e))
        print(f"Error in Processing Stream:{str(e)}")
        raise



def run_reverse_geocode(input_bucket=DEFAULT_INPUT_BUCKET, input_key=DEFAULT_INPUT_KEY, out_bucket=DEFAULT_OUT_BUCKET, out_key=DEFAULT_OUT_KEY):
    
    send_status("reverse_geocode","Started")
    print(f"InputBucket:{input_bucket} , Input_key:{input_key} , OutputBucket:{out_bucket} , OutputKey:{out_key}")
    
    # Initialize OpenLineage client
    ol_client = OpenLineageClient()
    run_id = str(uuid4())
    job_name = "reverse_geocode_batch"
    
    # Define schema for geo data
    geo_schema = [
        {"name": "latitude", "type": "DOUBLE", "description": "Latitude coordinate"},
        {"name": "longitude", "type": "DOUBLE", "description": "Longitude coordinate"},
        {"name": "city", "type": "STRING", "description": "City name from reverse geocoding"}
    ]
    
    # Create dataset definitions
    input_dataset = ol_client.create_s3_dataset(
        bucket=input_bucket,
        key=input_key,
        schema_fields=geo_schema[:2],  # Input has lat/lon only
        description=f"Raw input data for reverse geocoding from {input_bucket}/{input_key}"
    )
    
    reference_dataset = ol_client.create_s3_dataset(
        bucket=BUCKET,
        key=GEO_MASTER_CSV,
        schema_fields=geo_schema,
        description="Reference dataset for reverse geocoding lookup"
    )
    
    output_dataset = ol_client.create_s3_dataset(
        bucket=out_bucket,
        key=out_key,
        schema_fields=geo_schema,
        description=f"Enriched output with city information to {out_bucket}/{out_key}"
    )
    
    # Job facets
    job_facets = {
        "jobType": {
            "_producer": ol_client.producer,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
            "processingType": "BATCH",
            "integration": "S3",
            "jobType": "ENRICHMENT"
        }
    }
    
    # Emit START event
    run_facets = create_processing_facet(name="reverse-geocode-batch",mode="batch")
    ol_client.emit_start_event(
        job_name=job_name,
        run_id=run_id,
        inputs=[input_dataset, reference_dataset],
        job_facets=job_facets,
        run_facets=run_facets
    )
 
    try:
        # Step 1 - Download the input file 
        # s3_utils.download_file(input_bucket, input_key, LOCAL_INPUT)
        
        s3_utils.download_file(input_bucket,input_key, LOCAL_INPUT)
        # Step 2 - Download the geo_master file from S3
        s3_utils.download_file(BUCKET, GEO_MASTER_CSV, LOCAL_GEO_MASTER_CSV)

        # Step 3 - Read both files
        df = pd.read_csv(LOCAL_INPUT)
        geo_master_df = pd.read_csv(LOCAL_GEO_MASTER_CSV)

        # Merge on latitude and longitude
        merged_df = pd.merge(df, geo_master_df, how="left", on=["latitude","longitude"])
        merged_df.fillna({"city":"Unknown"}, inplace=True)
        merged_df.to_csv(LOCAL_OUTPUT, index=False)

        # Upload output
        s3_utils.upload_file(out_bucket, out_key, LOCAL_OUTPUT)
        
        # Add output statistics to dataset
        output_file_size = os.path.getsize(LOCAL_OUTPUT) if os.path.exists(LOCAL_OUTPUT) else None
        output_dataset_with_stats = add_output_statistics_to_dataset(
            output_dataset.copy(),
            len(merged_df),
            output_file_size
        )
        
        # Emit COMPLETE event
        run_facets_complete = create_processing_facet(name="reverse-geocode-batch",mode="batch", record_count=len(merged_df))
        ol_client.emit_complete_event(
            job_name=job_name,
            run_id=run_id,
            inputs=[input_dataset, reference_dataset],
            outputs=[output_dataset_with_stats],
            job_facets=job_facets,
            run_facets=run_facets_complete
        )
        
        send_status("reverse_geocode","Succeeded")
        

    except Exception as e:
        error_msg = str(e)
        
        # Emit FAIL event
        ol_client.emit_fail_event(
            job_name=job_name,
            run_id=run_id,
            error_message=error_msg,
            inputs=[input_dataset, reference_dataset],
            job_facets=job_facets
        )
        
        send_status("reverse_geocode","Failed",error=error_msg)
        print(f"ERROR in Downloading Input File: {error_msg}")
        raise
 
if __name__ == "__main__":
    mode = os.environ.get("MODE", "batch").lower()
    if mode=="batch":
        run_reverse_geocode(
            os.environ.get("INPUT_BUCKET"),
            os.environ.get("INPUT_KEY"),
            os.environ.get("OUT_BUCKET"),
            os.environ.get("OUT_KEY")
        )
    else:
        run_reverse_geocode_stream(
            os.environ.get("INPUT_TOPIC"),
            os.environ.get("OUTPUT_TOPIC")
        ) 
        