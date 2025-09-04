
import os
import sys
import pandas as pd
from datetime import datetime
from common.libs import s3_utils, progress, kafka_utils

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
    try:
        s3_utils.download_file(BUCKET,GEO_MASTER_CSV,LOCAL_GEO_MASTER_CSV)
        geo_master_df=pd.read_csv(LOCAL_GEO_MASTER_CSV)
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
                send_status("reverse_geocode","Succeeded(batch_fallback)")
                return
            else:
                df=pd.DataFrame(records)
                merged_df=pd.merge(df,geo_master_df,how="left",on=["latitude","longitude"])
                merged_df.fillna({"city":"Unknown"},inplace=True)

                enriched_records=merged_df.to_dict(orient="records")
            # kafka_utils.send_event(out_topic,enriched_records)
                for record in merged_df.to_dict(orient="records"):
                        kafka_utils.send_event(out_topic, record)

        send_status("reverse_geocode","Succeeded")
    except Exception as e:
        send_status("reverse_geocode","Failed",error=str(e))
        print(f"Error in Processing Stream:{str(e)}")
        sys.exit(1)



def run_reverse_geocode(input_bucket=DEFAULT_INPUT_BUCKET, input_key=DEFAULT_INPUT_KEY, out_bucket=DEFAULT_OUT_BUCKET, out_key=DEFAULT_OUT_KEY):
    
    send_status("reverse_geocode","Started")
 
    try:
        # Step 1 - Download the input file 
        s3_utils.download_file(input_bucket, input_key, LOCAL_INPUT)

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
        send_status("reverse_geocode","Succeeded")

    except Exception as e:
        error_msg = str(e)
        send_status("reverse_geocode","Failed",error=error_msg)
        print(f"ERROR in Downloading Input File: {error_msg}")
        sys.exit(1)
 
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