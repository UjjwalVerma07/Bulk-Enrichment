# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
# from common.libs import s3_utils,progress,kafka_utils
# from datetime import datetime
# import subprocess
# import pandas as pd,os,tempfile,json
# import sys


# BUCKET = "raw"
# LOCAL_INPUT="/samples/input/reverse_geocode.csv"
# LOCAL_OUTPUT="/samples/output/reverse_geocode_enriched.csv"
# LOCAL_GEO_MASTER_CSV="/reverse_geocode/data/geo_master.csv"
# GEO_MASTER_CSV="/reference/geo_master.csv"
# DEFAULT_INPUT_BUCKET="raw"
# DEFAULT_INPUT_KEY="customer_raw.csv"
# DEFAULT_OUT_BUCKET="enriched"
# DEFAULT_OUT_KEY="reverse_geocode_enriched.csv"

# def run_reverse_geocode(input_bucket,input_key,out_bucket,out_key):
#     kafka_utils.send_event("pipeline-progress",{
#         "stage":"reverse_geocode",
#         "status":"Started",
#         "time":datetime.now().isoformat()
#     })
#     progress.upload_progress_file(BUCKET,"progress",{
#         "stage":"reverse_geocode",
#         "status":"Started",
#         "time":datetime.now().isoformat()
#     })
    
#     try:
#     #Step 1-Download the input file 
#         local_input=LOCAL_INPUT
#         s3_utils.download_file(input_bucket,input_key,local_input)
#     #Step 2-Download the geo_master file from S3
#         local_geo_master=LOCAL_GEO_MASTER_CSV
#         s3_utils.download_file(BUCKET,GEO_MASTER_CSV,local_geo_master)

#     #Step 3 - Read both the files
    
#         df=pd.read_csv(local_input)
#         geo_master_df=pd.read_csv(local_geo_master)

#     #Merge on latitude and longitute
#         merged_df=pd.merge(df,geo_master_df,how="left",on=["latitude","longitude"])
#         merged_df.fillna({"city":"Unknown"},inplace=True)
#         local_output=LOCAL_OUTPUT
#         merged_df.to_csv(local_output,index=False)
#         s3_utils.upload_file(out_bucket,out_key,local_output)

#     #send the messsage via kakfa util

#         kafka_utils.send_event("pipeline-progress",{
#             "stage":"reverse_geocode",
#             "status":"Succeeded",
#             "time":datetime.now().isoformat()
#             })

#     #Upload progress.json file to S3

#         progress.upload_progress_file(BUCKET,"progress",{
#             "stage":"reverse_geocode",
#             "status":"Succeeded",
#             "time":datetime.now().isoformat()
#             })
#     except Exception as e:
#         kafka_utils.send_event("pipeline-progress",{
#             "stage":"reverse_geocode",
#             "status":"Failed",
#             "time":datetime.now().isoformat()
#         })

#         progress.upload_progress_file(BUCKET,"progress",{
#             "stage":"reverse_geocode",
#             "status":"Failed",
#             "time":datetime.now().isoformat()
#         })
#         sys.exit(1)
# if __name__ == "__main__":
#     run_reverse_geocode(
#         input_bucket=DEFAULT_INPUT_BUCKET,
#         input_key=DEFAULT_INPUT_KEY,
#         out_bucket=DEFAULT_OUT_BUCKET,
#         out_key=DEFAULT_OUT_KEY
#     )

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
            df=pd.DataFrame(records)

            merged_df=pd.merge(df,geo_master_df,how="left",on=["latitude","longitude"])
            merged_df.fillna({"city":"Unknown"},inplace=True)

            enriched_records=merged_df.to_dict(orient="records")
            kafka_utils.send_event(out_topic,enriched_records)

        send_status("reverse_geocode","Succeeded")
    except Exception as e:
        send_status("reverse_geocode","Failed",error=str(e))
        print(f"Error in Processing Stream:{str(e)}")
        sys.exit(1)



def run_reverse_geocode(input_bucket=DEFAULT_INPUT_BUCKET, input_key=DEFAULT_INPUT_KEY, out_bucket=DEFAULT_OUT_BUCKET, out_key=DEFAULT_OUT_KEY):

    kafka_utils.send_event("pipeline-progress",{
        "stage":"reverse_geocode",
        "status":"Started",
        "time":datetime.now().isoformat()
    })
    progress.upload_progress_file(BUCKET,"progress",{
        "stage":"reverse_geocode",
        "status":"Started",
        "time":datetime.now().isoformat()
    })

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

        # Send success messages
        kafka_utils.send_event("pipeline-progress",{
            "stage":"reverse_geocode",
            "status":"Succeeded",
            "time":datetime.now().isoformat()
        })
        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"reverse_geocode",
            "status":"Succeeded",
            "time":datetime.now().isoformat()
        })

    except Exception as e:
        error_msg = str(e)
        kafka_utils.send_event("pipeline-progress",{
            "stage":"reverse_geocode",
            "status":"Failed",
            "time":datetime.now().isoformat(),
            "error": error_msg
        })
        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"reverse_geocode",
            "status":"Failed",
            "time":datetime.now().isoformat(),
            "error": error_msg
        })
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
        run_reverse_geocode_stream()