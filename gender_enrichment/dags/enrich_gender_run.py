import sys
from common.libs import s3_utils,kafka_utils,progress
from datetime import datetime
import subprocess
import pandas as pd
import os,tempfile,json


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
    try:
        s3_utils.download_file(BUCKET,GENDER_MASTER_CSV,LOCAL_GENDER_MASTER_CSV)
        gender_master_df=pd.read_csv(LOCAL_GENDER_MASTER_CSV)
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
        send_status("gender-enrichment","Succeeded")
    except Exception as e:
        send_status("gender-enrichment","Failed",error=str(e))
        print(f"Error in Processing Stream:{str(e)}")
        sys.exit(1)



def enrich_gender(input_bucket=DEFAULT_INPUT_BUCKET,input_key=DEFAULT_INPUT_KEY,out_bucket=DEFAULT_OUT_BUCKET,out_key=DEFAULT_OUT_KEY):  
    kafka_utils.send_event("pipeline-progress",{
        "stage":"gender_enrichment",
        "status":"Started",
        "time":datetime.now().isoformat()
    })
    progress.upload_progress_file(BUCKET,"progress",{
        "stage":"gender_enrichment",
        "status":"Started",
        "time":datetime.now().isoformat()
    })

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
    #Step7- Send Message to kafka and update the progress file

        kafka_utils.send_event("pipeline-progress",{
            "stage":"gender_enrichment",
            "status":"Succeeded",
            "date":datetime.now().isoformat()
            })

        progress.upload_progress_file(BUCKET,"progress",{
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
        progress.upload_progress_file(BUCKET,"progress",{
            "stage":"gender_enrichment",
            "status":"Failed",
            "date":datetime.now().isoformat()
        })

    
 


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
