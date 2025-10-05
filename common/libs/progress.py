import json
import botocore
from common.libs import s3_utils,kafka_utils

def upload_progress_file(bucket_name, progress_name, progress_data):
    s3 = s3_utils.get_s3_client()
    key = f"{progress_name}.json"

    events = []
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        existing_data = json.loads(obj["Body"].read())
        
        if isinstance(existing_data, list):
            events = existing_data
        else:
            events = [existing_data]
    except s3.exceptions.NoSuchKey:
        events = []
    
    # Append new event
    events.append(progress_data)

    # Upload back to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(events, indent=2),
        ContentType="application/json"
    )
    print(f"Appended progress to s3://{bucket_name}/{key}")
