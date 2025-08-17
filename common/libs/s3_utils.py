import boto3
import os,json

# Get S3 client
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )

def upload_file(bucket, key, local_path):
    s3 = get_s3_client()
    s3.upload_file(local_path, bucket, key)
    print(f"Uploaded {local_path} to s3://{bucket}/{key}")

def download_file(bucket, key, local_path):
    s3 = get_s3_client()
    s3.download_file(bucket, key, local_path)
    print(f"Downloaded s3://{bucket}/{key} to {local_path}")


def upload_progress_file(bucket_name,progress_name,progress_data):
    s3=get_s3_client()
    #s3.put_object(Bucket=bucket_name,Key=progress_name,Body=progress_data)
    #Step1 -get the data

    # obj=s3.get_object(Bucket=bucket_name,Key=f"{progress_name}.json")
    # file_content=obj['Body'].read()
    # if not file_content:
    #     existing_data=[]
    # else:
    #     existing_data=json.loads(file_content)

    # existing_data.apppend(progress_data)
    
    s3.put_object(
        Bucket=bucket_name,
        Key=f"{progress_name}.json",
        Body=json.dumps(progress_data,indent=2),
        ContentType="application/json"
    )
    print(f"Uploaded progress file to s3://{bucket_name}/{progress_name}")
