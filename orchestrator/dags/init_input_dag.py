from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from common.libs import s3_utils

BUCKET_NAME = "raw"
LOCAL_INPUT = "/tmp/location_data.csv"
S3_INPUT_KEY = "customer_raw.csv"

LOCAL_GEO_MASTER = "/tmp/geo_master.csv"
S3_GEO_KEY = "reference/geo_master.csv"

LOCAL_GENDER_MASTER="/tmp/gender_master.csv"
S3_GENDER_KEY="reference/gender_master.csv"


def create_input_csv():
    os.makedirs("/tmp", exist_ok=True)
    with open(LOCAL_INPUT, "w") as f:
        f.write("id,name,email,latitude,longitude\n")
        f.write("1,Amit,amit@example.com,28.6139,77.2090\n")
        f.write("2,Neha,neha@example.com,19.0760,72.8777\n")
        f.write("3,Ravi,ravi@gmail.com,12.9716,77.5946\n")
        f.write("4,Divya,divya@example.com.,13.0827,80.2707\n")
        f.write("5,Sohan,sohan@outlook.com,22.5726,88.3639\n")
        f.write("6,Kiran,kiran@wizard.com,28.7041,77.1025\n")
        f.write("7,Priya,priya@example.com,17.3850,78.4867\n")
        f.write("8,Arjun,arjun@example.com,23.2599,77.4126\n")
        f.write("9,Anita,anita@example.com,26.9124,75.7873\n")
        f.write("10,Rahul,rahul@example.com,15.3173,75.7139\n")
    print(f"Sample input CSV created at {LOCAL_INPUT}")


def upload_input_csv():
    s3_utils.upload_file(BUCKET_NAME, S3_INPUT_KEY, LOCAL_INPUT)
    print(f"Uploaded {LOCAL_INPUT} to s3://{BUCKET_NAME}/{S3_INPUT_KEY}")


def create_geo_master_csv():
    with open(LOCAL_GEO_MASTER, "w") as f:
        f.write("latitude,longitude,city\n")
        f.write("28.6139,77.2090,New Delhi\n")
        f.write("19.0760,72.8777,Mumbai\n")
        f.write("12.9716,77.5946,Bangalore\n")
        f.write("13.0827,80.2707,Chennai\n")
        f.write("22.5726,88.3639,Kolkata\n")
        f.write("17.3850,78.4867,Hyderabad\n")
        f.write("23.2599,77.4126,Bhopal\n")
        f.write("26.9124,75.7873,Jaipur\n")
        f.write("15.3173,75.7139,Belgaum\n")
        f.write("21.1458,79.0882,Nagpur\n")
    print(f"Geo master CSV created at {LOCAL_GEO_MASTER}")


def upload_geo_master_csv():
    s3_utils.upload_file(BUCKET_NAME, S3_GEO_KEY, LOCAL_GEO_MASTER)
    print(f"Uploaded {LOCAL_GEO_MASTER} to s3://{BUCKET_NAME}/{S3_GEO_KEY}")


def create_gender_master_csv():
    with open(LOCAL_GENDER_MASTER,"w") as f:
        f.write("name,gender\n")
        f.write("Amit,M\n")
        f.write("Neha,F\n")
        f.write("Ravi,M\n")
        f.write("Divya,F\n")
        f.write("Sohan,M\n")
        f.write("Kiran,F\n")
        f.write("Priya,F\n")
        f.write("Arjun,M\n")
        f.write("Anita,F\n")
        f.write("Rahul,M\n")
    print(f"Gender master CSV created at {LOCAL_GENDER_MASTER}")

def upload_gender_master_csv():
    s3_utils.upload_file(BUCKET_NAME,S3_GENDER_KEY,LOCAL_GENDER_MASTER)
    print(f"Uploaded {LOCAL_GENDER_MASTER} to s3://{BUCKET_NAME}/{S3_GENDER_KEY}")

with DAG(
    dag_id="init_input_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Run only manually
    catchup=False,
    tags=["setup"],
) as dag:

    create_input = PythonOperator(
        task_id="create_input_csv",
        python_callable=create_input_csv
    )

    upload_input = PythonOperator(
        task_id="upload_input_csv",
        python_callable=upload_input_csv
    )

    create_geo = PythonOperator(
        task_id="create_geo_master_csv",
        python_callable=create_geo_master_csv
    )

    upload_geo = PythonOperator(
        task_id="upload_geo_master_csv",
        python_callable=upload_geo_master_csv
    )

    create_gender=PythonOperator(
        task_id="create_gender_master_csv",
        python_callable=create_gender_master_csv
    )

    upload_gender = PythonOperator(
        task_id="upload_gender_master_csv",
        python_callable=upload_gender_master_csv
    )

    create_input >> upload_input
    create_geo >> upload_geo
    create_gender >> upload_gender
