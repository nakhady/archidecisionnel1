# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import today
import pendulum
import os
import urllib.error

# Python Function to download the Parquet file
def download_parquet(**kwargs):
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"

    # Fetch the last month's data
    month: str = pendulum.now().subtract(months=1).format('YYYY-MM')  # Change to last month
    file_url = f"{url}{filename}_{month}{extension}"

    # Define the file path where the file will be saved locally
    file_path = os.path.join("./", f"yellow_tripdata_{month}.parquet")

    try:
        # Download the parquet file
        request.urlretrieve(file_url, file_path)
        print(f"File downloaded successfully: {file_path}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file : {str(e)}") from e

    # Return the path of the downloaded file to be used in the next task
    return file_path


# Python Function to upload file to Minio
def upload_file(**kwargs):
    # Fetch the file path from the previous task
    file_path = kwargs['ti'].xcom_pull(task_ids='download_parquet')
    
    # Setup Minio client
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    
    # Define the bucket name
    bucket: str = 'rawnyc'

    # Check if the bucket exists, otherwise create it
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    
    # Fetch the month for the last month
    month: str = pendulum.now().subtract(months=1).format('YYYY-MM')  # Change to last month
    object_name = f"yellow_tripdata_{month}.parquet"

    # Upload file to Minio
    try:
        client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=file_path
        )
        print(f"File uploaded successfully to Minio: {object_name}")
    except S3Error as e:
        raise RuntimeError(f"Failed to upload the file to Minio: {str(e)}") from e

    # After uploading, remove the downloaded file to avoid redundancy
    os.remove(file_path)


# Python Function to upload file to Minio
def upload_file(**kwargs):
    # Fetch the file path from the previous task
    file_path = kwargs['ti'].xcom_pull(task_ids='download_parquet')
    
    # Setup Minio client
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    
    # Define the bucket name
    bucket: str = 'rawnyc'

    # Check if the bucket exists, otherwise create it
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    
    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    object_name = f"yellow_tripdata_{month}.parquet"

    # Upload file to Minio
    try:
        client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=file_path
        )
        print(f"File uploaded successfully to Minio: {object_name}")
    except S3Error as e:
        raise RuntimeError(f"Failed to upload the file to Minio: {str(e)}") from e

    # After uploading, remove the downloaded file to avoid redundancy
    os.remove(file_path)


# Define the DAG
with DAG(
    dag_id='Grab_NYC_Data_to_Minio',
    start_date=today('UTC').add(days=-1),  # Utilise pendulum au lieu de `days_ago`
    schedule=None,  # Remplace `schedule_interval`
    catchup=False,
    tags=['minio', 'read', 'write']
) as dag:
    
    # Task to download the Parquet file
    t1 = PythonOperator(
        task_id='download_parquet',
        python_callable=download_parquet  # Suppression de `provide_context`
    )

    # Task to upload the file to Minio
    t2 = PythonOperator(
        task_id='upload_file_task',
        python_callable=upload_file  # Suppression de `provide_context`
    )

# Define task dependencies: first download the file, then upload it
t1 >> t2
