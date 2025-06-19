import boto3
import os

session = boto3.session.Session()
client = session.client(
    service_name='s3',
    aws_access_key_id=os.getenv("WASABI_KEY"),
    aws_secret_access_key=os.getenv("WASABI_SECRET"),
    endpoint_url=os.getenv("WASABI_ENDPOINT")
)

def upload_file(bucket, path, file_obj):
    client.upload_fileobj(file_obj, bucket, path)

def delete_file(bucket, path):
    client.delete_object(Bucket=bucket, Key=path)
