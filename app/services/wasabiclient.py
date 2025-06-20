import boto3
import os

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("WASABI_ENDPOINT"),
    aws_access_key_id=os.getenv("WASABI_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("WASABI_SECRET_KEY"),
    region_name=os.getenv("WASABI_REGION")
)

BUCKET_NAME = os.getenv("WASABI_BUCKET")

def upload_to_wasabi(key: str, content: bytes):
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content)

def delete_from_wasabi(key: str):
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)