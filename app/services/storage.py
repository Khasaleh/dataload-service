import boto3
import os
import logging

logger = logging.getLogger(__name__)

# --- Wasabi S3 Client Configuration ---
WASABI_ENDPOINT_URL = os.getenv("WASABI_ENDPOINT_URL")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")

if not all([WASABI_ENDPOINT_URL, WASABI_ACCESS_KEY, WASABI_SECRET_KEY]):
    logger.error(
        "Missing one or more required Wasabi environment variables: "
        "WASABI_ENDPOINT_URL, WASABI_ACCESS_KEY, WASABI_SECRET_KEY. "
        "S3 client may not work properly."
    )

# Initialize boto3 session and client
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    endpoint_url=WASABI_ENDPOINT_URL
)

def upload_file(bucket: str, path: str, file_obj):
    """
    Uploads a file-like object to the specified Wasabi bucket and path.
    """
    try:
        s3_client.upload_fileobj(file_obj, bucket, path)
        logger.info(f"File uploaded to Wasabi: bucket={bucket}, path={path}")
    except Exception as e:
        logger.error(f"Failed to upload file to Wasabi: {e}", exc_info=True)
        raise

def delete_file(bucket: str, path: str):
    """
    Deletes a file from the specified Wasabi bucket and path.
    """
    try:
        s3_client.delete_object(Bucket=bucket, Key=path)
        logger.info(f"File deleted from Wasabi: bucket={bucket}, path={path}")
    except Exception as e:
        logger.error(f"Failed to delete file from Wasabi: {e}", exc_info=True)
        raise
