import boto3
import logging
import tempfile
from app.core.config import settings

logger = logging.getLogger(__name__)

required_vars = [
    settings.WASABI_ENDPOINT_URL,
    settings.WASABI_ACCESS_KEY,
    settings.WASABI_SECRET_KEY,
]

if not all(required_vars):
    raise RuntimeError(
        "One or more required Wasabi settings are missing. "
        "Please check your environment variables or Kubernetes secrets."
    )

logger.info("Initializing Wasabi S3 client with configured endpoint and credentials.")

s3_client = boto3.client(
    "s3",
    endpoint_url=str(settings.WASABI_ENDPOINT_URL),
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    region_name=settings.WASABI_REGION
)

def upload_file(file_obj, bucket: str, path: str):
    """
    Streams the UploadFile to disk, then uses upload_file() to avoid aws-chunked encoding.
    """
    logger.info(f"Uploading to Wasabi bucket: {bucket}, path: {path}")

    with tempfile.NamedTemporaryFile(delete=True) as tmp:
        file_obj.seek(0)
        while chunk := file_obj.read(1024 * 1024):
            tmp.write(chunk)
        tmp.flush()
        s3_client.upload_file(tmp.name, bucket, path)

    logger.info(f"Successfully uploaded to Wasabi: {bucket}/{path}")

def delete_file(bucket: str, path: str):
    logger.info(f"Deleting from Wasabi bucket: {bucket}, path: {path}")
    s3_client.delete_object(Bucket=bucket, Key=path)
    logger.info(f"Successfully deleted from Wasabi: {bucket}/{path}")

client = s3_client
