import boto3
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

# --- Wasabi S3 Client Configuration ---

# Validate all required settings on module load
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

# Initialize the client once at import
s3_client = boto3.client(
    "s3",
    endpoint_url=settings.WASABI_ENDPOINT_URL,
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    region_name=settings.WASABI_REGION
)


def upload_file(file_obj, bucket: str, path: str):
    """
    Uploads a file-like object to Wasabi.
    """
    logger.info(f"Uploading to Wasabi bucket: {bucket}, path: {path}")
    s3_client.upload_fileobj(Fileobj=file_obj, Bucket=bucket, Key=path)
    logger.info(f"Successfully uploaded to Wasabi: {bucket}/{path}")


def delete_file(bucket: str, path: str):
    """
    Deletes an object from Wasabi.
    """
    logger.info(f"Deleting from Wasabi bucket: {bucket}, path: {path}")
    s3_client.delete_object(Bucket=bucket, Key=path)
    logger.info(f"Successfully deleted from Wasabi: {bucket}/{path}")
