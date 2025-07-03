import boto3
import logging
import tempfile
import os

from app.core.config import settings

logger = logging.getLogger(__name__)

# Validate required settings
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

# Initialize Wasabi S3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=str(settings.WASABI_ENDPOINT_URL),
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    region_name=settings.WASABI_REGION
)

def upload_file(file_obj, bucket: str, path: str):
    """
    Uploads a FastAPI UploadFile or file-like object to Wasabi S3 by writing to disk,
    then using upload_file() to avoid chunked encoding.
    """
    logger.info(f"[DEBUG] upload_file() called with bucket={bucket}, path={path}, file_obj type={type(file_obj)}")

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
        file_obj.seek(0)
        while chunk := file_obj.read(1024 * 1024):
            tmp.write(chunk)
        tmp.flush()

    logger.info(f"[DEBUG] Temp file created at {tmp_path}")

    try:
        s3_client.upload_file(
            Filename=tmp_path,
            Bucket=bucket,
            Key=path,
            ExtraArgs={'ACL': 'public-read'}
        )
        logger.info(f"[DEBUG] Uploaded {tmp_path} to Wasabi bucket={bucket}, key={path}")

    except Exception as e:
        logger.exception(f"Error uploading to Wasabi: {e}")
        raise
    finally:
        try:
            os.remove(tmp_path)
            logger.info(f"[DEBUG] Deleted temp file {tmp_path}")
        except OSError as cleanup_error:
            logger.warning(f"Could not delete temp file {tmp_path}: {cleanup_error}")

def delete_file(bucket: str, path: str):
    """
    Deletes an object from Wasabi S3.
    """
    logger.info(f"Deleting from Wasabi bucket: {bucket}, path: {path}")
    s3_client.delete_object(Bucket=bucket, Key=path)
    logger.info(f"Successfully deleted from Wasabi: {bucket}/{path}")

client = s3_client
