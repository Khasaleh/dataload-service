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
    Uploads a FastAPI UploadFile or file-like object to Wasabi, using the same
    working approach as your other uploader: write to disk, reopen in rb mode,
    then upload_fileobj with ACL.
    """
    logger.info(f"[DEBUG] upload_file() called with bucket={bucket}, path={path}, file_obj type={type(file_obj)}")

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        file_obj.seek(0)
        while chunk := file_obj.read(1024 * 1024):
            tmp.write(chunk)
        tmp_path = tmp.name

    logger.info(f"[DEBUG] Temp file created for upload: {tmp_path}")

    try:
        with open(tmp_path, "rb") as f:
            s3_client.upload_fileobj(
                f,
                bucket,
                path,
                ExtraArgs={'ACL': 'public-read'}  # Consistent with your working uploader
            )
            logger.info(f"[DEBUG] Uploaded {tmp_path} to Wasabi at {bucket}/{path}")

    except Exception as e:
        logger.exception(f"Error uploading to Wasabi: {e}")
        raise
    finally:
        # Always clean up temp file
        try:
            os.remove(tmp_path)
            logger.info(f"[DEBUG] Temp file {tmp_path} removed after upload")
        except OSError as cleanup_error:
            logger.warning(f"Could not delete temp file {tmp_path}: {cleanup_error}")

def delete_file(bucket: str, path: str):
    """
    Deletes an object from Wasabi.
    """
    logger.info(f"Deleting from Wasabi bucket: {bucket}, path: {path}")
    s3_client.delete_object(Bucket=bucket, Key=path)
    logger.info(f"Successfully deleted from Wasabi: {bucket}/{path}")

client = s3_client
