import boto3
import logging
from boto3.s3.transfer import TransferConfig
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

# Initialize the boto3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=str(settings.WASABI_ENDPOINT_URL).strip(),
    aws_access_key_id=settings.WASABI_ACCESS_KEY.strip(),
    aws_secret_access_key=settings.WASABI_SECRET_KEY.strip(),
    region_name=(settings.WASABI_REGION.strip() if settings.WASABI_REGION else None)
)

# Configure transfer settings for large files:
#   - multipart_threshold: switch to multipart uploads above this size
#   - max_concurrency: number of threads keeping multiple connections alive
#   - multipart_chunksize: size of each part
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,        # 8MB threshold
    multipart_chunksize=8 * 1024 * 1024,        # 8MB chunks
    max_concurrency=4,                          # 4 parallel threads
    use_threads=True
)

def upload_file(file_obj, bucket: str, path: str):
    """
    Uploads a file-like object to Wasabi, using multipart transfer config
    for large files and keeping connections alive.
    """
    logger.info(f"Starting upload to Wasabi bucket: {bucket}, path: {path}")
    try:
        s3_client.upload_fileobj(
            Fileobj=file_obj,
            Bucket=bucket,
            Key=path,
            Config=TRANSFER_CONFIG
        )
        logger.info(f"Successfully uploaded to Wasabi: {bucket}/{path}")
    except Exception as e:
        logger.error(f"Failed to upload file to Wasabi: {e}", exc_info=True)
        raise

def delete_file(bucket: str, path: str):
    """
    Deletes an object from Wasabi.
    """
    logger.info(f"Deleting from Wasabi bucket: {bucket}, path: {path}")
    try:
        s3_client.delete_object(Bucket=bucket, Key=path)
        logger.info(f"Successfully deleted from Wasabi: {bucket}/{path}")
    except Exception as e:
        logger.error(f"Failed to delete file from Wasabi: {e}", exc_info=True)
        raise

client = s3_client
