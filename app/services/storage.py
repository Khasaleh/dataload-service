import boto3
import logging
import tempfile
import os

from app.core.config import settings

logger = logging.getLogger(__name__)

# Validate Wasabi settings
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

# Create the client once
s3_client = boto3.client(
    "s3",
    endpoint_url=str(settings.WASABI_ENDPOINT_URL),
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    region_name=settings.WASABI_REGION
)


def upload_file(file_obj, bucket: str, path: str):
    """
    Saves the FastAPI UploadFile to disk, then uploads with boto3.upload_file()
    to avoid aws-chunked encoding and ensure Content-Length header.
    """
    logger.info(f"Uploading to Wasabi bucket={bucket}, path={path}")

    tmp_path = None

    try:
        # Write UploadFile to a local file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
            file_obj.seek(0)
            for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
                tmp.write(chunk)
            tmp.flush()

        logger.info(f"Local temp file for upload: {tmp_path}")

        # Now call boto3's upload_file (never uses chunked encoding)
        s3_client.upload_file(
            Filename=tmp_path,
            Bucket=bucket,
            Key=path,
            ExtraArgs={"ACL": "public-read"}
        )

        logger.info(f"Successfully uploaded to Wasabi: {bucket}/{path}")

    except Exception as e:
        logger.exception(f"Error uploading to Wasabi: {e}")
        raise

    finally:
        # Always try to delete the temp file
        if tmp_path and os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
                logger.debug(f"Deleted local temp file: {tmp_path}")
            except OSError as cleanup_error:
                logger.warning(f"Could not delete temp file {tmp_path}: {cleanup_error}")


def delete_file(bucket: str, path: str):
    """
    Deletes an object from Wasabi S3.
    """
    logger.info(f"Deleting from Wasabi bucket={bucket}, path={path}")
    s3_client.delete_object(Bucket=bucket, Key=path)
    logger.info(f"Successfully deleted from Wasabi: {bucket}/{path}")

client = s3_client
