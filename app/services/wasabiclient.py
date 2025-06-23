import boto3
import logging
from app.core.config import settings # Import the centralized settings

logger = logging.getLogger(__name__)

# Initialize s3 client and BUCKET_NAME to None initially
s3 = None
BUCKET_NAME = None

if settings.WASABI_ENDPOINT_URL and \
   settings.WASABI_ACCESS_KEY and \
   settings.WASABI_SECRET_KEY and \
   settings.WASABI_BUCKET_NAME:

    s3 = boto3.client(
        's3',
        endpoint_url=str(settings.WASABI_ENDPOINT_URL), # Convert HttpUrl to string
        aws_access_key_id=settings.WASABI_ACCESS_KEY,
        aws_secret_access_key=settings.WASABI_SECRET_KEY,
        region_name=settings.WASABI_REGION # This can be None if not set
    )
    BUCKET_NAME = settings.WASABI_BUCKET_NAME
    logger.info(f"Wasabi client configured for endpoint: {settings.WASABI_ENDPOINT_URL} and bucket: {BUCKET_NAME}")
else:
    logger.warning(
        "Wasabi S3 client is not configured. Missing one or more required settings: "
        "WASABI_ENDPOINT_URL, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET_NAME. "
        "File upload functionality will be impaired."
    )

def upload_to_wasabi(key: str, content: bytes):
    if not s3 or not BUCKET_NAME:
        logger.error("Wasabi client or bucket not configured. Cannot upload file.")
        raise ConnectionError("Wasabi client not configured. Upload failed.")
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content)

def delete_from_wasabi(key: str):
    if not s3 or not BUCKET_NAME:
        logger.error("Wasabi client or bucket not configured. Cannot delete file.")
        raise ConnectionError("Wasabi client not configured. Deletion failed.")
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)