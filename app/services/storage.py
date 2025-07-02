import boto3
import os
import logging

logger = logging.getLogger(__name__)

# --- Wasabi S3 Client Configuration ---
# The following environment variables are expected to be set for Wasabi S3 client initialization:
# - WASABI_ENDPOINT_URL: The S3 API endpoint URL for Wasabi.
# - WASABI_ACCESS_KEY:   The Wasabi access key ID.
# - WASABI_SECRET_KEY:   The Wasabi secret access key.
#
# Additionally, functions in this module or services calling this module might use:
# - WASABI_BUCKET_NAME:  The name of the Wasabi bucket to interact with.

WASABI_ENDPOINT_URL = os.getenv("WASABI_ENDPOINT_URL")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")

if not all([WASABI_ENDPOINT_URL, WASABI_ACCESS_KEY, WASABI_SECRET_KEY]):
    logger.error(
        "Missing one or more required Wasabi environment variables: "
        "WASABI_ENDPOINT_URL, WASABI_ACCESS_KEY, WASABI_SECRET_KEY. "
        "S3 client will not be initialized correctly."
    )
    # Depending on desired behavior, could raise an ImportError or allow client init to fail.
    # For now, allow init to proceed which will likely fail if credentials are None.
    # A more robust approach would be to raise an exception here if any are missing.

session = boto3.session.Session()
client = session.client(
    service_name='s3',
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    endpoint_url=WASABI_ENDPOINT_URL
)

def upload_file(bucket, path, file_obj):
    client.upload_fileobj(file_obj, bucket, path)

def delete_file(bucket, path):
    client.delete_object(Bucket=bucket, Key=path)
