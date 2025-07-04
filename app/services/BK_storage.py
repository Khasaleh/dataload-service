import os
import logging
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

from app.core.config import settings

logger = logging.getLogger(__name__)


class WasabiClient:
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str,
        connect_timeout: int = 30,
        read_timeout: int = 60,
        max_retries: int = 5,
    ):
        # Validate required settings
        if not (endpoint_url and access_key and secret_key):
            raise RuntimeError(
                "One or more required Wasabi settings are missing. "
                "Please check your environment variables or Kubernetes secrets."
            )

        # Configure timeouts and retries
        self._config = Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"max_attempts": max_retries, "mode": "standard"},
        )

        logger.info("Initializing Wasabi S3 client with endpoint %s", endpoint_url)
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=self._config,
        )

    def upload_file(self, file_obj, bucket: str, key: str) -> str:
        """
        Uploads a small file to Wasabi in one non-chunked PUT and returns the ETag as tracking ID.
        """
        logger.info("Uploading to Wasabi bucket=%s, key=%s", bucket, key)
        file_obj.seek(0)
        try:
            response = self._client.put_object(
                Bucket=bucket,
                Key=key,
                Body=file_obj,
                ACL="public-read",
            )
            etag = response.get("ETag")
            logger.info("Successfully uploaded to Wasabi: %s/%s, ETag=%s", bucket, key, etag)
            return etag

        except ConnectTimeoutError:
            logger.error("Connection to Wasabi timed out during upload: %s/%s", bucket, key)
            raise
        except ReadTimeoutError:
            logger.error("Read timed out while uploading to Wasabi: %s/%s", bucket, key)
            raise
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            logger.exception("ClientError during Wasabi upload: %s, code=%s", e, code)
            raise
        except Exception:
            logger.exception("Unexpected error uploading to Wasabi: %s/%s", bucket, key)
            raise

    def put_small_file(self, file_obj, bucket: str, key: str) -> str:
        """
        Alias for upload_file for backward-compatibility with WasabiClient.put_small_file calls.
        """
        return self.upload_file(file_obj, bucket, key)

    def delete_file(self, bucket: str, key: str) -> None:
        """
        Deletes an object from Wasabi S3.
        """
        logger.info("Deleting from Wasabi bucket=%s, key=%s", bucket, key)
        try:
            self._client.delete_object(Bucket=bucket, Key=key)
            logger.info("Successfully deleted: %s/%s", bucket, key)
        except ClientError:
            logger.exception("Error deleting Wasabi object %s/%s", bucket, key)
            raise


# Module-level client instance
_wasabi_client = WasabiClient(
    endpoint_url=str(settings.WASABI_ENDPOINT_URL),
    access_key=settings.WASABI_ACCESS_KEY,
    secret_key=settings.WASABI_SECRET_KEY,
    region=settings.WASABI_REGION,
)

# Exposed functions for importing elsewhere

def upload_file(file_obj, bucket: str, key: str) -> str:
    return _wasabi_client.upload_file(file_obj, bucket, key)


def delete_file(bucket: str, key: str) -> None:
    return _wasabi_client.delete_file(bucket, key)

# Alias for backward-compatibility
client = _wasabi_client
