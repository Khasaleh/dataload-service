import os
import logging
import tempfile

import boto3
from botocore.config import Config
from botocore.exceptions import (
    ReadTimeoutError,
    ConnectTimeoutError,
    ClientError
)

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
        # Validate settings
        if not (endpoint_url and access_key and secret_key):
            raise RuntimeError(
                "One or more required Wasabi settings are missing. "
                "Please check your environment variables or Kubernetes secrets."
            )

        # Configure timeouts and retries
        self._config = Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"max_attempts": max_retries, "mode": "standard"}
        )

        logger.info("Initializing Wasabi S3 client with endpoint %s", endpoint_url)
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=self._config
        )

    def upload_file(self, file_obj, bucket: str, key: str) -> None:
        """
        Uploads a file-like object to Wasabi, ensures Content-Length is set,
        and then confirms the upload via head_object.
        """
        logger.info("Uploading to Wasabi bucket=%s, key=%s", bucket, key)
        tmp_path = None

        try:
            # 1) Dump to temp file so boto3 can stat() it
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
                file_obj.seek(0)
                for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
                    tmp.write(chunk)

            logger.debug("Temp file written at %s", tmp_path)

            # 2) Perform the upload
            self._client.upload_file(
                Filename=tmp_path,
                Bucket=bucket,
                Key=key,
                ExtraArgs={"ACL": "public-read"}
            )

            # 3) Confirm via head_object
            self._client.head_object(Bucket=bucket, Key=key)
            logger.info("Confirmed upload: %s/%s", bucket, key)

        except ConnectTimeoutError:
            logger.error("Connection to Wasabi timed out during upload")
            raise
        except ReadTimeoutError:
            logger.error("Read timed out while uploading to Wasabi")
            raise
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.error("Upload did not show up in Wasabi: %s/%s", bucket, key)
            logger.exception("ClientError during Wasabi upload: %s", e)
            raise
        except Exception:
            logger.exception("Unexpected error uploading to Wasabi")
            raise
        finally:
            # Clean up temp file
            if tmp_path and os.path.isfile(tmp_path):
                try:
                    os.remove(tmp_path)
                    logger.debug("Deleted temp file: %s", tmp_path)
                except OSError as cleanup_error:
                    logger.warning("Could not delete temp file %s: %s", tmp_path, cleanup_error)

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

    def put_small_file(self, file_obj, bucket: str, key: str) -> None:
        """
        For small files (<~5 MB), bypass multipart upload entirely.
        """
        logger.info("Putting small file to Wasabi bucket=%s, key=%s", bucket, key)
        try:
            file_obj.seek(0)
            self._client.put_object(
                Bucket=bucket,
                Key=key,
                Body=file_obj,
                ACL="public-read"
            )
            self._client.head_object(Bucket=bucket, Key=key)
            logger.info("Confirmed small-file PUT: %s/%s", bucket, key)
        except Exception:
            logger.exception("Error in put_object to Wasabi")
            raise


# Instantiate a module-level client for ease of use:
wasabi_client = WasabiClient(
    endpoint_url=str(settings.WASABI_ENDPOINT_URL),
    access_key=settings.WASABI_ACCESS_KEY,
    secret_key=settings.WASABI_SECRET_KEY,
    region=settings.WASABI_REGION,
)
