import os
import logging
from typing import Optional

from app.core.config import settings

logger = logging.getLogger(__name__)

# Base directory for local file storage (ensure this exists or is creatable)
STORAGE_ROOT = getattr(settings, "LOCAL_STORAGE_PATH", "/tmp/uploads")

class LocalStorageClient:
    def __init__(self, storage_root: str = STORAGE_ROOT):
        self.storage_root = storage_root
        os.makedirs(self.storage_root, exist_ok=True)

    def upload_file(self, file_obj, bucket: str, key: str) -> str:
        """
        Saves the incoming file-like object to local disk under STORAGE_ROOT/bucket/key.
        Returns the full file path as a tracking ID.
        """
        dest_dir = os.path.join(self.storage_root, bucket)
        os.makedirs(dest_dir, exist_ok=True)

        file_path = os.path.join(dest_dir, key)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        file_obj.seek(0)
        with open(file_path, "wb") as f:
            f.write(file_obj.read())

        logger.info("Saved file locally to %s", file_path)
        return file_path

    def delete_file(self, bucket: str, key: str) -> None:
        """
        Deletes the local file at STORAGE_ROOT/bucket/key.
        """
        file_path = os.path.join(self.storage_root, bucket, key)
        try:
            os.remove(file_path)
            logger.info("Deleted local file %s", file_path)
        except FileNotFoundError:
            logger.warning("Local file %s not found for deletion", file_path)
        except Exception as e:
            logger.error("Error deleting local file %s: %s", file_path, e)
            raise

# Module-level client instance for local storage
_local_client = LocalStorageClient()

# Exposed functions for importing elsewhere

def upload_file(file_obj, bucket: str, key: str) -> str:
    return _local_client.upload_file(file_obj, bucket, key)


def delete_file(bucket: str, key: str) -> None:
    return _local_client.delete_file(bucket, key)

# Alias for backward compatibility
client = _local_client
