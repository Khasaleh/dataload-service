import io
import logging

from app.core.config import settings
from app.services.storage import WasabiClient

logger = logging.getLogger(__name__)

# Validate single source of truth for bucket
if not settings.WASABI_BUCKET_NAME:
    raise RuntimeError(
        "WASABI_BUCKET_NAME is not set! Please configure your environment or secrets."
    )

# Instantiate a WasabiClient with your existing settings
_wasabi = WasabiClient(
    endpoint_url=str(settings.WASABI_ENDPOINT_URL),
    access_key=settings.WASABI_ACCESS_KEY,
    secret_key=settings.WASABI_SECRET_KEY,
    region=settings.WASABI_REGION,
)

_BUCKET = settings.WASABI_BUCKET_NAME


def upload_to_wasabi(key: str, content: bytes) -> None:
    """
    Upload raw bytes to Wasabi under the given key.
    Uses put_small_file (no chunking) since we already have the full bytes in memory.
    """
    logger.info("Uploading bytes to Wasabi bucket=%s, key=%s", _BUCKET, key)
    try:
        bio = io.BytesIO(content)
        _wasabi.put_small_file(bio, _BUCKET, key)
    except Exception:
        logger.exception("Failed to upload to Wasabi: %s/%s", _BUCKET, key)
        raise


def delete_from_wasabi(key: str) -> None:
    """
    Delete the object at `key` from the configured Wasabi bucket.
    """
    logger.info("Deleting Wasabi object bucket=%s, key=%s", _BUCKET, key)
    try:
        _wasabi.delete_file(_BUCKET, key)
    except Exception:
        logger.exception("Failed to delete Wasabi object: %s/%s", _BUCKET, key)
        raise
