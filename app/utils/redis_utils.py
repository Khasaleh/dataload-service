import redis
import logging
from typing import Any, Iterator
from contextlib import contextmanager
from redis.client import Pipeline
from app.core.config import settings  # Import centralized settings

logger = logging.getLogger(__name__)

# Redis configuration from centralized settings
REDIS_HOST = settings.REDIS_HOST
REDIS_PORT = settings.REDIS_PORT
REDIS_DB_ID_MAPPING = settings.REDIS_DB_ID_MAPPING
REDIS_SESSION_TTL_SECONDS = settings.REDIS_SESSION_TTL_SECONDS
REDIS_PASSWORD = settings.REDIS_PASSWORD  # Get Redis password from settings

redis_client_instance = None
if REDIS_HOST and REDIS_PORT is not None and REDIS_DB_ID_MAPPING is not None:
    try:
        # Construct URL for redis-py, though it also accepts host/port/db directly
        # Using host/port/db for clarity matching previous setup
        redis_client_instance = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB_ID_MAPPING,
            password=REDIS_PASSWORD,  # Provide password if it exists
            decode_responses=True
        )
        redis_client_instance.ping()  # Check if Redis is reachable
        logger.info(f"Connected to Redis for ID mapping utilities: {REDIS_HOST}:{REDIS_PORT}, DB: {REDIS_DB_ID_MAPPING}")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection failed for ID mapping utilities ({REDIS_HOST}:{REDIS_PORT}, DB: {REDIS_DB_ID_MAPPING}): {e}")
        redis_client_instance = None
    except Exception as e:
        logger.error(f"Failed to initialize Redis client for ID mapping utilities: {e}", exc_info=True)
        redis_client_instance = None
else:
    logger.warning(
        "Redis client for ID mapping utilities is not configured. Missing one or more settings: "
        "REDIS_HOST, REDIS_PORT, REDIS_DB_ID_MAPPING."
    )


DB_PK_MAP_SUFFIX = "_db_pk"

def get_id_map_key_base(session_id: str) -> str:
    return f"id_map:session:{session_id}"

def add_to_id_map(session_id: str, map_type: str, key: str, value: Any, pipeline: Any = None) -> None:
    client_to_use = pipeline if pipeline else redis_client_instance
    if not client_to_use:
        logger.warning("Redis client not available. Skipping add_to_id_map.")
        return
    redis_hash_key = f"{get_id_map_key_base(session_id)}:{map_type}"
    try:
        client_to_use.hset(redis_hash_key, key, str(value))
    except Exception as e:
        logger.error(f"Redis HSET error for key {redis_hash_key}, field {key}: {e}", exc_info=True)

def get_from_id_map(session_id: str, map_type: str, key: str, pipeline: Any = None) -> Any:
    client_to_use = pipeline if pipeline else redis_client_instance
    if not client_to_use:
        logger.warning("Redis client not available. Skipping get_from_id_map, returning None.")
        return None
    redis_hash_key = f"{get_id_map_key_base(session_id)}:{map_type}"
    try:
        return client_to_use.hget(redis_hash_key, key)
    except Exception as e:
        logger.error(f"Redis HGET error for key {redis_hash_key}, field {key}: {e}", exc_info=True)
        return None

def set_id_map_ttl(session_id: str, map_type: str, client: redis.Redis | None) -> None:
    if not client:
        logger.warning(f"Redis client not provided to set_id_map_ttl for session {session_id}, map {map_type}. Skipping TTL set.")
        return
    redis_hash_key = f"{get_id_map_key_base(session_id)}:{map_type}"
    try:
        logger.debug(f"Setting TTL for Redis key: {redis_hash_key} to {REDIS_SESSION_TTL_SECONDS} seconds.")
        client.expire(redis_hash_key, time=REDIS_SESSION_TTL_SECONDS)
    except Exception as e:
        logger.error(f"Redis EXPIRE error for key {redis_hash_key}: {e}", exc_info=True)

@contextmanager
def get_redis_pipeline(client_instance_param: redis.Redis | None = None) -> Iterator[Pipeline | None]:
    effective_client = client_instance_param if client_instance_param else redis_client_instance
    if not effective_client:
        logger.error("Redis client is not available in utils.get_redis_pipeline for pipeline creation.")
        yield None
        return
    pipe = effective_client.pipeline()
    try:
        yield pipe
    finally:
        try:
            pipe.reset()
        except Exception as e:
            logger.error(f"Error resetting Redis pipeline in utils: {e}")
