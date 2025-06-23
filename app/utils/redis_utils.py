import redis
import os
import logging
from typing import Any, Iterator # Added Iterator
from contextlib import contextmanager # Added contextmanager
from redis.client import Pipeline # For type hinting pipeline

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB_ID_MAPPING = int(os.getenv("REDIS_DB_ID_MAPPING", 1))
DEFAULT_REDIS_SESSION_TTL_SECONDS = 24 * 60 * 60
REDIS_SESSION_TTL_SECONDS = int(os.getenv("REDIS_SESSION_TTL_SECONDS", DEFAULT_REDIS_SESSION_TTL_SECONDS))

redis_client_instance = None
try:
    redis_client_instance = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_ID_MAPPING, decode_responses=True)
    redis_client_instance.ping()
    logger.info(f"Connected to Redis for ID mapping utilities: {REDIS_HOST}:{REDIS_PORT}, DB: {REDIS_DB_ID_MAPPING}")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Redis connection failed for ID mapping utilities: {e}")
    redis_client_instance = None

DB_PK_MAP_SUFFIX = "_db_pk"

def get_id_map_key_base(session_id: str) -> str:
    return f"id_map:session:{session_id}"

def add_to_id_map(session_id: str, map_type: str, key: str, value: Any, pipeline: Any = None) -> None:
    # This function uses the global redis_client_instance if pipeline is not provided.
    # For testing, if this is called, redis_client_instance would need to be mocked.
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
    # Similar to add_to_id_map, relies on global redis_client_instance if pipeline is None.
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

# Corrected signature and logic for set_id_map_ttl
def set_id_map_ttl(session_id: str, map_type: str, client: redis.Redis | None) -> None:
    """
    Sets the Time-To-Live (TTL) for a specific session ID map type key in Redis.
    Requires a Redis client instance to be passed.
    """
    if not client:
        logger.warning(f"Redis client not provided to set_id_map_ttl for session {session_id}, map {map_type}. Skipping TTL set.")
        return

    redis_hash_key = f"{get_id_map_key_base(session_id)}:{map_type}"
    try:
        logger.debug(f"Setting TTL for Redis key: {redis_hash_key} to {REDIS_SESSION_TTL_SECONDS} seconds.")
        client.expire(redis_hash_key, time=REDIS_SESSION_TTL_SECONDS)
    except Exception as e:
        logger.error(f"Redis EXPIRE error for key {redis_hash_key}: {e}", exc_info=True)

# Corrected get_redis_pipeline to be a context manager
@contextmanager
def get_redis_pipeline(client_instance_param: redis.Redis | None = None) -> Iterator[Pipeline | None]:
    """
    Returns a Redis pipeline object from the provided client instance, for use in a 'with' statement.
    If no client_instance_param is provided, it attempts to use the global redis_client_instance from this module.
    Yields None if no effective client is found.
    """
    effective_client = client_instance_param if client_instance_param else redis_client_instance

    if not effective_client:
        logger.error("Redis client is not available in utils.get_redis_pipeline for pipeline creation.")
        yield None # Yield None if no client, so 'with' statement can handle it
        return

    pipe = effective_client.pipeline()
    try:
        yield pipe
    finally:
        try:
            pipe.reset()
        except Exception as e:
            logger.error(f"Error resetting Redis pipeline in utils: {e}")

[end of app/utils/redis_utils.py]
