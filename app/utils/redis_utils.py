import redis
import os
import logging
from typing import Any

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB_ID_MAPPING = int(os.getenv("REDIS_DB_ID_MAPPING", 1)) # Same DB as used in load_jobs
DEFAULT_REDIS_SESSION_TTL_SECONDS = 24 * 60 * 60
REDIS_SESSION_TTL_SECONDS = int(os.getenv("REDIS_SESSION_TTL_SECONDS", DEFAULT_REDIS_SESSION_TTL_SECONDS))

redis_client_instance = None
try:
    # Ensure this client configuration is consistent if other parts of the app also connect to Redis.
    redis_client_instance = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_ID_MAPPING, decode_responses=True)
    redis_client_instance.ping()
    logger.info(f"Connected to Redis for ID mapping utilities: {REDIS_HOST}:{REDIS_PORT}, DB: {REDIS_DB_ID_MAPPING}")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Redis connection failed for ID mapping utilities: {e}")
    redis_client_instance = None # Explicitly set to None on failure

# Suffix for Redis keys that map CSV identifiers to Database Primary Keys
DB_PK_MAP_SUFFIX = "_db_pk"

def get_id_map_key_base(session_id: str) -> str:
    """Base part of the Redis key for a session's ID maps."""
    return f"id_map:session:{session_id}"

def add_to_id_map(session_id: str, map_type: str, key: str, value: Any, pipeline: Any = None) -> None:
    """
    Adds a key-value pair to a hash in Redis, scoped by session_id and map_type.
    Uses the provided pipeline if available, otherwise uses the global redis_client_instance.
    """
    if not redis_client_instance:
        logger.warning("Redis client not available. Skipping add_to_id_map.")
        return

    r_conn = pipeline if pipeline else redis_client_instance
    redis_hash_key = f"{get_id_map_key_base(session_id)}:{map_type}"

    try:
        r_conn.hset(redis_hash_key, key, str(value))
    except Exception as e:
        logger.error(f"Redis HSET error for key {redis_hash_key}, field {key}: {e}", exc_info=True)


def get_from_id_map(session_id: str, map_type: str, key: str, pipeline: Any = None) -> Any:
    """
    Retrieves a value from a hash in Redis, scoped by session_id and map_type.
    Uses the provided pipeline if available, otherwise uses the global redis_client_instance.
    """
    if not redis_client_instance:
        logger.warning("Redis client not available. Skipping get_from_id_map, returning None.")
        return None

    r_conn = pipeline if pipeline else redis_client_instance
    redis_hash_key = f"{get_id_map_key_base(session_id)}:{map_type}"

    try:
        return r_conn.hget(redis_hash_key, key)
    except Exception as e:
        logger.error(f"Redis HGET error for key {redis_hash_key}, field {key}: {e}", exc_info=True)
        return None

def set_id_map_ttl(session_id: str, map_type: str, pipeline: Any = None) -> None:
    """
    Sets the Time-To-Live (TTL) for a specific session ID map type key in Redis.
    Uses the provided pipeline if available, otherwise uses the global redis_client_instance.
    If not using a pipeline, the EXPIRE command is executed immediately.
    """
    if not redis_client_instance:
        logger.warning("Redis client not available. Skipping set_id_map_ttl.")
        return

    r_conn = pipeline if pipeline else redis_client_instance
    redis_hash_key = f"{get_id_map_key_base(session_id)}:{map_type}"

    try:
        logger.debug(f"Setting TTL for Redis key: {redis_hash_key} to {REDIS_SESSION_TTL_SECONDS} seconds.")
        r_conn.expire(redis_hash_key, time=REDIS_SESSION_TTL_SECONDS)
        # If r_conn is redis_client_instance (i.e., pipeline is None), it's executed.
        # If r_conn is a pipeline, the caller is responsible for pipeline.execute().
    except Exception as e:
        logger.error(f"Redis EXPIRE error for key {redis_hash_key}: {e}", exc_info=True)

def get_redis_pipeline(use_pipeline: bool = True) -> Any:
    """Returns a Redis pipeline object if use_pipeline is True and client is available, else None."""
    if use_pipeline and redis_client_instance:
        return redis_client_instance.pipeline()
    return None
