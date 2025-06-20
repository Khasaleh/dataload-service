from celery import shared_task
from app.db.connection import get_session # Assumed for DB interaction
# from app.models.schemas import UploadSessionModel # Import for type hinting if using actual DB model
from datetime import datetime # For updating timestamps
from app.services.storage import client as wasabi_client # Already here, ensure it's used for delete
from app.services.validator import validate_csv
import csv
import io
import redis
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name") # Already here

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB_ID_MAPPING = int(os.getenv("REDIS_DB_ID_MAPPING", 1))

# TTL Configuration for Redis session keys
DEFAULT_REDIS_SESSION_TTL_SECONDS = 24 * 60 * 60  # 24 hours
REDIS_SESSION_TTL_SECONDS = int(os.getenv("REDIS_SESSION_TTL_SECONDS", DEFAULT_REDIS_SESSION_TTL_SECONDS))

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_ID_MAPPING, decode_responses=True)
    redis_client.ping()
    logger.info(f"Connected to Redis: {REDIS_HOST}:{REDIS_PORT}, DB: {REDIS_DB_ID_MAPPING}")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Redis connection failed: {e}")
    redis_client = None

# Utilities

def get_id_map_key(session_id: str):
    return f"id_map:session:{session_id}"

def add_to_id_map(session_id: str, map_type: str, key: str, value: any, pipeline=None):
    if not redis_client:
        return
    r = pipeline if pipeline else redis_client
    r.hset(f"{get_id_map_key(session_id)}:{map_type}", key, str(value))

def get_from_id_map(session_id: str, map_type: str, key: str, pipeline=None):
    if not redis_client:
        return None
    r = pipeline if pipeline else redis_client
    return r.hget(f"{get_id_map_key(session_id)}:{map_type}", key)

# Generic task processor

# Placeholder function for updating session status in DB
def _update_session_status(session_id: str, status: str, details: Optional[str] = None, record_count: Optional[int] = None, error_count: Optional[int] = None):
    # Conceptual:
    # db = get_session() # Or however you get a DB session in Celery task
    # try:
    #     session_record = db.query(UploadSessionModelDB).filter(UploadSessionModelDB.session_id == session_id).first()
    #     if session_record:
    #         session_record.status = status
    #         session_record.updated_at = datetime.utcnow()
    #         if details is not None: session_record.details = details
    #         if record_count is not None: session_record.record_count = record_count
    #         if error_count is not None: session_record.error_count = error_count
    #         db.commit()
    #     else:
    #         logger.error(f"Upload session {session_id} not found in DB for status update.")
    # except Exception as e:
    #     logger.error(f"Failed to update session {session_id} status to {status}: {e}")
    #     db.rollback()
    # finally:
    #     db.close()
    logger.info(f"Conceptual DB Update: Session {session_id} status to '{status}'. Details: {details}, Records: {record_count}, Errors: {error_count}")
    pass


def process_csv_task(business_id, session_id, wasabi_file_path, original_filename, record_key, id_prefix, map_type):
    logger.info(f"Processing {map_type} for business: {business_id} session: {session_id} file: {original_filename} ({wasabi_file_path})")

    # 1. Update status to "processing"
    _update_session_status(session_id, status="processing")

    db_engine_session = None # Renamed from db_session to avoid confusion if you have a different 'db_session' concept
    try:
        response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
        file_content = response['Body'].read().decode('utf-8')
        # original_records are plain dicts from CSV
        original_records = list(csv.DictReader(io.StringIO(file_content)))

        if not original_records:
            logger.warning(f"No records found in file: {wasabi_file_path}")
            return {"status": "no_data", "message": "No records found in the CSV file."}

        # Determine referenced_entity_map based on map_type (which is load_type for validator)
        # The map_type in process_csv_task corresponds to the entity type being loaded,
        # e.g., "brand", "product", "product_item".
        # These should match the keys used in get_from_id_map for lookups.
        # map_type is "brands", "products", etc.
        referenced_entity_map = {}
        if map_type == "products":
            referenced_entity_map = {'brand_name': 'brands', 'return_policy_code': 'return_policies'}
        elif map_type == "product_items":
            referenced_entity_map = {'product_name': 'products'}
        elif map_type == "product_prices":
            referenced_entity_map = {'product_name': 'products'}
        elif map_type == "meta_tags":
            referenced_entity_map = {'product_name': 'products'}

        # Validate CSV data
        # `map_type` here is the `load_type` for `validate_csv`
        # `record_key` is for file-level uniqueness
        validation_errors, validated_records = validate_csv(
            load_type=map_type,
            records=original_records,
            session_id=session_id,
            record_key=record_key,
            referenced_entity_map=referenced_entity_map
        )

        if validation_errors:
            logger.error(f"Validation errors for {map_type} in session {session_id}, file {original_filename}: {validation_errors}")
            _update_session_status(session_id, status="validation_failed", details=str(validation_errors), error_count=len(validation_errors))
            return {"status": "validation_failed", "errors": validation_errors, "processed_count": 0, "session_id": session_id}

        # If validation passes, validated_records contains Pydantic model dicts
        # Proceed with existing logic using validated_records

        db_engine_session = get_session(business_id) # Assuming get_session gives a SQLAlchemy session for DB ops
        redis_pipeline = redis_client.pipeline() if redis_client else None
        processed_count = 0

        for record_data in validated_records: # Use validated_records
            key_value = record_data.get(record_key) # Use record_data which is a dict
            if not key_value: # Should not happen if record_key is mandatory and validated
                logger.warning(f"Missing key_value for record_key {record_key} in validated data: {record_data}")
                continue

            # ID generation logic remains the same.
            # The map_type for add_to_id_map is the type of the current entity being processed (e.g. "brands", "products")
            generated_id = f"{id_prefix}:{key_value.lower().replace(' ', '_')}"
            if redis_pipeline:
                add_to_id_map(session_id, map_type, key_value, generated_id, redis_pipeline) # map_type is already plural e.g. "brands"

            # TODO: Add database operations here using record_data if needed for the task
            # For example, creating/updating records in the database
            # For now, the task primarily focuses on ID mapping.

            processed_count += 1

        if redis_pipeline:
            # Execute HSET commands
            redis_pipeline.execute()

            # Now, set TTL on the key for the current map_type
            # We need a new pipeline for this, or execute it directly if redis_client is not None.
            # For simplicity, let's use a new pipeline or direct command.
            # It's generally safe to add to the same pipeline if it hasn't been reset,
            # but the original execute() consumes the pipeline commands.
            # So, we need to re-initialize or use redis_client directly.

            # Re-acquire pipeline or use client directly for EXPIRE
            # (This assumes redis_client is the same instance used by the pipeline earlier)
            if redis_client: # Ensure client is available
                key_to_expire = f"{get_id_map_key(session_id)}:{map_type}"
                logger.info(f"Setting TTL for Redis key: {key_to_expire} to {REDIS_SESSION_TTL_SECONDS} seconds.")
                try:
                    # Using a new pipeline for the expire command for clarity
                    ttl_pipeline = redis_client.pipeline()
                    ttl_pipeline.expire(key_to_expire, time=REDIS_SESSION_TTL_SECONDS)
                    ttl_pipeline.execute()
                except Exception as ttl_error:
                    logger.error(f"Failed to set TTL for key {key_to_expire}: {ttl_error}", exc_info=True)
                    # Non-critical error, so we don't fail the task.

        if db_engine_session:
            # Example: db_engine_session.commit() if there were DB operations for the data itself
            pass

        # 5. Wasabi Cleanup
        try:
            logger.info(f"Attempting to delete {wasabi_file_path} from Wasabi bucket {WASABI_BUCKET_NAME}.")
            wasabi_client.delete_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            logger.info(f"Successfully deleted {wasabi_file_path} from Wasabi.")
        except Exception as cleanup_error:
            logger.error(f"Failed to delete {wasabi_file_path} from Wasabi: {cleanup_error}", exc_info=True)
            # Do not fail the entire task for cleanup failure, but log it.
            # Optionally, update session details with cleanup failure notice.

        # 6. Update status to "completed"
        _update_session_status(session_id, status="completed", record_count=len(validated_records)) # or processed_count
        logger.info(f"Successfully processed {processed_count} records for {map_type} in session {session_id}.")
        return {"status": "success", "processed_count": processed_count, "session_id": session_id}

    except Exception as e:
        logger.error(f"Error processing {map_type} for session {session_id}, file {original_filename}: {e}", exc_info=True)
        # 7. Update status to "failed" on general error
        _update_session_status(session_id, status="failed", details=str(e))
        if db_engine_session:
            db_engine_session.rollback()
        return {"status": "error", "message": str(e), "session_id": session_id}
    finally:
        if db_engine_session:
            db_engine_session.close()

# Specific loaders - need to add original_filename to their calls

@shared_task(name="process_brands_file")
def process_brands_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "brand_name", "brand", "brands")

@shared_task(name="process_attributes_file")
def process_attributes_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "attribute_name", "attr", "attributes")

@shared_task(name="process_return_policies_file")
def process_return_policies_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "return_policy_code", "rp", "return_policies")

@shared_task(name="process_products_file")
def process_products_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "product_name", "prod", "products")

@shared_task(name="process_product_items_file")
def process_product_items_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "variant_sku", "item", "product_items")

@shared_task(name="process_product_prices_file")
def process_product_prices_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "product_name", "price", "product_prices")

@shared_task(name="process_meta_tags_file")
def process_meta_tags_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "product_name", "meta", "meta_tags")
