from celery import shared_task
from app.db.connection import get_session
from app.db.models import ( # Import ORM Models
    BrandOrm, AttributeOrm, ReturnPolicyOrm, ProductOrm,
    ProductItemOrm, ProductPriceOrm, MetaTagOrm
)
from datetime import datetime
from app.services.storage import client as wasabi_client
from app.services.validator import validate_csv
from typing import Optional, Dict, Any, List # For type hints
import csv
import io
import redis
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Name of the Wasabi bucket used for storing uploaded CSV files.
WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name")

# Redis connection parameters for ID mapping and potentially other task-related data.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
# Redis database number used for storing ID mappings by this service.
REDIS_DB_ID_MAPPING = int(os.getenv("REDIS_DB_ID_MAPPING", 1))

# TTL Configuration for Redis session keys (e.g., id_map:session:{session_id}:{map_type})
DEFAULT_REDIS_SESSION_TTL_SECONDS = 24 * 60 * 60  # Default: 24 hours
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

# from app.db.models import UploadSessionOrm # Already imported with other ORM models
# from app.db.connection import get_session as get_db_session_sync # Already imported as get_session

# Function to update session status in DB
def _update_session_status(
    session_id: str,
    business_id: str, # Added business_id for context, though session_id should be unique
    status: str,
    details: Optional[str] = None,
    record_count: Optional[int] = None,
    error_count: Optional[int] = None
):
    logger.info(f"Attempting to update session {session_id} for business {business_id} to status '{status}'.")
    db = None
    try:
        # IMPORTANT: Celery tasks run in separate processes. Each task needs its own DB session.
        db = get_session(business_id=business_id) # Use existing get_session

        session_record = db.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == session_id).first()

        if session_record:
            session_record.status = status
            session_record.updated_at = datetime.utcnow() # Ensure datetime is available
            if details is not None:
                max_details_len = 1024 # Example limit, adjust based on DB schema (Text is usually large but good to have a cap)
                session_record.details = details[:max_details_len] if len(details) > max_details_len else details
            if record_count is not None:
                session_record.record_count = record_count
            if error_count is not None:
                session_record.error_count = error_count
            db.commit()
            logger.info(f"Successfully updated session {session_id} to status '{status}'.")
        else:
            logger.error(f"Upload session {session_id} not found in DB for status update for business {business_id}.")
    except Exception as e:
        logger.error(f"DB Error: Failed to update session {session_id} status to {status}: {e}", exc_info=True)
        if db:
            db.rollback()
    finally:
        if db:
            db.close()


def process_csv_task(business_id, session_id, wasabi_file_path, original_filename, record_key, id_prefix, map_type):
    logger.info(f"Processing {map_type} for business: {business_id} session: {session_id} file: {original_filename} ({wasabi_file_path})")

    # 1. Update status to "processing"
    _update_session_status(session_id, business_id, status="processing")

    db_engine_session = None
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
        # For referential integrity checks during validation, we need to check against DB PKs
        # that would have been populated by previous runs of this task for parent entities.
        # The map type for these DB PKs will be like "brands_db_pk".
        db_pk_map_suffix = "_db_pk"
        referenced_entity_map_for_validation = {}
        if map_type == "products":
            referenced_entity_map_for_validation = {
                'brand_name': f"brands{db_pk_map_suffix}",
                'return_policy_code': f"return_policies{db_pk_map_suffix}"
            }
        elif map_type == "product_items":
            referenced_entity_map_for_validation = {'product_name': f"products{db_pk_map_suffix}"}
        elif map_type == "product_prices":
            referenced_entity_map_for_validation = {'product_name': f"products{db_pk_map_suffix}"}
        elif map_type == "meta_tags":
            referenced_entity_map_for_validation = {'product_name': f"products{db_pk_map_suffix}"}

        # Validate CSV data (this step might perform referential integrity against _db_pk maps)
        validation_errors, validated_records = validate_csv(
            load_type=map_type,
            records=original_records,
            session_id=session_id,
            record_key=record_key, # For file-level uniqueness of this key
            referenced_entity_map=referenced_entity_map_for_validation
        )

        if validation_errors:
            logger.error(f"Initial validation errors for {map_type} in session {session_id}, file {original_filename}: {validation_errors}")
            _update_session_status(session_id, business_id, status="validation_failed", details=str(validation_errors), error_count=len(validation_errors))
            return {"status": "validation_failed", "errors": validation_errors, "processed_count": 0, "session_id": session_id}

        db_engine_session = get_session(business_id)

        # Pipeline for original string-to-string ID mapping (if still needed)
        # For now, this part is kept, but its utility might diminish.
        # It is used for the `generated_id` which is not directly a DB field.
        string_id_redis_pipeline = redis_client.pipeline() if redis_client else None

        # Pipeline for CSV_key-to-DB_PK mapping
        db_pk_redis_pipeline = redis_client.pipeline() if redis_client else None

        processed_count = 0
        db_error_count = 0
        fk_resolution_errors = []

        # --- Database Interaction Loop ---
        for i, record_data in enumerate(validated_records):
            csv_row_number = i + 1 # For logging/error reporting
            orm_instance = None
            db_pk = None

            # Get the main unique key from the CSV record (e.g., brand_name, product_name, variant_sku)
            csv_unique_key_value = record_data.get(record_key)
            if not csv_unique_key_value:
                logger.warning(f"Row {csv_row_number}: Missing record_key '{record_key}' in validated data: {record_data}. Skipping.")
                fk_resolution_errors.append({"row": csv_row_number, "error": f"Missing record_key '{record_key}'."})
                db_error_count += 1
                continue

            # Convert Pydantic dict (record_data) to a dict suitable for ORM, resolving FKs
            orm_data = {"business_id": business_id}
            skip_this_record = False

            if map_type == "brands":
                orm_data.update({k: v for k, v in record_data.items() if k in BrandOrm.__table__.columns})
                orm_instance = db_engine_session.query(BrandOrm).filter_by(
                    business_id=business_id, brand_name=record_data['brand_name']
                ).first()

            elif map_type == "attributes":
                orm_data.update({k: v for k, v in record_data.items() if k in AttributeOrm.__table__.columns})
                orm_instance = db_engine_session.query(AttributeOrm).filter_by(
                    business_id=business_id, attribute_name=record_data['attribute_name']
                ).first()

            elif map_type == "return_policies":
                orm_data.update({k: v for k, v in record_data.items() if k in ReturnPolicyOrm.__table__.columns})
                orm_instance = db_engine_session.query(ReturnPolicyOrm).filter_by(
                    business_id=business_id, return_policy_code=record_data['return_policy_code']
                ).first()

            elif map_type == "products":
                brand_name_csv = record_data.get('brand_name')
                brand_db_id = get_from_id_map(session_id, f"brands{db_pk_map_suffix}", brand_name_csv, pipeline=None) # Use direct client for immediate read
                rp_code_csv = record_data.get('return_policy_code')
                rp_db_id = get_from_id_map(session_id, f"return_policies{db_pk_map_suffix}", rp_code_csv, pipeline=None)

                if not brand_db_id or not rp_db_id:
                    logger.error(f"Row {csv_row_number} (Product: {record_data.get('product_name')}): Could not resolve FKs. Brand '{brand_name_csv}': {brand_db_id}, Policy '{rp_code_csv}': {rp_db_id}")
                    fk_resolution_errors.append({"row": csv_row_number, "product_name": record_data.get('product_name'), "error": "FK resolution failed.", "missing_brand_id": not brand_db_id, "missing_policy_id": not rp_db_id})
                    db_error_count += 1
                    continue # Skip this record

                orm_data.update({k: v for k, v in record_data.items() if k in ProductOrm.__table__.columns and k not in ['brand_name', 'return_policy_code']})
                orm_data.update({"brand_id": int(brand_db_id), "return_policy_id": int(rp_db_id)})
                orm_instance = db_engine_session.query(ProductOrm).filter_by(
                    business_id=business_id, product_name=record_data['product_name']
                ).first()

            elif map_type == "product_items":
                product_name_csv = record_data.get('product_name')
                product_db_id = get_from_id_map(session_id, f"products{db_pk_map_suffix}", product_name_csv, pipeline=None)
                if not product_db_id:
                    logger.error(f"Row {csv_row_number} (Item SKU: {record_data.get('variant_sku')}): Could not resolve product_db_id for product '{product_name_csv}'.")
                    fk_resolution_errors.append({"row": csv_row_number, "variant_sku": record_data.get('variant_sku'), "error": "Product FK resolution failed."})
                    db_error_count += 1
                    continue
                orm_data.update({k: v for k, v in record_data.items() if k in ProductItemOrm.__table__.columns and k != 'product_name'})
                orm_data["product_id"] = int(product_db_id)
                orm_instance = db_engine_session.query(ProductItemOrm).filter_by(
                    business_id=business_id, variant_sku=record_data['variant_sku']
                ).first()

            elif map_type == "product_prices":
                product_name_csv = record_data.get('product_name')
                product_db_id = get_from_id_map(session_id, f"products{db_pk_map_suffix}", product_name_csv, pipeline=None)
                if not product_db_id:
                    logger.error(f"Row {csv_row_number} (Price for Product: {product_name_csv}): Could not resolve product_db_id.")
                    fk_resolution_errors.append({"row": csv_row_number, "product_name": product_name_csv, "error": "Product FK for price resolution failed."})
                    db_error_count += 1
                    continue
                orm_data.update({k: v for k, v in record_data.items() if k in ProductPriceOrm.__table__.columns and k != 'product_name'})
                orm_data["product_id"] = int(product_db_id)
                # Assuming one price per product, so product_id is unique key for lookup here
                orm_instance = db_engine_session.query(ProductPriceOrm).filter_by(
                    business_id=business_id, product_id=int(product_db_id)
                ).first()

            elif map_type == "meta_tags":
                product_name_csv = record_data.get('product_name')
                product_db_id = get_from_id_map(session_id, f"products{db_pk_map_suffix}", product_name_csv, pipeline=None)
                if not product_db_id:
                    logger.error(f"Row {csv_row_number} (Meta for Product: {product_name_csv}): Could not resolve product_db_id.")
                    fk_resolution_errors.append({"row": csv_row_number, "product_name": product_name_csv, "error": "Product FK for meta tags resolution failed."})
                    db_error_count += 1
                    continue
                orm_data.update({k: v for k, v in record_data.items() if k in MetaTagOrm.__table__.columns and k != 'product_name'})
                orm_data["product_id"] = int(product_db_id)
                orm_instance = db_engine_session.query(MetaTagOrm).filter_by(
                    business_id=business_id, product_id=int(product_db_id)
                ).first()

            # Create or Update
            if orm_instance: # Update existing
                for key, value in orm_data.items():
                    setattr(orm_instance, key, value)
                db_pk = orm_instance.id # Already has an ID
            else: # Create new
                # Dynamically select ORM class - this could be cleaner with a map
                orm_class_map = {
                    "brands": BrandOrm, "attributes": AttributeOrm, "return_policies": ReturnPolicyOrm,
                    "products": ProductOrm, "product_items": ProductItemOrm,
                    "product_prices": ProductPriceOrm, "meta_tags": MetaTagOrm
                }
                OrmClass = orm_class_map.get(map_type)
                if OrmClass:
                    orm_instance = OrmClass(**orm_data)
                    db_engine_session.add(orm_instance)
                    try:
                        db_engine_session.flush() # Flush to get the new ID for db_pk mapping
                        db_pk = orm_instance.id
                    except Exception as flush_exc: # Catch IntegrityError, etc.
                        logger.error(f"Row {csv_row_number}: DB flush error for {csv_unique_key_value}: {flush_exc}")
                        db_engine_session.rollback() # Rollback this specific flush
                        fk_resolution_errors.append({"row": csv_row_number, "key": csv_unique_key_value, "error": f"DB flush error: {str(flush_exc)}"})
                        db_error_count +=1
                        continue # Skip to next record
                else:
                    logger.error(f"Row {csv_row_number}: Unknown map_type '{map_type}' for ORM class mapping. Skipping.")
                    fk_resolution_errors.append({"row": csv_row_number, "error": f"Unknown map_type '{map_type}'."})
                    db_error_count += 1
                    continue

            if db_pk is not None and db_pk_redis_pipeline:
                # Store CSV key -> DB PK mapping
                add_to_id_map(session_id, f"{map_type}{db_pk_map_suffix}", csv_unique_key_value, db_pk, pipeline=db_pk_redis_pipeline)

            # Original string-to-string ID mapping (if still used by some part of validation or cross-referencing)
            if string_id_redis_pipeline:
                generated_id = f"{id_prefix}:{csv_unique_key_value.lower().replace(' ', '_')}"
                add_to_id_map(session_id, map_type, csv_unique_key_value, generated_id, pipeline=string_id_redis_pipeline)

            processed_count += 1
        # --- End Database Interaction Loop ---

        if db_error_count > 0:
            logger.warning(f"Session {session_id}: Encountered {db_error_count} errors during DB processing for {map_type}. Rolling back DB changes.")
            if db_engine_session: db_engine_session.rollback()
            _update_session_status(session_id, business_id, status="db_processing_failed",
                                   details=f"DB processing failed for {db_error_count} records. Errors: {str(fk_resolution_errors)}",
                                   error_count=db_error_count + len(validation_errors))
            return {"status": "db_error", "message": f"DB processing failed for {db_error_count} records.", "errors": fk_resolution_errors, "processed_db_count": 0, "session_id": session_id}
        else:
            if db_engine_session: db_engine_session.commit()
            if string_id_redis_pipeline : string_id_redis_pipeline.execute() # Execute original string ID map pipeline
            if db_pk_redis_pipeline: db_pk_redis_pipeline.execute() # Execute DB PK map pipeline

            # Set TTL for the original string ID map key (if pipeline was used)
            if string_id_redis_pipeline and redis_client:
                key_to_expire = f"{get_id_map_key(session_id)}:{map_type}"
                logger.info(f"Setting TTL for Redis key: {key_to_expire} to {REDIS_SESSION_TTL_SECONDS} seconds.")
                try:
                    ttl_pipeline_string_ids = redis_client.pipeline()
                    ttl_pipeline_string_ids.expire(key_to_expire, time=REDIS_SESSION_TTL_SECONDS)
                    ttl_pipeline_string_ids.execute()
                except Exception as ttl_error:
                    logger.error(f"Failed to set TTL for string ID key {key_to_expire}: {ttl_error}", exc_info=True)

            # Set TTL for the DB PK map key (if pipeline was used)
            if db_pk_redis_pipeline and redis_client:
                key_to_expire_db_pk = f"{get_id_map_key(session_id)}:{map_type}{db_pk_map_suffix}"
                logger.info(f"Setting TTL for Redis DB PK key: {key_to_expire_db_pk} to {REDIS_SESSION_TTL_SECONDS} seconds.")
                try:
                    ttl_pipeline_db_pk = redis_client.pipeline()
                    ttl_pipeline_db_pk.expire(key_to_expire_db_pk, time=REDIS_SESSION_TTL_SECONDS)
                    ttl_pipeline_db_pk.execute()
                except Exception as ttl_error:
                    logger.error(f"Failed to set TTL for DB PK key {key_to_expire_db_pk}: {ttl_error}", exc_info=True)


        # Cleanup and final status update
        if db_engine_session: # Ensure it's closed if it was opened
            pass # Will be closed in finally block

        # 5. Wasabi Cleanup (moved after successful DB commit and Redis operations)
        # Wasabi Cleanup should happen only if everything else (DB, Redis for this map_type) is successful
        try:
            logger.info(f"Attempting to delete {wasabi_file_path} from Wasabi bucket {WASABI_BUCKET_NAME}.")
            wasabi_client.delete_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            logger.info(f"Successfully deleted {wasabi_file_path} from Wasabi.")
        except Exception as cleanup_error:
            logger.error(f"Failed to delete {wasabi_file_path} from Wasabi: {cleanup_error}", exc_info=True)
            # Non-critical, but good to note. Maybe update session details with this warning.
            _update_session_status(session_id, business_id, status="completed_with_cleanup_warning",
                                   details=f"File processed and data saved. Wasabi cleanup failed: {str(cleanup_error)}",
                                   record_count=processed_count,
                                   error_count=db_error_count + len(validation_errors) # Total errors
                                  )
            return {"status": "success_with_cleanup_warning", "processed_db_count": processed_count, "session_id": session_id, "message": f"Wasabi cleanup failed: {str(cleanup_error)}"}

        _update_session_status(session_id, business_id, status="completed",
                               record_count=processed_count,
                               error_count=db_error_count + len(validation_errors) # Total errors
                              )
        logger.info(f"Successfully processed and saved {processed_count} records for {map_type} in session {session_id}.")
        return {"status": "success", "processed_db_count": processed_count, "session_id": session_id}

    except Exception as e:
        logger.error(f"Major error processing {map_type} for session {session_id}, file {original_filename}: {e}", exc_info=True)
        if db_engine_session: db_engine_session.rollback()
        _update_session_status(session_id, business_id, status="failed", details=f"Major processing error: {str(e)}")
        return {"status": "error", "message": str(e), "session_id": session_id}
    finally:
        if db_engine_session:
            db_engine_session.close()

# Specific loaders (ensure they pass all necessary params if process_csv_task signature changes)
# These should ideally pass the 'record_key' specific to their map_type for CSV key -> DB PK mapping.
# For example, for 'brands', record_key is 'brand_name'. For 'products', it's 'product_name'.

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

@shared_task(name="process_categories_file")
def process_categories_file(business_id: int, session_id: str, wasabi_file_path: str, original_filename: str): # business_id type hint updated
    """
    Celery task to process a CSV file containing category data.
    It calls the generic process_csv_task with parameters specific to categories.
    """
    logger.info(f"Category file processing task started for session: {session_id}, file: {original_filename}, business_id: {business_id}")
    return process_csv_task(
        business_id=business_id,
        session_id=session_id,
        wasabi_file_path=wasabi_file_path,
        original_filename=original_filename,
        record_key="category_path",  # The unique key field in the Category CSV (e.g., "L1/L2/L3")
        id_prefix="cat",             # Prefix for any generated string IDs (if still used)
        map_type="categories"        # The type of entity being processed, matches Pydantic model and ORM map
    )


# This map is imported by other modules (e.g., graphql_mutations) to dispatch tasks.
CELERY_TASK_MAP = {
    "brands": process_brands_file,
    "attributes": process_attributes_file,
    "return_policies": process_return_policies_file,
    "products": process_products_file,
    "product_items": process_product_items_file,
    "product_prices": process_product_prices_file,
    "meta_tags": process_meta_tags_file,
    "categories": process_categories_file,  # New entry
}
