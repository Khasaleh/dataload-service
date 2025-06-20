from celery import shared_task # Using shared_task is often preferred for app-independent tasks
from app.db.connection import get_session
from app.services.storage import client as wasabi_client # Assuming direct client usage for get_object
                                                      # Or add a get_file_content function to storage.py
import csv
import io
import redis
import os
import json # For storing complex data like lists of errors in Redis if needed
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) # Ensure logger is configured

# Initialize Redis client for temporary ID mapping
# Connection details should ideally come from environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "redis") # Default to 'redis' for Docker Compose
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB_ID_MAPPING = int(os.getenv("REDIS_DB_ID_MAPPING", 1)) # Use a separate Redis DB for this

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_ID_MAPPING, decode_responses=True)
    redis_client.ping()
    logger.info(f"Successfully connected to Redis for ID mapping: {REDIS_HOST}:{REDIS_PORT}, DB: {REDIS_DB_ID_MAPPING}")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Failed to connect to Redis for ID mapping: {e}. ID mapping will not work.")
    redis_client = None # Set to None if connection fails

WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name")

# --- Helper for ID Mapping ---
def get_id_map_key(session_id: str):
    return f"id_map:session:{session_id}"

def add_to_id_map(session_id: str, map_type: str, key: str, value: any, pipeline = None):
    if not redis_client: return
    r = pipeline if pipeline else redis_client
    r.hset(f"{get_id_map_key(session_id)}:{map_type}", key, str(value)) # Store as string

def get_from_id_map(session_id: str, map_type: str, key: str, pipeline = None):
    if not redis_client: return None
    r = pipeline if pipeline else redis_client
    return r.hget(f"{get_id_map_key(session_id)}:{map_type}", key) # Returns string or None

# --- Refactored Celery Tasks (Brands and Attributes as examples) ---

@shared_task(name="process_brands_file")
def process_brands_file(business_id: str, session_id: str, wasabi_file_path: str):
    logger.info(f"[{business_id}/{session_id}] Starting 'brands' file processing from {wasabi_file_path}")
    db_session = None # Initialize db_session to ensure it's closed in finally

    try:
        # 1. Download file from Wasabi
        try:
            response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            file_content_bytes = response['Body'].read()
            file_content_str = file_content_bytes.decode('utf-8')
            logger.info(f"[{business_id}/{session_id}] Successfully downloaded {wasabi_file_path} from Wasabi.")
        except Exception as e:
            logger.error(f"[{business_id}/{session_id}] Failed to download from Wasabi: {e}")
            # Here, update load status table to reflect failure (Task 8)
            return {"status": "error", "message": f"Failed to download from Wasabi: {str(e)}"}

        # 2. Validate CSV content (This will be enhanced in Task 6)
        # For now, just basic parsing. Real validation will come from app.services.validator
        csvfile = io.StringIO(file_content_str)
        reader = csv.DictReader(csvfile)
        records = list(reader)

        # Placeholder for calling enhanced validator.py function (Task 6)
        # errors, validated_records = await validate_brands_csv_enhanced(records, business_id, session_id)
        # if errors:
        #     logger.error(f"[{business_id}/{session_id}] Validation failed for {wasabi_file_path}: {errors}")
        #     # Store errors in load status table (Task 8)
        #     return {"status": "validation_error", "errors": errors}
        # records = validated_records # Use validated records

        if not records:
            logger.warning(f"[{business_id}/{session_id}] No records found in {wasabi_file_path}.")
            return {"status": "no_data"}

        # 3. Process records and store IDs
        db_session = get_session(business_id) # Get DB session for this business
        processed_count = 0
        redis_pipeline = redis_client.pipeline() if redis_client else None

        for record in records:
            brand_name = record.get('brand_name')
            if not brand_name:
                logger.warning(f"[{business_id}/{session_id}] Skipping record due to missing brand_name: {record}")
                continue # Or log as error

            # Simplified DB interaction for now (actual SQL from original file to be adapted)
            # This part needs to implement "get or create" logic and retrieve DB-generated ID
            # For example:
            # existing_brand_id = db_session.execute(text("SELECT brand_id FROM product_brands WHERE brand_name = :name"), {"name": brand_name}).scalar_one_or_none()
            # if existing_brand_id:
            #    brand_db_id = existing_brand_id
            # else:
            #    result = db_session.execute(text("INSERT INTO product_brands (brand_name) VALUES (:name) RETURNING brand_id"), {"name": brand_name})
            #    brand_db_id = result.scalar_one()
            #    db_session.commit() # Commit per insert or batch

            # Mocking DB ID generation for now
            brand_db_id = f"db_brand_id_{brand_name.lower().replace(' ', '_')}_{session_id[:4]}"
            logger.info(f"[{business_id}/{session_id}] Processed brand: {brand_name} -> {brand_db_id}")

            # Add to temporary ID map
            if redis_pipeline:
                add_to_id_map(session_id, "brand_name_to_id", brand_name, brand_db_id, pipeline=redis_pipeline)

            processed_count += 1

        if redis_pipeline:
            redis_pipeline.execute()
        if db_session: # only commit if operations were done
            db_session.commit()

        logger.info(f"[{business_id}/{session_id}] Successfully processed {processed_count} brand records.")
        # Update load status table to success (Task 8)
        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"[{business_id}/{session_id}] Error processing brands file {wasabi_file_path}: {e}", exc_info=True)
        if db_session:
            db_session.rollback()
        # Update load status table to reflect failure (Task 8)
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()


@shared_task(name="process_return_policies_file")
def process_return_policies_file(business_id: str, session_id: str, wasabi_file_path: str):
    logger.info(f"[{business_id}/{session_id}] Starting 'return_policies' file processing from {wasabi_file_path}")
    db_session = None
    try:
        # 1. Download file from Wasabi
        try:
            response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            file_content_bytes = response['Body'].read()
            file_content_str = file_content_bytes.decode('utf-8')
            logger.info(f"[{business_id}/{session_id}] Successfully downloaded {wasabi_file_path} from Wasabi.")
        except Exception as e:
            logger.error(f"[{business_id}/{session_id}] Failed to download return_policies file from Wasabi: {e}")
            # Update load status table (Task 8)
            return {"status": "error", "message": f"Failed to download from Wasabi: {str(e)}"}

        # 2. Validate CSV (Placeholder - to be enhanced in Task 6)
        csvfile = io.StringIO(file_content_str)
        reader = csv.DictReader(csvfile)
        records = list(reader)
        if not records:
            logger.warning(f"[{business_id}/{session_id}] No records found in {wasabi_file_path}.")
            return {"status": "no_data"}

        # Placeholder for calling enhanced validator from validator.py (Task 6)
        # errors, validated_records = await validate_return_policies_csv_enhanced(records, business_id, session_id)
        # if errors: ... store errors, return ...
        # records = validated_records

        # 3. Process records
        db_session = get_session(business_id)
        processed_count = 0
        redis_pipeline = redis_client.pipeline() if redis_client else None

        for record in records:
            policy_code = record.get('return_policy_code')
            if not policy_code:
                logger.warning(f"[{business_id}/{session_id}] Skipping record due to missing return_policy_code: {record}")
                continue

            # Mocking DB ID generation for now (Task 5 will make this real)
            policy_db_id = f"db_rp_id_{policy_code.lower().replace(' ', '_')}_{session_id[:4]}"
            logger.info(f"[{business_id}/{session_id}] Processed return_policy: {policy_code} -> {policy_db_id}")

            if redis_pipeline:
                add_to_id_map(session_id, "return_policy_code_to_id", policy_code, policy_db_id, pipeline=redis_pipeline)
            processed_count += 1

        if redis_pipeline:
            redis_pipeline.execute()
        if db_session: # only commit if operations were done
            db_session.commit()

        logger.info(f"[{business_id}/{session_id}] Successfully processed {processed_count} return policy records.")
        # Update load status table (Task 8)
        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"[{business_id}/{session_id}] Error processing return_policies file {wasabi_file_path}: {e}", exc_info=True)
        if db_session:
            db_session.rollback()
        # Update load status table (Task 8)
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()

@shared_task(name="process_products_file")
def process_products_file(business_id: str, session_id: str, wasabi_file_path: str):
    logger.info(f"[{business_id}/{session_id}] Starting 'products' file processing from {wasabi_file_path}")
    db_session = None
    try:
        # 1. Download file from Wasabi
        try:
            response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            file_content_bytes = response['Body'].read()
            file_content_str = file_content_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"[{business_id}/{session_id}] Failed to download products file from Wasabi: {e}")
            return {"status": "error", "message": f"Failed to download from Wasabi: {str(e)}"}

        # 2. Validate CSV (Placeholder - Task 6)
        csvfile = io.StringIO(file_content_str)
        reader = csv.DictReader(csvfile)
        records = list(reader)
        if not records:
            return {"status": "no_data"}

        # 3. Process records
        db_session = get_session(business_id)
        processed_count = 0
        redis_pipeline = redis_client.pipeline() if redis_client else None

        for record in records:
            product_name = record.get('product_name')
            if not product_name:
                logger.warning(f"[{business_id}/{session_id}] Skipping product due to missing product_name: {record}")
                continue

            # In Task 5, this is where we'd resolve brand_name, category_path, return_policy_code
            # using get_from_id_map(session_id, "brand_name_to_id", record.get("brand_name")) etc.
            # For now, we are just focusing on storing the product's own ID.

            # Mocking DB ID generation (Task 5 will make this real)
            product_db_id = f"db_prod_id_{product_name.lower().replace(' ', '_')}_{session_id[:4]}"
            logger.info(f"[{business_id}/{session_id}] Processed product: {product_name} -> {product_db_id}")

            # Add product's own mapping (e.g., product_name to its new DB ID)
            if redis_pipeline:
                # Using product_name as key, could also be product_url if that's the main unique human key
                add_to_id_map(session_id, "product_name_to_id", product_name, product_db_id, pipeline=redis_pipeline)
            processed_count += 1

        if redis_pipeline:
            redis_pipeline.execute()
        if db_session:
            db_session.commit()

        logger.info(f"[{business_id}/{session_id}] Successfully processed {processed_count} product records.")
        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"[{business_id}/{session_id}] Error processing products file {wasabi_file_path}: {e}", exc_info=True)
        if db_session:
            db_session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()

@shared_task(name="process_attributes_file")
def process_attributes_file(business_id: str, session_id: str, wasabi_file_path: str):
    logger.info(f"[{business_id}/{session_id}] Starting 'attributes' file processing from {wasabi_file_path}")
    db_session = None
    try:
        # 1. Download file from Wasabi
        try:
            response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            file_content_bytes = response['Body'].read()
            file_content_str = file_content_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"[{business_id}/{session_id}] Failed to download attributes file from Wasabi: {e}")
            return {"status": "error", "message": f"Failed to download from Wasabi: {str(e)}"}

        # 2. Validate CSV (placeholder)
        csvfile = io.StringIO(file_content_str)
        reader = csv.DictReader(csvfile)
        records = list(reader)
        if not records:
            return {"status": "no_data"}

        # 3. Process records
        db_session = get_session(business_id)
        processed_count = 0
        redis_pipeline = redis_client.pipeline() if redis_client else None

        for record in records:
            attribute_name = record.get('attribute_name')
            # allowed_values = record.get('allowed_values') # Will be used
            if not attribute_name:
                continue

            # Mocking DB ID generation
            attr_db_id = f"db_attr_id_{attribute_name.lower().replace(' ', '_')}_{session_id[:4]}"
            logger.info(f"[{business_id}/{session_id}] Processed attribute: {attribute_name} -> {attr_db_id}")

            if redis_pipeline:
                add_to_id_map(session_id, "attribute_name_to_id", attribute_name, attr_db_id, pipeline=redis_pipeline)
            processed_count += 1

        if redis_pipeline:
            redis_pipeline.execute()
        if db_session:
             db_session.commit()

        logger.info(f"[{business_id}/{session_id}] Successfully processed {processed_count} attribute records.")
        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"[{business_id}/{session_id}] Error processing attributes file {wasabi_file_path}: {e}", exc_info=True)
        if db_session:
            db_session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()

@shared_task(name="process_product_items_file")
def process_product_items_file(business_id: str, session_id: str, wasabi_file_path: str):
    logger.info(f"[{business_id}/{session_id}] Starting 'product_items' file processing from {wasabi_file_path}")
    db_session = None
    try:
        # 1. Download file from Wasabi
        try:
            response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            file_content_bytes = response['Body'].read()
            file_content_str = file_content_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"[{business_id}/{session_id}] Failed to download product_items file from Wasabi: {e}")
            return {"status": "error", "message": f"Failed to download from Wasabi: {str(e)}"}

        # 2. Validate CSV (Placeholder - Task 6)
        csvfile = io.StringIO(file_content_str)
        reader = csv.DictReader(csvfile)
        records = list(reader)
        if not records:
            return {"status": "no_data"}

        # 3. Process records
        db_session = get_session(business_id)
        processed_count = 0
        # No new IDs are typically generated from product_items themselves for mapping,
        # but they *consume* product_name_to_id mappings.
        # Actual DB insertion will use resolved product_id.

        for record in records:
            product_name = record.get('product_name')
            variant_sku = record.get('variant_sku')
            if not product_name or not variant_sku:
                logger.warning(f"[{business_id}/{session_id}] Skipping item due to missing product_name or variant_sku: {record}")
                continue

            # In Task 5, resolve product_name to product_db_id using:
            # product_db_id = get_from_id_map(session_id, "product_name_to_id", product_name)
            # if not product_db_id:
            #     logger.error(f"[{business_id}/{session_id}] Could not find product_id for product_name '{product_name}'. Skipping item '{variant_sku}'.")
            #     continue
            # Then use product_db_id in the INSERT statement for product_items.

            logger.info(f"[{business_id}/{session_id}] Processed product_item: {variant_sku} for product {product_name} (Product ID resolution in Task 5)")
            processed_count += 1

        if db_session: # only commit if actual db operations were done (will be in Task 5)
            db_session.commit()

        logger.info(f"[{business_id}/{session_id}] Successfully processed {processed_count} product_item records (structure pass).")
        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"[{business_id}/{session_id}] Error processing product_items file {wasabi_file_path}: {e}", exc_info=True)
        if db_session:
            db_session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()

@shared_task(name="process_product_prices_file")
def process_product_prices_file(business_id: str, session_id: str, wasabi_file_path: str):
    logger.info(f"[{business_id}/{session_id}] Starting 'product_prices' file processing from {wasabi_file_path}")
    db_session = None
    try:
        # 1. Download file from Wasabi
        try:
            response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            file_content_bytes = response['Body'].read()
            file_content_str = file_content_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"[{business_id}/{session_id}] Failed to download product_prices file from Wasabi: {e}")
            return {"status": "error", "message": f"Failed to download from Wasabi: {str(e)}"}

        # 2. Validate CSV (Placeholder - Task 6)
        csvfile = io.StringIO(file_content_str)
        reader = csv.DictReader(csvfile)
        records = list(reader)
        if not records:
            return {"status": "no_data"}

        # 3. Process records
        db_session = get_session(business_id)
        processed_count = 0
        # Similar to product_items, prices depend on products.

        for record in records:
            product_name = record.get('product_name')
            price = record.get('price')
            if not product_name or price is None: # price can be 0, so check for None
                logger.warning(f"[{business_id}/{session_id}] Skipping price due to missing product_name or price: {record}")
                continue

            # In Task 5, resolve product_name to product_db_id.
            # product_db_id = get_from_id_map(session_id, "product_name_to_id", product_name)
            # ... error handling if not found ...

            logger.info(f"[{business_id}/{session_id}] Processed product_price for product {product_name} (Product ID resolution in Task 5)")
            processed_count += 1

        if db_session:
            db_session.commit()

        logger.info(f"[{business_id}/{session_id}] Successfully processed {processed_count} product_price records (structure pass).")
        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"[{business_id}/{session_id}] Error processing product_prices file {wasabi_file_path}: {e}", exc_info=True)
        if db_session:
            db_session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()

@shared_task(name="process_meta_tags_file")
def process_meta_tags_file(business_id: str, session_id: str, wasabi_file_path: str):
    logger.info(f"[{business_id}/{session_id}] Starting 'meta_tags' file processing from {wasabi_file_path}")
    db_session = None
    try:
        # 1. Download file from Wasabi
        try:
            response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            file_content_bytes = response['Body'].read()
            file_content_str = file_content_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"[{business_id}/{session_id}] Failed to download meta_tags file from Wasabi: {e}")
            return {"status": "error", "message": f"Failed to download from Wasabi: {str(e)}"}

        # 2. Validate CSV (Placeholder - Task 6)
        csvfile = io.StringIO(file_content_str)
        reader = csv.DictReader(csvfile)
        records = list(reader)
        if not records:
            return {"status": "no_data"}

        # 3. Process records
        db_session = get_session(business_id)
        processed_count = 0
        # Meta tags also depend on products.

        for record in records:
            product_name = record.get('product_name')
            if not product_name:
                logger.warning(f"[{business_id}/{session_id}] Skipping meta_tag due to missing product_name: {record}")
                continue

            # In Task 5, resolve product_name to product_db_id.
            # product_db_id = get_from_id_map(session_id, "product_name_to_id", product_name)
            # ... error handling if not found ...

            logger.info(f"[{business_id}/{session_id}] Processed meta_tag for product {product_name} (Product ID resolution in Task 5)")
            processed_count += 1

        if db_session:
            db_session.commit()

        logger.info(f"[{business_id}/{session_id}] Successfully processed {processed_count} meta_tag records (structure pass).")
        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"[{business_id}/{session_id}] Error processing meta_tags file {wasabi_file_path}: {e}", exc_info=True)
        if db_session:
            db_session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()
