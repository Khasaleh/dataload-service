from celery import shared_task
from app.db.connection import get_session
from app.services.storage import client as wasabi_client
import csv
import io
import redis
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB_ID_MAPPING = int(os.getenv("REDIS_DB_ID_MAPPING", 1))

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

def process_csv_task(business_id, session_id, wasabi_file_path, record_key, id_prefix, map_type):
    logger.info(f"Processing {map_type} for business: {business_id} session: {session_id} file: {wasabi_file_path}")
    db_session = None
    try:
        response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
        file_content = response['Body'].read().decode('utf-8')
        records = list(csv.DictReader(io.StringIO(file_content)))

        if not records:
            logger.warning("No records found")
            return {"status": "no_data"}

        db_session = get_session(business_id)
        redis_pipeline = redis_client.pipeline() if redis_client else None
        processed_count = 0

        for record in records:
            key_value = record.get(record_key)
            if not key_value:
                continue
            generated_id = f"{id_prefix}:{key_value.lower().replace(' ', '_')}"
            if redis_pipeline:
                add_to_id_map(session_id, map_type, key_value, generated_id, redis_pipeline)
            processed_count += 1

        if redis_pipeline:
            redis_pipeline.execute()
        if db_session:
            db_session.commit()

        return {"status": "success", "processed_count": processed_count}

    except Exception as e:
        logger.error(f"Error processing {map_type}: {e}")
        if db_session:
            db_session.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if db_session:
            db_session.close()

# Specific loaders

@shared_task(name="process_brands_file")
def process_brands_file(business_id: str, session_id: str, wasabi_file_path: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, "brand_name", "brand", "brand")

@shared_task(name="process_attributes_file")
def process_attributes_file(business_id: str, session_id: str, wasabi_file_path: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, "attribute_name", "attribute", "attribute")

@shared_task(name="process_return_policies_file")
def process_return_policies_file(business_id: str, session_id: str, wasabi_file_path: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, "return_policy_code", "policy", "return_policy")

@shared_task(name="process_products_file")
def process_products_file(business_id: str, session_id: str, wasabi_file_path: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, "product_name", "product", "product")

@shared_task(name="process_product_items_file")
def process_product_items_file(business_id: str, session_id: str, wasabi_file_path: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, "variant_sku", "item", "product_item")

@shared_task(name="process_product_prices_file")
def process_product_prices_file(business_id: str, session_id: str, wasabi_file_path: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, "product_name", "price", "product_price")

@shared_task(name="process_meta_tags_file")
def process_meta_tags_file(business_id: str, session_id: str, wasabi_file_path: str):
    return process_csv_task(business_id, session_id, wasabi_file_path, "product_name", "meta", "meta_tag")
