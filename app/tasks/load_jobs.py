import csv
import io
import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Optional, Dict, Any, List

from celery import shared_task
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError, TimeoutError as SQLAlchemyTimeoutError
from botocore.exceptions import EndpointConnectionError as BotoEndpointConnectionError, ReadTimeoutError as BotoReadTimeoutError
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError, BusyLoadingError as RedisBusyLoadingError
from pydantic import ValidationError

from app.db.connection import get_session
from app.db.models import UploadSessionOrm
from app.models import UploadJobStatus, ErrorDetailModel, ErrorType
from app.services.storage import client as wasabi_client
from app.services.validator import validate_csv
from app.services.db_loaders import (
    load_category_to_db, load_brand_to_db, load_attribute_to_db,
    load_return_policy_to_db, load_price_to_db
)
from app.dataload.product_loader import load_product_record_to_db
from app.dataload.models.product_csv import ProductCsvModel
from app.dataload.meta_tags_loader import load_meta_tags_from_csv, DataloadSummary
from app.utils.redis_utils import (
    redis_client_instance as redis_client,
    add_to_id_map,
    DB_PK_MAP_SUFFIX,
    set_id_map_ttl,
    get_redis_pipeline
)

logger = logging.getLogger(__name__)

WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name-set-in-env")

RETRYABLE_EXCEPTIONS = (
    SQLAlchemyOperationalError, SQLAlchemyTimeoutError,
    BotoEndpointConnectionError, BotoReadTimeoutError,
    RedisConnectionError, RedisTimeoutError, RedisBusyLoadingError
)

COMMON_RETRY_KWARGS = {
    "max_retries": 3,
    "default_retry_delay": 60,
}

# ----------------------------
# TASK REGISTRATION MAP
# ----------------------------

# Populated *below* after task functions are defined
CELERY_TASK_MAP: Dict[str, Any] = {}


# ----------------------------
# TASK HELPERS
# ----------------------------

def _update_session_status(session_id: str, business_id_str: str, status: UploadJobStatus,
                           details: Optional[List[ErrorDetailModel]] = None,
                           record_count: Optional[int] = None, error_count: Optional[int] = None):
    status_value = status.value
    logger.info(f"Updating session {session_id} for business {business_id_str} to status '{status_value}'.")
    db = None
    try:
        db = get_session(business_id=int(business_id_str))
        session_record = db.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == session_id).first()
        if session_record:
            session_record.status = status_value
            session_record.updated_at = datetime.utcnow()
            if details is not None:
                error_details_json = json.dumps([err.model_dump(mode='json') for err in details])
                if len(error_details_json) > 4000:
                    error_details_json = error_details_json[:3972] + "... TRUNCATED"
                session_record.details = error_details_json
            elif status.is_failure() and not session_record.details:
                session_record.details = json.dumps([])
            if record_count is not None:
                session_record.record_count = record_count
            if error_count is not None:
                session_record.error_count = error_count
            db.commit()
            logger.info(f"Session {session_id} updated to status '{status_value}'.")
        else:
            logger.error(f"Session {session_id} not found for business {business_id_str}.")
    except Exception as e:
        logger.error(f"DB Error updating session {session_id}: {e}", exc_info=True)
        if db:
            db.rollback()
    finally:
        if db:
            db.close()


# ----------------------------
# CELERY WRAPPER TASKS
# ----------------------------

@shared_task(bind=True, name="process_brands_file", autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_brands_file(self, business_id, session_id, wasabi_file_path, original_filename):
    return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "name", "brand", "brands")


@shared_task(bind=True, name="process_attributes_file", autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_attributes_file(self, business_id, session_id, wasabi_file_path, original_filename):
    return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "attribute_name", "attr", "attributes")


@shared_task(bind=True, name="process_return_policies_file", autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_return_policies_file(self, business_id, session_id, wasabi_file_path, original_filename):
    return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "policy_name", "rp", "return_policies")


@shared_task(bind=True, name="process_products_file", autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_products_file(self, business_id, session_id, wasabi_file_path, original_filename):
    return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "self_gen_product_id", "prod", "products")


@shared_task(bind=True, name="process_product_items_file", autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_product_items_file(self, business_id, session_id, wasabi_file_path, original_filename):
    return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "variant_sku", "item", "product_items")


@shared_task(bind=True, name="process_product_prices_file", autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_product_prices_file(self, business_id, session_id, wasabi_file_path, original_filename):
    return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "product_id", "price", "product_prices")


@shared_task(bind=True, name="process_meta_tags_file", autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_meta_tags_file(self, business_id, session_id, wasabi_file_path, original_filename):
    return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "meta_tag_key", "meta", "meta_tags")


# ----------------------------
# TASK MAP
# ----------------------------

CELERY_TASK_MAP.update({
    "brands": process_brands_file,
    "attributes": process_attributes_file,
    "return_policies": process_return_policies_file,
    "products": process_products_file,
    "product_items": process_product_items_file,
    "product_prices": process_product_prices_file,
    "meta_tags": process_meta_tags_file,
})
