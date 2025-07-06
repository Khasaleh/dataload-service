import os
import logging
import json
from datetime import datetime
from celery import shared_task
from sqlalchemy.exc import (
    OperationalError as SQLAlchemyOperationalError,
    TimeoutError as SQLAlchemyTimeoutError,
)
from botocore.exceptions import (
    EndpointConnectionError as BotoEndpointConnectionError,
    ReadTimeoutError as BotoReadTimeoutError,
)
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    BusyLoadingError as RedisBusyLoadingError,
)

from app.core.config import settings
from app.db.connection import get_session
from app.db.models import UploadSessionOrm
from app.utils.redis_utils import get_from_id_map, redis_client_instance
from app.services.validator import validate_csv
from app.services.db_loaders import (
    load_brand_to_db,
    load_attribute_to_db,
    load_return_policy_to_db,
    load_category_to_db,
    load_price_to_db,
)
from app.dataload.product_loader import load_product_record_to_db
from app.dataload.meta_tags_loader import load_meta_tags_from_csv
from app.models import UploadJobStatus, ErrorDetailModel, ErrorType
import csv

logger = logging.getLogger(__name__)
STORAGE_ROOT = settings.LOCAL_STORAGE_PATH

RETRYABLE_EXCEPTIONS = (
    SQLAlchemyOperationalError,
    SQLAlchemyTimeoutError,
    BotoEndpointConnectionError,
    BotoReadTimeoutError,
    RedisConnectionError,
    RedisTimeoutError,
    RedisBusyLoadingError,
)
COMMON_RETRY_KWARGS = {"max_retries": 3, "default_retry_delay": 60}


def _update_session_status(
    db,
    session_id: str,
    status: UploadJobStatus,
    details=None,
    record_count=None,
    error_count=None,
):
    sess = db.query(UploadSessionOrm).filter_by(session_id=session_id).first()
    if not sess:
        logger.error("Session %s not found for status update", session_id)
        return
    sess.status = status.value
    sess.updated_at = datetime.utcnow()

    if details is not None:
        normalized = []
        for d in details:
            normalized.append(d.model_dump() if hasattr(d, "model_dump") else d)
        sess.details = json.dumps(normalized)

    if record_count is not None:
        sess.record_count = record_count
    if error_count is not None:
        sess.error_count = error_count

    db.commit()


def process_csv_task(
    business_id: str,
    session_id: str,
    wasabi_file_path: str,
    original_filename: str,
    record_key: str,
    id_prefix: str,
    map_type: str,
    user_id: int,
    db_key: str | None = None,
):
    """
    Generic CSV processing pipeline.
    If db_key is provided, get_session will pick the alternate DB URL.
    """
    # 1) Always open a “meta” session on the default DB for upload_sessions updates
    meta_db = get_session(business_id=int(business_id), db_key=None)
    # 2) If requested, open a second “data” session on DB2; else reuse meta_db
    data_db = get_session(business_id=int(business_id), db_key=db_key) if db_key else meta_db

    _update_session_status(meta_db, session_id, UploadJobStatus.DOWNLOADING_FILE)

    abs_path = os.path.join(STORAGE_ROOT, business_id, wasabi_file_path)
    with open(abs_path, newline="", encoding="utf-8") as f:
        original_records = list(csv.DictReader(f))
    if not original_records:
        _update_session_status(
            meta_db,
            session_id,
            UploadJobStatus.COMPLETED_EMPTY_FILE,
            record_count=0,
            error_count=0,
        )
        return

    _update_session_status(meta_db, session_id, UploadJobStatus.VALIDATING_SCHEMA)
    init_errors, validated = validate_csv(map_type, original_records, session_id)
    if init_errors:
        _update_session_status(
            meta_db,
            session_id,
            UploadJobStatus.FAILED_VALIDATION,
            details=init_errors,
            record_count=len(original_records),
            error_count=len(init_errors),
        )
        return

    _update_session_status(
        meta_db,
        session_id,
        UploadJobStatus.DB_PROCESSING_STARTED,
        record_count=len(validated),
    )

    processed = 0
    errors = []

    # route to the correct loader (writes go into data_db)
    if map_type == "brands":
        summary = load_brand_to_db(data_db, int(business_id), validated, session_id, None, user_id)
        processed = summary.get("inserted", 0) + summary.get("updated", 0)

    elif map_type == "return_policies":
        # this loader writes into DB2 (data_db), metadata remains on meta_db
        summary = load_return_policy_to_db(data_db, int(business_id), validated, session_id, None)
        processed = summary.get("inserted", 0) + summary.get("updated", 0)

    elif map_type == "product_prices":
        summary = load_price_to_db(data_db, int(business_id), validated, session_id)
        processed = summary.get("inserted", 0) + summary.get("updated", 0)

    else:
        for idx, rec in enumerate(validated, start=2):
            try:
                if map_type == "attributes":
                    load_attribute_to_db(data_db, int(business_id), rec, session_id, None)
                elif map_type == "products":
                    load_product_record_to_db(data_db, int(business_id), rec, session_id, None)
                elif map_type == "meta_tags":
                    load_meta_tags_from_csv(data_db, int(business_id), rec, session_id, None)
                elif map_type == "categories":
                    load_category_to_db(data_db, int(business_id), rec, session_id, None, user_id)
                processed += 1
            except Exception as e:
                errors.append(
                    ErrorDetailModel(
                        row_number=idx,
                        error_message=str(e),
                        error_type=ErrorType.UNEXPECTED_ROW_ERROR,
                    )
                )

    # commit both DBs if distinct
    meta_db.commit()
    if data_db is not meta_db:
        data_db.commit()

    final_status = (
        UploadJobStatus.COMPLETED
        if not errors
        else UploadJobStatus.COMPLETED_WITH_ERRORS
    )
    _update_session_status(
        meta_db,
        session_id,
        final_status,
        details=errors if errors else None,
        record_count=len(validated),
        error_count=len(errors),
    )

    try:
        os.remove(abs_path)
    except OSError:
        pass

    meta_db.close()
    if data_db is not meta_db:
        data_db.close()

    return {
        "status": final_status.value,
        "processed": processed,
        "errors": [e.model_dump() for e in errors],
    }


# -----------------------------------------------------------------------------
# Celery wrapper tasks (unchanged)
# -----------------------------------------------------------------------------

@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_brands_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "name",
        "brand",
        "brands",
        user_id,
    )


@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_return_policies_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    db_key = settings.LOADTYPE_DB_MAP.get("return_policies")
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "policy_name",
        "rp",
        "return_policies",
        user_id,
        db_key=db_key,
    )


@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_attributes_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "attribute_name",
        "attr",
        "attributes",
        user_id,
    )


@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_products_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "self_gen_product_id",
        "prod",
        "products",
        user_id,
    )


@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_product_items_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "variant_sku",
        "item",
        "product_items",
        user_id,
    )


@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_product_prices_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "product_id",
        "price",
        "product_prices",
        user_id,
    )


@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_meta_tags_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "meta_tag_key",
        "meta",
        "meta_tags",
        user_id,
    )


@shared_task(bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_categories_file(self, business_id, session_id, wasabi_file_path, original_filename, user_id):
    return process_csv_task(
        business_id,
        session_id,
        wasabi_file_path,
        original_filename,
        "category_path",
        "cat",
        "categories",
        user_id,
    )
