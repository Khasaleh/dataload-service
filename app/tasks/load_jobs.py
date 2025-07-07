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
    - meta_db: always the default DB, used only to update upload_sessions (status/details).
    - data_db: either meta_db or the alternate (DB2) if db_key="DB2", used for actual row upserts.
    Any unexpected exception in:
      • file read   → FAILED_VALIDATION
      • schema valid → FAILED_VALIDATION
      • DB load     → FAILED_PROCESSING
    will be caught, the session status updated, and both sessions closed.
    """
    # 1) “meta” session for upload_sessions
    meta_db = get_session(business_id=int(business_id), db_key=None)
    # 2) “data” session for actual writes
    data_db = get_session(business_id=int(business_id), db_key=db_key) if db_key else meta_db

    # helper to fail early
    def fail(status, detail_list, rec_count=None, err_count=None):
        _update_session_status(
            meta_db,
            session_id,
            status,
            details=detail_list,
            record_count=rec_count,
            error_count=err_count,
        )
        try:
            meta_db.rollback()
        except: pass
        if data_db is not meta_db:
            try:
                data_db.rollback()
            except: pass
        meta_db.close()
        if data_db is not meta_db:
            data_db.close()

    # PHASE 1: DOWNLOAD
    _update_session_status(meta_db, session_id, UploadJobStatus.DOWNLOADING_FILE)

    # PHASE 2: READ FILE
    abs_path = os.path.join(STORAGE_ROOT, business_id, wasabi_file_path)
    try:
        with open(abs_path, newline="", encoding="utf-8") as f:
            original_records = list(csv.DictReader(f))
    except Exception as e:
        detail = [{"row": None, "field": None, "error": f"Failed reading file: {e}"}]
        return fail(UploadJobStatus.FAILED_VALIDATION, detail, rec_count=None, err_count=1)

    if not original_records:
        _update_session_status(
            meta_db,
            session_id,
            UploadJobStatus.COMPLETED_EMPTY_FILE,
            record_count=0,
            error_count=0,
        )
        meta_db.close()
        if data_db is not meta_db:
            data_db.close()
        return

    # PHASE 3: VALIDATE SCHEMA & BUSINESS RULES
    _update_session_status(meta_db, session_id, UploadJobStatus.VALIDATING_SCHEMA)
    try:
        init_errors, validated = validate_csv(map_type, original_records, session_id)
    except Exception as e:
        detail = [{"row": None, "field": None, "error": f"Schema validator error: {type(e).__name__}: {e}"}]
        return fail(UploadJobStatus.FAILED_VALIDATION, detail, rec_count=len(original_records), err_count=1)

    if init_errors:
        return fail(
            UploadJobStatus.FAILED_VALIDATION,
            init_errors,
            rec_count=len(original_records),
            err_count=len(init_errors),
        )

    # PHASE 4: START DB PROCESSING
    _update_session_status(
        meta_db,
        session_id,
        UploadJobStatus.DB_PROCESSING_STARTED,
        record_count=len(validated),
    )

    # PHASE 5: DISPATCH TO LOADERS (and collect per-row errors)
    processed = 0
    row_errors: list[ErrorDetailModel] = []

    try:
        if map_type == "brands":
            summary = load_brand_to_db(data_db, int(business_id), validated, session_id, None, user_id)
            processed = summary.get("inserted", 0) + summary.get("updated", 0)

        elif map_type == "return_policies":
            summary = load_return_policy_to_db(data_db, int(business_id), validated, session_id)
            processed = summary.get("inserted", 0) + summary.get("updated", 0)

        elif map_type == "product_prices":
            summary = load_price_to_db(data_db, int(business_id), validated, session_id, None)
            processed = summary.get("inserted", 0) + summary.get("updated", 0)

        else:
            for idx, rec in enumerate(validated, start=2):
                try:
                    if map_type == "attributes":
                        load_attribute_to_db(data_db, int(business_id), rec, session_id, None, user_id)
                    elif map_type == "products":
                        load_product_record_to_db(data_db, int(business_id), rec, session_id, None)
                    elif map_type == "meta_tags":
                        load_meta_tags_from_csv(data_db, int(business_id), rec, session_id, None)
                    elif map_type == "categories":
                        load_category_to_db(data_db, int(business_id), rec, session_id, None, user_id)
                    processed += 1
                except Exception as e:
                    row_errors.append(
                        ErrorDetailModel(
                            row_number=idx,
                            error_message=str(e),
                            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
                        )
                    )

        # PHASE 6: COMMIT BOTH DBs
        meta_db.commit()
        if data_db is not meta_db:
            data_db.commit()

    except Exception as e:
        # Any exception in loader or commit → FAILED_PROCESSING
        detail = [{
            "row": None,
            "field": None,
            "error": f"Processing error: {type(e).__name__}: {e}"
        }]
        return fail(UploadJobStatus.FAILED_PROCESSING, detail, rec_count=len(validated), err_count=1)

    # PHASE 7: FINALIZE
    final_status = (
        UploadJobStatus.COMPLETED
        if not row_errors
        else UploadJobStatus.COMPLETED_WITH_ERRORS
    )
    _update_session_status(
        meta_db,
        session_id,
        final_status,
        details=[e.model_dump() for e in row_errors] if row_errors else None,
        record_count=len(validated),
        error_count=len(row_errors),
    )

    # CLEANUP
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
        "errors": [e.model_dump() for e in row_errors],
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
def process_return_policies_file(
    self,
    business_id: str,
    session_id: str,
    wasabi_file_path: str,
    original_filename: str,
    user_id: int
):
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
