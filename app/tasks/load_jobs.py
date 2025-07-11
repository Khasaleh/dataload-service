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
from app.dataload.product_loader import load_products_to_db
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
from app.dataload.item_loader import load_items_to_db # Added for item/variant loading
# from app.dataload.product_loader import load_product_record_to_db # Unused and causes ImportError
from app.dataload.meta_tags_loader import load_meta_tags_from_csv
from app.models import UploadJobStatus, ErrorDetailModel, ErrorType
import csv
from app.dataload.models.product_csv import ProductCsvModel
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
            # load_price_to_db might return detailed errors in summary["errors_list"]
            if "errors_list" in summary and summary["errors_list"]:
                row_errors.extend(summary["errors_list"])


        elif map_type == "products":
            # Products are processed as a batch by load_products_to_db
            from app.dataload.product_loader import load_products_to_db
            # Assuming db_pk_redis_pipeline is not strictly needed here or handled by default None
            product_summary = load_products_to_db(data_db, int(business_id), validated, session_id, None, user_id)
            processed = product_summary.get("inserted", 0) + product_summary.get("updated", 0)
            # load_products_to_db currently returns summary["errors"] as a count.
            # If it were to return detailed ErrorDetailModel list, we'd append them to row_errors.
            # For now, this error count will be part of the final _update_session_status.
            # To make it consistent with other row_errors, we'd need load_products_to_db to collect ErrorDetailModel.
            # Let's assume for now that if product_summary["errors"] > 0, we create a generic error detail.
            if product_summary.get("errors", 0) > 0:
                 pass # The summary["errors"] from load_products_to_db will be used in final_status update
        
        elif map_type == "product_items": # New handler for item/variants
            item_summary = load_items_to_db(
                data_db, int(business_id), validated, session_id, user_id
            )
            # load_items_to_db returns: 
            # {"csv_rows_processed": count, "csv_rows_with_errors": count, "total_main_skus_created_or_updated": count}
            # 'processed' for this task usually means successfully processed input records (CSV rows).
            # For items, one CSV row can result in many SKUs.
            # We'll use csv_rows_processed - csv_rows_with_errors for the 'processed' count here.
            processed = item_summary.get("csv_rows_processed", 0) - item_summary.get("csv_rows_with_errors", 0)
            # The detailed errors are logged within load_items_to_db.
            # If load_items_to_db were to return a list of ErrorDetailModel, we would append to row_errors.
            # For now, item_summary["csv_rows_with_errors"] will be used for the final error count.

        else: # Handles attributes, meta_tags, categories (record by record)
            for idx, rec in enumerate(validated, start=2):
                try:
                    if map_type == "attributes":
                        load_attribute_to_db(data_db, int(business_id), rec, session_id, None, user_id)
                    # Removed "products" from here
                    elif map_type == "meta_tags":
                        load_meta_tags_from_csv(data_db, int(business_id), rec, session_id, None)
                    elif map_type == "categories":
                        load_category_to_db(data_db, int(business_id), rec, session_id, None, user_id)
                    else:
                        logger.warning(f"Unhandled map_type '{map_type}' in per-record processing loop.")
                        # Add a generic error if an unhandled map_type reaches here, though caught by initial checks usually.
                        row_errors.append(ErrorDetailModel(row_number=idx, error_message=f"Unhandled map_type: {map_type}", error_type=ErrorType.UNEXPECTED_ROW_ERROR))
                        continue # Skip processed += 1 for this iteration
                    processed += 1
                except Exception as e: # Catches errors from load_attribute_to_db, etc.
                    # Attempt to get offending value if DataLoaderError
                    offending_val = None
                    field_name = None
                    if hasattr(e, 'offending_value'):
                        offending_val = str(e.offending_value)
                    if hasattr(e, 'field_name'):
                        field_name = str(e.field_name)
                    
                    row_errors.append(
                        ErrorDetailModel(
                            row_number=idx,
                            field_name=field_name,
                            error_message=str(e),
                            error_type=getattr(e, 'error_type', ErrorType.UNEXPECTED_ROW_ERROR),
                            offending_value=offending_val
                        )
                    )

        # PHASE 6: COMMIT BOTH DBs
        # The final error count for _update_session_status will include errors from product_summary
        # This commit happens *after* all processing for the given map_type batch
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
    final_error_count = len(row_errors) # Default from per-record processing (attributes, categories, etc.)
    
    if map_type == "products" and 'product_summary' in locals(): 
        final_error_count = product_summary.get("errors", 0)
        if final_error_count > 0 and not row_errors: # If products loader had errors but no detailed row_errors collected here
            row_errors.append(ErrorDetailModel(
                row_number=None, 
                error_message=f"{final_error_count} error(s) occurred during product batch processing.",
                error_type=ErrorType.BATCH_PROCESSING_ERROR # General error type for batch
            ))
    elif map_type == "product_items" and 'item_summary' in locals(): # Handle item_summary
        final_error_count = item_summary.get("csv_rows_with_errors", 0)
        if final_error_count > 0 and not row_errors: # If item_loader had errors but no detailed row_errors collected here
            # Note: load_items_to_db currently logs row errors but doesn't return them as ErrorDetailModel list.
            # This generic message indicates that some rows failed as per the summary.
            row_errors.append(ErrorDetailModel(
                row_number=None, 
                error_message=f"{final_error_count} CSV row(s) failed during item/variant processing.",
                error_type=ErrorType.BATCH_PROCESSING_ERROR 
            ))
    elif map_type == "product_prices" and "summary" in locals() and "errors_list" in summary:
        # product_prices already populates row_errors if summary["errors_list"] exists
        # final_error_count is already len(row_errors) in this case.
        pass


    _update_session_status(
        meta_db,
        session_id,
        final_status,
        details=[e.model_dump() for e in row_errors] if row_errors else None,
        record_count=len(validated),
        error_count=final_error_count, # Use the potentially adjusted error count
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
