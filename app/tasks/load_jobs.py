from celery import shared_task
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError, TimeoutError as SQLAlchemyTimeoutError
from botocore.exceptions import EndpointConnectionError as BotoEndpointConnectionError, ReadTimeoutError as BotoReadTimeoutError
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError, BusyLoadingError as RedisBusyLoadingError

from app.db.connection import get_session
from app.db.models import (
    BrandOrm, AttributeOrm, ReturnPolicyOrm, ProductOrm,
    ProductItemOrm, ProductPriceOrm, MetaTagOrm, UploadSessionOrm,
    CategoryOrm, AttributeValueOrm
)
from datetime import datetime
from app.services.storage import client as wasabi_client
from app.services.validator import validate_csv
from typing import Optional, Dict, Any, List
import csv
import io
import os
import logging
from pydantic import ValidationError

from app.utils.redis_utils import (
    redis_client_instance as redis_client,
    add_to_id_map,
    get_from_id_map,
    DB_PK_MAP_SUFFIX,
    set_id_map_ttl,
    get_redis_pipeline
)
from app.services.db_loaders import (
    load_category_to_db, load_brand_to_db, load_attribute_to_db,
    load_return_policy_to_db, load_price_to_db
)
from app.dataload.product_loader import load_product_record_to_db
from app.dataload.models.product_csv import ProductCsvModel
from app.models import UploadJobStatus, ErrorDetailModel, ErrorType
from app.exceptions import DataLoaderError
import json
import tempfile
from app.dataload.meta_tags_loader import load_meta_tags_from_csv, DataloadSummary


logger = logging.getLogger(__name__)
WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name-set-in-env")

RETRYABLE_EXCEPTIONS = (
    SQLAlchemyOperationalError, SQLAlchemyTimeoutError,
    BotoEndpointConnectionError, BotoReadTimeoutError, # General S3/Wasabi connection issues
    RedisConnectionError, RedisTimeoutError, RedisBusyLoadingError
    # Add other specific, transient exceptions as identified
)

COMMON_RETRY_KWARGS = {
    "max_retries": 3,
    "default_retry_delay": 60, # seconds; Celery's default is 180s.
    # For exponential backoff, one might use:
    # "countdown": 60, # Initial countdown for first retry
    # "retry_backoff": True,
    # "retry_backoff_max": 600, # Max countdown
    # "retry_jitter": True
}

def _update_session_status(
    session_id: str, business_id_str: str, status: UploadJobStatus,
    details: Optional[List[ErrorDetailModel]] = None,
    record_count: Optional[int] = None, error_count: Optional[int] = None
):
    status_value = status.value
    logger.info(f"Attempting to update session {session_id} for business {business_id_str} to status '{status_value}'.")
    db = None
    try:
        # Ensure business_id type matches what get_session expects (likely int)
        db = get_session(business_id=int(business_id_str))
        session_record = db.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == session_id).first()
        if session_record:
            session_record.status = status_value
            session_record.updated_at = datetime.utcnow()
            if details is not None:
                error_details_json = json.dumps([err.model_dump(mode='json') for err in details])
                max_details_len = 4000
                if len(error_details_json) > max_details_len:
                    error_details_json = error_details_json[:max_details_len - len('... TRUNCATED') ] + "... TRUNCATED"
                session_record.details = error_details_json
            elif status.is_failure() and not session_record.details:
                 session_record.details = json.dumps([])
            if record_count is not None: session_record.record_count = record_count
            if error_count is not None: session_record.error_count = error_count
            db.commit()
            logger.info(f"Successfully updated session {session_id} to status '{status_value}'.")
        else:
            logger.error(f"Upload session {session_id} not found in DB for status update for business {business_id_str}.")
    except Exception as e:
        logger.error(f"DB Error: Failed to update session {session_id} status to {status_value}: {e}", exc_info=True)
        if db: db.rollback()
    finally:
        if db: db.close()

def process_csv_task(business_id_str: str, session_id: str, wasabi_file_path: str, original_filename: str, record_key: str, id_prefix: str, map_type: str):
    try:
        business_id = int(business_id_str)
    except ValueError:
        msg = f"Invalid business_id format: '{business_id_str}'. Must be an integer."
        logger.error(msg)
        # Not calling _update_session_status here as this is a fundamental arg issue before task logic.
        # The calling task's final error handler will catch this if it propagates.
        raise ValueError(msg) # Let Celery handle this as a task failure directly.

    logger.info(f"Processing {map_type} for business: {business_id} session: {session_id} file: {original_filename} ({wasabi_file_path})")
    _update_session_status(session_id, str(business_id), status=UploadJobStatus.DOWNLOADING_FILE)

    db_engine_session = None
    initial_validation_errors: List[ErrorDetailModel] = []
    try:
        response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
        file_content = response['Body'].read().decode('utf-8')
        logger.info(f"File {original_filename} downloaded from Wasabi for session {session_id}.")
        original_records = list(csv.DictReader(io.StringIO(file_content)))

        if not original_records:
            logger.warning(f"No records found in file: {wasabi_file_path}")
            _update_session_status(session_id, str(business_id), status=UploadJobStatus.COMPLETED_EMPTY_FILE, record_count=0, error_count=0)
            return {"status": UploadJobStatus.COMPLETED_EMPTY_FILE.value, "message": "No records found in the CSV file.", "session_id": session_id}

        db_pk_map_suffix = "_db_pk"
        referenced_entity_map_for_validation = {}
        if map_type == "products":
            referenced_entity_map_for_validation = {'brand_name': f"brands{db_pk_map_suffix}", 'return_policy_code': f"return_policies{db_pk_map_suffix}"}
        elif map_type == "product_items":
            referenced_entity_map_for_validation = {'product_name': f"products{db_pk_map_suffix}"}

        _update_session_status(session_id, str(business_id), status=UploadJobStatus.VALIDATING_SCHEMA)
        initial_validation_errors, validated_records = validate_csv(
            load_type=map_type, records=original_records, session_id=session_id,
            record_key=record_key, referenced_entity_map=referenced_entity_map_for_validation
        )

        if initial_validation_errors:
            logger.error(f"Initial validation errors for {map_type} in session {session_id}: {initial_validation_errors}")
            _update_session_status(session_id, str(business_id), status=UploadJobStatus.FAILED_VALIDATION,
                                   details=initial_validation_errors, error_count=len(initial_validation_errors), record_count=len(original_records))
            return {"status": UploadJobStatus.FAILED_VALIDATION.value, "errors": [err.model_dump() for err in initial_validation_errors], "processed_count": 0, "session_id": session_id}

        _update_session_status(session_id, str(business_id), status=UploadJobStatus.DB_PROCESSING_STARTED, record_count=len(validated_records))
        db_engine_session = get_session(business_id=business_id)
        string_id_redis_pipeline = get_redis_pipeline()
        db_pk_redis_pipeline = get_redis_pipeline()
        processed_csv_records_count = 0; processed_db_count = 0
        db_error_details_list: List[ErrorDetailModel] = []

        if map_type in ["brands", "return_policies", "product_prices"]:
            loader_summary = {}
            if map_type == "brands": loader_summary = load_brand_to_db(db_engine_session, business_id, validated_records, session_id, db_pk_redis_pipeline)
            elif map_type == "return_policies": loader_summary = load_return_policy_to_db(db_engine_session, business_id, validated_records, session_id, db_pk_redis_pipeline)
            elif map_type == "product_prices": loader_summary = load_price_to_db(db_engine_session, business_id, validated_records, session_id, db_pk_redis_pipeline)

            processed_db_count = loader_summary.get("inserted", 0) + loader_summary.get("updated", 0)
            num_loader_errors = loader_summary.get("errors", 0)
            if num_loader_errors > 0:
                 db_error_details_list.append(ErrorDetailModel(error_message=f"{num_loader_errors} {map_type} record(s) had issues post-bulk operation. Check logs.", error_type=ErrorType.DATABASE))
            processed_csv_records_count = len(validated_records)
        else:
            for i, record_data in enumerate(validated_records):
                csv_row_number = i + 2; db_pk = None
                current_record_key_value = record_data.get(record_key, 'N/A')
                try:
                    if map_type == "categories": db_pk = load_category_to_db(db_engine_session, business_id, record_data, session_id, db_pk_redis_pipeline)
                    elif map_type == "attributes": db_pk = load_attribute_to_db(db_engine_session, business_id, record_data, session_id, db_pk_redis_pipeline)
                    elif map_type == "products":
                        record_data_for_model = {**record_data, 'business_details_id': business_id}
                        product_csv_instance = ProductCsvModel(**record_data_for_model)
                        current_record_key_value = product_csv_instance.self_gen_product_id
                        db_pk = load_product_record_to_db(db_engine_session, business_id, product_csv_instance, session_id)
                    else:
                        logger.info(f"Row {csv_row_number}: No specific DB loader for map_type: '{map_type}'. Record: {current_record_key_value}")
                        if string_id_redis_pipeline and record_key and id_prefix and current_record_key_value != 'N/A':
                            add_to_id_map(session_id, map_type, current_record_key_value, f"{id_prefix}:{str(current_record_key_value).lower().replace(' ', '_')}", pipeline=string_id_redis_pipeline)
                    if db_pk is not None: processed_db_count +=1
                except DataLoaderError as dle:
                    db_error_details_list.append(ErrorDetailModel(row_number=csv_row_number, field_name=dle.field_name, error_message=dle.message,error_type=dle.error_type, offending_value=dle.offending_value))
                except ValidationError as ve:
                    for err_dict in ve.errors(): db_error_details_list.append(ErrorDetailModel(row_number=csv_row_number, field_name=".".join(str(f) for f in err_dict['loc']) if err_dict['loc'] else None, error_message=err_dict['msg'], error_type=ErrorType.VALIDATION, offending_value=str(err_dict.get('input', 'N/A'))[:255]))
                except Exception as e_row:
                    db_error_details_list.append(ErrorDetailModel(row_number=csv_row_number, error_message=f"Unexpected error: {str(e_row)}", error_type=ErrorType.UNEXPECTED_ROW_ERROR, offending_value=str(record_data.get(record_key, record_data))[:255]))
                processed_csv_records_count += 1

        final_error_count = len(initial_validation_errors) + len(db_error_details_list)
        all_accumulated_errors = initial_validation_errors + db_error_details_list

        if db_error_details_list:
            logger.warning(f"Session {session_id}: {len(db_error_details_list)} errors during DB processing for {map_type}. Rolling back.")
            if db_engine_session: db_engine_session.rollback()
            _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_DB_PROCESSING, all_accumulated_errors, processed_csv_records_count, final_error_count)
            return {"status": UploadJobStatus.FAILED_DB_PROCESSING.value, "errors": [err.model_dump() for err in all_accumulated_errors], "processed_db_count": processed_db_count, "session_id": session_id}

        if string_id_redis_pipeline : string_id_redis_pipeline.execute()
        if db_pk_redis_pipeline: db_pk_redis_pipeline.execute()
        set_id_map_ttl(session_id, map_type, redis_client)
        set_id_map_ttl(session_id, f"{map_type}{DB_PK_MAP_SUFFIX}", redis_client)

        if db_engine_session: db_engine_session.commit(); logger.info(f"DB session committed for {session_id}.")
        _update_session_status(session_id, str(business_id), UploadJobStatus.CLEANING_UP, None, processed_csv_records_count, final_error_count)

        final_status_enum = UploadJobStatus.COMPLETED
        final_user_message = f"Successfully processed {processed_db_count} records."
        final_details_for_user = None
        if final_error_count > 0 :
             final_status_enum = UploadJobStatus.COMPLETED_WITH_ERRORS
             final_user_message = f"Processed {processed_csv_records_count} records with {final_error_count} errors (initial validation)."
             final_details_for_user = all_accumulated_errors
        elif processed_db_count == 0 and processed_csv_records_count > 0:
             final_status_enum = UploadJobStatus.COMPLETED_NO_CHANGES
             final_user_message = "File processed, but no records resulted in database changes."

        try:
            wasabi_client.delete_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            logger.info(f"Successfully deleted {wasabi_file_path} from Wasabi.")
        except Exception as cleanup_error:
            logger.error(f"Failed to delete {wasabi_file_path} from Wasabi: {cleanup_error}", exc_info=True)
            final_user_message += f" Warning: Wasabi cleanup failed: {str(cleanup_error)}"
            if final_details_for_user is None: final_details_for_user = []
            final_details_for_user.append(ErrorDetailModel(error_message=f"Wasabi cleanup failed: {str(cleanup_error)}",error_type=ErrorType.TASK_EXCEPTION))
            if final_status_enum == UploadJobStatus.COMPLETED: final_status_enum = UploadJobStatus.COMPLETED_WITH_ERRORS

        _update_session_status(session_id, str(business_id), final_status_enum, final_details_for_user, processed_csv_records_count, final_error_count)
        return {"status": final_status_enum.value, "message": final_user_message, "processed_db_count": processed_db_count, "total_records_in_file": len(original_records), "errors_count": final_error_count, "session_id": session_id}

    except tuple(RETRYABLE_EXCEPTIONS) as e: # Catch retryable exceptions explicitly to allow Celery to handle them
        logger.warning(f"RETRYING task for session {session_id} due to {type(e).__name__}: {e}", exc_info=True)
        # Note: _update_session_status is not called here with a "retrying" status because Celery's autoretry
        # handles this. If we wanted a specific "RETRYING" status in DB, we'd need manual retry logic:
        # self.retry(exc=e, countdown=...) and _update_session_status(..., status=UploadJobStatus.RETRYING, ...)
        # For now, the status remains as it was before the retryable error.
        raise # Re-raise for Celery's autoretry mechanism
    except Exception as e: # Catches non-retryable errors from process_csv_task's main block
        logger.error(f"Major error processing {map_type} for session {session_id}: {e}", exc_info=True)
        if db_engine_session: db_engine_session.rollback()
        critical_error_detail = [ErrorDetailModel(error_message=f"Major processing error: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)]
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION, critical_error_detail)
        # This exception will be caught by the task's own final try-except and re-raised.
        raise # Important to re-raise so Celery marks task as FAILED if not handled by autoretry

# --- Wrapper Celery Tasks ---
@shared_task(name="process_brands_file", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_brands_file(self, business_id: int, session_id: str, wasabi_file_path: str, original_filename: str):
    try:
        return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "name", "brand", "brands")
    except Exception as e:
        logger.error(f"Task process_brands_file FAILED ultimately for session {session_id}: {e}", exc_info=True)
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
                               [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise

@shared_task(name="process_attributes_file", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_attributes_file(self, business_id: int, session_id: str, wasabi_file_path: str, original_filename: str):
    try:
        return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "attribute_name", "attr", "attributes")
    except Exception as e:
        logger.error(f"Task process_attributes_file FAILED ultimately for session {session_id}: {e}", exc_info=True)
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
                               [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise

@shared_task(name="process_return_policies_file", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_return_policies_file(self, business_id: int, session_id: str, wasabi_file_path: str, original_filename: str):
    try:
        return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "policy_name", "rp", "return_policies")
    except Exception as e:
        logger.error(f"Task process_return_policies_file FAILED ultimately for session {session_id}: {e}", exc_info=True)
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
                               [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise

@shared_task(name="process_products_file", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_products_file(self, business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    try:
        return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "self_gen_product_id", "prod", "products")
    except Exception as e:
        logger.error(f"Task process_products_file FAILED ultimately for session {session_id}: {e}", exc_info=True)
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
                               [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise

@shared_task(name="process_product_items_file", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_product_items_file(self, business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    try:
        return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "variant_sku", "item", "product_items")
    except Exception as e:
        logger.error(f"Task process_product_items_file FAILED ultimately for session {session_id}: {e}", exc_info=True)
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
                               [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise

@shared_task(name="process_product_prices_file", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_product_prices_file(self, business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    try:
        return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "product_id", "price", "product_prices")
    except Exception as e:
        logger.error(f"Task process_product_prices_file FAILED ultimately for session {session_id}: {e}", exc_info=True)
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
                               [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise

@shared_task(name="process_meta_tags_file_specific", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_meta_tags_file_specific(self, business_id_str: str, session_id: str, wasabi_file_path: str, original_filename: str):
    try: # Outer try for the task signature itself, e.g. int conversion of business_id
        business_id = int(business_id_str)
    except ValueError:
        msg = f"Invalid business_id format: '{business_id_str}' for meta_tags. Must be an integer."
        logger.error(msg)
        _update_session_status(session_id, business_id_str, UploadJobStatus.FAILED_UNHANDLED_EXCEPTION, [ErrorDetailModel(error_message=msg, error_type=ErrorType.CONFIGURATION)])
        # This is a non-retryable config error from the task's perspective
        return {"status": UploadJobStatus.FAILED_UNHANDLED_EXCEPTION.value, "message": msg, "session_id": session_id}

    logger.info(f"Meta Tags specific processing task started for session: {session_id}, file: {original_filename}, business_id: {business_id}")
    db_session = None
    temp_local_path = None
    try: # Main logic block that might encounter retryable errors
        _update_session_status(session_id, str(business_id), status=UploadJobStatus.DOWNLOADING_FILE)
        response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
        with tempfile.NamedTemporaryFile(delete=False, mode='wb', suffix=".csv") as tmp_file_obj:
            tmp_file_obj.write(response['Body'].read())
            temp_local_path = tmp_file_obj.name
        logger.info(f"File {original_filename} downloaded from Wasabi to {temp_local_path} for session {session_id}.")
        _update_session_status(session_id, str(business_id), status=UploadJobStatus.VALIDATING_DATA)
        db_session = get_session(business_id=business_id)
        summary: DataloadSummary = load_meta_tags_from_csv(db=db_session, csv_file_path=temp_local_path)
        db_session.commit()
        total_records_processed = summary.total_rows_processed
        standardized_error_details: List[ErrorDetailModel] = []
        if summary.error_details:
            for err in summary.error_details:
                error_type = ErrorType.VALIDATION
                if "not found" in err.message.lower(): error_type = ErrorType.LOOKUP
                elif "database" in err.message.lower(): error_type = ErrorType.DATABASE
                standardized_error_details.append(ErrorDetailModel(
                    row_number=err.row_number if err.row_number != 0 else None,
                    field_name=err.field, error_message=err.message, error_type=error_type,
                    offending_value=str(err.value) if err.value is not None else None ))
        total_errors = len(standardized_error_details)
        final_status_enum: UploadJobStatus
        if any(err.row_number is None for err in standardized_error_details): final_status_enum = UploadJobStatus.FAILED_VALIDATION
        elif total_errors > 0:
            if summary.successful_updates == 0 and total_records_processed > 0 and total_errors >= total_records_processed: final_status_enum = UploadJobStatus.FAILED_DB_PROCESSING
            else: final_status_enum = UploadJobStatus.COMPLETED_WITH_ERRORS
        elif summary.successful_updates > 0: final_status_enum = UploadJobStatus.COMPLETED
        elif total_records_processed > 0 and summary.successful_updates == 0: final_status_enum = UploadJobStatus.COMPLETED_NO_CHANGES
        elif total_records_processed == 0: final_status_enum = UploadJobStatus.COMPLETED_EMPTY_FILE
        else: final_status_enum = UploadJobStatus.FAILED_UNHANDLED_EXCEPTION; logger.warning(f"Meta tags task {session_id} ended in undetermined state. Summary: {summary}")
        if final_status_enum == UploadJobStatus.COMPLETED_EMPTY_FILE: total_records_processed = 0
        _update_session_status(session_id, str(business_id), final_status_enum, standardized_error_details if standardized_error_details else None, total_records_processed, total_errors)
        logger.info(f"Meta Tags processing finished for session {session_id}. Status: {final_status_enum.value}, Processed: {total_records_processed}, Successful: {summary.successful_updates}, Errors: {total_errors}.")
        if not final_status_enum.is_failure():
            _update_session_status(session_id, str(business_id), status=UploadJobStatus.CLEANING_UP)
            try:
                wasabi_client.delete_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
                logger.info(f"Successfully deleted {wasabi_file_path} from Wasabi for session {session_id}.")
            except Exception as cleanup_error: logger.error(f"Failed to delete {wasabi_file_path} from Wasabi for session {session_id}: {cleanup_error}", exc_info=True)
        return {"status": final_status_enum.value, "processed_count": summary.successful_updates, "total_records": total_records_processed, "errors_count": total_errors, "session_id": session_id}
    except Exception as e:
        logger.error(f"Task process_meta_tags_file_specific FAILED ultimately for session {session_id}: {str(e)}", exc_info=True)
        if db_session: db_session.rollback()
        _update_session_status(session_id, str(business_id_str), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION, [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise
    finally:
        if db_session: db_session.close()
        if temp_local_path and os.path.exists(temp_local_path):
            try: os.remove(temp_local_path); logger.info(f"Temp file {temp_local_path} deleted.")
            except Exception as e_remove: logger.error(f"Error deleting temp file {temp_local_path}: {e_remove}", exc_info=True)

@shared_task(name="process_categories_file", bind=True, autoretry_for=RETRYABLE_EXCEPTIONS, **COMMON_RETRY_KWARGS)
def process_categories_file(self, business_id: int, session_id: str, wasabi_file_path: str, original_filename: str):
    logger.info(f"Category file processing task started for session: {session_id}, file: {original_filename}, business_id: {business_id}")
    try:
        return process_csv_task(str(business_id), session_id, wasabi_file_path, original_filename, "category_path", "cat", "categories")
    except Exception as e:
        logger.error(f"Task process_categories_file FAILED ultimately for session {session_id}: {e}", exc_info=True)
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
                               [ErrorDetailModel(error_message=f"Task failed: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)])
        raise

# This task was a simple wrapper, now process_meta_tags_file_specific is the one in CELERY_TASK_MAP
# @shared_task(name="process_meta_tags_file")
# def process_meta_tags_file(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
#     logger.warning(f"Legacy process_meta_tags_file called for session {session_id}. Using process_meta_tags_file_specific.")
#     return process_meta_tags_file_specific.delay(business_id, session_id, wasabi_file_path, original_filename)

CELERY_TASK_MAP = {
    "brands": process_brands_file,
    "attributes": process_attributes_file,
    "return_policies": process_return_policies_file,
    "products": process_products_file,
    "product_items": process_product_items_file,
    "product_prices": process_product_prices_file,
    "meta_tags": process_meta_tags_file_specific,
    "categories": process_categories_file,
}
