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
)

COMMON_RETRY_KWARGS = {
    "max_retries": 3,
    "default_retry_delay": 60, # seconds
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
        db = get_session(business_id=int(business_id_str))
        session_record = db.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == session_id).first()
        if session_record:
            session_record.status = status_value
            session_record.updated_at = datetime.utcnow()
            if details is not None:
                error_details_json = json.dumps([err.model_dump(mode='json') for err in details])
                max_details_len = 4000
                if len(error_details_json) > max_details_len:
                    error_details_json = error_details_json[:max_details_len - len('... TRUNCATED')] + "... TRUNCATED"
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
        raise ValueError(msg)
    
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

    except tuple(RETRYABLE_EXCEPTIONS) as e:
        logger.warning(f"RETRYING task for session {session_id} due to {type(e).__name__}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Major error processing {map_type} for session {session_id}: {e}", exc_info=True)
        if db_engine_session: db_engine_session.rollback()
        critical_error_detail = [ErrorDetailModel(error_message=f"Major processing error: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)]
        _update_session_status(session_id, str(business_id), UploadJobStatus.FAILED_UNHANDLED_EXCEPTION, critical_error_detail)
        raise
