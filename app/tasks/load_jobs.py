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
# import redis # Moved to redis_utils
import os
import logging
from typing import Optional
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Name of the Wasabi bucket used for storing uploaded CSV files.
WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name")

# Import Redis utilities from the new location
from app.utils.redis_utils import (
    redis_client_instance as redis_client, # Use the instance from redis_utils
    add_to_id_map,
    get_from_id_map,
    DB_PK_MAP_SUFFIX, # If it was used directly, though it seems not
    REDIS_SESSION_TTL_SECONDS,
    set_id_map_ttl,
    get_redis_pipeline
)

# Generic task processor

# from app.db.models import UploadSessionOrm # Already imported with other ORM models
from app.db.models import CategoryOrm, AttributeOrm, AttributeValueOrm # Ensure Attribute ORM models are available if needed for context, though not directly used in process_csv_task
# from app.db.connection import get_session as get_db_session_sync # Already imported as get_session
from app.services.db_loaders import load_category_to_db, load_brand_to_db, load_attribute_to_db, load_return_policy_to_db

# Import product specific loader and model
from app.dataload.product_loader import load_product_record_to_db
from app.dataload.models.product_csv import ProductCsvModel

from app.models import UploadJobStatus, ErrorDetailModel # Import new Enum and Pydantic model
import json # For serializing List[ErrorDetailModel]

# Function to update session status in DB
def _update_session_status(
    session_id: str,
    business_id: str,
    status: UploadJobStatus, # Use the Enum for type hinting status
    details: Optional[List[ErrorDetailModel]] = None, # Expect a list of ErrorDetailModel
    record_count: Optional[int] = None,
    error_count: Optional[int] = None
):
    status_value = status.value # Get the string value from Enum for DB
    logger.info(f"Attempting to update session {session_id} for business {business_id} to status '{status_value}'.")
    db = None
    try:
        db = get_session(business_id=business_id)
        session_record = db.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == session_id).first()

        if session_record:
            session_record.status = status_value
            session_record.updated_at = datetime.utcnow()

            if details is not None:
                # Serialize List[ErrorDetailModel] to JSON string
                error_details_json = json.dumps([err.model_dump(mode='json') for err in details])
                # Truncate if necessary (example: 4000 chars for details column)
                # This length should ideally come from a config or be based on DB schema knowledge
                max_details_len = 4000
                if len(error_details_json) > max_details_len:
                    # Simple truncation for now. Could be smarter (e.g., keep first N errors).
                    error_details_json = error_details_json[:max_details_len - len('... TRUNCATED') ] + "... TRUNCATED"
                session_record.details = error_details_json
            elif status.is_failure() and session_record.details is None: # Ensure details is cleared if not provided on failure
                 session_record.details = None # Or set to empty list JSON '[]'

            if record_count is not None:
                session_record.record_count = record_count
            if error_count is not None:
                session_record.error_count = error_count

            db.commit()
            logger.info(f"Successfully updated session {session_id} to status '{status_value}'.")
        else:
            logger.error(f"Upload session {session_id} not found in DB for status update for business {business_id}.")
    except Exception as e:
        logger.error(f"DB Error: Failed to update session {session_id} status to {status_value}: {e}", exc_info=True)
        if db:
            db.rollback()
    finally:
        if db:
            db.close()


def process_csv_task(business_id, session_id, wasabi_file_path, original_filename, record_key, id_prefix, map_type):
    logger.info(f"Processing {map_type} for business: {business_id} session: {session_id} file: {original_filename} ({wasabi_file_path})")

    # Initial status update
    # The API might set it to PENDING. Celery task immediately updates to QUEUED upon being picked up.
    # (Actual QUEUED status is hard to set reliably from within the task itself once it *starts* running)
    # So, the first status update from the task itself will be DOWNLOADING_FILE.

    _update_session_status(session_id, business_id, status=UploadJobStatus.DOWNLOADING_FILE)

    db_engine_session = None
    try:
        response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
        file_content = response['Body'].read().decode('utf-8')
        logger.info(f"File {original_filename} downloaded from Wasabi for session {session_id}.")
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
                'return_policy_code': f"return_policies{db_pk_map_suffix}" # This key might need update if return policy CSV key changed
            }
        elif map_type == "product_items": # Assuming products_db_pk map is used for linking items to products
            referenced_entity_map_for_validation = {'product_name': f"products{db_pk_map_suffix}"}
        # Other map_types might also need their referenced_entity_map defined if they do lookups in validate_csv

        # Update status before validation
        _update_session_status(session_id, business_id, status=UploadJobStatus.VALIDATING_SCHEMA) # Or VALIDATING_DATA if validate_csv does more

        # Validate CSV data
        # TODO: Refactor validate_csv to return List[ErrorDetailModel] for validation_errors
        raw_validation_errors, validated_records = validate_csv(
            load_type=map_type,
            records=original_records,
            session_id=session_id,
            record_key=record_key,
            referenced_entity_map=referenced_entity_map_for_validation
        )

        # Convert raw_validation_errors to List[ErrorDetailModel]
        # This is a placeholder conversion. `validate_csv` should ideally return ErrorDetailModel instances.
        # For now, assuming raw_validation_errors is a list of strings or simple dicts.
        error_detail_list: List[ErrorDetailModel] = []
        if raw_validation_errors:
            for i, err_msg in enumerate(raw_validation_errors): # Assuming it's a list of messages for now
                error_detail_list.append(ErrorDetailModel(
                    row_number= i + 1, # This is a guess, validate_csv needs to provide row numbers
                    error_message=str(err_msg),
                    error_type=ErrorType.VALIDATION
                ))
            logger.error(f"Initial validation errors for {map_type} in session {session_id}, file {original_filename}: {error_detail_list}")
            _update_session_status(
                session_id, business_id,
                status=UploadJobStatus.FAILED_VALIDATION,
                details=error_detail_list,
                error_count=len(error_detail_list)
            )
            return {"status": UploadJobStatus.FAILED_VALIDATION.value, "errors": [err.model_dump() for err in error_detail_list], "processed_count": 0, "session_id": session_id}

        _update_session_status(session_id, business_id, status=UploadJobStatus.DB_PROCESSING_STARTED, record_count=len(validated_records))

        db_engine_session = get_session(business_id)
        string_id_redis_pipeline = get_redis_pipeline()
        db_pk_redis_pipeline = get_redis_pipeline()

        processed_csv_records_count = 0
        processed_db_count = 0
        db_error_details_list: List[ErrorDetailModel] = [] # Store ErrorDetailModel instances

        # --- Dispatch to Specific DB Loader Loop ---
        for i, record_data in enumerate(validated_records):
            csv_row_number = i + 2 # Assuming row 1 is header
            db_pk = None
            current_record_key_value = record_data.get(record_key, 'N/A')

            try: # Wrap individual record processing for more granular error capture
                if map_type == "categories":
                    db_pk = load_category_to_db(db_engine_session, business_id, record_data, session_id, db_pk_redis_pipeline)
                elif map_type == "brands":
                    db_pk = load_brand_to_db(db_engine_session, business_id, record_data, session_id, db_pk_redis_pipeline)
                elif map_type == "attributes":
                    db_pk = load_attribute_to_db(db_engine_session, business_id, record_data, session_id, db_pk_redis_pipeline)
                elif map_type == "return_policies":
                    db_pk = load_return_policy_to_db(db_engine_session, business_id, record_data, session_id, db_pk_redis_pipeline)
                elif map_type == "products":
                    record_data_for_model = record_data.copy()
                    record_data_for_model['business_details_id'] = business_id
                    product_csv_instance = ProductCsvModel(**record_data_for_model)
                    current_record_key_value = product_csv_instance.self_gen_product_id # More specific key for products
                    db_pk = load_product_record_to_db(db_engine_session, business_id, product_csv_instance, session_id)
                    if db_pk is not None and db_pk_redis_pipeline:
                        add_to_id_map(session_id, f"products{DB_PK_MAP_SUFFIX}", product_csv_instance.self_gen_product_id, db_pk, pipeline=db_pk_redis_pipeline)
                else:
                    logger.info(f"Row {csv_row_number}: No specific DB loader for map_type: '{map_type}'. Record: {current_record_key_value}")
                    if string_id_redis_pipeline and record_key and id_prefix and current_record_key_value != 'N/A':
                        generated_id = f"{id_prefix}:{str(current_record_key_value).lower().replace(' ', '_')}"
                        add_to_id_map(session_id, map_type, current_record_key_value, generated_id, pipeline=string_id_redis_pipeline)

                if db_pk is None and map_type in ["categories", "brands", "attributes", "return_policies", "products"]:
                    # This implies the loader itself logged an error and returned None.
                    # We create a generic error detail here if the loader didn't provide one.
                    # Ideally, loaders should raise specific exceptions or return error details.
                    logger.warning(f"Row {csv_row_number}: DB loader for '{map_type}' returned None for record: {current_record_key_value}. This indicates a processing error for this row.")
                    db_error_details_list.append(ErrorDetailModel(
                        row_number=csv_row_number,
                        error_message=f"Failed to process record for '{map_type}' with key '{current_record_key_value}'. Check worker logs for specifics.",
                        error_type=ErrorType.DATABASE # Or more specific if known
                    ))
                elif db_pk is not None:
                    processed_db_count +=1

            except Exception as e_row_processing: # Catch errors from Pydantic validation within loop or loader issues
                logger.error(f"Row {csv_row_number}: Error processing record {current_record_key_value} for {map_type}: {e_row_processing}", exc_info=True)
                db_error_details_list.append(ErrorDetailModel(
                    row_number=csv_row_number,
                    field_name=getattr(e_row_processing, 'field', None), # Attempt to get field if Pydantic error
                    error_message=str(e_row_processing),
                    error_type=ErrorType.UNEXPECTED_ROW_ERROR # Or VALIDATION if it's a Pydantic error
                ))

            processed_csv_records_count += 1
        # --- End Dispatch to Specific DB Loader Loop ---

        all_errors = error_detail_list # Combine initial validation errors (already handled) with DB errors

        if db_error_details_list: # Check if any errors occurred during DB processing loop
            logger.warning(f"Session {session_id}: Encountered {len(db_error_details_list)} errors during DB processing for {map_type}. Rolling back DB changes.")
            if db_engine_session: db_engine_session.rollback()
            _update_session_status(
                session_id, business_id,
                status=UploadJobStatus.FAILED_DB_PROCESSING,
                details=db_error_details_list, # Pass the list of ErrorDetailModel
                record_count=processed_csv_records_count,
                error_count=len(db_error_details_list) # Just DB errors for this status, initial validation errors led to earlier exit
            )
            return {"status": UploadJobStatus.FAILED_DB_PROCESSING.value, "errors": [err.model_dump() for err in db_error_details_list], "processed_db_count": processed_db_count, "session_id": session_id}

        # If no DB errors during the loop
        if string_id_redis_pipeline : string_id_redis_pipeline.execute()
        if db_pk_redis_pipeline: db_pk_redis_pipeline.execute()

        set_id_map_ttl(session_id, map_type, redis_client)
        set_id_map_ttl(session_id, f"{map_type}{DB_PK_MAP_SUFFIX}", redis_client)

        if db_engine_session:
            db_engine_session.commit()
            logger.info(f"DB session committed for session {session_id} after processing {map_type}.")

        _update_session_status(session_id, business_id, status=UploadJobStatus.CLEANING_UP)
        final_status = UploadJobStatus.COMPLETED
        final_message = f"Successfully processed {processed_db_count} records."

        try:
            logger.info(f"Attempting to delete {wasabi_file_path} from Wasabi bucket {WASABI_BUCKET_NAME}.")
            wasabi_client.delete_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
            logger.info(f"Successfully deleted {wasabi_file_path} from Wasabi.")
        except Exception as cleanup_error:
            logger.error(f"Failed to delete {wasabi_file_path} from Wasabi: {cleanup_error}", exc_info=True)
            # Note: Status remains COMPLETED, but we might log this as an issue or add to details.
            # For now, we won't change status but this could be a new "COMPLETED_CLEANUP_FAILED" status.
            final_message += f" Warning: Wasabi cleanup failed: {str(cleanup_error)}"
            # Potentially add a system-level error to error_detail_list here if it's critical
            # error_detail_list.append(ErrorDetailModel(error_message=f"Wasabi cleanup failed: {str(cleanup_error)}", error_type=ErrorType.TASK_EXCEPTION))
            # This would then make the status COMPLETED_WITH_ERRORS if we re-evaluate based on error_detail_list.

        # Determine final status based on errors accumulated *during DB processing*
        # Initial validation errors already led to an early exit.
        if db_error_details_list: # Should be empty if we reached here, but as a safeguard
            final_status = UploadJobStatus.COMPLETED_WITH_ERRORS
            final_message = f"Processed {processed_csv_records_count} records with {len(db_error_details_list)} errors during DB operations."
        elif processed_db_count == 0 and processed_csv_records_count > 0:
             final_status = UploadJobStatus.COMPLETED_NO_CHANGES
             final_message = "File processed, but no records resulted in database changes."
        elif processed_csv_records_count == 0 and not original_records : # File was empty or header only
            final_status = UploadJobStatus.COMPLETED_EMPTY_FILE
            final_message = "CSV file was empty or contained only headers."

        _update_session_status(
            session_id, business_id,
            status=final_status,
            details=db_error_details_list if db_error_details_list else [ErrorDetailModel(error_message=final_message, error_type=ErrorType.UNKNOWN)] if final_status != UploadJobStatus.COMPLETED else None,
            record_count=processed_csv_records_count,
            error_count=len(db_error_details_list)
        )
        return {"status": final_status.value, "processed_db_count": processed_db_count, "total_records_in_file": len(original_records), "errors": [err.model_dump() for err in db_error_details_list], "session_id": session_id}

    except Exception as e:
        logger.error(f"Major error processing {map_type} for session {session_id}, file {original_filename}: {e}", exc_info=True)
        if db_engine_session:
            db_engine_session.rollback()
            logger.info(f"DB session rolled back for session {session_id} due to major error.")

        critical_error_detail = [ErrorDetailModel(error_message=f"Major processing error: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)]
        _update_session_status(session_id, business_id, status=UploadJobStatus.FAILED_UNHANDLED_EXCEPTION, details=critical_error_detail)
        return {"status": UploadJobStatus.FAILED_UNHANDLED_EXCEPTION.value, "message": str(e), "session_id": session_id}
    finally:
        if db_engine_session:
            db_engine_session.close()
            logger.info(f"DB session closed for session {session_id}.")

# Specific loaders (ensure they pass all necessary params if process_csv_task signature changes)
# These should ideally pass the 'record_key' specific to their map_type for CSV key -> DB PK mapping.
# For example, for 'brands', record_key is 'brand_name'. For 'products', it's 'product_name'.

@shared_task(name="process_brands_file")
def process_brands_file(business_id: int, session_id: str, wasabi_file_path: str, original_filename: str): # business_id type hint updated
    return process_csv_task(
        business_id=business_id,
        session_id=session_id,
        wasabi_file_path=wasabi_file_path,
        original_filename=original_filename,
        record_key="name",  # Changed from "brand_name" to "name"
        id_prefix="brand",
        map_type="brands"
    )

@shared_task(name="process_attributes_file")
def process_attributes_file(business_id: int, session_id: str, wasabi_file_path: str, original_filename: str): # business_id type hint updated
    return process_csv_task(
        business_id=business_id,
        session_id=session_id,
        wasabi_file_path=wasabi_file_path,
        original_filename=original_filename,
        record_key="attribute_name", # This is the key in AttributeCsvModel
        id_prefix="attr",
        map_type="attributes"
    )

@shared_task(name="process_return_policies_file")
def process_return_policies_file(business_id: int, session_id: str, wasabi_file_path: str, original_filename: str): # business_id type hint updated
    return process_csv_task(
        business_id=business_id,
        session_id=session_id,
        wasabi_file_path=wasabi_file_path,
        original_filename=original_filename,
        record_key="policy_name", # Changed from "return_policy_code"
        id_prefix="rp",
        map_type="return_policies"
    )

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
    # This is the old generic handler. We will replace its use in CELERY_TASK_MAP.
    # For now, let's keep it to avoid breaking other parts if they call it directly by name,
    # but the new task process_meta_tags_file_specific should be used for the "meta_tags" load_type.
    logger.warning(f"Legacy process_meta_tags_file called for session {session_id}. Consider using process_meta_tags_file_specific.")
    return process_csv_task(business_id, session_id, wasabi_file_path, original_filename, "product_name", "meta", "meta_tags")

# New specific Celery task for meta_tags
from app.dataload.meta_tags_loader import load_meta_tags_from_csv, DataloadSummary # DataloadErrorDetail was specific to that loader
# from app.db.models import UploadSessionOrm # Already imported
import tempfile
# import json # Already imported
import os
from app.models.schemas import ErrorDetailModel, ErrorType # Import standardized error model

@shared_task(name="process_meta_tags_file_specific")
def process_meta_tags_file_specific(business_id: str, session_id: str, wasabi_file_path: str, original_filename: str):
    logger.info(f"Meta Tags specific processing task started for session: {session_id}, file: {original_filename}, business_id: {business_id}")

    _update_session_status(session_id, business_id, status=UploadJobStatus.DOWNLOADING_FILE)
    db_session = None
    temp_local_path = None

    try:
        response = wasabi_client.get_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
        with tempfile.NamedTemporaryFile(delete=False, mode='wb', suffix=".csv") as tmp_file_obj:
            tmp_file_obj.write(response['Body'].read())
            temp_local_path = tmp_file_obj.name
        logger.info(f"File {original_filename} downloaded from Wasabi to {temp_local_path} for session {session_id}.")

        # Assuming meta_tags_loader doesn't have complex schema vs data validation stages,
        # we can go to DB_PROCESSING_STARTED or a generic VALIDATING_DATA.
        _update_session_status(session_id, business_id, status=UploadJobStatus.VALIDATING_DATA) # Or DB_PROCESSING_STARTED

        db_session = get_session(business_id=business_id)
        summary: DataloadSummary = load_meta_tags_from_csv(db=db_session, csv_file_path=temp_local_path)

        total_records_processed = summary.total_rows_processed

        # Convert DataloadSummary.error_details (which are specific to meta_tags_loader)
        # to the standardized List[ErrorDetailModel]
        standardized_error_details: List[ErrorDetailModel] = []
        if summary.error_details:
            for err in summary.error_details:
                # Map meta_tags_loader's DataloadErrorDetail fields to ErrorDetailModel
                # This requires knowing the structure of meta_tags_loader.DataloadErrorDetail
                # Assuming it has 'row_number', 'field', 'message', 'value'
                error_type = ErrorType.VALIDATION # Default, can be more specific if err has a type
                if "not found" in err.message.lower(): # Basic heuristic
                    error_type = ErrorType.LOOKUP
                elif "database" in err.message.lower():
                     error_type = ErrorType.DATABASE

                standardized_error_details.append(ErrorDetailModel(
                    row_number=err.row_number if err.row_number != 0 else None, # row 0 might mean file-level
                    field_name=err.field,
                    error_message=err.message,
                    error_type=error_type,
                    offending_value=str(err.value) if err.value is not None else None
                ))

        total_errors = len(standardized_error_details)

        # Determine final status using UploadJobStatus Enum
        final_status_enum: UploadJobStatus
        if any(err.row_number is None for err in standardized_error_details): # Implies file-level error from loader
            final_status_enum = UploadJobStatus.FAILED_VALIDATION # Or FAILED_FILE_FORMAT
        elif total_errors > 0:
            if summary.successful_updates == 0 and total_records_processed > 0 and total_errors >= total_records_processed:
                final_status_enum = UploadJobStatus.FAILED_DB_PROCESSING # All rows failed
            else:
                final_status_enum = UploadJobStatus.COMPLETED_WITH_ERRORS
        elif summary.successful_updates > 0:
            final_status_enum = UploadJobStatus.COMPLETED
        elif total_records_processed > 0 and summary.successful_updates == 0:
            final_status_enum = UploadJobStatus.COMPLETED_NO_CHANGES
        elif total_records_processed == 0:
            final_status_enum = UploadJobStatus.COMPLETED_EMPTY_FILE
        else:
            final_status_enum = UploadJobStatus.FAILED_UNHANDLED_EXCEPTION # Should ideally not be reached
            logger.warning(f"Meta tags task for session {session_id} ended in an undetermined state. Summary: {summary}")

        if final_status_enum == UploadJobStatus.COMPLETED_EMPTY_FILE:
            total_records_processed = 0

        _update_session_status(
            session_id=session_id,
            business_id=business_id,
            status=final_status_enum,
            details=standardized_error_details if standardized_error_details else None,
            record_count=total_records_processed,
            error_count=total_errors
        )

        logger.info(f"Meta Tags processing finished for session {session_id}. Status: {final_status_enum.value}, Processed: {total_records_processed}, Successful: {summary.successful_updates}, Errors: {total_errors}.")

        if not final_status_enum.is_failure(): # Use Enum helper method
            _update_session_status(session_id, business_id, status=UploadJobStatus.CLEANING_UP)
            try:
                wasabi_client.delete_object(Bucket=WASABI_BUCKET_NAME, Key=wasabi_file_path)
                logger.info(f"Successfully deleted {wasabi_file_path} from Wasabi for session {session_id}.")
            except Exception as cleanup_error:
                logger.error(f"Failed to delete {wasabi_file_path} from Wasabi for session {session_id}: {cleanup_error}", exc_info=True)
                # Optionally update status to a specific "COMPLETED_CLEANUP_FAILED" or add to details
                # For now, logging the error is sufficient.

        return {"status": final_status_enum.value, "processed_count": summary.successful_updates, "total_records": total_records_processed, "errors_count": total_errors, "session_id": session_id}

    except Exception as e:
        logger.error(f"Critical error in process_meta_tags_file_specific for session {session_id}: {str(e)}", exc_info=True)
        if db_session:
            db_session.rollback()
        _update_session_status(
            session_id, business_id,
            status=UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
            details=[ErrorDetailModel(error_message=f"Critical task error: {str(e)}", error_type=ErrorType.TASK_EXCEPTION)]
        )
        return {"status": UploadJobStatus.FAILED_UNHANDLED_EXCEPTION.value, "message": str(e), "session_id": session_id}
    finally:
        if db_session:
            db_session.close()
        if temp_local_path and os.path.exists(temp_local_path):
            try:
                os.remove(temp_local_path)
                logger.info(f"Temporary file {temp_local_path} deleted for session {session_id}.")
            except Exception as e_remove:
                logger.error(f"Error deleting temporary file {temp_local_path} for session {session_id}: {e_remove}", exc_info=True)


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
    "meta_tags": process_meta_tags_file_specific, # Updated to specific task
    "categories": process_categories_file,
}
