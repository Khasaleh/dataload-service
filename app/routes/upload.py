import logging
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool # For running sync DB code
from app.dependencies.auth import get_current_user
from app.services.storage import upload_file as upload_to_wasabi
from app.db.models import UploadSessionOrm # Use the ORM model
from app.db.connection import get_session # For DB session
from datetime import datetime
import uuid
import os
from typing import Optional

logger = logging.getLogger(__name__)
router = APIRouter()

# Define upload sequence dependencies: key is the load_type, value is a list of prerequisite load_types.
# This logic can be kept or removed based on whether it's actively used or enforced.
# For now, keeping it as it was in the original file.
UPLOAD_SEQUENCE_DEPENDENCIES = {
    "products": ["brands", "return_policies"],
    "product_items": ["products"],
    "product_prices": ["products"],
    "meta_tags": ["products"],
}

ROLE_PERMISSIONS = {
    "ROLE_ADMIN": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags"},
    "catalog_editor": {"products", "product_items", "product_prices", "meta_tags"},
    "viewer": set(),
}

from app.tasks.load_jobs import (
    process_brands_file, process_attributes_file,
    process_return_policies_file, process_products_file,
    process_product_items_file, process_product_prices_file, process_meta_tags_file
)

CELERY_TASK_MAP = {
    "brands": process_brands_file,
    "attributes": process_attributes_file,
    "return_policies": process_return_policies_file,
    "products": process_products_file,
    "product_items": process_product_items_file,
    "product_prices": process_product_prices_file,
    "meta_tags": process_meta_tags_file,
}

WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name")

# Define a Pydantic model for the response
from pydantic import BaseModel
class UploadResponseModel(BaseModel):
    message: str
    session_id: str
    load_type: str
    wasabi_path: str
    status: str
    task_id: Optional[str] = None # Celery task ID

def create_upload_session_in_db_sync(
    session_id_str: str,
    user_business_id: int, # Ensure this is the integer ID
    load_type_str: str,
    original_filename_str: str,
    wasabi_path_str: str
) -> UploadSessionOrm:
    """Synchronous function to create and save UploadSessionOrm instance."""
    db = None
    try:
        db = get_session(business_id=user_business_id)
        new_session_orm = UploadSessionOrm(
            session_id=session_id_str,
            business_details_id=user_business_id, # Use the correct field name
            load_type=load_type_str,
            original_filename=original_filename_str,
            wasabi_path=wasabi_path_str,
            status="pending",
            # created_at and updated_at have server_default, so not strictly needed here
        )
        db.add(new_session_orm)
        db.commit()
        db.refresh(new_session_orm)
        logger.info(f"Upload session record created in DB for session_id: {session_id_str}")
        return new_session_orm
    except Exception as e_db:
        logger.error(f"DB Error: Failed to create upload session record for session_id {session_id_str}: {e_db}", exc_info=True)
        if db:
            db.rollback()
        # Re-raise a more specific or generic error to be caught by the endpoint
        raise HTTPException(status_code=500, detail=f"Failed to create upload session record in DB: {str(e_db)}")
    finally:
        if db:
            db.close()

@router.post("/api/v1/business/{business_id}/upload/{load_type}",
             summary="Upload catalog file, store in Wasabi, create DB session, and queue for processing",
             status_code=202, # Accepted
             response_model=UploadResponseModel, # Use the Pydantic response model
             responses={
                 202: {
                     "description": "File accepted for processing.",
                     "content": {
                         "application/json": {
                             "example": {
                                 "message": "File accepted for processing.",
                                 "session_id": "uuid",
                                 "load_type": "brands",
                                 "wasabi_path": "uploads/123/uuid/brands/brands.csv",
                                 "status": "pending",
                                 "task_id": "celery-task-uuid"
                             }
                         }
                     }
                 },
                 400: {"description": "Invalid request (e.g., invalid load type, empty file, non-CSV)"},
                 403: {"description": "Permission denied or business ID mismatch"},
                 500: {"description": "Internal server error (e.g., DB operation failed, Wasabi upload failed)"}
             })
async def upload_file_and_queue_for_processing( # Renamed for clarity
    business_id: str, # Path parameter, will be string
    load_type: str,
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user) # user["business_id"] is an int
):
    # Validate business_id from token against path param
    # user["business_id"] is already an int from get_current_user
    # path business_id is a string, so cast for comparison or cast user's to string
    try:
        path_business_id_int = int(business_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid business_id format in path. Must be an integer.")

    if path_business_id_int != user["business_id"]:
        raise HTTPException(status_code=403, detail="Token's business_id does not match business_id in path.")

    # Role permission check
    # user_roles = user.get("roles", []) # Assuming get_current_user returns "roles" as a list of strings
    # For simplicity, if "role" is a single string in the token payload as per existing structure:
    user_role = user.get("roles")[0] if user.get("roles") else "viewer" # Simplified: take first role or default

    if user_role not in ROLE_PERMISSIONS or load_type not in ROLE_PERMISSIONS[user_role]:
        raise HTTPException(status_code=403, detail=f"User role '{user_role}' does not have permission to upload load_type '{load_type}'.")

    # Validate load_type
    if load_type not in CELERY_TASK_MAP:
        raise HTTPException(status_code=400, detail=f"Invalid load type: {load_type}. Supported types are: {list(CELERY_TASK_MAP.keys())}")

    # File validations
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename cannot be empty.")
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV files are allowed.")

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty CSV file submitted.")
    await file.seek(0) # Reset file pointer for Wasabi upload and DB model

    # Generate session ID and Wasabi path
    session_id_str = str(uuid.uuid4())
    # Use the validated integer business_id for path construction
    wasabi_path_str = f"uploads/{path_business_id_int}/{session_id_str}/{load_type}/{file.filename}"

    # --- Create UploadSession DB record ---
    # user["business_id"] is the integer ID from the token, which has been validated against path_business_id_int
    user_int_business_id = user["business_id"]
    try:
        new_session_orm = await run_in_threadpool(
            create_upload_session_in_db_sync, # The synchronous DB function
            session_id_str=session_id_str,
            user_business_id=user_int_business_id,
            load_type_str=load_type,
            original_filename_str=file.filename,
            wasabi_path_str=wasabi_path_str
        )
    except HTTPException as http_exc: # Catch HTTPException re-raised from sync function
        raise http_exc
    except Exception as e: # Catch any other unexpected errors from run_in_threadpool or sync function
        logger.error(f"Error during DB session creation via threadpool for session {session_id_str}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while preparing upload session: {str(e)}")


    # --- Upload to Wasabi ---
    try:
        # Use run_in_threadpool for boto3 as it's also synchronous
        await run_in_threadpool(upload_to_wasabi, bucket=WASABI_BUCKET_NAME, path=wasabi_path_str, file_obj=file.file)
        logger.info(f"File successfully uploaded to Wasabi: {wasabi_path_str} for session_id: {session_id_str}")
    except Exception as e_wasabi:
        logger.error(f"Wasabi Error: Failed to upload file for session_id {session_id_str}: {e_wasabi}", exc_info=True)
        # Potentially update DB session status to 'failed_upload' here if critical
        # For now, just raise error as per requirement (Celery task would eventually fail if file not found)
        # However, it's better to fail fast. Let's update status if DB record exists.
        if new_session_orm: # If DB record was created
            def update_status_to_failed_upload_sync(s_id: str, b_id: int, error_details: str):
                db_sess_fail = None
                try:
                    db_sess_fail = get_session(business_id=b_id)
                    session_to_fail = db_sess_fail.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == s_id).first()
                    if session_to_fail:
                        session_to_fail.status = "failed_wasabi_upload"
                        session_to_fail.details = error_details
                        session_to_fail.updated_at = datetime.utcnow()
                        db_sess_fail.commit()
                    logger.info(f"Updated session {s_id} to failed_wasabi_upload due to Wasabi error.")
                except Exception as db_e:
                    logger.error(f"Failed to update session {s_id} status after Wasabi error: {db_e}", exc_info=True)
                    if db_sess_fail: db_sess_fail.rollback()
                finally:
                    if db_sess_fail: db_sess_fail.close()
            await run_in_threadpool(update_status_to_failed_upload_sync, s_id=new_session_orm.session_id, b_id=user_int_business_id, error_details=f"Failed to upload to Wasabi: {str(e_wasabi)[:1000]}") # Truncate error
        raise HTTPException(status_code=500, detail=f"Failed to upload file to Wasabi: {str(e_wasabi)}")

    # --- Dispatch Celery Task ---
    celery_task_fn = CELERY_TASK_MAP.get(load_type)
    # This check is technically redundant due to earlier validation, but good for safety.
    if not celery_task_fn:
        # This state should ideally not be reached if validations are correct.
        logger.error(f"Critical error: Celery task not found for load_type '{load_type}' after initial validation passed.")
        raise HTTPException(status_code=500, detail=f"Internal configuration error: No Celery task for load type: {load_type}")

    try:
        task_instance = celery_task_fn.delay(
            business_id=str(user_int_business_id), # Celery tasks expect business_id as string
            session_id=new_session_orm.session_id,
            wasabi_file_path=new_session_orm.wasabi_path,
            original_filename=new_session_orm.original_filename
        )
        logger.info(f"Celery task {task_instance.id} dispatched for session_id: {new_session_orm.session_id}")
    except Exception as e_celery:
        logger.error(f"Celery Error: Failed to dispatch task for session_id {new_session_orm.session_id}: {e_celery}", exc_info=True)
        # Update DB session status to 'failed_dispatch'
        def update_status_to_failed_dispatch_sync(s_id: str, b_id: int, error_details: str):
            db_sess_dispatch_fail = None
            try:
                db_sess_dispatch_fail = get_session(business_id=b_id)
                session_to_fail = db_sess_dispatch_fail.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == s_id).first()
                if session_to_fail:
                    session_to_fail.status = "failed_celery_dispatch"
                    session_to_fail.details = error_details
                    session_to_fail.updated_at = datetime.utcnow()
                    db_sess_dispatch_fail.commit()
                logger.info(f"Updated session {s_id} to failed_celery_dispatch due to Celery dispatch error.")
            except Exception as db_e:
                logger.error(f"Failed to update session {s_id} status after Celery dispatch error: {db_e}", exc_info=True)
                if db_sess_dispatch_fail: db_sess_dispatch_fail.rollback()
            finally:
                if db_sess_dispatch_fail: db_sess_dispatch_fail.close()
        await run_in_threadpool(update_status_to_failed_dispatch_sync, s_id=new_session_orm.session_id, b_id=user_int_business_id, error_details=f"Failed to dispatch Celery task: {str(e_celery)[:1000]}")
        raise HTTPException(status_code=500, detail=f"Failed to dispatch processing task: {str(e_celery)}")

    # Return JSON response as per requirements
    return UploadResponseModel(
        message="File accepted for processing.",
        session_id=new_session_orm.session_id,
        load_type=new_session_orm.load_type,
        wasabi_path=new_session_orm.wasabi_path,
        status=new_session_orm.status, # Should be "pending"
        task_id=task_instance.id
    )

# Note: UPLOAD_SEQUENCE_DEPENDENCIES and prerequisite checks were present in the original file.
# This logic has been kept but is not actively used (pass # Placeholder).
# If these checks are required, they would also need to be implemented using
# await run_in_threadpool for any DB queries.
# For this refactor, focusing on the core upload path.
