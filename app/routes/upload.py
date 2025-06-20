from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from app.dependencies.auth import get_current_user
# from app.services.validator import validate_csv # Not directly used in route
from app.services.storage import upload_file as upload_to_wasabi
from app.models.schemas import UploadSessionModel # Import new model
from datetime import datetime # For setting timestamps

# Placeholder for DB session - replace with actual dependency
# from app.db.connection import get_db_session_for_fastapi_dependency_wrapper as get_db_session
# For now, we'll assume db_session is available if we were to uncomment DB logic below.

import uuid # For generating session_id
import os # For potential bucket name from env

router = APIRouter()

# Define upload sequence dependencies: key is the load_type, value is a list of prerequisite load_types.
UPLOAD_SEQUENCE_DEPENDENCIES = {
    "products": ["brands", "return_policies"],
    "product_items": ["products"],
    "product_prices": ["products"],
    "meta_tags": ["products"],
    # "brands", "attributes", "return_policies" have no dependencies
}

# Define which load_types are considered "parent" types that might restrict re-upload if children exist
# This is a more advanced check, perhaps for future, not explicitly in this task but good to note.
# PARENT_LOAD_TYPES = {"brands", "return_policies", "products"}

ROLE_PERMISSIONS = {
    "admin": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags"},
    "catalog_editor": {"products", "product_items", "product_prices", "meta_tags"},
    "viewer": set(),
}

# Define a mapping from load_type to the refactored Celery tasks
# These task names are placeholders; actual task names might change when refactored in load_jobs.py
# Import the tasks by their actual function names or ensure Celery discovers them by name string.
# For direct import and clarity:
from app.tasks.load_jobs import (
    process_brands_file, process_attributes_file,
    process_return_policies_file, process_products_file,
    process_product_items_file, process_product_prices_file, process_meta_tags_file # Add new tasks
)
# ... (and eventually all other refactored file-level tasks) ...
# ... and the old tasks if they are still needed during transition or for other purposes ...

# ...
CELERY_TASK_MAP = {
    "brands": process_brands_file,
    "attributes": process_attributes_file,
    "return_policies": process_return_policies_file,
    "products": process_products_file,
    "product_items": process_product_items_file,
    "product_prices": process_product_prices_file,
    "meta_tags": process_meta_tags_file,

    # Temporarily point old load types to a generic handler or log a warning
    # if they are not yet refactored, to avoid errors.
    # Or, ensure the upload endpoint only allows refactored load_types for now.
    # For now, the upload.py already checks if load_type is in CELERY_TASK_MAP.
    # So, only 'brands' and 'attributes' will work after this change if map is updated.
}

WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME", "your-default-bucket-name") # Get bucket name from env

@router.post("/api/v1/business/{business_id}/upload/{load_type}",
             summary="Upload catalog file to Wasabi and queue for processing",
             status_code=202, # Accepted
             responses={
                 202: {
                     "description": "File uploaded to Wasabi and accepted for processing",
                     "content": {
                         "application/json": {
                             "example": {
                                 "message": "File accepted for processing.",
                                 "session_id": "some-uuid",
                                 "load_type": "products",
                                 "wasabi_path": "uploads/business_xyz/some-uuid/products/filename.csv"
                             }
                         }
                     }
                 },
                 400: {"description": "Invalid request (e.g., invalid load type, empty file, non-CSV)"},
                 403: {"description": "Permission denied"},
                 422: {"description": "Initial validation failed (e.g. if basic checks were done before wasabi upload)"}
             })
async def upload_file_to_wasabi_and_queue(
    business_id: str,
    load_type: str,
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    if business_id != user["business_id"]:
        raise HTTPException(status_code=403, detail="Token does not match business")

    role = user.get("role")
    if role not in ROLE_PERMISSIONS or load_type not in ROLE_PERMISSIONS[role]:
        raise HTTPException(status_code=403, detail="User does not have permission to upload this type")

    if load_type not in CELERY_TASK_MAP: # Check against our map now
        raise HTTPException(status_code=400, detail=f"Invalid load type: {load_type}")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename cannot be empty.")

    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV files are allowed.")

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty CSV file submitted.")
    await file.seek(0) # Reset file pointer for wasabi upload

    session_id = str(uuid.uuid4())
    wasabi_path = f"uploads/{business_id}/{session_id}/{load_type}/{file.filename}"

    # --- Database Session Placeholder ---
    # db = get_db_session() # Acquire DB session if direct DB interaction was enabled here

    # --- Upload Sequence and Concurrency Checks (Conceptual DB Query) ---
    # Conceptual: Check for active (pending/processing) uploads of the same load_type for this business_id
    # Example:
    # active_sessions = db.query(UploadSessionModel).filter(
    #     UploadSessionModel.business_id == business_id,
    #     UploadSessionModel.load_type == load_type,
    #     UploadSessionModel.status.in_(["pending", "processing"])
    # ).first()
    # if active_sessions:
    #     raise HTTPException(status_code=409, detail=f"An upload for {load_type} is already in progress.")

    # Check for prerequisite load_types
    prerequisites = UPLOAD_SEQUENCE_DEPENDENCIES.get(load_type, [])
    for prereq_load_type in prerequisites:
        # Conceptual: Check if a "completed" session exists for the prerequisite load_type
        # Example:
        # completed_prereq = db.query(UploadSessionModel).filter(
        #     UploadSessionModel.business_id == business_id,
        #     UploadSessionModel.load_type == prereq_load_type,
        #     UploadSessionModel.status == "completed"
        # ).order_by(UploadSessionModel.updated_at.desc()).first() # Get the latest one
        # if not completed_prereq:
        #     raise HTTPException(status_code=409,
        #                         detail=f"Prerequisite load type '{prereq_load_type}' must be successfully uploaded before '{load_type}'.")
        pass # Placeholder for actual DB query logic

    # --- Create UploadSession Record (Conceptual DB Save) ---
    new_session_record = UploadSessionModel(
        session_id=session_id,
        business_id=business_id,
        load_type=load_type,
        original_filename=file.filename,
        wasabi_path=wasabi_path,
        status="pending", # Initial status
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    # Conceptual: Add to DB and commit
    # try:
    #     db.add(new_session_record)
    #     db.commit()
    #     db.refresh(new_session_record)
    # except Exception as e:
    #     db.rollback()
    #     # Log e
    #     raise HTTPException(status_code=500, detail="Failed to create upload session record.")
    # finally:
    #     db.close()


    # --- Upload to Wasabi ---
    try:
        upload_to_wasabi(bucket=WASABI_BUCKET_NAME, path=wasabi_path, file_obj=file.file)
    except Exception as e:
        # Log the exception e
        # Conceptual: Update session status to "failed" if DB record was created
        # new_session_record.status = "failed"
        # new_session_record.details = f"Failed to upload to Wasabi: {str(e)}"
        # new_session_record.updated_at = datetime.utcnow()
        # db.commit() (after adding and flushing new_session_record earlier)
        raise HTTPException(status_code=500, detail=f"Failed to upload file to Wasabi: {str(e)}")

    # Get the appropriate Celery task from the map
    celery_task = CELERY_TASK_MAP.get(load_type)
    if not celery_task: # Should have been caught earlier, but as a safeguard
        raise HTTPException(status_code=500, detail=f"No Celery task configured for load type: {load_type}")

    # Dispatch the Celery task with the Wasabi path and session_id
    # The Celery task itself will handle downloading from Wasabi, validation, and processing
    task_instance = celery_task.delay(
        business_id=business_id,
        session_id=session_id, # This session_id is from the UploadSessionModel
        wasabi_file_path=wasabi_path,
        original_filename=file.filename # Pass original_filename
    )

    return {
        "message": "File accepted for processing. Session created.",
        "session_id": new_session_record.session_id,
        "task_id": task_instance.id,
        "load_type": new_session_record.load_type,
        "original_filename": new_session_record.original_filename,
        "wasabi_path": new_session_record.wasabi_path,
        "status": new_session_record.status
    }

# The old upload_file function can be removed or commented out if this new one replaces it.
# Ensure that the Celery tasks in load_jobs.py are refactored in subsequent steps
# to accept (business_id, session_id, wasabi_file_path) and handle file-level processing.
