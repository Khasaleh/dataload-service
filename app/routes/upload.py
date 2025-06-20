from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from app.dependencies.auth import get_current_user
from app.services.validator import validate_csv # This will be used by Celery task later
from app.services.storage import upload_file as upload_to_wasabi # Import wasabi upload
from app.tasks.load_jobs import ( # Assuming these will be refactored to file-level tasks
    load_product_data, load_item_data, load_meta_data,
    load_price_data, load_brand_data, load_attribute_data,
    load_return_policy_data
)
import csv # Will be used by Celery task
import io  # Will be used by Celery task
import uuid # For generating session_id
import os # For potential bucket name from env

router = APIRouter()

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
    await file.seek(0) # Reset file pointer for wasabi upload after reading for empty check

    session_id = str(uuid.uuid4())
    # Construct a unique path in Wasabi
    wasabi_path = f"uploads/{business_id}/{session_id}/{load_type}/{file.filename}"

    try:
        # Use a temporary file object for Boto3 if file.file is not suitable directly
        # For UploadFile, file.file is a SpooledTemporaryFile, which should work.
        upload_to_wasabi(bucket=WASABI_BUCKET_NAME, path=wasabi_path, file_obj=file.file)
    except Exception as e:
        # Log the exception e
        raise HTTPException(status_code=500, detail=f"Failed to upload file to Wasabi: {str(e)}")

    # Get the appropriate Celery task from the map
    celery_task = CELERY_TASK_MAP.get(load_type)
    if not celery_task: # Should have been caught earlier, but as a safeguard
        raise HTTPException(status_code=500, detail=f"No Celery task configured for load type: {load_type}")

    # Dispatch the Celery task with the Wasabi path and session_id
    # The Celery task itself will handle downloading from Wasabi, validation, and processing
    task_instance = celery_task.delay(
        business_id=business_id,
        session_id=session_id,
        wasabi_file_path=wasabi_path
        # Add original_filename if tasks need it: original_filename=file.filename
    )

    return {
        "message": "File accepted for processing.",
        "session_id": session_id,
        "task_id": task_instance.id, # Celery task ID for potential tracking
        "load_type": load_type,
        "wasabi_path": wasabi_path
    }

# The old upload_file function can be removed or commented out if this new one replaces it.
# Ensure that the Celery tasks in load_jobs.py are refactored in subsequent steps
# to accept (business_id, session_id, wasabi_file_path) and handle file-level processing.
