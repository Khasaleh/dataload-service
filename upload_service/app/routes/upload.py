from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Path, BackgroundTasks
from app.services.validator import validate_brands_csv, validate_attributes_csv, validate_return_policies_csv # Add new validator
from app.models.schemas import BrandValidationResult, AttributeValidationResult, ReturnPolicyValidationResult # Add new result model
from app.tasks.load_jobs import process_brands_data, process_attributes_data, process_return_policies_data # Import the new Celery task

router = APIRouter()

@router.post("/business/{business_id}/upload/brands",
             # response_model=BrandValidationResult, # Task will run in background
             status_code=202, # Accepted for processing
             tags=["Uploads"])
async def upload_brands_file(
    business_id: int = Path(..., description="The ID of the business"),
    file: UploadFile = File(..., description="brands.csv file"),
    background_tasks: BackgroundTasks = Depends() # Use BackgroundTasks for Celery
):
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV is allowed.")

    contents_bytes = await file.read()

    validation_result = await validate_brands_csv(contents_bytes)

    if not validation_result.is_valid:
        raise HTTPException(status_code=422, detail=validation_result.errors)

    # Convert bytes to string for Celery task (or pass bytes if preferred and handle decoding in task)
    try:
        contents_str = contents_bytes.decode('utf-8')
    except UnicodeDecodeError:
         # This should ideally be caught by validator, but as a safeguard:
        raise HTTPException(status_code=400, detail="File is not valid UTF-8.")

    # Dispatch Celery task
    # Using background_tasks.add_task is an alternative for simple, non-critical background work.
    # For robust, distributed task queuing, calling .delay() or .apply_async() directly is standard.
    task = process_brands_data.delay(business_id, contents_str)

    # Return a message indicating acceptance and optionally the task ID
    return {
        "message": "Brand file accepted for processing.",
        "business_id": business_id,
        "task_id": task.id # Celery task ID
    }

@router.post("/business/{business_id}/upload/attributes",
             status_code=202, # Accepted for processing
             tags=["Uploads"])
async def upload_attributes_file(
    business_id: int = Path(..., description="The ID of the business"),
    file: UploadFile = File(..., description="attributes.csv file")
    # background_tasks: BackgroundTasks = Depends() # Not needed if calling .delay directly
):
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV is allowed.")

    contents_bytes = await file.read()

    validation_result = await validate_attributes_csv(contents_bytes)

    if not validation_result.is_valid:
        raise HTTPException(status_code=422, detail=validation_result.errors)

    try:
        contents_str = contents_bytes.decode('utf-8')
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File is not valid UTF-8.")

    task = process_attributes_data.delay(business_id, contents_str)

    return {
        "message": "Attribute file accepted for processing.",
        "business_id": business_id,
        "task_id": task.id
    }

@router.post("/business/{business_id}/upload/return_policies",
             status_code=202,
             tags=["Uploads"])
async def upload_return_policies_file(
    business_id: int = Path(..., description="The ID of the business"),
    file: UploadFile = File(..., description="return_policies.csv file")
):
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV is allowed.")

    contents_bytes = await file.read()

    validation_result = await validate_return_policies_csv(contents_bytes)

    if not validation_result.is_valid:
        raise HTTPException(status_code=422, detail=validation_result.errors)

    try:
        contents_str = contents_bytes.decode('utf-8')
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File is not valid UTF-8.")

    task = process_return_policies_data.delay(business_id, contents_str)

    return {
        "message": "Return policy file accepted for processing.",
        "business_id": business_id,
        "task_id": task.id
    }
