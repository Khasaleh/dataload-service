import logging
import uuid
import json
from datetime import datetime
from io import BytesIO
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.dependencies.auth import get_current_user
from app.db.models import UploadSessionOrm
from app.db.connection import get_session
from app.models import ErrorDetailModel, ErrorType
from app.services.storage import upload_file as local_upload_file, delete_file as local_delete_file
from app.tasks.load_jobs import (
    process_brands_file, process_attributes_file,
    process_return_policies_file, process_products_file,
    process_product_items_file, process_product_prices_file,
    process_meta_tags_file, process_categories_file
)
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

UPLOAD_SEQUENCE_DEPENDENCIES = {
    "products": ["brands", "return_policies"],
    "product_items": ["products"],
    "product_prices": ["products"],
    "meta_tags": ["products"],
    "categories": ["products"]
}

ROLE_PERMISSIONS = {
    "ROLE_ADMIN": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags", "categories"},
    "ROLE_INVENTORY_SPECIALIST": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags", "categories"},
    "ROLE_IT_TECHNICAL_SUPPORT": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags", "categories"},
    "ROLE_MARKETING_COORDINATOR": {"product_prices", "meta_tags"},
    "ROLE_STORE_MANAGER": {"return_policies"},
    "viewer": set()
}

CELERY_TASK_MAP = {
    "brands": process_brands_file,
    "attributes": process_attributes_file,
    "return_policies": process_return_policies_file,
    "products": process_products_file,
    "product_items": process_product_items_file,
    "product_prices": process_product_prices_file,
    "meta_tags": process_meta_tags_file,
    "categories": process_categories_file,
}

class UploadResponseModel(BaseModel):
    message: str
    session_id: str
    load_type: str
    storage_path: str
    status: str
    task_id: Optional[str] = None
    tracking_id: Optional[str] = None


def create_upload_session_in_db_sync(
    session_id_str: str,
    user_business_id: int,
    load_type_str: str,
    original_filename_str: str,
    storage_path_str: str
) -> UploadSessionOrm:
    db = None
    try:
        db = get_session(business_id=user_business_id)
        new_session_orm = UploadSessionOrm(
            session_id=session_id_str,
            business_details_id=user_business_id,
            load_type=load_type_str,
            original_filename=original_filename_str,
            wasabi_path=storage_path_str,
            status="pending",
        )
        db.add(new_session_orm)
        db.commit()
        db.refresh(new_session_orm)
        logger.info(f"Upload session record created for session_id: {session_id_str}")
        return new_session_orm
    except Exception as e_db:
        logger.error("DB Error creating upload session: %s", e_db, exc_info=True)
        if db:
            db.rollback()
        raise HTTPException(status_code=500, detail=str(e_db))
    finally:
        if db:
            db.close()

@router.post(
    "/api/v1/business/{business_id}/upload/{load_type}",
    summary="Upload catalog file to local storage, create DB session, and queue processing",
    status_code=202,
    response_model=UploadResponseModel
)
async def upload_file_and_queue_for_processing(
    business_id: str,
    load_type: str,
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    # Validate business
    try:
        biz_id = int(business_id)
    except ValueError:
        raise HTTPException(400, "Invalid business_id; must be integer.")
    if biz_id != user["business_id"]:
        raise HTTPException(403, "Unauthorized business_id.")

    # Check permissions
    role = user.get("roles", ["viewer"])[0]
    if role not in ROLE_PERMISSIONS or load_type not in ROLE_PERMISSIONS[role]:
        raise HTTPException(403, "Insufficient permissions.")
    if load_type not in CELERY_TASK_MAP:
        raise HTTPException(400, f"Unsupported load_type: {load_type}")
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(400, "Only CSV files allowed.")

    # Read file content
    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(400, "Empty file.")
    file_stream = BytesIO(file_bytes)

    # Prepare session and storage
    session_id = str(uuid.uuid4())
    storage_key = f"uploads/{biz_id}/{session_id}/{load_type}/{file.filename}"

    session_orm = await run_in_threadpool(
        create_upload_session_in_db_sync,
        session_id, biz_id, load_type, file.filename, storage_key
    )

    # Store locally
    try:
        tracking_id = await run_in_threadpool(
            local_upload_file,
            file_stream,
            str(biz_id),
            storage_key
        )
    except Exception as e_loc:
        # handle storage failure
        ...

    # Dispatch Celery task including user_id
    task = CELERY_TASK_MAP[load_type].delay(
        business_id=str(biz_id),
        session_id=session_orm.session_id,
        storage_path=storage_key,
        original_filename=session_orm.original_filename,
        user_id=user['user_id']
    )

    return UploadResponseModel(
        message="File accepted.",
        session_id=session_id,
        load_type=load_type,
        storage_path=storage_key,
        status=session_orm.status,
        task_id=task.id,
        tracking_id=tracking_id
    )
