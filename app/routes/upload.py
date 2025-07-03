import logging
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from app.dependencies.auth import get_current_user
from app.services.storage import upload_file as upload_to_wasabi
from app.db.models import UploadSessionOrm
from app.db.connection import get_session
from datetime import datetime
import uuid
import os
from typing import Optional, List
import json

# ✅ NEW IMPORT for ErrorDetailModel
from app.models import ErrorDetailModel, ErrorType

logger = logging.getLogger(__name__)
router = APIRouter()

UPLOAD_SEQUENCE_DEPENDENCIES = {
    "products": ["brands", "return_policies"],
    "product_items": ["products"],
    "product_prices": ["products"],
    "meta_tags": ["products"],
}

ROLE_PERMISSIONS = {
    "ROLE_ADMIN": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags", "categories"},
    "ROLE_INVENTORY_SPECIALIST": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags", "categories"},
    "ROLE_IT_TECHNICAL_SUPPORT": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags", "categories"},
    "ROLE_MARKETING_COORDINATOR": {"product_prices", "meta_tags"},
    "ROLE_STORE_MANAGER": {"return_policies"},
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

from pydantic import BaseModel
class UploadResponseModel(BaseModel):
    message: str
    session_id: str
    load_type: str
    wasabi_path: str
    status: str
    task_id: Optional[str] = None


def create_upload_session_in_db_sync(
    session_id_str: str,
    user_business_id: int,
    load_type_str: str,
    original_filename_str: str,
    wasabi_path_str: str
) -> UploadSessionOrm:
    db = None
    try:
        db = get_session(business_id=user_business_id)
        new_session_orm = UploadSessionOrm(
            session_id=session_id_str,
            business_details_id=user_business_id,
            load_type=load_type_str,
            original_filename=original_filename_str,
            wasabi_path=wasabi_path_str,
            status="pending",
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
        raise HTTPException(status_code=500, detail=f"Failed to create upload session record in DB: {str(e_db)}")
    finally:
        if db:
            db.close()


@router.post("/api/v1/business/{business_id}/upload/{load_type}",
             summary="Upload catalog file, store in Wasabi, create DB session, and queue for processing",
             status_code=202,
             response_model=UploadResponseModel)
async def upload_file_and_queue_for_processing(
    business_id: str,
    load_type: str,
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    try:
        path_business_id_int = int(business_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid business_id format in path. Must be an integer.")

    if path_business_id_int != user["business_id"]:
        raise HTTPException(status_code=403, detail="Token's business_id does not match business_id in path.")

    user_role = user.get("roles")[0] if user.get("roles") else "viewer"
    if user_role not in ROLE_PERMISSIONS or load_type not in ROLE_PERMISSIONS[user_role]:
        raise HTTPException(status_code=403, detail=f"User role '{user_role}' does not have permission to upload load_type '{load_type}'.")

    if load_type not in CELERY_TASK_MAP:
        raise HTTPException(status_code=400, detail=f"Invalid load type: {load_type}. Supported types are: {list(CELERY_TASK_MAP.keys())}")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename cannot be empty.")
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV files are allowed.")

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Empty CSV file submitted.")
    await file.seek(0)

    session_id_str = str(uuid.uuid4())
    wasabi_path_str = f"uploads/{path_business_id_int}/{session_id_str}/{load_type}/{file.filename}"
    user_int_business_id = user["business_id"]

    try:
        new_session_orm = await run_in_threadpool(
            create_upload_session_in_db_sync,
            session_id_str=session_id_str,
            user_business_id=user_int_business_id,
            load_type_str=load_type,
            original_filename_str=file.filename,
            wasabi_path_str=wasabi_path_str
        )
    except Exception as e:
        logger.error(f"Error during DB session creation for session {session_id_str}: {e}", exc_info=True)
        raise

    try:
        await run_in_threadpool(upload_to_wasabi, bucket=WASABI_BUCKET_NAME, path=wasabi_path_str, file_obj=file.file)
        logger.info(f"File successfully uploaded to Wasabi: {wasabi_path_str} for session_id: {session_id_str}")
    except Exception as e_wasabi:
        logger.error(f"Wasabi Error: Failed to upload file for session_id {session_id_str}: {e_wasabi}", exc_info=True)
        if new_session_orm:
            def update_status_to_failed_upload_sync(s_id: str, b_id: int, error_message: str):
                db_sess_fail = None
                try:
                    db_sess_fail = get_session(business_id=b_id)
                    session_to_fail = db_sess_fail.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == s_id).first()
                    if session_to_fail:
                        session_to_fail.status = "failed_wasabi_upload"
                        # ✅ FIX: Store JSON array of ErrorDetailModel
                        session_to_fail.details = json.dumps([
                            ErrorDetailModel(
                                error_message=error_message,
                                error_type=ErrorType.TASK_EXCEPTION
                            ).model_dump()
                        ])
                        session_to_fail.updated_at = datetime.utcnow()
                        db_sess_fail.commit()
                    logger.info(f"Updated session {s_id} to failed_wasabi_upload due to Wasabi error.")
                except Exception as db_e:
                    logger.error(f"Failed to update session {s_id} status after Wasabi error: {db_e}", exc_info=True)
                    if db_sess_fail: db_sess_fail.rollback()
                finally:
                    if db_sess_fail: db_sess_fail.close()

            await run_in_threadpool(update_status_to_failed_upload_sync,
                                    s_id=new_session_orm.session_id,
                                    b_id=user_int_business_id,
                                    error_message=f"Failed to upload to Wasabi: {str(e_wasabi)[:1000]}")
        raise HTTPException(status_code=500, detail=f"Failed to upload file to Wasabi: {str(e_wasabi)}")

    celery_task_fn = CELERY_TASK_MAP.get(load_type)
    if not celery_task_fn:
        logger.error(f"Critical error: Celery task not found for load_type '{load_type}'.")
        raise HTTPException(status_code=500, detail=f"Internal configuration error: No Celery task for load type: {load_type}")

    try:
        task_instance = celery_task_fn.delay(
            business_id=str(user_int_business_id),
            session_id=new_session_orm.session_id,
            wasabi_file_path=new_session_orm.wasabi_path,
            original_filename=new_session_orm.original_filename
        )
        logger.info(f"Celery task {task_instance.id} dispatched for session_id: {new_session_orm.session_id}")
    except Exception as e_celery:
        logger.error(f"Celery Error: Failed to dispatch task for session_id {new_session_orm.session_id}: {e_celery}", exc_info=True)

        def update_status_to_failed_dispatch_sync(s_id: str, b_id: int, error_message: str):
            db_sess_dispatch_fail = None
            try:
                db_sess_dispatch_fail = get_session(business_id=b_id)
                session_to_fail = db_sess_dispatch_fail.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == s_id).first()
                if session_to_fail:
                    session_to_fail.status = "failed_celery_dispatch"
                    # ✅ FIX: Store JSON array of ErrorDetailModel
                    session_to_fail.details = json.dumps([
                        ErrorDetailModel(
                            error_message=error_message,
                            error_type=ErrorType.TASK_EXCEPTION
                        ).model_dump()
                    ])
                    session_to_fail.updated_at = datetime.utcnow()
                    db_sess_dispatch_fail.commit()
                logger.info(f"Updated session {s_id} to failed_celery_dispatch due to Celery dispatch error.")
            except Exception as db_e:
                logger.error(f"Failed to update session {s_id} status after Celery dispatch error: {db_e}", exc_info=True)
                if db_sess_dispatch_fail: db_sess_dispatch_fail.rollback()
            finally:
                if db_sess_dispatch_fail: db_sess_dispatch_fail.close()

        await run_in_threadpool(update_status_to_failed_dispatch_sync,
                                s_id=new_session_orm.session_id,
                                b_id=user_int_business_id,
                                error_message=f"Failed to dispatch Celery task: {str(e_celery)[:1000]}")
        raise HTTPException(status_code=500, detail=f"Failed to dispatch processing task: {str(e_celery)}")

    return UploadResponseModel(
        message="File accepted for processing.",
        session_id=new_session_orm.session_id,
        load_type=new_session_orm.load_type,
        wasabi_path=new_session_orm.wasabi_path,
        status=new_session_orm.status,
        task_id=task_instance.id
    )
