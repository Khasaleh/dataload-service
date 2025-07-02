from fastapi import APIRouter, Depends, HTTPException, Query as FastAPIQuery
from typing import List, Optional
from sqlalchemy.orm import Session as SQLAlchemySession
from fastapi.concurrency import run_in_threadpool

from app.dependencies.auth import get_current_user
from app.models.schemas import SessionResponseSchema, SessionListResponseSchema
from app.db.models import UploadSessionOrm
from app.db.connection import get_session as get_sync_db_session # Renamed for clarity

router = APIRouter(
    prefix="/sessions",  # This prefix will result in /api/v1/sessions
    tags=["Sessions"]
)

# Helper function for synchronous DB access for get_session_by_id
def _get_session_by_id_sync(db: SQLAlchemySession, session_id_str: str, user_business_id: int) -> Optional[UploadSessionOrm]:
    session_orm = db.query(UploadSessionOrm).filter(
        UploadSessionOrm.session_id == session_id_str,
        UploadSessionOrm.business_details_id == user_business_id
    ).first()
    return session_orm

@router.get("/{session_id}", response_model=SessionResponseSchema)
async def get_upload_session_by_id(
    session_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get a specific upload session by its ID.
    The session must belong to the authenticated user's business.
    """
    user_business_id = current_user["business_id"]

    db_sync = get_sync_db_session(business_id=user_business_id)
    try:
        session_orm = await run_in_threadpool(_get_session_by_id_sync, db_sync, session_id, user_business_id)
    finally:
        if db_sync:
            await run_in_threadpool(db_sync.close) # Close session in threadpool

    if not session_orm:
        raise HTTPException(status_code=404, detail="Upload session not found or not authorized for this business.")

    return session_orm # Pydantic will convert from ORM model due to Config.from_attributes = True

# Helper function for synchronous DB access for list_sessions
def _list_sessions_sync(
    db: SQLAlchemySession,
    user_business_id: int,
    skip: int,
    limit: int,
    status: Optional[str]
) -> (List[UploadSessionOrm], int):
    query = db.query(UploadSessionOrm).filter(UploadSessionOrm.business_details_id == user_business_id)

    if status:
        query = query.filter(UploadSessionOrm.status == status)

    total_count = query.count() # Get total count before pagination for this filter

    sessions_orm_list = query.order_by(UploadSessionOrm.created_at.desc()).offset(skip).limit(limit).all()
    return sessions_orm_list, total_count

@router.get("/", response_model=SessionListResponseSchema)
async def list_upload_sessions(
    current_user: dict = Depends(get_current_user),
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = FastAPIQuery(None, description="Filter sessions by status string")
):
    """
    List upload sessions for the authenticated user's business.
    Supports pagination (skip, limit) and filtering by status.
    """
    user_business_id = current_user["business_id"]

    # Validate skip and limit to be non-negative
    if skip < 0:
        raise HTTPException(status_code=400, detail="Skip parameter cannot be negative.")
    if limit < 1: # Limit should be at least 1, or handle 0 if it means "no limit" (not typical)
        raise HTTPException(status_code=400, detail="Limit parameter must be at least 1.")

    db_sync = get_sync_db_session(business_id=user_business_id)
    try:
        sessions_orm_list, total_count = await run_in_threadpool(
            _list_sessions_sync,
            db_sync,
            user_business_id,
            skip,
            limit,
            status
        )
    finally:
        if db_sync:
            await run_in_threadpool(db_sync.close)

    return SessionListResponseSchema(items=sessions_orm_list, total=total_count)
