# session_api.py

from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query as FastAPIQuery
from typing import List, Optional
from sqlalchemy.orm import Session as SQLAlchemySession
from fastapi.concurrency import run_in_threadpool

from app.dependencies.auth import get_current_user
from app.models.schemas import SessionResponseSchema, SessionListResponseSchema
from app.db.models import UploadSessionOrm
from app.db.connection import get_session as get_sync_db_session

router = APIRouter(
    prefix="/sessions",
    tags=["Sessions"]
)

def _get_session_by_id_sync(
    db: SQLAlchemySession,
    session_id_str: str,
    user_business_id: int
) -> Optional[UploadSessionOrm]:
    return (
        db.query(UploadSessionOrm)
          .filter(
              UploadSessionOrm.session_id == session_id_str,
              UploadSessionOrm.business_details_id == user_business_id
          )
          .first()
    )

def _list_sessions_sync(
    db: SQLAlchemySession,
    user_business_id: int,
    skip: int,
    limit: int,
    status: Optional[str]
) -> (List[UploadSessionOrm], int):
    query = db.query(UploadSessionOrm).filter(
        UploadSessionOrm.business_details_id == user_business_id
    )
    if status:
        query = query.filter(UploadSessionOrm.status == status)

    total_count = query.count()
    sessions = (
        query.order_by(UploadSessionOrm.created_at.desc())
             .offset(skip)
             .limit(limit)
             .all()
    )
    return sessions, total_count

def _orm_to_response(session: UploadSessionOrm) -> SessionResponseSchema:
    """
    Copy out the ORM attributes, cast the UUID to str,
    and drop SQLAlchemy internals so that Pydantic only
    ever sees plain Python types.
    """
    data = session.__dict__.copy()
    # Remove the SQLAlchemy instance state
    data.pop("_sa_instance_state", None)
    # Cast the UUID to string
    data["session_id"] = str(data["session_id"])
    return SessionResponseSchema(**data)

@router.get("/{session_id}", response_model=SessionResponseSchema)
async def get_upload_session_by_id(
    session_id: UUID,                         # ← parse input as UUID
    current_user: dict = Depends(get_current_user)
):
    user_business_id = current_user["business_id"]
    db_sync = get_sync_db_session(business_id=user_business_id)

    try:
        session_orm = await run_in_threadpool(
            _get_session_by_id_sync,
            db_sync,
            str(session_id),                  # ← lookup via str(UUID)
            user_business_id
        )
    finally:
        if db_sync:
            await run_in_threadpool(db_sync.close)

    if not session_orm:
        raise HTTPException(
            status_code=404,
            detail="Upload session not found or not authorized for this business."
        )

    # ← return a SessionResponseSchema built from plain types
    return _orm_to_response(session_orm)

@router.get("/", response_model=SessionListResponseSchema)
async def list_upload_sessions(
    current_user: dict = Depends(get_current_user),
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = FastAPIQuery(None, description="Filter sessions by status")
):
    if skip < 0:
        raise HTTPException(status_code=400, detail="Skip parameter cannot be negative.")
    if limit < 1:
        raise HTTPException(status_code=400, detail="Limit parameter must be at least 1.")

    user_business_id = current_user["business_id"]
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

    # map each ORM → Pydantic model (with session_id as str)
    items = [ _orm_to_response(s) for s in sessions_orm_list ]
    return SessionListResponseSchema(items=items, total=total_count)
