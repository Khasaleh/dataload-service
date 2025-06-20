from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from app.models.schemas import UploadSessionModel
from app.dependencies.auth import get_current_user # Assuming this is your auth dependency

# Placeholder for DB session - replace with actual dependency and model interaction
# from app.db.connection import get_db_session # Example
# For now, we'll mock DB interactions conceptually.

router = APIRouter(
    prefix="/api/v1/business/{business_id}/upload_status",
    tags=["Upload Status"],
    # dependencies=[Depends(get_current_user)] # Apply auth to all routes in this router if needed
)

# --- Mock Database (Conceptual) ---
# In a real scenario, this would be your actual database.
# For this subtask, we'll simulate it with a dictionary.
# This mock DB will be reset on each run of the agent, so it won't persist state between tool calls.
MOCK_DB_UPLOAD_SESSIONS = {}

# Helper to add a mock session (used for testing purposes by other parts of the system conceptually)
def _add_mock_session(session: UploadSessionModel):
    MOCK_DB_UPLOAD_SESSIONS[session.session_id] = session

# Helper to get a mock session
def _get_mock_session(business_id: str, session_id: str) -> Optional[UploadSessionModel]:
    session = MOCK_DB_UPLOAD_SESSIONS.get(session_id)
    if session and session.business_id == business_id:
        return session
    return None

# Helper to get all mock sessions for a business
def _get_mock_sessions_for_business(business_id: str, skip: int = 0, limit: int = 100) -> List[UploadSessionModel]:
    sessions = [
        s for s in MOCK_DB_UPLOAD_SESSIONS.values() if s.business_id == business_id
    ]
    return sessions[skip : skip + limit]
# --- End Mock Database ---


@router.get("/{session_id}", response_model=UploadSessionModel,
            summary="Get status of a specific upload session",
            responses={
                200: {"description": "Successful operation"},
                403: {"description": "Permission denied"},
                404: {"description": "Upload session not found"}
            })
async def get_upload_session_status(
    business_id: str,
    session_id: str,
    user: dict = Depends(get_current_user) # Auth
    # db: Session = Depends(get_db_session) # DB dependency placeholder
):
    if user["business_id"] != business_id:
        raise HTTPException(status_code=403, detail="User not authorized for this business.")

    # Conceptual DB query:
    # session = db.query(UploadSessionModelDB).filter(
    #     UploadSessionModelDB.session_id == session_id,
    #     UploadSessionModelDB.business_id == business_id
    # ).first()
    session = _get_mock_session(business_id, session_id) # Using mock helper

    if not session:
        raise HTTPException(status_code=404, detail="Upload session not found.")
    return session


@router.get("/", response_model=List[UploadSessionModel],
            summary="Get status of all upload sessions for a business",
            responses={
                200: {"description": "Successful operation"},
                403: {"description": "Permission denied"}
            })
async def get_all_upload_sessions_for_business(
    business_id: str,
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records to return"),
    user: dict = Depends(get_current_user) # Auth
    # db: Session = Depends(get_db_session) # DB dependency placeholder
):
    if user["business_id"] != business_id:
        raise HTTPException(status_code=403, detail="User not authorized for this business.")

    # Conceptual DB query with pagination:
    # sessions = db.query(UploadSessionModelDB).filter(
    #     UploadSessionModelDB.business_id == business_id
    # ).offset(skip).limit(limit).all()
    sessions = _get_mock_sessions_for_business(business_id, skip, limit) # Using mock helper

    return sessions
