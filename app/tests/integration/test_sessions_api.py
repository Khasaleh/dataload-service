import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from typing import Generator, Any, List
import uuid
from datetime import datetime

from app.main import app # Import the FastAPI app instance
from app.dependencies.auth import get_current_user # Import the actual dependency
from app.db.models import UploadSessionOrm

# Mock user data
MOCK_USER_BUSINESS_ID_SESSIONS = 789
MOCK_USER_ID_SESSIONS = "session_test_user"
MOCK_USERNAME_SESSIONS = "session_user"
MOCK_COMPANY_ID_STR_SESSIONS = f"FAZ-{MOCK_USER_ID_SESSIONS}-{MOCK_USER_BUSINESS_ID_SESSIONS}-2024-03-randomDEF"
MOCK_USER_ROLES_SESSIONS = ["admin"]

def mock_get_current_user_for_sessions_endpoint():
    return {
        "user_id": MOCK_USER_ID_SESSIONS,
        "username": MOCK_USERNAME_SESSIONS,
        "business_id": MOCK_USER_BUSINESS_ID_SESSIONS,
        "company_id_str": MOCK_COMPANY_ID_STR_SESSIONS,
        "roles": MOCK_USER_ROLES_SESSIONS,
    }

@pytest.fixture(scope="module")
def client() -> Generator[TestClient, Any, None]:
    with TestClient(app) as c:
        yield c

@pytest.fixture
def mock_db_session_for_sessions(mocker: MagicMock) -> MagicMock:
    mock_db = MagicMock()
    mocker.patch("app.db.connection.get_session", return_value=mock_db)
    return mock_db

# --- Tests for GET /api/v1/sessions/{session_id} ---

def test_get_session_by_id_success(client: TestClient, mock_db_session_for_sessions: MagicMock, mocker: MagicMock):
    app.dependency_overrides[get_current_user] = mock_get_current_user_for_sessions_endpoint

    test_session_id = str(uuid.uuid4())
    mock_session_orm = UploadSessionOrm(
        id=1,
        session_id=test_session_id,
        business_details_id=MOCK_USER_BUSINESS_ID_SESSIONS,
        load_type="brands",
        original_filename="brands.csv",
        wasabi_path=f"uploads/{MOCK_USER_BUSINESS_ID_SESSIONS}/{test_session_id}/brands/brands.csv",
        status="completed",
        details="Processed 10 records.",
        record_count=10,
        error_count=0,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )

    # Mock the internal synchronous helper function's DB interaction part
    # The helper _get_session_by_id_sync is called via run_in_threadpool
    # So we mock what the helper returns after it uses the db session
    mocker.patch("app.routes.sessions_api._get_session_by_id_sync", return_value=mock_session_orm)

    response = client.get(f"/api/v1/sessions/{test_session_id}")

    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == test_session_id
    assert data["business_details_id"] == MOCK_USER_BUSINESS_ID_SESSIONS
    assert data["status"] == "completed"
    assert data["record_count"] == 10

    del app.dependency_overrides[get_current_user]

def test_get_session_by_id_not_found(client: TestClient, mock_db_session_for_sessions: MagicMock, mocker: MagicMock):
    app.dependency_overrides[get_current_user] = mock_get_current_user_for_sessions_endpoint

    test_session_id = str(uuid.uuid4())
    mocker.patch("app.routes.sessions_api._get_session_by_id_sync", return_value=None) # Simulate not found

    response = client.get(f"/api/v1/sessions/{test_session_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Upload session not found or not authorized for this business."

    del app.dependency_overrides[get_current_user]

# --- Tests for GET /api/v1/sessions ---

def test_list_sessions_success(client: TestClient, mock_db_session_for_sessions: MagicMock, mocker: MagicMock):
    app.dependency_overrides[get_current_user] = mock_get_current_user_for_sessions_endpoint

    mock_sessions_list = [
        UploadSessionOrm(id=i, session_id=str(uuid.uuid4()), business_details_id=MOCK_USER_BUSINESS_ID_SESSIONS, load_type="brands", status="completed", created_at=datetime.utcnow(), updated_at=datetime.utcnow())
        for i in range(3)
    ]
    mock_total_count = len(mock_sessions_list)

    mocker.patch("app.routes.sessions_api._list_sessions_sync", return_value=(mock_sessions_list, mock_total_count))

    response = client.get("/api/v1/sessions?limit=5&skip=0")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == mock_total_count
    assert len(data["items"]) == mock_total_count
    if mock_total_count > 0:
        assert data["items"][0]["business_details_id"] == MOCK_USER_BUSINESS_ID_SESSIONS

    del app.dependency_overrides[get_current_user]

def test_list_sessions_with_status_filter(client: TestClient, mock_db_session_for_sessions: MagicMock, mocker: MagicMock):
    app.dependency_overrides[get_current_user] = mock_get_current_user_for_sessions_endpoint

    # Simulate that the _list_sessions_sync function handles the status filter correctly
    # For this test, we just care that the 'status' param is passed down.
    # The mock will simulate a filtered result.
    mock_filtered_list = [
        UploadSessionOrm(id=1, session_id=str(uuid.uuid4()), business_details_id=MOCK_USER_BUSINESS_ID_SESSIONS, load_type="products", status="pending", created_at=datetime.utcnow(), updated_at=datetime.utcnow())
    ]
    mock_filtered_count = 1

    # We need to capture args passed to the mocked sync function
    sync_list_mock = mocker.patch("app.routes.sessions_api._list_sessions_sync", return_value=(mock_filtered_list, mock_filtered_count))

    response = client.get("/api/v1/sessions?status=pending")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == mock_filtered_count
    assert len(data["items"]) == mock_filtered_count
    if mock_filtered_count > 0:
        assert data["items"][0]["status"] == "pending"

    # Check if the status was passed to the sync function
    sync_list_mock.assert_called_once()
    call_args = sync_list_mock.call_args[0] # Positional arguments
    assert call_args[4] == "pending" # status is the 5th arg (0-indexed) to _list_sessions_sync

    del app.dependency_overrides[get_current_user]

def test_list_sessions_empty(client: TestClient, mock_db_session_for_sessions: MagicMock, mocker: MagicMock):
    app.dependency_overrides[get_current_user] = mock_get_current_user_for_sessions_endpoint

    mocker.patch("app.routes.sessions_api._list_sessions_sync", return_value=([], 0)) # Simulate empty result

    response = client.get("/api/v1/sessions")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert len(data["items"]) == 0

    del app.dependency_overrides[get_current_user]

def test_list_sessions_pagination_validation(client: TestClient):
    app.dependency_overrides[get_current_user] = mock_get_current_user_for_sessions_endpoint

    response_skip = client.get("/api/v1/sessions?skip=-1")
    assert response_skip.status_code == 400
    assert "Skip parameter cannot be negative" in response_skip.json()["detail"]

    response_limit = client.get("/api/v1/sessions?limit=0")
    assert response_limit.status_code == 400
    assert "Limit parameter must be at least 1" in response_limit.json()["detail"]

    del app.dependency_overrides[get_current_user]
