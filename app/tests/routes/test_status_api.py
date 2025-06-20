import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from datetime import datetime

from app.main import app # Main FastAPI application
from app.models.schemas import UploadSessionModel

# Mock user for authentication
MOCK_USER_VALID = {"business_id": "biz_test_456", "role": "admin", "user_id": "status_user"}

# Data for mock sessions
MOCK_SESSION_1 = UploadSessionModel(
    session_id="sess_1", business_id="biz_test_456", load_type="products",
    original_filename="products.csv", wasabi_path="p/products.csv", status="completed",
    record_count=100, created_at=datetime.utcnow(), updated_at=datetime.utcnow()
)
MOCK_SESSION_2 = UploadSessionModel(
    session_id="sess_2", business_id="biz_test_456", load_type="brands",
    original_filename="brands.csv", wasabi_path="p/brands.csv", status="failed",
    details="Something went wrong", error_count=10, created_at=datetime.utcnow(), updated_at=datetime.utcnow()
)
MOCK_SESSION_DIFFERENT_BIZ = UploadSessionModel(
    session_id="sess_3", business_id="biz_other_789", load_type="products",
    original_filename="other.csv", wasabi_path="p/other.csv", status="pending"
)

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_mocks():
    # Patch get_current_user for authentication
    with patch('app.routes.status_api.get_current_user', return_value=MOCK_USER_VALID) as mock_user, \
         patch('app.routes.status_api._get_mock_session') as mock_get_single, \
         patch('app.routes.status_api._get_mock_sessions_for_business') as mock_get_all:

        # Configure side effects for mock DB helpers
        def get_single_side_effect(business_id, session_id):
            if business_id == MOCK_SESSION_1.business_id and session_id == MOCK_SESSION_1.session_id:
                return MOCK_SESSION_1
            if business_id == MOCK_SESSION_2.business_id and session_id == MOCK_SESSION_2.session_id:
                return MOCK_SESSION_2
            return None
        mock_get_single.side_effect = get_single_side_effect

        def get_all_side_effect(business_id, skip, limit):
            if business_id == MOCK_USER_VALID["business_id"]:
                all_sessions = [MOCK_SESSION_1, MOCK_SESSION_2]
                return all_sessions[skip : skip + limit]
            return []
        mock_get_all.side_effect = get_all_side_effect

        yield mock_user, mock_get_single, mock_get_all


def test_get_specific_upload_status_found():
    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/{MOCK_SESSION_1.session_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == MOCK_SESSION_1.session_id
    assert data["business_id"] == MOCK_USER_VALID["business_id"]
    assert data["status"] == "completed"

def test_get_specific_upload_status_not_found():
    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/sess_nonexistent")
    assert response.status_code == 404
    assert "Upload session not found" in response.json()["detail"]

def test_get_specific_upload_status_auth_biz_mismatch(setup_mocks):
    mock_user, _, _ = setup_mocks
    mock_user.return_value = {"business_id": "biz_wrong", "role": "admin"} # User belongs to different biz

    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/{MOCK_SESSION_1.session_id}")
    assert response.status_code == 403
    assert "User not authorized" in response.json()["detail"]


def test_get_all_upload_statuses_for_business():
    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["session_id"] == MOCK_SESSION_1.session_id
    assert data[1]["session_id"] == MOCK_SESSION_2.session_id

def test_get_all_upload_statuses_pagination():
    # Test limit
    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/?limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["session_id"] == MOCK_SESSION_1.session_id # Assuming MOCK_SESSION_1 is first

    # Test skip
    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/?skip=1&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["session_id"] == MOCK_SESSION_2.session_id # Assuming MOCK_SESSION_2 is second

def test_get_all_upload_statuses_auth_biz_mismatch(setup_mocks):
    mock_user, _, mock_get_all_sessions = setup_mocks
    mock_user.return_value = {"business_id": "biz_wrong", "role": "admin"}

    # Need to ensure the mock_get_all_sessions also returns empty for "biz_wrong" if that's the case
    # or that the permission check happens before the call.
    # The current setup_mocks patches _get_mock_sessions_for_business which has biz_id check.
    # The route has the primary auth check `if user["business_id"] != business_id:`.

    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/")
    assert response.status_code == 403
    assert "User not authorized" in response.json()["detail"]


def test_get_all_upload_statuses_no_sessions_found(setup_mocks):
    _, _, mock_get_all_sessions = setup_mocks
    mock_get_all_sessions.return_value = [] # No sessions for this business

    response = client.get(f"/api/v1/business/{MOCK_USER_VALID['business_id']}/upload_status/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0

# To run: pytest app/tests/routes/test_status_api.py
```
