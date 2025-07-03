# app/tests/integration/test_upload_endpoint.py

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import io
import uuid
from typing import Generator, Any

from app.main import app # Import the FastAPI app instance
from app.db.models import UploadSessionOrm # To verify DB record
from app.core.config import settings # For WASABI_BUCKET_NAME
from app.dependencies.auth import get_current_user # Import the actual dependency

# A fixture to provide a TestClient instance
@pytest.fixture(scope="module")
def client() -> Generator[TestClient, Any, None]:
    with TestClient(app) as c:
        yield c

# Mock user data
MOCK_USER_BUSINESS_ID = 123
MOCK_USER_ID = "testuser_id_123"
MOCK_USERNAME = "test_uploader"
MOCK_COMPANY_ID_STR = f"FAZ-{MOCK_USER_ID}-{MOCK_USER_BUSINESS_ID}-2024-01-randomXYZ"
MOCK_USER_ROLES = ["admin"] # A role that has permission for 'brands'

MOCK_LOAD_TYPE = "brands"
MOCK_FILENAME = "brands.csv"

def mock_get_current_user_dependency():
    return {
        "user_id": MOCK_USER_ID,
        "username": MOCK_USERNAME,
        "business_id": MOCK_USER_BUSINESS_ID,
        "company_id_str": MOCK_COMPANY_ID_STR,
        "roles": MOCK_USER_ROLES,
    }

@pytest.fixture
def test_db_session_mock(mocker: MagicMock) -> MagicMock:
    mock_db_session = MagicMock()
    added_objects = []

    def add_obj(obj):
        added_objects.append(obj)

    def refresh_obj(obj):
        if isinstance(obj, UploadSessionOrm) and not obj.id:
            obj.id = 1 # Simulate autogen ID

    mock_db_session.add = MagicMock(side_effect=add_obj)
    mock_db_session.commit = MagicMock()
    mock_db_session.refresh = MagicMock(side_effect=refresh_obj)
    mock_db_session.close = MagicMock()

    mocker.patch("app.db.connection.get_session", return_value=mock_db_session)
    # Store added_objects on the mock so the test can access it
    mock_db_session.added_objects = added_objects
    return mock_db_session


def test_successful_file_upload(client: TestClient, test_db_session_mock: MagicMock, mocker: MagicMock):
    app.dependency_overrides[get_current_user] = mock_get_current_user_dependency

    mock_upload_to_wasabi = mocker.patch("app.services.storage.upload_file", return_value=None)
    # Ensure the full path to the specific task's delay method is mocked
    mock_celery_task_delay = MagicMock(return_value=MagicMock(id=str(uuid.uuid4())))
    mocker.patch(f"app.tasks.load_jobs.{CELERY_TASK_MAP[MOCK_LOAD_TYPE].__name__}.delay", new=mock_celery_task_delay)


    csv_content = "col1,col2\nval1,val2"
    csv_file = io.BytesIO(csv_content.encode('utf-8'))

    upload_url = f"/api/v1/business/{MOCK_USER_BUSINESS_ID}/upload/{MOCK_LOAD_TYPE}"

    response = client.post(
        upload_url,
        files={"file": (MOCK_FILENAME, csv_file, "text/csv")}
    )

    assert response.status_code == 202
    response_data = response.json()
    assert response_data["message"] == "File accepted for processing."
    assert response_data["load_type"] == MOCK_LOAD_TYPE
    # The filename in the response comes from new_session_orm.original_filename
    assert response_data["original_filename"] == MOCK_FILENAME
    assert response_data["status"] == "pending"
    assert MOCK_LOAD_TYPE in response_data["wasabi_path"]
    assert str(MOCK_USER_BUSINESS_ID) in response_data["wasabi_path"]
    assert MOCK_FILENAME in response_data["wasabi_path"]
    session_id_from_response = response_data["session_id"]
    assert uuid.UUID(session_id_from_response)

    assert test_db_session_mock.add.call_count == 1
    assert test_db_session_mock.commit.call_count == 1

    assert len(test_db_session_mock.added_objects) == 1
    added_orm_instance = test_db_session_mock.added_objects[0]
    assert isinstance(added_orm_instance, UploadSessionOrm)
    assert added_orm_instance.session_id == session_id_from_response
    assert added_orm_instance.business_details_id == MOCK_USER_BUSINESS_ID
    assert added_orm_instance.load_type == MOCK_LOAD_TYPE
    assert added_orm_instance.original_filename == MOCK_FILENAME
    assert added_orm_instance.status == "pending"
    assert added_orm_instance.wasabi_path == response_data["wasabi_path"]

    mock_upload_to_wasabi.assert_called_once()
    args, kwargs = mock_upload_to_wasabi.call_args
    assert kwargs["bucket"] == settings.WASABI_BUCKET_NAME
    assert kwargs["path"] == response_data["wasabi_path"]
    assert "file_obj" in kwargs

    mock_celery_task_delay.assert_called_once_with(
        business_id=str(MOCK_USER_BUSINESS_ID),
        session_id=session_id_from_response,
        wasabi_file_path=response_data["wasabi_path"],
        original_filename=MOCK_FILENAME
    )

    del app.dependency_overrides[get_current_user]


# Example of how CELERY_TASK_MAP is defined in app.routes.upload to make the mock path dynamic
# This is just for context, not to be included in the test file itself.
# from app.tasks.load_jobs import process_brands_file (and others)
# CELERY_TASK_MAP = { "brands": process_brands_file, ... }
# So, for "brands", the path is "app.tasks.load_jobs.process_brands_file.delay"

# A helper to get the actual Celery task map for constructing mock paths, if needed for more complex scenarios
# For this test, directly using the known task for "brands" is fine.
from app.routes.upload import CELERY_TASK_MAP
