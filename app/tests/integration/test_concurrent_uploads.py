import pytest
import asyncio
import io
from unittest.mock import patch, MagicMock, call # ANY might be useful too

from fastapi.testclient import TestClient

# Assuming your FastAPI app instance is in app.main.app
from app.main import app
from app.dependencies.auth import get_current_user # To override this dependency
from app.models.schemas import UploadSessionModel # For type hinting or validating response structure


# Use a real TestClient instance for integration tests
client = TestClient(app)

# --- Configuration for the test ---
NUM_CONCURRENT_UPLOADS = 3 # Example: 3 concurrent uploads

# Mock user data for different businesses
MOCK_USERS = [
    {"business_id": f"biz_int_{i}", "role": "admin", "user_id": f"user_int_{i}"}
    for i in range(NUM_CONCURRENT_UPLOADS)
]

# Mock file data for different uploads
MOCK_FILES_DATA = [
    {"load_type": "brands", "filename": f"brands_{i}.csv", "content": f"brand_name\nBrand_{i}_{j}\n".encode('utf-8')}
    for i in range(NUM_CONCURRENT_UPLOADS) for j in range(1) # Simple CSV content
]
# Adjust MOCK_FILES_DATA if you want truly different load_types or more varied content per user
# For simplicity, let's make load_types cycle for more coverage
MOCK_LOAD_TYPES_CYCLE = ["brands", "products", "attributes"]
for i in range(NUM_CONCURRENT_UPLOADS):
    MOCK_FILES_DATA[i]["load_type"] = MOCK_LOAD_TYPES_CYCLE[i % len(MOCK_LOAD_TYPES_CYCLE)]


async def make_upload_request(
    client_instance: TestClient, # Though TestClient is not async, its methods are. We use it within an async test.
    user_mock: dict,
    business_id: str,
    load_type: str,
    filename: str,
    file_content: bytes
):
    """
    Helper function to make a single upload request.
    It temporarily overrides the get_current_user dependency for this specific call.
    """
    # Override dependency for this specific "request" context
    # This is the key to making concurrent calls with different users.
    app.dependency_overrides[get_current_user] = lambda: user_mock

    response = client_instance.post(
        f"/api/v1/business/{business_id}/upload/{load_type}",
        files={"file": (filename, io.BytesIO(file_content), "text/csv")}
    )

    # Crucially, clear the override after the call so it doesn't leak to other tests or calls
    # if the TestClient instance is reused across non-async tests or other asyncio events.
    # For asyncio.gather, each task having its own override set just before the call should be okay.
    app.dependency_overrides = {}
    return response


@pytest.mark.asyncio
async def test_handle_concurrent_uploads():
    """
    Integration test for handling multiple concurrent file upload requests to the API.

    Purpose:
    - Verifies that the API layer can correctly process simultaneous uploads from different
      simulated users/businesses.
    - Ensures unique session IDs are generated for each upload.
    - Confirms that parameters are correctly passed to downstream services (Wasabi, Celery).

    Scope:
    - This test focuses on the API endpoint (`/api/v1/business/{business_id}/upload/{load_type}`)
      and its immediate interactions.
    - It simulates concurrency using `asyncio.gather` with FastAPI's `TestClient`.
    - It does NOT test the full end-to-end concurrent processing by Celery workers,
      actual database concurrency for session record creation (as DB interactions in routes
      were conceptual), or the actual processing logic within Celery tasks.

    Mocking Strategy:
    - `get_current_user`: Mocked to simulate different authenticated users for each concurrent request.
      This is achieved by temporarily overriding the dependency within the `make_upload_request` helper.
    - `upload_to_wasabi`: Mocked to prevent actual file uploads to Wasabi cloud storage,
      isolating the test to the application's handling of the request.
    - `CELERY_TASK_MAP`: The dictionary mapping load types to Celery tasks is mocked. Its `get()`
      method returns a mock Celery task object whose `.delay()` method is also mocked to
      prevent actual Celery task queuing and execution.
    - Mocking is used to control the test environment, ensure reproducibility, and focus on
      the API layer's behavior.

    Key Assertions:
    - Each concurrent API request receives a successful (202 Accepted) response.
    - Each response contains a unique `session_id`.
    - Mocked `upload_to_wasabi` is called appropriately for each simulated upload.
    - The correct Celery task's `.delay()` method is called with accurate parameters
      (business_id, session_id, wasabi_path, original_filename) for each upload.

    Assumptions:
    - The FastAPI application instance (`app.main.app`) is correctly configured for testing.
    - This test is intended to be run using `pytest`.
    """

    # --- Mock External Dependencies ---
    # Patch services directly used by the app.routes.upload module.
    with patch('app.routes.upload.upload_to_wasabi', autospec=True) as mock_upload_to_wasabi, \
         patch('app.routes.upload.CELERY_TASK_MAP', new_callable=MagicMock) as mock_celery_task_map:

        # Setup a generic mock for all Celery task objects that would be retrieved from CELERY_TASK_MAP.
        # This mock represents a Celery task function (e.g., process_brands_file).
        mock_task_object_with_delay = MagicMock(name="MockCeleryTaskObject")
        # Mock the .delay() method on this task object.
        mock_general_celery_task_delay = MagicMock(name="MockCeleryTaskDelay")
        mock_general_celery_task_delay.return_value = MagicMock(id="mock_celery_task_id_123", name="MockCeleryTaskInstance") # Represents the AsyncResult
        mock_task_object_with_delay.delay = mock_general_celery_task_delay

        # Configure the mock CELERY_TASK_MAP's get() method to return our fully mocked task object.
        mock_celery_task_map.get.return_value = mock_task_object_with_delay

        # --- Prepare and Execute Concurrent Uploads using asyncio.gather ---
        # Each call to make_upload_request will simulate a client making an API request.
        # Dependency overrides for get_current_user are handled within make_upload_request.
        tasks = []
        for i in range(NUM_CONCURRENT_UPLOADS):
            user = MOCK_USERS[i]
            file_data = MOCK_FILES_DATA[i]

            tasks.append(
                make_upload_request(
                    client_instance=client, # Global TestClient
                    user_mock=user,
                    business_id=user["business_id"],
                    load_type=file_data["load_type"],
                    filename=file_data["filename"],
                    file_content=file_data["content"]
                )
            )

        responses = await asyncio.gather(*tasks)

        # --- Assertions ---
        assert len(responses) == NUM_CONCURRENT_UPLOADS

        all_session_ids = []
        expected_wasabi_calls = []
        expected_celery_delay_calls = []

        for i, response in enumerate(responses):
            user = MOCK_USERS[i]
            file_data = MOCK_FILES_DATA[i]

            assert response.status_code == 202, f"Request {i} failed: {response.text}"

            data = response.json()
            assert data["message"] == "File accepted for processing. Session created."
            assert data["load_type"] == file_data["load_type"]
            assert data["original_filename"] == file_data["filename"]
            assert data["status"] == "pending"
            assert "session_id" in data
            assert "task_id" in data # From the mocked delay().id

            session_id = data["session_id"]
            all_session_ids.append(session_id)

            expected_wasabi_path = f"uploads/{user['business_id']}/{session_id}/{file_data['load_type']}/{file_data['filename']}"
            assert data["wasabi_path"] == expected_wasabi_path

            expected_wasabi_calls.append(
                call(bucket=ANY, path=expected_wasabi_path, file_obj=ANY)
            )
            expected_celery_delay_calls.append(
                call(
                    business_id=user["business_id"],
                    session_id=session_id,
                    wasabi_file_path=expected_wasabi_path,
                    original_filename=file_data["filename"]
                )
            )

        # Assert uniqueness of session_ids
        assert len(set(all_session_ids)) == NUM_CONCURRENT_UPLOADS, "Session IDs are not unique"

        # Assert calls to mocked services
        # Wasabi calls - order might not be guaranteed by asyncio.gather
        mock_upload_to_wasabi.assert_has_calls(expected_wasabi_calls, any_order=True)
        assert mock_upload_to_wasabi.call_count == NUM_CONCURRENT_UPLOADS

        # Celery task calls - order might not be guaranteed
        mock_celery_task_map.get.call_count == NUM_CONCURRENT_UPLOADS # Ensure get was called for each

        # Check that the .delay method on the object returned by .get was called correctly
        mock_general_celery_task_delay.assert_has_calls(expected_celery_delay_calls, any_order=True)
        assert mock_general_celery_task_delay.call_count == NUM_CONCURRENT_UPLOADS

    # Clear dependency overrides after the test module finishes if necessary,
    # though individual overrides are cleared in make_upload_request.
    # Global client's overrides should be managed per test or per call.
    app.dependency_overrides = {}
```
