import pytest
from unittest.mock import patch, MagicMock, call
import io

# Module to test
from app.tasks import load_jobs # Imports the module to find process_csv_task etc.
from app.tasks.load_jobs import process_csv_task # Direct import for easier patching if needed

# Sample data
SAMPLE_CSV_CONTENT_VALID = "col1,col2\nval1,val2\nval3,val4"
SAMPLE_CSV_RECORDS_VALID = [{"col1": "val1", "col2": "val2"}, {"col1": "val3", "col2": "val4"}]

SAMPLE_BUSINESS_ID = "biz_123"
SAMPLE_SESSION_ID = "sess_abc"
SAMPLE_WASABI_PATH = "uploads/some/file.csv"
SAMPLE_ORIGINAL_FILENAME = "file.csv"
SAMPLE_RECORD_KEY = "col1"
SAMPLE_ID_PREFIX = "myprefix"
SAMPLE_MAP_TYPE = "products" # Assuming 'products' is a valid map_type / load_type


@pytest.fixture
def mock_wasabi_client():
    with patch('app.tasks.load_jobs.wasabi_client', autospec=True) as mock_client:
        # Mock get_object response
        mock_response = MagicMock()
        mock_response['Body'].read.return_value = SAMPLE_CSV_CONTENT_VALID.encode('utf-8')
        mock_client.get_object.return_value = mock_response
        yield mock_client

@pytest.fixture
def mock_db_session_get():
    # Mocks get_session from app.db.connection, assuming it's imported in load_jobs
    with patch('app.tasks.load_jobs.get_session') as mock_get:
        mock_session_instance = MagicMock()
        mock_get.return_value = mock_session_instance
        yield mock_get

@pytest.fixture
def mock_redis_client():
    # Mocks redis_client used in load_jobs
    with patch('app.tasks.load_jobs.redis_client', autospec=True) as mock_redis:
        # Configure pipeline() to return a new MagicMock each time it's called
        # This allows us to distinguish between the pipeline for HSETs and the pipeline for EXPIRE
        mock_hset_pipeline = MagicMock(name="hset_pipeline")
        mock_expire_pipeline = MagicMock(name="expire_pipeline")

        # Set up side_effect to return different pipeline mocks if needed, or a default one
        # For this test, we'll make it return distinct mocks for two calls
        mock_redis.pipeline.side_effect = [mock_hset_pipeline, mock_expire_pipeline, MagicMock(), MagicMock()] # Add more if other tests call pipeline more

        yield mock_redis

@pytest.fixture
def mock_validate_csv():
    with patch('app.tasks.load_jobs.validate_csv') as mock_validate:
        # Default: successful validation, returns all records as valid
        mock_validate.return_value = ([], SAMPLE_CSV_RECORDS_VALID)
        yield mock_validate

@pytest.fixture
def mock_update_session_status():
    # Mocks the conceptual _update_session_status function
    with patch('app.tasks.load_jobs._update_session_status') as mock_update:
        yield mock_update


def test_process_csv_task_success(
    mock_wasabi_client, mock_db_session_get, mock_redis_client,
    mock_validate_csv, mock_update_session_status
):
    result = process_csv_task(
        business_id=SAMPLE_BUSINESS_ID,
        session_id=SAMPLE_SESSION_ID,
        wasabi_file_path=SAMPLE_WASABI_PATH,
        original_filename=SAMPLE_ORIGINAL_FILENAME,
        record_key=SAMPLE_RECORD_KEY,
        id_prefix=SAMPLE_ID_PREFIX,
        map_type=SAMPLE_MAP_TYPE
    )

    assert result["status"] == "success"
    assert result["processed_count"] == len(SAMPLE_CSV_RECORDS_VALID)
    assert result["session_id"] == SAMPLE_SESSION_ID

    mock_wasabi_client.get_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)
    mock_validate_csv.assert_called_once()
    # Check if add_to_id_map was called (indirectly via redis_pipeline.hset)
    # Calls to hset are: (name, key, value)
    # f"{get_id_map_key(session_id)}:{map_type}"
    expected_redis_hset_calls = [
        call.hset(f"id_map:session:{SAMPLE_SESSION_ID}:{SAMPLE_MAP_TYPE}", "val1", f"{SAMPLE_ID_PREFIX}:val1"),
        call.hset(f"id_map:session:{SAMPLE_SESSION_ID}:{SAMPLE_MAP_TYPE}", "val3", f"{SAMPLE_ID_PREFIX}:val3")
    ]

    # The first call to mock_redis_client.pipeline() returns mock_hset_pipeline
    mock_hset_pipeline = mock_redis_client.pipeline.side_effect[0]
    mock_hset_pipeline.hset.assert_has_calls(expected_redis_hset_calls, any_order=True)
    mock_hset_pipeline.execute.assert_called_once()

    # The second call to mock_redis_client.pipeline() returns mock_expire_pipeline
    mock_expire_pipeline = mock_redis_client.pipeline.side_effect[1]
    expected_key_to_expire = f"id_map:session:{SAMPLE_SESSION_ID}:{SAMPLE_MAP_TYPE}"
    mock_expire_pipeline.expire.assert_called_once_with(expected_key_to_expire, time=load_jobs.REDIS_SESSION_TTL_SECONDS)
    mock_expire_pipeline.execute.assert_called_once()

    mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)

    # Check status updates
    expected_status_calls = [
        call(SAMPLE_SESSION_ID, status="processing"),
        call(SAMPLE_SESSION_ID, status="completed", record_count=len(SAMPLE_CSV_RECORDS_VALID))
    ]
    mock_update_session_status.assert_has_calls(expected_status_calls)


def test_process_csv_task_wasabi_download_error(
    mock_wasabi_client, mock_update_session_status
):
    mock_wasabi_client.get_object.side_effect = Exception("S3 Download Failed")

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        SAMPLE_RECORD_KEY, SAMPLE_ID_PREFIX, SAMPLE_MAP_TYPE
    )

    assert result["status"] == "error"
    assert "S3 Download Failed" in result["message"]
    mock_update_session_status.assert_any_call(SAMPLE_SESSION_ID, status="processing")
    mock_update_session_status.assert_any_call(SAMPLE_SESSION_ID, status="failed", details="Exception('S3 Download Failed')")


def test_process_csv_task_empty_file(
    mock_wasabi_client, mock_update_session_status
):
    mock_wasabi_client.get_object.return_value['Body'].read.return_value = b"" # Empty CSV

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        SAMPLE_RECORD_KEY, SAMPLE_ID_PREFIX, SAMPLE_MAP_TYPE
    )
    assert result["status"] == "no_data"
    # Status should be updated to processing, then potentially to failed or a specific "no_data_found" status.
    # Current _update_session_status in load_jobs.py does not have a specific state for "no_data" after "processing".
    # Depending on desired behavior, it might go to "failed" or "completed" with 0 records.
    # For now, let's assume it eventually calls failed if no_data is an error for the session.
    # The task returns before setting a final status in this case.
    # Based on current code, it updates to "processing", then returns. The "finally" block won't update status.
    # This might be an area to refine in load_jobs.py - what should session status be for "no_data"?
    # For now, just check "processing" was called.
    mock_update_session_status.assert_any_call(SAMPLE_SESSION_ID, status="processing")
    # And no "completed" or "failed" call from the main try-except. (This depends on how no_data is handled by caller)


def test_process_csv_task_validation_errors(
    mock_wasabi_client, mock_validate_csv, mock_update_session_status
):
    validation_errs = [{"row": 1, "field": "col1", "error": "is bad"}]
    mock_validate_csv.return_value = (validation_errs, []) # Has errors, no valid rows

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        SAMPLE_RECORD_KEY, SAMPLE_ID_PREFIX, SAMPLE_MAP_TYPE
    )

    assert result["status"] == "validation_failed"
    assert result["errors"] == validation_errs

    expected_status_calls = [
        call(SAMPLE_SESSION_ID, status="processing"),
        call(SAMPLE_SESSION_ID, status="validation_failed", details=str(validation_errs), error_count=len(validation_errs))
    ]
    mock_update_session_status.assert_has_calls(expected_status_calls)
    mock_wasabi_client.delete_object.assert_not_called() # No cleanup on validation failure


def test_process_csv_task_redis_error_on_add(
    mock_wasabi_client, mock_redis_client, mock_validate_csv, mock_update_session_status
):
    mock_redis_client.pipeline.return_value.execute.side_effect = Exception("Redis Write Error")

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        SAMPLE_RECORD_KEY, SAMPLE_ID_PREFIX, SAMPLE_MAP_TYPE
    )

    assert result["status"] == "error"
    assert "Redis Write Error" in result["message"]
    mock_update_session_status.assert_any_call(SAMPLE_SESSION_ID, status="processing")
    mock_update_session_status.assert_any_call(SAMPLE_SESSION_ID, status="failed", details="Exception('Redis Write Error')")
    mock_wasabi_client.delete_object.assert_not_called() # No cleanup on error during processing


def test_process_csv_task_wasabi_cleanup_error_does_not_fail_task(
    mock_wasabi_client, mock_validate_csv, mock_update_session_status, mock_redis_client, mock_db_session_get
):
    mock_wasabi_client.delete_object.side_effect = Exception("S3 Delete Failed")

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        SAMPLE_RECORD_KEY, SAMPLE_ID_PREFIX, SAMPLE_MAP_TYPE
    )

    assert result["status"] == "success" # Task itself succeeds
    assert result["processed_count"] == len(SAMPLE_CSV_RECORDS_VALID)

    # Check that delete was attempted
    mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)

    # Status should still be completed
    expected_status_calls = [
        call(SAMPLE_SESSION_ID, status="processing"),
        # The log message about cleanup error would be in logs, not session details by default.
        call(SAMPLE_SESSION_ID, status="completed", record_count=len(SAMPLE_CSV_RECORDS_VALID))
    ]
    mock_update_session_status.assert_has_calls(expected_status_calls)

# To run these tests (ensure pytest is installed and in the correct directory):
# Terminal: pip install pytest
# Then: pytest app/tests/tasks/test_load_jobs.py
```
