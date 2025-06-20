import pytest
from unittest.mock import patch, MagicMock, call
import io

# Module to test
from app.tasks import load_jobs
from app.tasks.load_jobs import process_csv_task, _update_session_status # Import function to test directly
from app.db.models import ( # Import ORM Models
    BrandOrm, AttributeOrm, ReturnPolicyOrm, ProductOrm,
    ProductItemOrm, ProductPriceOrm, MetaTagOrm, UploadSessionOrm
)
from app.models.schemas import BrandModel # For constructing validated_records if needed
import datetime # For _update_session_status test

# Sample data
# For 'brands' map_type, record_key='brand_name'
SAMPLE_CSV_CONTENT_BRANDS = "brand_name\nBrand Alpha\nBrand Beta" # Used if not mocking get_object's content directly for this test
SAMPLE_BRAND_RECORDS_VALIDATED = [ # This is what validate_csv would return
    BrandModel(brand_name="Brand Alpha").model_dump(),
    BrandModel(brand_name="Brand Beta").model_dump()
]

SAMPLE_BUSINESS_ID = "biz_123"
SAMPLE_SESSION_ID = "sess_abc"
SAMPLE_WASABI_PATH = "uploads/some/brands.csv" # Specific for brands test
SAMPLE_ORIGINAL_FILENAME = "brands.csv" # Specific for brands test
# SAMPLE_RECORD_KEY, SAMPLE_ID_PREFIX, SAMPLE_MAP_TYPE will be set in specific tests or fixtures.


@pytest.fixture
def mock_wasabi_client():
    with patch('app.tasks.load_jobs.wasabi_client', autospec=True) as mock_client:
        # Mock get_object response
        mock_response = MagicMock()
        mock_response['Body'].read.return_value = SAMPLE_CSV_CONTENT_VALID.encode('utf-8')
        mock_client.get_object.return_value = mock_response
        yield mock_client

@pytest.fixture
def mock_db_session_get(): # This patches 'app.tasks.load_jobs.get_session'
    with patch('app.tasks.load_jobs.get_session') as mock_get_session_func:
        # Configure the mock_get_session_func to return a new MagicMock (session) each time it's called by default
        # This helps isolate session instances if get_session is called multiple times across different parts of code under test.
        mock_get_session_func.return_value = MagicMock(name="db_session_mock_instance")
        yield mock_get_session_func

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
        mock_validate.return_value = ([], []) # Default to no records to avoid unexpected DB calls in unrelated tests
        yield mock_validate

@pytest.fixture # Renamed from mock_update_session_status to avoid conflict when testing the actual function
def mock_update_session_status_func():
    with patch('app.tasks.load_jobs._update_session_status') as mock_update:
        yield mock_update

# This fixture will be used specifically for test_process_csv_task_success_brands_new_only
@pytest.fixture
def mock_validate_csv_for_brands():
    with patch('app.tasks.load_jobs.validate_csv') as mock_validate:
        # Use the brands sample data
        mock_validate.return_value = ([], SAMPLE_BRAND_RECORDS_VALIDATED)
        yield mock_validate

@pytest.fixture
def mock_add_to_id_map_func(): # Specific fixture for add_to_id_map, if needed per test
    with patch('app.tasks.load_jobs.add_to_id_map') as mock_add:
        yield mock_add


# This test is being replaced by test_process_csv_task_success_brands_new_only
# def test_process_csv_task_success(
#     mock_wasabi_client, mock_db_session_get, mock_redis_client,
#     mock_validate_csv, mock_update_session_status_func # use renamed fixture
# ):
#     # This test needs to be updated significantly to mock DB interactions
#     # For now, it's commented out and replaced by a more specific test below.
#     pass

#     # Example of old assertions, these will be part of the new test or other tests
#     # assert result["status"] == "success"
#     # assert result["processed_count"] == len(SAMPLE_CSV_RECORDS_VALID)
#     # assert result["session_id"] == SAMPLE_SESSION_ID

#     # mock_wasabi_client.get_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)
#     # mock_validate_csv.assert_called_once()
#     # expected_redis_hset_calls = [
#     #     call.hset(f"id_map:session:{SAMPLE_SESSION_ID}:{SAMPLE_MAP_TYPE}", "val1", f"{SAMPLE_ID_PREFIX}:val1"),
#     #     call.hset(f"id_map:session:{SAMPLE_SESSION_ID}:{SAMPLE_MAP_TYPE}", "val3", f"{SAMPLE_ID_PREFIX}:val3")
#     # ]

#     # mock_hset_pipeline = mock_redis_client.pipeline.side_effect[0]
#     # mock_hset_pipeline.hset.assert_has_calls(expected_redis_hset_calls, any_order=True)
#     # mock_hset_pipeline.execute.assert_called_once()

#     # mock_expire_pipeline = mock_redis_client.pipeline.side_effect[1]
#     # expected_key_to_expire = f"id_map:session:{SAMPLE_SESSION_ID}:{SAMPLE_MAP_TYPE}"
#     # mock_expire_pipeline.expire.assert_called_once_with(expected_key_to_expire, time=load_jobs.REDIS_SESSION_TTL_SECONDS)
#     # mock_expire_pipeline.execute.assert_called_once()

#     # mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)

#     # expected_status_calls = [
#     #     call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"), # Added business_id
#     #     call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="completed", record_count=len(SAMPLE_CSV_RECORDS_VALID))
#     # ]
#     # mock_update_session_status_func.assert_has_calls(expected_status_calls)


@patch('app.tasks.load_jobs.add_to_id_map') # Patch add_to_id_map for this specific, more detailed test
def test_process_csv_task_success_brands_new_only(
    mock_add_to_id_map_in_test, # Renamed to avoid conflict with fixture name
    mock_wasabi_client,
    mock_db_session_get, # This is the mock for get_session function itself
    mock_redis_client,
    mock_validate_csv_for_brands, # Specific fixture for this test
    mock_update_session_status_func
):
    # --- Configure Mocks for this specific test ---
    mock_db_session = mock_db_session_get.return_value # This is the MagicMock for the session instance
    mock_db_session.query(BrandOrm).filter_by().first.return_value = None # Simulate all brands are new

    # Simulate DB assigning an ID on flush
    added_instances = []
    def capture_and_set_id(instance):
        nonlocal added_instances
        # Simulate simple auto-increment ID for testing
        instance.id = len(added_instances) + 1
        added_instances.append(instance)
    # When add is called, it will trigger this side_effect.
    # flush() itself doesn't need a side_effect here if we set id directly in add's side_effect.
    mock_db_session.add.side_effect = capture_and_set_id

    map_type = "brands"
    record_key = "brand_name"
    id_prefix = "brand"

    # Override wasabi client to return brand-specific CSV content for this test
    mock_wasabi_client.get_object.return_value['Body'].read.return_value = SAMPLE_CSV_CONTENT_BRANDS.encode('utf-8')

    result = process_csv_task(
        business_id=SAMPLE_BUSINESS_ID,
        session_id=SAMPLE_SESSION_ID,
        wasabi_file_path=SAMPLE_WASABI_PATH,
        original_filename=SAMPLE_ORIGINAL_FILENAME,
        record_key=record_key,
        id_prefix=id_prefix,
        map_type=map_type
    )

    assert result["status"] == "success"
    assert result["processed_db_count"] == len(SAMPLE_BRAND_RECORDS_VALIDATED)

    mock_wasabi_client.get_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)
    mock_validate_csv_for_brands.assert_called_once()

    # DB assertions
    assert mock_db_session.add.call_count == len(SAMPLE_BRAND_RECORDS_VALIDATED)
    assert mock_db_session.flush.call_count == len(SAMPLE_BRAND_RECORDS_VALIDATED)
    mock_db_session.commit.assert_called_once()

    assert len(added_instances) == 2
    assert isinstance(added_instances[0], BrandOrm)
    assert added_instances[0].brand_name == "Brand Alpha"
    assert added_instances[0].business_id == SAMPLE_BUSINESS_ID
    assert added_instances[0].id == 1
    assert isinstance(added_instances[1], BrandOrm)
    assert added_instances[1].brand_name == "Brand Beta"
    assert added_instances[1].id == 2

    # Redis DB PK mapping assertions using the mock_add_to_id_map_in_test
    db_pk_map_suffix = "_db_pk"
    expected_db_pk_redis_calls = [
        call(SAMPLE_SESSION_ID, f"{map_type}{db_pk_map_suffix}", "Brand Alpha", 1, pipeline=mock_redis_client.pipeline.side_effect[1]), # db_pk_redis_pipeline
        call(SAMPLE_SESSION_ID, f"{map_type}{db_pk_map_suffix}", "Brand Beta", 2, pipeline=mock_redis_client.pipeline.side_effect[1])
    ]
    db_pk_calls = [c for c in mock_add_to_id_map_in_test.call_args_list if c[0][1].endswith(db_pk_map_suffix)]
    assert len(db_pk_calls) == len(expected_db_pk_redis_calls)
    for expected_call in expected_db_pk_redis_calls:
        assert expected_call in db_pk_calls

    # Redis string ID mapping assertions
    expected_string_id_redis_calls = [
        call(SAMPLE_SESSION_ID, map_type, "Brand Alpha", f"{id_prefix}:brand_alpha", pipeline=mock_redis_client.pipeline.side_effect[0]), # string_id_redis_pipeline
        call(SAMPLE_SESSION_ID, map_type, "Brand Beta", f"{id_prefix}:brand_beta", pipeline=mock_redis_client.pipeline.side_effect[0])
    ]
    string_id_calls = [c for c in mock_add_to_id_map_in_test.call_args_list if not c[0][1].endswith(db_pk_map_suffix)]
    assert len(string_id_calls) == len(expected_string_id_redis_calls)
    for expected_call in expected_string_id_redis_calls:
        assert expected_call in string_id_calls

    # Redis pipeline execution
    assert mock_redis_client.pipeline.call_count == 4 # string_id, db_pk, ttl_string, ttl_db_pk
    mock_redis_client.pipeline.side_effect[0].execute.assert_called_once()
    mock_redis_client.pipeline.side_effect[1].execute.assert_called_once()
    mock_redis_client.pipeline.side_effect[2].expire.assert_called_once_with(
        f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}", time=load_jobs.REDIS_SESSION_TTL_SECONDS
    )
    mock_redis_client.pipeline.side_effect[2].execute.assert_called_once()
    mock_redis_client.pipeline.side_effect[3].expire.assert_called_once_with(
        f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}{db_pk_map_suffix}", time=load_jobs.REDIS_SESSION_TTL_SECONDS
    )
    mock_redis_client.pipeline.side_effect[3].execute.assert_called_once()

    mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)

    expected_status_calls = [
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"),
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="completed", record_count=len(SAMPLE_BRAND_RECORDS_VALIDATED), error_count=0)
    ]
    mock_update_session_status_func.assert_has_calls(expected_status_calls)


# --- Tests for _update_session_status ---

@patch('app.tasks.load_jobs.get_session') # Patch get_session used by _update_session_status
def test_update_session_status_found_and_updated(mock_get_session_for_status_update):
    mock_db = MagicMock() # This is the mock for the session instance
    mock_session_orm = MagicMock(spec=UploadSessionOrm)
    mock_get_session_for_status_update.return_value = mock_db # get_session() returns mock_db
    mock_db.query(UploadSessionOrm).filter().first.return_value = mock_session_orm

    _update_session_status(
        session_id="test_sess_id",
        business_id="biz_for_status",
        status="new_status",
        details="new details",
        record_count=10,
        error_count=1
    )

    mock_get_session_for_status_update.assert_called_once_with(business_id="biz_for_status")
    # Query was made on the mock_db object
    mock_db.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == "test_sess_id").first.assert_called_once()

    assert mock_session_orm.status == "new_status"
    assert mock_session_orm.details == "new details"
    assert mock_session_orm.record_count == 10
    assert mock_session_orm.error_count == 1
    assert isinstance(mock_session_orm.updated_at, datetime.datetime)
    mock_db.commit.assert_called_once()
    mock_db.close.assert_called_once()


@patch('app.tasks.load_jobs.get_session')
def test_update_session_status_not_found(mock_get_session_for_status_update):
    mock_db = MagicMock()
    mock_get_session_for_status_update.return_value = mock_db
    mock_db.query(UploadSessionOrm).filter().first.return_value = None # Simulate session not found

    _update_session_status("test_sess_id", "biz_for_status", "new_status")

    mock_db.commit.assert_not_called()
    mock_db.close.assert_called_once()


@patch('app.tasks.load_jobs.get_session')
def test_update_session_status_db_error_on_commit(mock_get_session_for_status_update):
    mock_db = MagicMock()
    mock_session_orm = MagicMock(spec=UploadSessionOrm)
    mock_get_session_for_status_update.return_value = mock_db
    mock_db.query(UploadSessionOrm).filter().first.return_value = mock_session_orm
    mock_db.commit.side_effect = Exception("DB Commit Error")

    _update_session_status("test_sess_id", "biz_for_status", "new_status")

    mock_db.rollback.assert_called_once()
    mock_db.close.assert_called_once()


# The following original tests are kept but might need adjustments for their specific map_types
# if we want to test their DB interaction logic in detail like done for 'brands'.
# For now, their primary purpose (testing error conditions before DB logic or higher-level flows)
# might still be valid if their mock_validate_csv is not returning records that would go to DB.
# If they *do* go to DB part, their db_session mocks need to be configured.
# For simplicity in this pass, I'll assume their existing mocks for validate_csv
# (which returns generic SAMPLE_CSV_RECORDS_VALID) would hit the generic ORM class mapping error
# in the new DB logic, or we'd need to provide specific valid_records and ORM instance mocks for them.

@pytest.fixture
def mock_validate_csv_generic_no_records(): # New fixture for other tests
    with patch('app.tasks.load_jobs.validate_csv') as mock_validate:
        mock_validate.return_value = ([], []) # No validation errors, no records
        yield mock_validate


def test_process_csv_task_wasabi_download_error( # Pass business_id to mock_update_session_status_func
    mock_wasabi_client, mock_update_session_status_func
):
    mock_wasabi_client.get_object.side_effect = Exception("S3 Download Failed")

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        "some_record_key", "some_prefix", "some_map_type" # Generic values
    )

    assert result["status"] == "error"
    assert "S3 Download Failed" in result["message"]
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing")
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="failed", details="Exception('S3 Download Failed')")


def test_process_csv_task_empty_file( # Pass business_id
    mock_wasabi_client, mock_update_session_status_func
):
    mock_wasabi_client.get_object.return_value['Body'].read.return_value = b""

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        "some_record_key", "some_prefix", "some_map_type"
    )
    assert result["status"] == "no_data"
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing")


def test_process_csv_task_validation_errors( # Pass business_id
    mock_wasabi_client, mock_validate_csv, mock_update_session_status_func
):
    validation_errs = [{"row": 1, "field": "col1", "error": "is bad"}]
    mock_validate_csv.return_value = (validation_errs, [])

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        "some_record_key", "some_prefix", "some_map_type"
    )

    assert result["status"] == "validation_failed"
    assert result["errors"] == validation_errs

    expected_status_calls = [
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"),
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="validation_failed", details=str(validation_errs), error_count=len(validation_errs))
    ]
    mock_update_session_status_func.assert_has_calls(expected_status_calls)
    mock_wasabi_client.delete_object.assert_not_called()


@patch('app.tasks.load_jobs.validate_csv', return_value = ([], [])) # Ensure no records go to DB part for this old test
def test_process_csv_task_redis_error_on_add( # Pass business_id, mock validate_csv to return no records
    mock_validate_csv_empty, # Use the new fixture
    mock_wasabi_client,
    mock_redis_client,
    # mock_validate_csv, # This is now the new fixture
    mock_update_session_status_func,
    mock_db_session_get # Added mock_db_session_get
):
    # This test checks error if original redis_pipeline.execute() fails.
    # We need to ensure that the test setup correctly triggers the specific redis_pipeline.
    # The original test assumed only one pipeline. Now there are potentially three (string_ids, db_pk_ids, ttl).
    # The string_id_redis_pipeline is the first one that might execute data commands.
    mock_redis_client.pipeline.side_effect[0].execute.side_effect = Exception("Redis Write Error for string_id_map")


    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME,
        "some_record_key", "some_prefix", "some_map_type"
    )

    assert result["status"] == "db_error" # Because it now rolls back DB if redis fails after loop before commit
    assert "Redis Write Error for string_id_map" in result["message"] # This error is now caught by the main try-except

    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing")
    # The status update for failure path after loop if db_error_count > 0 or redis error
    # If redis error on string_id_pipeline.execute(), it will be caught by the main try-except
    # and db_engine_session.commit() will not be called.
    # The final status update will be "failed" due to the broad exception.
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="failed", details="Exception('Redis Write Error for string_id_map')")
    mock_wasabi_client.delete_object.assert_not_called()


@patch('app.tasks.load_jobs.add_to_id_map')
def test_process_csv_task_wasabi_cleanup_error_does_not_fail_task( # Pass business_id
    mock_add_to_id_map_for_cleanup_test, # New specific mock for add_to_id_map
    mock_wasabi_client,
    mock_validate_csv_for_brands, # Use brands fixture
    mock_update_session_status_func,
    mock_redis_client,
    mock_db_session_get
):
    # This test needs to successfully pass the DB stage.
    mock_db_session = mock_db_session_get.return_value
    mock_db_session.query(BrandOrm).filter_by().first.return_value = None # New records
    added_instances_cleanup = [] # Use a different name to avoid scope collision if tests run in parallel in future
    def capture_and_set_id_cleanup(instance):
        nonlocal added_instances_cleanup
        instance.id = len(added_instances_cleanup) + 1
        added_instances_cleanup.append(instance)
    mock_db_session.add.side_effect = capture_and_set_id_cleanup

    mock_wasabi_client.delete_object.side_effect = Exception("S3 Delete Failed")

    result = process_csv_task(
        SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, "brands.csv",
        "brand_name", "brand", "brands" # Use brands map_type
    )

    assert result["status"] == "success_with_cleanup_warning"
    assert result["processed_db_count"] == len(SAMPLE_BRAND_RECORDS_VALIDATED)

    mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)

    expected_status_calls = [
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"),
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="completed_with_cleanup_warning",
             details=f"File processed and data saved. Wasabi cleanup failed: Exception('S3 Delete Failed')",
             record_count=len(SAMPLE_BRAND_RECORDS_VALIDATED),
             error_count=0)
    ]
    mock_update_session_status_func.assert_has_calls(expected_status_calls, any_order=False)

# To run these tests (ensure pytest is installed and in the correct directory):
# Terminal: pip install pytest
# Then: pytest app/tests/tasks/test_load_jobs.py
```

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
