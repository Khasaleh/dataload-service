import pytest
from unittest.mock import patch, MagicMock, call, ANY
import io

# Module to test
from app.tasks import load_jobs
from app.tasks.load_jobs import process_csv_task, _update_session_status # Import function to test directly
from app.db.models import ( # Import ORM Models
    BrandOrm, AttributeOrm, ReturnPolicyOrm, ProductOrm,
    ProductItemOrm, ProductPriceOrm, MetaTagOrm, UploadSessionOrm, CategoryOrm
)
# For sample data and patching, ensure relevant models and loaders are imported
from app.models.schemas import BrandCsvModel, CategoryCsvModel, ReturnPolicyCsvModel
from app.services.db_loaders import load_return_policy_to_db # For patching target if not aliased in load_jobs
import datetime # For _update_session_status test

# Sample data
SAMPLE_CSV_CONTENT_BRANDS = "name,logo\nBrand Alpha,logoA.png\nBrand Beta,logoB.png"
SAMPLE_CSV_CONTENT_VALID = SAMPLE_CSV_CONTENT_BRANDS
SAMPLE_BRAND_RECORDS_VALIDATED = [
    BrandCsvModel(name="Brand Alpha", logo="logoA.png").model_dump(),
    BrandCsvModel(name="Brand Beta", logo="logoB.png").model_dump()
]

SAMPLE_BUSINESS_ID = "biz_123"
SAMPLE_SESSION_ID = "sess_abc"
SAMPLE_WASABI_PATH = "uploads/some/brands.csv"
SAMPLE_ORIGINAL_FILENAME = "brands.csv"


@pytest.fixture
def mock_wasabi_client():
    with patch('app.tasks.load_jobs.wasabi_client', autospec=True) as mock_client:
        mock_response = MagicMock()
        mock_response['Body'].read.return_value = SAMPLE_CSV_CONTENT_VALID.encode('utf-8')
        mock_client.get_object.return_value = mock_response
        yield mock_client

@pytest.fixture
def mock_db_session_get():
    with patch('app.tasks.load_jobs.get_session') as mock_get_session_func:
        mock_get_session_func.return_value = MagicMock(name="db_session_mock_instance")
        yield mock_get_session_func

@pytest.fixture
def mock_redis_client():
    with patch('app.tasks.load_jobs.redis_client') as mock_global_redis_client_object:
        # mock_global_redis_client_object is the mock for load_jobs.redis_client
        # Its .pipeline() method needs to be a mock.
        mock_pipeline_method = MagicMock(name="pipeline_method_from_fixture")
        mock_global_redis_client_object.pipeline = mock_pipeline_method

        # Default side_effect for the pipeline() method for tests that don't override it.
        # Each element in this list should be a pipeline mock (context manager).
        default_pipeline_mocks = [MagicMock(name=f"default_fixture_pipeline_{idx}") for idx in range(5)] # Changed i to idx
        for p_mock in default_pipeline_mocks: # Changed p to p_mock
            p_mock.__enter__.return_value = p_mock # The pipeline obj itself
            p_mock.__exit__.return_value = None
        mock_pipeline_method.side_effect = default_pipeline_mocks

        yield mock_global_redis_client_object # This is what tests get as mock_redis_client

@pytest.fixture
def mock_validate_csv():
    with patch('app.tasks.load_jobs.validate_csv') as mock_validate:
        mock_validate.return_value = ([], [])
        yield mock_validate

@pytest.fixture
def mock_update_session_status_func():
    with patch('app.tasks.load_jobs._update_session_status') as mock_update:
        yield mock_update

@pytest.fixture
def mock_validate_csv_for_brands():
    with patch('app.tasks.load_jobs.validate_csv') as mock_validate:
        mock_validate.return_value = ([], SAMPLE_BRAND_RECORDS_VALIDATED)
        yield mock_validate

@pytest.fixture
def mock_add_to_id_map_func():
    with patch('app.tasks.load_jobs.add_to_id_map') as mock_add:
        yield mock_add


@patch('app.tasks.load_jobs.load_brand_to_db')
@patch('app.tasks.load_jobs.add_to_id_map')
def test_process_csv_task_brands_success(
    mock_add_to_id_map_direct,
    mock_load_brand_to_db,
    mock_wasabi_client,
    mock_db_session_get,
    mock_validate_csv_for_brands, # Fixture providing validated data
    mock_update_session_status_func,
    mock_redis_client  # The pytest fixture defined above
):
    mock_db_session = mock_db_session_get.return_value
    mock_load_brand_to_db.side_effect = [1, 2] # Simulate DB PKs being returned
    map_type = "brands"
    record_key = "name"
    id_prefix = "brand"

    mock_wasabi_client.get_object.return_value['Body'].read.return_value = SAMPLE_CSV_CONTENT_BRANDS.encode('utf-8')

    pipeline_mocks_for_get_redis_pipeline = [MagicMock(name=f"brands_pipeline_via_get_redis_pipeline_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_get_redis_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None

    pipeline_mocks_for_client_dot_pipeline = [MagicMock(name=f"brands_pipeline_via_client_dot_pipeline_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_client_dot_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None
    mock_redis_client.pipeline.side_effect = pipeline_mocks_for_client_dot_pipeline
    mock_redis_client.__bool__.return_value = True

    load_jobs.redis_client = mock_redis_client

    with patch("app.tasks.load_jobs.get_redis_pipeline", side_effect=pipeline_mocks_for_get_redis_pipeline) as mock_load_jobs_grp, \
         patch("app.utils.redis_utils.get_redis_pipeline", side_effect=pipeline_mocks_for_client_dot_pipeline) as mock_utils_grp:
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
    assert result["processed_db_count"] == 2

    string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[0]
    pk_to_string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[1]

    expected_db_calls = [
        call(db_session=mock_db_session, business_details_id=SAMPLE_BUSINESS_ID, record_data=SAMPLE_BRAND_RECORDS_VALIDATED[0], session_id=SAMPLE_SESSION_ID, db_pk_redis_pipeline=pk_to_string_id_map_pipeline_mock),
        call(db_session=mock_db_session, business_details_id=SAMPLE_BUSINESS_ID, record_data=SAMPLE_BRAND_RECORDS_VALIDATED[1], session_id=SAMPLE_SESSION_ID, db_pk_redis_pipeline=pk_to_string_id_map_pipeline_mock),
    ]
    mock_load_brand_to_db.assert_has_calls(expected_db_calls, any_order=False)

    string_id_map_pipeline_mock.execute.assert_called_once()
    pk_to_string_id_map_pipeline_mock.execute.assert_called_once()

    ttl_pipeline_1 = pipeline_mocks_for_client_dot_pipeline[0]
    ttl_pipeline_2 = pipeline_mocks_for_client_dot_pipeline[1]

    string_id_map_key = f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}"
    pk_map_suffix = "_db_pk"
    pk_to_string_id_map_key = f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}{pk_map_suffix}"

    ttl_pipeline_1.expire.assert_called_once_with(string_id_map_key, load_jobs.REDIS_SESSION_TTL_SECONDS)
    ttl_pipeline_1.execute.assert_called_once()
    ttl_pipeline_2.expire.assert_called_once_with(pk_to_string_id_map_key, load_jobs.REDIS_SESSION_TTL_SECONDS)
    ttl_pipeline_2.execute.assert_called_once()

    mock_db_session.commit.assert_called_once()
    mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)
    expected_status_calls = [
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"),
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="completed", record_count=2, error_count=0)
    ]
    mock_update_session_status_func.assert_has_calls(expected_status_calls, any_order=False)

SAMPLE_CATEGORY_RECORDS_VALIDATED = [
    {"category_path": "Electronics/Audio", "name": "Audio", "description": "Audio Devices"},
    {"category_path": "Electronics/Audio/Headphones", "name": "Headphones", "description": "All Headphones"},
]

@pytest.fixture
def mock_validate_csv_for_categories():
    with patch('app.tasks.load_jobs.validate_csv') as mock_validate:
        mock_validate.return_value = ([], SAMPLE_CATEGORY_RECORDS_VALIDATED)
        yield mock_validate

@patch('app.tasks.load_jobs.load_category_to_db')
@patch('app.tasks.load_jobs.add_to_id_map')
def test_process_csv_task_categories_success(
    mock_add_to_id_map_direct,
    mock_load_category_to_db,
    mock_wasabi_client,
    mock_db_session_get,
    mock_validate_csv_for_categories,
    mock_update_session_status_func,
    mock_redis_client  # The pytest fixture
):
    mock_db_session = mock_db_session_get.return_value
    mock_load_category_to_db.side_effect = [1, 2]
    map_type = "categories"
    record_key = "category_path"
    id_prefix = "cat"

    mock_wasabi_client.get_object.return_value['Body'].read.return_value = b"category_path,name,description\nElectronics/Audio,Audio,Audio Devices"

    pipeline_mocks_for_get_redis_pipeline = [MagicMock(name=f"cat_pipeline_grp_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_get_redis_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None

    pipeline_mocks_for_client_dot_pipeline = [MagicMock(name=f"cat_pipeline_cdp_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_client_dot_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None
    mock_redis_client.pipeline.side_effect = pipeline_mocks_for_client_dot_pipeline
    mock_redis_client.__bool__.return_value = True

    load_jobs.redis_client = mock_redis_client

    with patch("app.tasks.load_jobs.get_redis_pipeline", side_effect=pipeline_mocks_for_get_redis_pipeline) as mock_load_jobs_grp, \
         patch("app.utils.redis_utils.get_redis_pipeline", side_effect=pipeline_mocks_for_client_dot_pipeline) as mock_utils_grp:
        result = process_csv_task(
            business_id=SAMPLE_BUSINESS_ID,
            session_id=SAMPLE_SESSION_ID,
            wasabi_file_path="uploads/some/categories.csv",
            original_filename="categories.csv",
            record_key=record_key,
            id_prefix=id_prefix,
            map_type=map_type
        )

    assert result["status"] == "success"
    assert result["processed_db_count"] == 2

    string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[0]
    pk_to_string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[1]

    expected_db_calls = [
        call(db_session=mock_db_session, business_details_id=SAMPLE_BUSINESS_ID, record_data=SAMPLE_CATEGORY_RECORDS_VALIDATED[0], session_id=SAMPLE_SESSION_ID, db_pk_redis_pipeline=pk_to_string_id_map_pipeline_mock),
        call(db_session=mock_db_session, business_details_id=SAMPLE_BUSINESS_ID, record_data=SAMPLE_CATEGORY_RECORDS_VALIDATED[1], session_id=SAMPLE_SESSION_ID, db_pk_redis_pipeline=pk_to_string_id_map_pipeline_mock),
    ]
    mock_load_category_to_db.assert_has_calls(expected_db_calls, any_order=False)

    string_id_map_pipeline_mock.execute.assert_called_once()
    pk_to_string_id_map_pipeline_mock.execute.assert_called_once()

    ttl_pipeline_1 = pipeline_mocks_for_client_dot_pipeline[0]
    ttl_pipeline_2 = pipeline_mocks_for_client_dot_pipeline[1]

    string_id_map_key = f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}"
    pk_map_suffix = "_db_pk"
    pk_to_string_id_map_key = f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}{pk_map_suffix}"

    ttl_pipeline_1.expire.assert_called_once_with(string_id_map_key, load_jobs.REDIS_SESSION_TTL_SECONDS)
    ttl_pipeline_1.execute.assert_called_once()
    ttl_pipeline_2.expire.assert_called_once_with(pk_to_string_id_map_key, load_jobs.REDIS_SESSION_TTL_SECONDS)
    ttl_pipeline_2.execute.assert_called_once()

    mock_db_session.commit.assert_called_once()
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="completed", record_count=2, error_count=0)

@patch('app.tasks.load_jobs.load_category_to_db')
def test_process_csv_task_categories_loader_fails_for_one_record(
    mock_load_category_to_db, mock_wasabi_client, mock_db_session_get,
    mock_redis_client, mock_validate_csv_for_categories, mock_update_session_status_func
):
    mock_db_session = mock_db_session_get.return_value
    mock_load_category_to_db.side_effect = [123, None]
    map_type = "categories"

    mock_pipeline_string_id = MagicMock(name="cat_fail_string_id_pipeline")
    mock_pipeline_db_pk = MagicMock(name="cat_fail_db_pk_pipeline")
    with patch('app.tasks.load_jobs.get_redis_pipeline') as mock_get_pipeline_direct:
        mock_get_pipeline_direct.side_effect = [mock_pipeline_string_id, mock_pipeline_db_pk, MagicMock(), MagicMock()]
        result = process_csv_task(
            business_id=SAMPLE_BUSINESS_ID, session_id=SAMPLE_SESSION_ID,
            wasabi_file_path="uploads/some/categories.csv", original_filename="categories.csv",
            record_key="category_path", id_prefix="cat", map_type=map_type
        )
    assert result["status"] == "db_error"
    assert result["processed_db_count"] == 0
    assert mock_load_category_to_db.call_count == 2
    mock_db_session.rollback.assert_called_once()
    mock_db_session.commit.assert_not_called()
    mock_pipeline_db_pk.execute.assert_not_called()
    mock_pipeline_string_id.execute.assert_not_called()
    mock_update_session_status_func.assert_any_call(
        SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="db_processing_failed", details=ANY, error_count=1
    )

@patch('app.tasks.load_jobs.get_session')
def test_update_session_status_found_and_updated(mock_get_session_for_status_update):
    mock_db = MagicMock(); mock_session_orm = MagicMock(spec=UploadSessionOrm)
    mock_get_session_for_status_update.return_value = mock_db
    mock_db.query(UploadSessionOrm).filter().first.return_value = mock_session_orm
    _update_session_status("test_sess_id", SAMPLE_BUSINESS_ID, "new_status", "new details", 10, 1)
    mock_get_session_for_status_update.assert_called_once_with(business_id=SAMPLE_BUSINESS_ID)
    mock_db.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == "test_sess_id").first.assert_called_once()
    assert mock_session_orm.status == "new_status"; assert mock_session_orm.details == "new details"
    assert mock_session_orm.record_count == 10; assert mock_session_orm.error_count == 1
    assert mock_session_orm.updated_at is not None; assert isinstance(mock_session_orm.updated_at, datetime.datetime)
    mock_db.commit.assert_called_once(); mock_db.close.assert_called_once()

@patch('app.tasks.load_jobs.get_session')
def test_update_session_status_not_found(mock_get_session_for_status_update):
    mock_db = MagicMock(); mock_get_session_for_status_update.return_value = mock_db
    mock_db.query(UploadSessionOrm).filter().first.return_value = None
    _update_session_status("test_sess_id", SAMPLE_BUSINESS_ID, "new_status")
    mock_db.commit.assert_not_called(); mock_db.close.assert_called_once()

@patch('app.tasks.load_jobs.get_session')
def test_update_session_status_db_error_on_commit(mock_get_session_for_status_update):
    mock_db = MagicMock(); mock_session_orm = MagicMock(spec=UploadSessionOrm)
    mock_get_session_for_status_update.return_value = mock_db
    mock_db.query(UploadSessionOrm).filter().first.return_value = mock_session_orm
    mock_db.commit.side_effect = Exception("DB Commit Error")
    _update_session_status("test_sess_id", SAMPLE_BUSINESS_ID, "new_status")
    mock_db.rollback.assert_called_once(); mock_db.close.assert_called_once()

@pytest.fixture
def mock_validate_csv_generic_no_records():
    with patch('app.tasks.load_jobs.validate_csv', return_value = ([], [])) as mock_validate: yield mock_validate

def test_process_csv_task_wasabi_download_error(mock_wasabi_client, mock_update_session_status_func):
    mock_wasabi_client.get_object.side_effect = Exception("S3 Download Failed")
    result = process_csv_task(SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME, "some_key", "prefix", "some_map_type")
    assert result["status"] == "error"; assert "S3 Download Failed" in result["message"]
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing")
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="failed", details=f"Major processing error: {str(Exception('S3 Download Failed'))}")

def test_process_csv_task_empty_file(mock_wasabi_client, mock_update_session_status_func):
    mock_wasabi_client.get_object.return_value['Body'].read.return_value = b""
    result = process_csv_task(SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME, "some_key", "prefix", "some_map_type")
    assert result["status"] == "no_data"
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing")
    final_status_calls = [c for c in mock_update_session_status_func.call_args_list if c.kwargs.get("status") in ["completed", "failed", "completed_with_cleanup_warning", "db_processing_failed", "validation_failed"]]
    assert len(final_status_calls) == 0; assert mock_update_session_status_func.call_count == 1

def test_process_csv_task_validation_errors(mock_wasabi_client, mock_validate_csv, mock_update_session_status_func):
    validation_errs = [{"row": 1, "field": "col1", "error": "is bad"}]
    mock_validate_csv.return_value = (validation_errs, [])
    result = process_csv_task(SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, SAMPLE_ORIGINAL_FILENAME, "some_key", "prefix", "some_map_type")
    assert result["status"] == "validation_failed"; assert result["errors"] == validation_errs
    expected_status_calls = [call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"), call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="validation_failed", details=str(validation_errs), error_count=len(validation_errs))]
    mock_update_session_status_func.assert_has_calls(expected_status_calls)
    mock_wasabi_client.delete_object.assert_not_called()

@patch('app.tasks.load_jobs.validate_csv', return_value=([], SAMPLE_BRAND_RECORDS_VALIDATED[:1]))
@patch('app.tasks.load_jobs.load_brand_to_db')
def test_process_csv_task_redis_error_on_add(
    mock_load_brand_to_db,
    mock_validate_csv_empty,
    mock_wasabi_client,
    mock_update_session_status_func,
    mock_db_session_get,
    mock_redis_client  # The pytest fixture
):
    mock_load_brand_to_db.return_value = 1

    faulty_pipeline_mock = MagicMock(name="faulty_pipeline")
    faulty_pipeline_mock.execute.side_effect = Exception("Redis error on execute")
    faulty_pipeline_mock.__enter__.return_value = faulty_pipeline_mock
    faulty_pipeline_mock.__exit__.return_value = None

    other_pipelines = [MagicMock(name=f"other_pipeline_cdp_{i}") for i in range(4)]
    for p_mock in other_pipelines:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None
    mock_redis_client.pipeline.side_effect = [faulty_pipeline_mock] + other_pipelines
    mock_redis_client.__bool__.return_value = True

    load_jobs.redis_client = mock_redis_client

    with patch("app.tasks.load_jobs.get_redis_pipeline", side_effect=[faulty_pipeline_mock] + other_pipelines):
        result = process_csv_task(
            SAMPLE_BUSINESS_ID,
            SAMPLE_SESSION_ID,
            SAMPLE_WASABI_PATH,
            SAMPLE_ORIGINAL_FILENAME,
            record_key="name",
            id_prefix="brand",
            map_type="brands"
        )

    assert result["status"] == "error"
    assert "Redis error on execute" in result["message"]

    expected_status_calls = [
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"),
        call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="failed", details=f"Major processing error: {str(Exception('Redis error on execute'))}")
    ]
    mock_update_session_status_func.assert_has_calls(expected_status_calls, any_order=False)
    mock_wasabi_client.delete_object.assert_not_called()
    mock_db_session_get.return_value.rollback.assert_called_once()
    mock_db_session_get.return_value.commit.assert_not_called()

@patch('app.tasks.load_jobs.add_to_id_map')
def test_process_csv_task_wasabi_cleanup_error_does_not_fail_task(
    mock_add_to_id_map_for_cleanup_test, mock_wasabi_client, mock_validate_csv_for_brands,
    mock_update_session_status_func, mock_redis_client, mock_db_session_get
):
    mock_db_session = mock_db_session_get.return_value
    with patch('app.tasks.load_jobs.load_brand_to_db') as mock_load_brand:
        mock_load_brand.side_effect = [db_pk for db_pk, _ in enumerate(SAMPLE_BRAND_RECORDS_VALIDATED, 1)]
        mock_wasabi_client.delete_object.side_effect = Exception("S3 Delete Failed")

        mock_pipeline_string_id = MagicMock(name="cleanup_string_id_pipeline")
        mock_pipeline_db_pk = MagicMock(name="cleanup_db_pk_pipeline")
        mock_pipeline_ttl1 = MagicMock(name="cleanup_ttl1_pipeline")
        mock_pipeline_ttl2 = MagicMock(name="cleanup_ttl2_pipeline")
        with patch('app.tasks.load_jobs.get_redis_pipeline') as mock_get_pipeline_direct:
            mock_get_pipeline_direct.side_effect = [mock_pipeline_string_id, mock_pipeline_db_pk, mock_pipeline_ttl1, mock_pipeline_ttl2, MagicMock()]
            result = process_csv_task(
                SAMPLE_BUSINESS_ID, SAMPLE_SESSION_ID, SAMPLE_WASABI_PATH, "brands.csv", "name", "brand", "brands"
            )
        assert result["status"] == "success_with_cleanup_warning"
        assert result["processed_db_count"] == len(SAMPLE_BRAND_RECORDS_VALIDATED)
        mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key=SAMPLE_WASABI_PATH)
        expected_status_calls = [
            call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="processing"),
            call(SAMPLE_SESSION_ID, SAMPLE_BUSINESS_ID, status="completed_with_cleanup_warning",
                    details=f"File processed and data saved ({len(SAMPLE_BRAND_RECORDS_VALIDATED)} records). Wasabi cleanup failed: S3 Delete Failed",
                record_count=len(SAMPLE_BRAND_RECORDS_VALIDATED), error_count=0)
        ]
        mock_update_session_status_func.assert_has_calls(expected_status_calls, any_order=False)

SAMPLE_RETURN_POLICY_RECORDS_VALIDATED = [
    {"id": 1, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy A", "time_period_return": 14, "business_details_id": 1},
    {"id": None, "return_policy_type": "SALES_ARE_FINAL", "business_details_id": 1},
    {"id": 2, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy B", "time_period_return": 30, "business_details_id": 1, "grace_period_return": 5},
]

@pytest.fixture
def mock_validate_csv_for_return_policies(mocker):
    mock_validator = mocker.patch('app.tasks.load_jobs.validate_csv')
    mock_validator.return_value = ([], SAMPLE_RETURN_POLICY_RECORDS_VALIDATED)
    return mock_validator

@patch('app.tasks.load_jobs.load_return_policy_to_db')
@patch('app.tasks.load_jobs.add_to_id_map')
def test_process_csv_task_return_policies_success(
    mock_add_to_id_map_direct,
    mock_load_return_policy_to_db,
    mock_wasabi_client,
    mock_db_session_get,
    mock_validate_csv_for_return_policies,
    mock_update_session_status_func,
    mock_redis_client  # The pytest fixture
):
    mock_db_session = mock_db_session_get.return_value
    mock_load_return_policy_to_db.side_effect = [1, 2, 3]
    map_type = "return_policies"
    record_key = "policy_name"
    id_prefix = "rp"
    business_id_int = 1 # Matching the data in SAMPLE_RETURN_POLICY_RECORDS_VALIDATED

    mock_wasabi_client.get_object.return_value['Body'].read.return_value = b"id,return_policy_type,policy_name\n1,SALES_RETURN_ALLOWED,Policy A"

    pipeline_mocks_for_get_redis_pipeline = [MagicMock(name=f"rp_pipeline_grp_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_get_redis_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None

    pipeline_mocks_for_client_dot_pipeline = [MagicMock(name=f"rp_pipeline_cdp_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_client_dot_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None
    mock_redis_client.pipeline.side_effect = pipeline_mocks_for_client_dot_pipeline
    mock_redis_client.__bool__.return_value = True

    load_jobs.redis_client = mock_redis_client

    with patch("app.tasks.load_jobs.get_redis_pipeline", side_effect=pipeline_mocks_for_get_redis_pipeline) as mock_load_jobs_grp, \
         patch("app.utils.redis_utils.get_redis_pipeline", side_effect=pipeline_mocks_for_client_dot_pipeline) as mock_utils_grp:
        result = process_csv_task(
            business_id=business_id_int,
            session_id=SAMPLE_SESSION_ID,
            wasabi_file_path="uploads/some/return_policies.csv",
            original_filename="return_policies.csv",
            record_key=record_key,
            id_prefix=id_prefix,
            map_type=map_type
        )

    assert result["status"] == "success"
    assert result["processed_db_count"] == 3

    string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[0]
    pk_to_string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[1]

    expected_db_calls = [
        call(db_session=mock_db_session, business_details_id=business_id_int, record_data=SAMPLE_RETURN_POLICY_RECORDS_VALIDATED[i], session_id=SAMPLE_SESSION_ID, db_pk_redis_pipeline=pk_to_string_id_map_pipeline_mock)
        for i in range(len(SAMPLE_RETURN_POLICY_RECORDS_VALIDATED))
    ]
    mock_load_return_policy_to_db.assert_has_calls(expected_db_calls, any_order=False)

    string_id_map_pipeline_mock.execute.assert_called_once()
    pk_to_string_id_map_pipeline_mock.execute.assert_called_once()

    ttl_pipeline_1 = pipeline_mocks_for_client_dot_pipeline[0]
    ttl_pipeline_2 = pipeline_mocks_for_client_dot_pipeline[1]

    string_id_map_key = f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}"
    pk_map_suffix = "_db_pk"
    pk_to_string_id_map_key = f"id_map:session:{SAMPLE_SESSION_ID}:{map_type}{pk_map_suffix}"

    ttl_pipeline_1.expire.assert_called_once_with(string_id_map_key, load_jobs.REDIS_SESSION_TTL_SECONDS)
    ttl_pipeline_1.execute.assert_called_once()
    ttl_pipeline_2.expire.assert_called_once_with(pk_to_string_id_map_key, load_jobs.REDIS_SESSION_TTL_SECONDS)
    ttl_pipeline_2.execute.assert_called_once()

    mock_db_session.commit.assert_called_once()
    mock_wasabi_client.delete_object.assert_called_once_with(Bucket=load_jobs.WASABI_BUCKET_NAME, Key="uploads/some/return_policies.csv")
    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, business_id_int, status="completed", record_count=3, error_count=0)

@patch('app.tasks.load_jobs.load_return_policy_to_db')
@patch('app.tasks.load_jobs.add_to_id_map')
def test_process_csv_task_return_policies_loader_fails(
    mock_add_to_id_map_direct,
    mock_load_return_policy_to_db,
    mock_wasabi_client,
    mock_db_session_get,
    mock_validate_csv_for_return_policies,
    mock_update_session_status_func,
    mock_redis_client  # The pytest fixture
):
    mock_db_session = mock_db_session_get.return_value
    mock_load_return_policy_to_db.side_effect = [1, None, 3]
    map_type = "return_policies"
    record_key = "policy_name"
    id_prefix = "rp"
    business_id_int = 1

    mock_wasabi_client.get_object.return_value['Body'].read.return_value = b"id,return_policy_type,policy_name\n1,SALES_RETURN_ALLOWED,Policy A"

    pipeline_mocks_for_get_redis_pipeline = [MagicMock(name=f"rp_fail_grp_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_get_redis_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None

    pipeline_mocks_for_client_dot_pipeline = [MagicMock(name=f"rp_fail_cdp_{i}") for i in range(5)]
    for p_mock in pipeline_mocks_for_client_dot_pipeline:
        p_mock.__enter__.return_value = p_mock
        p_mock.__exit__.return_value = None
    mock_redis_client.pipeline.side_effect = pipeline_mocks_for_client_dot_pipeline
    mock_redis_client.__bool__.return_value = True # Ensure mock evaluates to True

    load_jobs.redis_client = mock_redis_client


    with patch("app.tasks.load_jobs.get_redis_pipeline", side_effect=pipeline_mocks_for_get_redis_pipeline):
        result = process_csv_task(
            business_id=business_id_int,
            session_id=SAMPLE_SESSION_ID,
            wasabi_file_path="uploads/some/return_policies.csv",
            original_filename="return_policies.csv",
            record_key=record_key,
            id_prefix=id_prefix,
        map_type=map_type,
        )

    assert result["status"] == "db_error"
    assert result["processed_db_count"] == 0

    mock_db_session.rollback.assert_called_once()
    mock_db_session.commit.assert_not_called()

    string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[0]
    pk_to_string_id_map_pipeline_mock = pipeline_mocks_for_get_redis_pipeline[1]

    string_id_map_pipeline_mock.execute.assert_not_called()
    pk_to_string_id_map_pipeline_mock.execute.assert_not_called()

    mock_update_session_status_func.assert_any_call(SAMPLE_SESSION_ID, business_id_int, status="db_processing_failed", details=ANY, error_count=1)
    mock_wasabi_client.delete_object.assert_not_called()

# To run these tests (ensure pytest is installed and in the correct directory):
# Terminal: pip install pytest
# Then: pytest app/tests/tasks/test_load_jobs.py
