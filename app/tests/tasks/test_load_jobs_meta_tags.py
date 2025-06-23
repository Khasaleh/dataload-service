import pytest
from unittest.mock import MagicMock, patch
import os # For os.path.exists and os.remove

from app.tasks.load_jobs import process_meta_tags_file_specific
from app.dataload.meta_tags_loader import DataloadSummary, DataloadErrorDetail

# This will be used by the task. Patch it if its value matters for the test.
# from app.tasks.load_jobs import WASABI_BUCKET_NAME # or patch directly

@pytest.fixture(autouse=True) # Autouse to ensure it's patched for all tests in this module
def patch_wasabi_bucket_name():
    with patch('app.tasks.load_jobs.WASABI_BUCKET_NAME', "test-bucket") as _mock_bucket:
        yield _mock_bucket

class TestProcessMetaTagsFileSpecific:

    @patch('app.tasks.load_jobs.wasabi_client')
    @patch('app.tasks.load_jobs.get_session')
    @patch('app.tasks.load_jobs.load_meta_tags_from_csv')
    @patch('app.tasks.load_jobs._update_session_status')
    @patch('app.tasks.load_jobs.os.path.exists')
    @patch('app.tasks.load_jobs.os.remove')
    @patch('app.tasks.load_jobs.tempfile.NamedTemporaryFile')
    def test_successful_processing(
        self, mock_tempfile_constructor, mock_os_remove, mock_os_exists,
        mock_update_status_func, mock_load_csv_func, mock_get_session_func, mock_wasabi_client_module
    ):
        # --- Mock Setup ---
        # 1. Wasabi client (module level patch)
        mock_s3_object_body = MagicMock()
        mock_s3_object_body.read.return_value = b"meta_type,target_identifier\nPRODUCT,Prod1"
        mock_wasabi_client_module.get_object.return_value = {'Body': mock_s3_object_body}

        # 2. tempfile.NamedTemporaryFile
        mock_temp_file_instance = MagicMock()
        mock_temp_file_instance.name = "/tmp/fake_temp.csv" # Path used by the task
        # Ensure it behaves like a context manager if used with 'with'
        mock_temp_file_instance.__enter__.return_value = mock_temp_file_instance
        mock_temp_file_instance.__exit__.return_value = None
        mock_tempfile_constructor.return_value = mock_temp_file_instance

        # 3. DB session
        mock_db_sess = MagicMock(name="mock_db_session_instance")
        mock_get_session_func.return_value = mock_db_sess

        # 4. load_meta_tags_from_csv returns a success summary
        success_summary = DataloadSummary(
            total_rows_processed=1, successful_updates=1,
            validation_errors=0, target_not_found_errors=0, database_errors=0,
            error_details=[]
        )
        mock_load_csv_func.return_value = success_summary

        # 5. os.path.exists for cleanup (assume file exists to test removal)
        mock_os_exists.return_value = True

        # --- Call the task ---
        result = process_meta_tags_file_specific(
            business_id="biz123", session_id="sess456",
            wasabi_file_path="s3://path/to/file.csv", original_filename="file.csv"
        )

        # --- Assertions ---
        mock_update_status_func.assert_any_call("sess456", "biz123", status="processing")

        mock_wasabi_client_module.get_object.assert_called_once_with(
            Bucket="test-bucket", # Patched value
            Key="s3://path/to/file.csv"
        )
        mock_tempfile_constructor.assert_called_once_with(delete=False, mode='wb', suffix=".csv")
        mock_temp_file_instance.write.assert_called_once_with(b"meta_type,target_identifier\nPRODUCT,Prod1")

        mock_get_session_func.assert_called_once_with(business_id="biz123")
        mock_load_csv_func.assert_called_once_with(db=mock_db_sess, csv_file_path="/tmp/fake_temp.csv")

        final_status_call = mock_update_status_func.call_args_list[-1]
        _args, kwargs = final_status_call
        assert kwargs['session_id'] == "sess456"
        assert kwargs['status'] == "completed"
        assert kwargs['record_count'] == 1
        assert kwargs['error_count'] == 0
        assert kwargs['details'] == '[]'

        mock_wasabi_client_module.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="s3://path/to/file.csv"
        )
        mock_os_exists.assert_called_once_with("/tmp/fake_temp.csv")
        mock_os_remove.assert_called_once_with("/tmp/fake_temp.csv")
        mock_db_sess.close.assert_called_once()

        assert result["status"] == "completed"
        assert result["processed_count"] == 1

    @patch('app.tasks.load_jobs.wasabi_client')
    @patch('app.tasks.load_jobs.get_session')
    @patch('app.tasks.load_jobs.load_meta_tags_from_csv')
    @patch('app.tasks.load_jobs._update_session_status')
    @patch('app.tasks.load_jobs.os.path.exists', return_value=True)
    @patch('app.tasks.load_jobs.os.remove')
    @patch('app.tasks.load_jobs.tempfile.NamedTemporaryFile')
    def test_processing_with_errors_from_loader(
        self, mock_tempfile_constructor, mock_os_remove, _mock_os_exists, # _mock_os_exists not used directly
        mock_update_status_func, mock_load_csv_func, mock_get_session_func, mock_wasabi_client_module
    ):
        mock_s3_object_body = MagicMock()
        mock_s3_object_body.read.return_value = b"csv,header\nvalue1,value2"
        mock_wasabi_client_module.get_object.return_value = {'Body': mock_s3_object_body}

        mock_temp_file_instance = MagicMock()
        mock_temp_file_instance.name = "/tmp/fake_temp.csv"
        mock_temp_file_instance.__enter__.return_value = mock_temp_file_instance
        mock_tempfile_constructor.return_value = mock_temp_file_instance

        mock_db_sess = MagicMock()
        mock_get_session_func.return_value = mock_db_sess

        error_detail = DataloadErrorDetail(row_number=2, raw_data={"h":"v"}, error_type="Validation", error_message="Invalid data")
        error_summary = DataloadSummary(
            total_rows_processed=1, successful_updates=0,
            validation_errors=1, target_not_found_errors=0, database_errors=0,
            error_details=[error_detail]
        )
        mock_load_csv_func.return_value = error_summary

        result = process_meta_tags_file_specific("b", "s", "w_path", "orig.csv")

        final_status_call = mock_update_status_func.call_args_list[-1]
        _args, kwargs = final_status_call
        assert kwargs['status'] == "failed"
        assert kwargs['error_count'] == 1
        assert error_detail.error_message in kwargs['details']

        # If the final status is "failed" due to processing errors, wasabi delete is skipped.
        mock_wasabi_client_module.delete_object.assert_not_called()
        mock_os_remove.assert_called_once_with("/tmp/fake_temp.csv")
        assert result["status"] == "failed"

    @patch('app.tasks.load_jobs.wasabi_client')
    @patch('app.tasks.load_jobs.get_session')
    @patch('app.tasks.load_jobs.load_meta_tags_from_csv')
    @patch('app.tasks.load_jobs._update_session_status')
    # No os.path or os.remove or tempfile patches needed if download fails early
    def test_wasabi_download_failure(
        self, mock_update_status_func, mock_load_csv_func, mock_get_session_func, mock_wasabi_client_module
    ):
        mock_wasabi_client_module.get_object.side_effect = Exception("S3 Network Error")

        result = process_meta_tags_file_specific("b", "s", "w_path", "orig.csv")

        final_status_call = mock_update_status_func.call_args_list[-1]
        _args, kwargs = final_status_call
        assert kwargs['status'] == "failed"
        assert "S3 Network Error" in kwargs['details']

        mock_load_csv_func.assert_not_called()
        assert result["status"] == "failed"
        assert "S3 Network Error" in result["message"]
        mock_get_session_func.return_value.close.assert_not_called() # DB session not opened

    @patch('app.tasks.load_jobs.wasabi_client')
    @patch('app.tasks.load_jobs.get_session')
    @patch('app.tasks.load_jobs.load_meta_tags_from_csv')
    @patch('app.tasks.load_jobs._update_session_status')
    @patch('app.tasks.load_jobs.tempfile.NamedTemporaryFile')
    @patch('app.tasks.load_jobs.os.path.exists', return_value=False) # Temp file does not exist for removal
    @patch('app.tasks.load_jobs.os.remove')
    def test_temp_file_already_deleted(
        self, mock_os_remove, _mock_os_exists, mock_tempfile_constructor,
        mock_update_status_func, mock_load_csv_func, mock_get_session_func, mock_wasabi_client_module
    ):
        mock_s3_object_body = MagicMock()
        mock_s3_object_body.read.return_value = b""
        mock_wasabi_client_module.get_object.return_value = {'Body': mock_s3_object_body}

        mock_temp_file_instance = MagicMock()
        mock_temp_file_instance.name = "/tmp/already_gone.csv"
        mock_temp_file_instance.__enter__.return_value = mock_temp_file_instance
        mock_tempfile_constructor.return_value = mock_temp_file_instance

        mock_load_csv_func.return_value = DataloadSummary() # Empty summary
        mock_get_session_func.return_value = MagicMock()


        process_meta_tags_file_specific("b", "s", "w_path", "orig.csv")

        # os.remove should not be called if os.path.exists returns False
        mock_os_remove.assert_not_called()
        _mock_os_exists.assert_called_once_with("/tmp/already_gone.csv")

    @patch('app.tasks.load_jobs.wasabi_client')
    @patch('app.tasks.load_jobs.get_session')
    @patch('app.tasks.load_jobs.load_meta_tags_from_csv')
    @patch('app.tasks.load_jobs._update_session_status')
    @patch('app.tasks.load_jobs.tempfile.NamedTemporaryFile')
    def test_json_details_truncation(
        self, mock_tempfile_constructor, mock_update_status_func,
        mock_load_csv_func, mock_get_session_func, mock_wasabi_client_module
    ):
        mock_s3_object_body = MagicMock()
        mock_s3_object_body.read.return_value = b"csv,header\nvalue1,value2"
        mock_wasabi_client_module.get_object.return_value = {'Body': mock_s3_object_body}

        mock_temp_file_instance = MagicMock()
        mock_temp_file_instance.name = "/tmp/fake_temp.csv"
        mock_temp_file_instance.__enter__.return_value = mock_temp_file_instance
        mock_tempfile_constructor.return_value = mock_temp_file_instance

        mock_db_sess = MagicMock()
        mock_get_session_func.return_value = mock_db_sess

        # Create many error details to force truncation
        error_details = [
            DataloadErrorDetail(row_number=i, raw_data={"h":"v"}, error_type="Validation", error_message="Error " * 50)
            for i in range(100) # 100 errors, each message is long
        ]
        error_summary = DataloadSummary(
            total_rows_processed=100, successful_updates=0,
            validation_errors=100, error_details=error_details
        )
        mock_load_csv_func.return_value = error_summary

        with patch('app.tasks.load_jobs.os.path.exists', return_value=True), \
             patch('app.tasks.load_jobs.os.remove'):
            process_meta_tags_file_specific("b", "s", "w_path", "orig.csv")

        final_status_call = mock_update_status_func.call_args_list[-1]
        _args, kwargs = final_status_call
        assert kwargs['status'] == "failed"
        assert len(kwargs['details']) <= 4000 + 50 # Check if details string is truncated (approx)
        assert "TRUNCATED" in kwargs['details']

    @patch('app.tasks.load_jobs.wasabi_client')
    @patch('app.tasks.load_jobs.get_session')
    @patch('app.tasks.load_jobs.load_meta_tags_from_csv')
    @patch('app.tasks.load_jobs._update_session_status')
    @patch('app.tasks.load_jobs.os.path.exists', return_value=True)
    @patch('app.tasks.load_jobs.os.remove')
    @patch('app.tasks.load_jobs.tempfile.NamedTemporaryFile')
    def test_completed_empty_file_status(
        self, mock_tempfile_constructor, mock_os_remove, mock_os_exists,
        mock_update_status_func, mock_load_csv_func, mock_get_session_func, mock_wasabi_client_module
    ):
        mock_s3_object_body = MagicMock()
        mock_s3_object_body.read.return_value = b"" # Empty file content
        mock_wasabi_client_module.get_object.return_value = {'Body': mock_s3_object_body}

        mock_temp_file_instance = MagicMock()
        mock_temp_file_instance.name = "/tmp/fake_temp.csv"
        mock_temp_file_instance.__enter__.return_value = mock_temp_file_instance
        mock_tempfile_constructor.return_value = mock_temp_file_instance

        mock_db_sess = MagicMock()
        mock_get_session_func.return_value = mock_db_sess

        # Loader returns 0 rows processed, no errors
        empty_summary = DataloadSummary(total_rows_processed=0, error_details=[])
        mock_load_csv_func.return_value = empty_summary

        process_meta_tags_file_specific("b", "s", "w_path", "orig.csv")

        final_status_call = mock_update_status_func.call_args_list[-1]
        _args, kwargs = final_status_call
        assert kwargs['status'] == "completed_empty_file"
        assert kwargs['record_count'] == 0
        assert kwargs['error_count'] == 0
        assert kwargs['details'] == '[]'

    @patch('app.tasks.load_jobs.wasabi_client')
    @patch('app.tasks.load_jobs.get_session')
    @patch('app.tasks.load_jobs.load_meta_tags_from_csv')
    @patch('app.tasks.load_jobs._update_session_status')
    @patch('app.tasks.load_jobs.os.path.exists', return_value=True)
    @patch('app.tasks.load_jobs.os.remove')
    @patch('app.tasks.load_jobs.tempfile.NamedTemporaryFile')
    def test_completed_no_changes_status(
        self, mock_tempfile_constructor, mock_os_remove, mock_os_exists,
        mock_update_status_func, mock_load_csv_func, mock_get_session_func, mock_wasabi_client_module
    ):
        mock_s3_object_body = MagicMock()
        mock_s3_object_body.read.return_value = b"meta_type,target_identifier\nPRODUCT,Prod1"
        mock_wasabi_client_module.get_object.return_value = {'Body': mock_s3_object_body}

        mock_temp_file_instance = MagicMock()
        mock_temp_file_instance.name = "/tmp/fake_temp.csv"
        mock_temp_file_instance.__enter__.return_value = mock_temp_file_instance
        mock_tempfile_constructor.return_value = mock_temp_file_instance

        mock_db_sess = MagicMock()
        mock_get_session_func.return_value = mock_db_sess

        # Loader returns rows processed, but 0 successful updates and 0 errors
        no_change_summary = DataloadSummary(
            total_rows_processed=1, successful_updates=0,
            validation_errors=0, target_not_found_errors=0, database_errors=0,
            error_details=[]
        )
        mock_load_csv_func.return_value = no_change_summary

        process_meta_tags_file_specific("b", "s", "w_path", "orig.csv")

        final_status_call = mock_update_status_func.call_args_list[-1]
        _args, kwargs = final_status_call
        assert kwargs['status'] == "completed_no_changes"
        assert kwargs['record_count'] == 1
        assert kwargs['error_count'] == 0
        assert kwargs['details'] == '[]'
