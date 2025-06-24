from enum import Enum

class UploadJobStatus(str, Enum):
    """
    Defines the possible statuses for an upload job session.
    """
    # Initial states
    PENDING = "pending"                 # Session created, file uploaded to Wasabi, task not yet picked up or just picked up.
    QUEUED = "queued"                   # Task acknowledged by Celery, waiting for a worker. (Could be same as PENDING depending on flow)

    # Processing states
    DOWNLOADING_FILE = "downloading_file" # Worker is downloading the file from Wasabi.
    VALIDATING_SCHEMA = "validating_schema" # Worker is performing initial schema/format validation (e.g., Pydantic model validation on rows).
    VALIDATING_DATA = "validating_data"     # Worker is performing deeper data validation (e.g., referential integrity, business rules).
    DB_PROCESSING_STARTED = "db_processing_started" # Worker has started iterating through records to save to DB.
    DB_PROCESSING_BATCH = "db_processing_batch" # Worker is processing a batch of records for DB (optional, if batching implemented).
    # If row-by-row progress is needed, it's usually handled by record_count/error_count + DB_PROCESSING_STARTED status

    # Post-processing states
    CLEANING_UP = "cleaning_up"           # Worker is performing cleanup tasks (e.g., deleting file from Wasabi).

    # Terminal states
    COMPLETED = "completed"               # All records processed successfully, no errors.
    COMPLETED_WITH_ERRORS = "completed_with_errors" # Some records processed successfully, but some errors occurred.
    COMPLETED_NO_CHANGES = "completed_no_changes" # File processed, but no actual changes/updates were made to the DB.
    COMPLETED_EMPTY_FILE = "completed_empty_file" # File was empty (or only had headers).

    FAILED_VALIDATION = "failed_validation" # Validation (schema or data) failed critically for too many rows or a fatal file error.
    FAILED_DB_PROCESSING = "failed_db_processing" # Critical error during database interaction for many/all records.
    FAILED_WASABI_UPLOAD = "failed_wasabi_upload" # Initial upload to Wasabi from API failed (set by API, not Celery task).
    FAILED_UNHANDLED_EXCEPTION = "failed_unhandled_exception" # An unexpected error occurred in the Celery task.

    # Could add more specific failure reasons if needed, e.g., FAILED_FILE_DOWNLOAD, FAILED_CLEANUP

    def is_terminal(self) -> bool:
        return self in [
            UploadJobStatus.COMPLETED,
            UploadJobStatus.COMPLETED_WITH_ERRORS,
            UploadJobStatus.COMPLETED_NO_CHANGES,
            UploadJobStatus.COMPLETED_EMPTY_FILE,
            UploadJobStatus.FAILED_VALIDATION,
            UploadJobStatus.FAILED_DB_PROCESSING,
            UploadJobStatus.FAILED_WASABI_UPLOAD, # Though usually set before task starts
            UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
        ]

    def is_success(self) -> bool:
        return self in [
            UploadJobStatus.COMPLETED,
            UploadJobStatus.COMPLETED_NO_CHANGES,
            UploadJobStatus.COMPLETED_EMPTY_FILE,
            # COMPLETED_WITH_ERRORS is a partial success, frontend might treat differently
        ]

    def is_failure(self) -> bool:
        return self in [
            UploadJobStatus.FAILED_VALIDATION,
            UploadJobStatus.FAILED_DB_PROCESSING,
            UploadJobStatus.FAILED_WASABI_UPLOAD,
            UploadJobStatus.FAILED_UNHANDLED_EXCEPTION,
        ]

# Example of another Enum if needed for Error Types, etc.
# class ErrorType(str, Enum):
#     VALIDATION = "VALIDATION"
#     DATABASE = "DATABASE"
#     LOOKUP = "LOOKUP"
#     FILE_FORMAT = "FILE_FORMAT"
#     UNKNOWN = "UNKNOWN"

# Ensure app/models/__init__.py exists if it doesn't, to make 'app.models' a package
# For example, app/models/__init__.py could contain:
# from .enums import UploadJobStatus
# from .schemas import ErrorDetailModel # (once ErrorDetailModel is defined)
# (and other model/schema imports)
