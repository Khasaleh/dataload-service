from typing import Optional, Any
from app.models.schemas import ErrorType # Use the existing ErrorType Enum

class DataLoaderError(Exception):
    """
    Custom exception for errors encountered during data loading operations
    within specific loader functions (e.g., load_product_to_db).
    """
    def __init__(
        self,
        message: str,
        error_type: ErrorType = ErrorType.UNKNOWN,
        # row_number is best set by the calling task which knows the CSV context.
        # Loaders can provide field_name and offending_value.
        field_name: Optional[str] = None,
        offending_value: Optional[Any] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.field_name = field_name
        self.offending_value = str(offending_value)[:255] if offending_value is not None else None # Truncate
        self.original_exception = original_exception

    def __str__(self):
        # Basic string representation, can be enhanced if needed for direct logging of exception.
        # The primary use is to carry structured data to be converted into ErrorDetailModel.
        return f"DataLoaderError ({self.error_type.value}): {self.message}" \
               f"{f' | Field: {self.field_name}' if self.field_name else ''}" \
               f"{f' | Value: {self.offending_value}' if self.offending_value is not None else ''}" \
               f"{f' | Original: {type(self.original_exception).__name__}: {str(self.original_exception)}' if self.original_exception else ''}"

# Example of more specific exceptions if desired later:

# class DatabaseOperationError(DataLoaderError):
#     def __init__(self, message, field_name=None, offending_value=None, original_exception=None):
#         super().__init__(message, ErrorType.DATABASE, field_name, offending_value, original_exception)

# class LookupFailedError(DataLoaderError):
#     def __init__(self, entity_name: str, lookup_key: str, lookup_value: Any, field_name: Optional[str] = None, original_exception: Optional[Exception] = None):
#         message = f"Lookup failed for {entity_name}: Key '{lookup_key}' with value '{str(lookup_value)[:50]}' not found."
#         # field_name here would be the field in the *current* CSV row that triggered the lookup.
#         super().__init__(message, ErrorType.LOOKUP, field_name, str(lookup_value)[:50], original_exception)
