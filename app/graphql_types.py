import strawberry
import datetime
import uuid # For strawberry.ID or if we use uuid.UUID directly
from typing import Optional, List # List might be used for future types

# Note: Strawberry automatically handles conversion for many Python built-in types
# (str, int, float, bool, datetime.datetime, uuid.UUID) to corresponding GraphQL scalars.
# For UUIDs, strawberry.ID is often used for GraphQL schema IDs.

@strawberry.type
class UploadSessionType:
    """
    Represents an upload session, providing status and details of a file upload.
    Corresponds to the Pydantic UploadSessionModel.
    """
    session_id: strawberry.ID  # GraphQL ID type, typically a string. Strawberry handles UUID to ID conversion.
    business_id: str
    load_type: str
    original_filename: str
    wasabi_path: str
    status: str
    details: Optional[str] = None
    record_count: Optional[int] = None
    error_count: Optional[int] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime

@strawberry.type
class UserType:
    """
    Represents an authenticated user's information relevant to the API.
    Derived from the data returned by the get_current_user dependency.
    """
    business_id: str
    role: str
    # Example: if get_current_user also returned user_id
    # user_id: Optional[str] = None

@strawberry.type
class TokenResponseType:
    """
    Represents the response when a new authentication token is generated.
    """
    token: str
    token_type: str

@strawberry.type
class StandardMessageType:
    """
    A standard message type for simple mutation responses,
    indicating success or failure with a message.
    """
    message: str
    success: bool = True # Defaults to True, can be set to False for errors.

# Regarding other Pydantic Models (BrandModel, ProductModel, etc.):
# These are primarily used for data validation within CSV processing (Celery tasks)
# and are not directly exposed via the initial set of GraphQL queries/mutations.
# If future GraphQL queries need to return these entities, corresponding Strawberry types
# (e.g., BrandType, ProductType) would be defined here, potentially using Strawberry's
# Pydantic integration features if desired (e.g., strawberry.experimental.pydantic.type).

# Regarding File Upload Scalar:
# Strawberry handles file uploads using `strawberry.file_uploads.Upload`.
# This type is used directly in mutation arguments like: `file: strawberry.file_uploads.Upload`
# No custom definition for the scalar itself is needed here.

# Regarding Input Types for Mutations (`@strawberry.input`):
# These will be defined closer to their respective mutations in the GraphQL schema/resolvers files.
# This co-location helps in understanding the expected input for each mutation directly.
# For example, an input type for token generation might be:
# @strawberry.input
# class GenerateTokenInput:
#     username: str
#     password: str # Or other credentials like client_id/client_secret for machine auth
```
