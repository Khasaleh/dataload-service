import strawberry
from typing import Optional, Dict, Any # Dict and Any are used in authenticate_user_placeholder
import datetime
import uuid
from app.core.config import settings
from app.graphql_types import TokenResponseType # UploadSessionType and UserType removed
from jose import jwt
from datetime import timedelta

# --- GraphQL Input Types ---
@strawberry.input
class GenerateTokenInput:
    """Input type for generating an authentication token."""
    username: str
    password: str

@strawberry.input
class RefreshTokenInput:
    """Input type for refreshing an authentication token."""
    refreshToken: str

# Removed UploadFileInput as it's no longer needed
# @strawberry.input
# class UploadFileInput:
#     """Input type for the file upload mutation."""
#     load_type: str

# Placeholder for authentication logic - replace with actual implementation
# These would typically involve password hashing, DB lookups, etc.
_MOCK_USERS_DB = {
    "testuser": {
        "username": "testuser",
        "user_id": "user123",
        "business_id": "FAZ-user123-1-2023-10-randomXYZ", # Example companyId string
        "roles": ["catalog_editor"],
        "hashed_password": "hashed_password_for_testuser" # Store hashed passwords
    },
    "adminuser": {
        "username": "adminuser",
        "user_id": "admin456",
        "business_id": "FAZ-admin456-2-2023-11-randomABC", # Example companyId string
        "roles": ["admin"],
        "hashed_password": "hashed_password_for_adminuser"
    }
}

def authenticate_user_placeholder(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Placeholder for user authentication.
    In a real app, this would check credentials against a database.
    """
    user = _MOCK_USERS_DB.get(username)
    if user: # and check_password(password, user["hashed_password"]): # Replace with actual password check
        # Simulate password check for now
        if (username == "testuser" and password == "testpassword") or \
           (username == "adminuser" and password == "adminpassword"):
            return user
    return None

# --- GraphQL Root Mutation Type ---
@strawberry.type
class Mutation:
    """
    Root GraphQL Mutation type.
    Defines all available write operations in the GraphQL API.
    """

    @strawberry.mutation
    def generate_token(self, input: GenerateTokenInput) -> Optional[TokenResponseType]:
        """
        Generates an authentication token (JWT) and a refresh token for a user
        given valid credentials.
        """
        # In a real app, authenticate_user_placeholder would validate against DB, check hashed passwords etc.
        user_details = authenticate_user_placeholder(username=input.username, password=input.password)

        if not user_details:
            raise strawberry.GraphQLError("Invalid username or password.")

        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token_payload = {
            "sub": user_details["username"],
            "userId": user_details["user_id"],
            "companyId": user_details["business_id"], # This is the string like "FAZ-user123-1-..."
            "role": [{"authority": role_name} for role_name in user_details["roles"]], # Standard role claim
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + access_token_expires
        }
        encoded_jwt = jwt.encode(access_token_payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

        # For simplicity, refresh token generation is also mocked.
        # In production, refresh tokens should be securely generated, stored, and managed.
        refresh_token_value = f"mock-rt-{user_details['username']}-{uuid.uuid4()}"

        return TokenResponseType(
            token=encoded_jwt,
            token_type="bearer",
            refreshToken=refresh_token_value
        )

    @strawberry.mutation
    async def refresh_token(self, input: RefreshTokenInput) -> Optional[TokenResponseType]:
        """
        Refreshes an authentication token set (access and refresh tokens)
        using a provided refresh token.
        Placeholder implementation.
        """
        # In a real app, this would involve validating the refresh token against a store
        # and ensuring it hasn't been revoked or expired.
        user_details_for_refresh = None # This would be fetched based on validated refresh token

        # Mock validation based on username encoded in the mock refresh token
        if input.refreshToken and input.refreshToken.startswith("mock-rt-testuser"):
            user_details_for_refresh = _MOCK_USERS_DB.get("testuser")
        elif input.refreshToken and input.refreshToken.startswith("mock-rt-adminuser"):
            user_details_for_refresh = _MOCK_USERS_DB.get("adminuser")

        if not user_details_for_refresh:
            raise strawberry.GraphQLError("Invalid or expired refresh token.")

        new_access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        new_access_token_payload = {
            "sub": user_details_for_refresh["username"],
            "userId": user_details_for_refresh["user_id"],
            "companyId": user_details_for_refresh["business_id"],
            "role": [{"authority": role_name} for role_name in user_details_for_refresh["roles"]],
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + new_access_token_expires
        }
        new_access_token = jwt.encode(new_access_token_payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

        # Optionally, generate a new refresh token as well (good practice)
        new_refresh_token_value = f"mock-rt-{user_details_for_refresh['username']}-{uuid.uuid4()}"

        return TokenResponseType(
            token=new_access_token,
            token_type="bearer",
            refreshToken=new_refresh_token_value # Send back the new refresh token
        )

    # The upload_file mutation has been removed.
    # @strawberry.mutation
    # async def upload_file(self, info: strawberry.types.Info, file: Upload, input: UploadFileInput) -> UploadSessionType:
    #     """
    #     Handles a file upload, creates an upload session record,
    #     uploads the file to Wasabi, and dispatches a Celery task for processing.
    #     Requires authentication.
    #     """
    #     # ... implementation was here ...
    #     pass # Mutation removed
