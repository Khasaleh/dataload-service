import strawberry
from typing import Optional, Any, Dict
import datetime
import uuid # For session_id generation
import os # For WASABI_BUCKET_NAME

from app.graphql_types import UploadSessionType, TokenResponseType, UserType
from strawberry.file_uploads import Upload
from jose import jwt # Added
from app.dependencies.auth import SECRET_KEY, ALGORITHM # Added
from datetime import timedelta # Added


# --- Placeholder/Conceptual Service Imports & Functions ---

# This is a placeholder. In a real app, this would query your user database
# and use passlib for password verification.
# from passlib.context import CryptContext
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

_MOCK_USERS_DB = {
    "testuser": {
        "username": "testuser", "hashed_password": "password", # In real app, use: pwd_context.hash("password") e.g. "$2b$12$yourbcryptstringhere"
        "user_id": "user_123", "business_id": "biz_789", "roles": ["ROLE_USER"], "disabled": False
    },
    "adminuser": {
        "username": "adminuser", "hashed_password": "adminpassword", # Replace with a real bcrypt hash for testing if needed
        "user_id": "admin_456", "business_id": "biz_789", "roles": ["ROLE_ADMIN", "ROLE_USER"], "disabled": False
    }
}
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30))

def authenticate_user_placeholder(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Placeholder for user authentication.
    In a real app, this would verify against hashed passwords in a database.
    Returns user details dict if authentication is successful, else None.
    """
    user = _MOCK_USERS_DB.get(username)
    if not user or user["disabled"]:
        return None
    # Simulate password check for placeholder - REPLACE with pwd_context.verify(password, user["hashed_password"])
    if user["hashed_password"] != password: # Simple string comparison for mock
        return None
    return {"username": user["username"], "user_id": user["user_id"], "business_id": user["business_id"], "roles": user["roles"]}

# --- End Placeholder Functions for Authentication ---


# --- Actual Service/Task Imports ---
from app.services.storage import upload_file as upload_to_wasabi
from strawberry.exceptions import GraphQLError # Correct import for GraphQLError
# Updated import to fetch individual task functions
from app.tasks.load_jobs import (
    CELERY_TASK_MAP, # Import CELERY_TASK_MAP
    process_brands_file,
    process_attributes_file,
    process_return_policies_file,
    process_products_file,
    process_product_items_file,
    process_product_prices_file,
    process_meta_tags_file
    # process_categories_file # This will be imported once created in load_jobs.py
)
from app.db.connection import get_session as get_db_session_sync # For DB operations
from app.db.models import UploadSessionOrm # For DB operations
import logging # For logging DB errors

logger = logging.getLogger(__name__)


# --- GraphQL Input Types ---
@strawberry.input
class GenerateTokenInput:
    """Input type for generating an authentication token."""
    username: str
    password: str
    # client_id: Optional[str] = None  # Example for OAuth client credentials flow
    # client_secret: Optional[str] = None

@strawberry.input
class RefreshTokenInput:
    """Input type for refreshing an authentication token."""
    refreshToken: str

@strawberry.input
class UploadFileInput:
    """Input type for the file upload mutation."""
    load_type: str
    # business_id will be derived from the authenticated user's context.
    # original_filename will be derived from the Upload object (file.filename).


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
        user_details = authenticate_user_placeholder(username=input.username, password=input.password)

        if not user_details:
            raise strawberry.GraphQLError("Invalid username or password.")
            # Alternatively, for some GraphQL patterns, returning None is also an option:
            # return None

        # Create the JWT access token payload
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token_payload = {
            "sub": user_details["username"],
            "userId": user_details["user_id"],
            "companyId": user_details["business_id"], # Using companyId in token as per original spec
            "role": [{"authority": role_name} for role_name in user_details["roles"]],
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + access_token_expires
        }
        encoded_jwt = jwt.encode(access_token_payload, SECRET_KEY, algorithm=ALGORITHM)

        # Generate a refresh token (e.g., a simple UUID for this example)
        # In a real system, this refresh token would be stored securely (e.g., in a database)
        # associated with the user_id and have its own expiry. It would be used to obtain
        # new access tokens without requiring the user to re-enter credentials.
        # For placeholder validation in refreshToken mutation, we make it predictable.
        refresh_token_value = f"mock-rt-{user_details['username']}-{uuid.uuid4()}"
        # Conceptual: store_refresh_token(user_id=user_details["user_id"], token=refresh_token_value, expires_in=...)

        return TokenResponseType(
            token=encoded_jwt,
            token_type="bearer",
            refreshToken=refresh_token_value
        )

    @strawberry.mutation
    async def refresh_token(self, input: RefreshTokenInput) -> Optional[TokenResponseType]:
        """
        Refreshes an authentication token set (access and refresh tokens)
        using a provided refresh token. Implements refresh token rotation.
        """

        # --- Placeholder Refresh Token Validation Logic ---
        # This is highly conceptual. A real system needs a secure refresh token store and validation.
        user_details_for_refresh = None
        # Example: Crude check for a mock refresh token.
        # A real system would look up `input.refreshToken` in a database,
        # verify its validity/expiry, and retrieve the associated user_id.
        if input.refreshToken and input.refreshToken.startswith("mock-rt-testuser"):
            # Simulate fetching user details for "testuser" if the token format matches.
            # This implies the token was originally issued for "testuser".
            user_details_for_refresh = _MOCK_USERS_DB.get("testuser")
        elif input.refreshToken and input.refreshToken.startswith("mock-rt-adminuser"):
            user_details_for_refresh = _MOCK_USERS_DB.get("adminuser")
        # Add more conditions if other users' refresh tokens need to be "valid" for this placeholder.

        if not user_details_for_refresh:
            raise strawberry.GraphQLError("Invalid or expired refresh token.")

        if user_details_for_refresh.get("disabled", False):
            raise strawberry.GraphQLError("User account is disabled.")
        # --- End of Placeholder Validation ---

        # Generate new access token
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token_payload = {
            "sub": user_details_for_refresh["username"],
            "userId": user_details_for_refresh["user_id"],
            "companyId": user_details_for_refresh["business_id"],
            "role": [{"authority": role_name} for role_name in user_details_for_refresh["roles"]],
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + access_token_expires
        }
        new_access_token = jwt.encode(access_token_payload, SECRET_KEY, algorithm=ALGORITHM)

        # Generate new refresh token (implementing rotation)
        new_refresh_token_value = f"mock-rt-{user_details_for_refresh['username']}-{uuid.uuid4()}" # New token

        # --- Conceptual: Update Refresh Token Store ---
        # In a real application:
        # 1. Invalidate the old refresh token (input.refreshToken) in your secure store.
        #    (e.g., mark as used, delete, or add to a denylist until its original expiry).
        # 2. Store the new_refresh_token_value, associated with user_details_for_refresh["user_id"],
        #    with a new (potentially long) expiry.
        # logger.info(f"User {user_details_for_refresh['username']} refreshed token. Old RT: {input.refreshToken}, New RT: {new_refresh_token_value}")
        # --- End Conceptual Store Update ---

        return TokenResponseType(
            token=new_access_token,
            token_type="bearer",
            refreshToken=new_refresh_token_value
        )

    @strawberry.mutation
    async def upload_file(
        self,
        info: strawberry.types.Info,
        file: Upload,
        input: UploadFileInput
    ) -> UploadSessionType:
        """
        Handles a file upload, creates an upload session record,
        uploads the file to Wasabi, and dispatches a Celery task for processing.

        Requires authentication; user details are sourced from `info.context`.
        """
        current_user_data = info.context.get("current_user")
        if not current_user_data or not current_user_data.get("business_id"):
            raise strawberry.GraphQLError("Authentication required: User or business ID not found in context.")

        business_id = current_user_data["business_id"]
        user_roles = current_user_data.get("roles", []) # Get list of roles

        # --- Basic Validation (similar to app/routes/upload.py) ---
        # TODO: Implement/integrate actual ROLE_PERMISSIONS check if needed.
        # Example conceptual check (replace with actual permission logic):
        # Assume ROLE_PERMISSIONS is a dict like:
        # ROLE_PERMISSIONS = { "ROLE_ADMIN": {"brands", "products"}, "ROLE_UPLOADER": {"products"} }
        #
        # allowed_for_role = False
        # for role in user_roles:
        #     if role in ROLE_PERMISSIONS and input.load_type in ROLE_PERMISSIONS[role]:
        #         allowed_for_role = True
        #         break
        # if not allowed_for_role:
        #     raise strawberry.GraphQLError(f"Permission denied for load type '{input.load_type}' based on user roles: {user_roles}")

        if input.load_type not in CELERY_TASK_MAP:
            raise strawberry.GraphQLError(f"Invalid load type: {input.load_type}. Supported types are: {list(CELERY_TASK_MAP.keys())}")

        if not file.filename: # strawberry.file_uploads.Upload provides filename
            raise strawberry.GraphQLError("Filename cannot be empty.")
        if not file.filename.lower().endswith('.csv'):
            raise strawberry.GraphQLError("Invalid file type. Only CSV files are allowed.")

        contents = await file.read() # strawberry.file_uploads.Upload has an async read()
        if not contents:
            raise strawberry.GraphQLError("Empty CSV file submitted.")

        # After reading, the file pointer is at the end.
        # For boto3's upload_fileobj, it needs a file-like object that can be read from the start.
        # The `file` object of type `strawberry.file_uploads.Upload` is itself a file-like object.
        # It typically wraps an underlying SpooledTemporaryFile or similar.
        # Strawberry's Upload object should be directly passable to upload_fileobj if it implements read().
        # If direct passing fails, one might need to access `file.file.seek(0)` if `file.file` is the underlying spooled file.
        # However, Boto3 is generally good at handling various file-like objects.

        session_id = str(uuid.uuid4())
        original_filename = file.filename # Use filename from the Upload object

        # Construct Wasabi path
        wasabi_path = f"uploads/{business_id}/{session_id}/{input.load_type}/{original_filename}"

        # Get Wasabi bucket name from environment
        WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")
        if not WASABI_BUCKET_NAME:
            # This should ideally be checked at app startup.
            raise strawberry.GraphQLError("Wasabi bucket name is not configured on the server.")

        # --- Conceptual: Upload Sequence Checks (Not Implemented Here) ---
        # e.g., check_upload_sequence(db_session, business_id, input.load_type)
        # if sequence_check_fails:
        #    raise strawberry.GraphQLError("Upload sequence prerequisite not met.")

        # --- Create UploadSession record (Conceptual DB save) ---
        session_data_to_save = {
            "session_id": session_id,
            "business_id": business_id,
            "load_type": input.load_type,
            "original_filename": original_filename,
            "wasabi_path": wasabi_path,
            "status": "pending", # Initial status
            "created_at": datetime.datetime.utcnow(),
            "updated_at": datetime.datetime.utcnow(),
            "details": None,
            "record_count": None,
            "error_count": None
        }
        # Simulate saving to DB and getting the saved data (could have defaults/triggers in real DB)
        # created_session_dict = create_upload_session_in_db(session_data_to_save) # Old placeholder

        # --- Create UploadSession record in DB ---
        db_session = None
        new_session_orm_instance = None
        try:
            db_session = get_db_session_sync(business_id=business_id) # Get a synchronous session

            # Create ORM instance from session_data_to_save (which already includes UTC timestamps)
            new_session_orm_instance = UploadSessionOrm(**session_data_to_save)
            db_session.add(new_session_orm_instance)
            db_session.commit()
            db_session.refresh(new_session_orm_instance) # To get DB-generated fields like 'id' (PK)

            # Convert ORM instance to dict to pass to UploadSessionType
            created_session_dict = {c.name: getattr(new_session_orm_instance, c.name) for c in new_session_orm_instance.__table__.columns}
            logger.info(f"Upload session record created in DB for session_id: {session_id}")

        except Exception as e_db:
            logger.error(f"DB Error: Failed to create upload session record for session_id {session_id}: {e_db}", exc_info=True)
            if db_session:
                db_session.rollback()
            raise strawberry.GraphQLError(f"Failed to create upload session record in DB: {str(e_db)}")
        finally:
            if db_session:
                db_session.close()

        # --- Upload to Wasabi ---
        try:
            upload_to_wasabi(bucket=WASABI_BUCKET_NAME, path=wasabi_path, file_obj=file)
            logger.info(f"File successfully uploaded to Wasabi: {wasabi_path} for session_id: {session_id}")
        except Exception as e_wasabi:
            logger.error(f"Wasabi Error: Failed to upload file for session_id {session_id}: {e_wasabi}", exc_info=True)
            # Update session status to "failed_wasabi_upload" in DB
            db_update_session = None
            try:
                db_update_session = get_db_session_sync(business_id=business_id)
                session_to_update = db_update_session.query(UploadSessionOrm).filter(UploadSessionOrm.session_id == session_id).first()
                if session_to_update:
                    session_to_update.status = "failed_wasabi_upload"
                    session_to_update.details = f"Failed to upload file to Wasabi: {str(e_wasabi)}"
                    session_to_update.updated_at = datetime.datetime.utcnow()
                    db_update_session.commit()
                    logger.info(f"Updated session {session_id} status to 'failed_wasabi_upload' due to Wasabi error.")
            except Exception as e_db_update:
                logger.error(f"DB Error: Failed to update session {session_id} status after Wasabi failure: {e_db_update}", exc_info=True)
                if db_update_session:
                    db_update_session.rollback()
            finally:
                if db_update_session:
                    db_update_session.close()
            raise strawberry.GraphQLError(f"Failed to upload file to Wasabi: {str(e_wasabi)}")

        # --- Dispatch Celery Task ---
        celery_task_fn = CELERY_TASK_MAP.get(input.load_type)
        # This check is technically redundant if the one at the beginning is comprehensive,
        # but good as a safeguard.
        if not celery_task_fn:
            raise strawberry.GraphQLError(f"Internal Server Error: No Celery task found for load type: {input.load_type}")

        try:
            task_instance = celery_task_fn.delay(
                business_id=business_id,
                session_id=session_id,
                wasabi_file_path=wasabi_path,
                original_filename=original_filename
            )
            # Optional: Store task_instance.id with the session if needed for tracking via API.
            # created_session_dict["celery_task_id"] = task_instance.id
            # (Requires UploadSessionType to have this field)
        except Exception as e:
            # Conceptual: Update session status to "failed" due to Celery dispatch error
            # update_session_status_in_db(session_id, "failed", f"Celery dispatch error: {str(e)}")
            raise strawberry.GraphQLError(f"Failed to dispatch Celery task: {str(e)}")

        # Return the UploadSessionType based on the (conceptually) created DB record
        return UploadSessionType(**created_session_dict)
