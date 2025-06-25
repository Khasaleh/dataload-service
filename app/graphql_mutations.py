import strawberry
from typing import Optional, Any, Dict
import datetime
import uuid  # For session_id generation
from app.core.config import settings  # Import centralized settings
from app.graphql_types import UploadSessionType, TokenResponseType, UserType
from strawberry.file_uploads import Upload
from jose import jwt
from datetime import timedelta
import logging

from app.services.storage import upload_file as upload_to_wasabi
from app.tasks.load_jobs import (
    CELERY_TASK_MAP,  # Import CELERY_TASK_MAP
    process_brands_file,
    process_attributes_file,
    process_return_policies_file,
    process_products_file,
    process_product_items_file,
    process_meta_tags_file_specific,
    process_categories_file,
)
from app.db.connection import get_session as get_db_session_sync
from app.db.models import UploadSessionOrm, PriceOrm, ProductOrm, ProductItemOrm
from app.graphql_types import PriceTypeGQL, PriceInput, PriceType as PriceResponseType
from app.dataload.models.price_csv import PriceCsv, PriceTypeEnum as PriceCsvTypeEnum
import logging  # For logging DB errors

logger = logging.getLogger(__name__)

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

@strawberry.input
class UploadFileInput:
    """Input type for the file upload mutation."""
    load_type: str

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

        # Create the JWT access token payload
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token_payload = {
            "sub": user_details["username"],
            "userId": user_details["user_id"],
            "companyId": user_details["business_id"],
            "role": [{"authority": role_name} for role_name in user_details["roles"]],
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + access_token_expires
        }
        encoded_jwt = jwt.encode(access_token_payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

        # Generate a refresh token (for simplicity)
        refresh_token_value = f"mock-rt-{user_details['username']}-{uuid.uuid4()}"

        return TokenResponseType(
            token=encoded_jwt,
            token_type="bearer",
            refreshToken=refresh_token_value
        )

    @strawberry.mutation
    async def refresh_token(self, input: RefreshTokenInput) -> Optional[TokenResponseType]:
        """
        Refreshes an authentication token using the provided refresh token.
        """
        user_details_for_refresh = None
        if input.refreshToken and input.refreshToken.startswith("mock-rt-testuser"):
            user_details_for_refresh = _MOCK_USERS_DB.get("testuser")
        elif input.refreshToken and input.refreshToken.startswith("mock-rt-adminuser"):
            user_details_for_refresh = _MOCK_USERS_DB.get("adminuser")

        if not user_details_for_refresh:
            raise strawberry.GraphQLError("Invalid or expired refresh token.")

        if user_details_for_refresh.get("disabled", False):
            raise strawberry.GraphQLError("User account is disabled.")

        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token_payload = {
            "sub": user_details_for_refresh["username"],
            "userId": user_details_for_refresh["user_id"],
            "companyId": user_details_for_refresh["business_id"],
            "role": [{"authority": role_name} for role_name in user_details_for_refresh["roles"]],
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + access_token_expires
        }
        new_access_token = jwt.encode(access_token_payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

        new_refresh_token_value = f"mock-rt-{user_details_for_refresh['username']}-{uuid.uuid4()}"

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
        """
        current_user_data = info.context.get("current_user")
        if not current_user_data or not current_user_data.get("business_id"):
            raise strawberry.GraphQLError("Authentication required: User or business ID not found in context.")

        business_id = current_user_data["business_id"]

        if input.load_type not in CELERY_TASK_MAP:
            raise strawberry.GraphQLError(f"Invalid load type: {input.load_type}. Supported types are: {list(CELERY_TASK_MAP.keys())}")

        if not file.filename:
            raise strawberry.GraphQLError("Filename cannot be empty.")
        if not file.filename.lower().endswith('.csv'):
            raise strawberry.GraphQLError("Invalid file type. Only CSV files are allowed.")

        contents = await file.read()
        if not contents:
            raise strawberry.GraphQLError("Empty CSV file submitted.")

        session_id = str(uuid.uuid4())
        original_filename = file.filename
        wasabi_path = f"uploads/{business_id}/{session_id}/{input.load_type}/{original_filename}"

        WASABI_BUCKET_NAME = settings.WASABI_BUCKET_NAME
        if not WASABI_BUCKET_NAME:
            logger.error("WASABI_BUCKET_NAME is not configured in settings.")
            raise strawberry.GraphQLError("Server configuration error: Wasabi bucket name is not set.")

        # Create UploadSession record
        session_data_to_save = {
            "session_id": session_id,
            "business_id": business_id,
            "load_type": input.load_type,
            "original_filename": original_filename,
            "wasabi_path": wasabi_path,
            "status": "pending",
            "created_at": datetime.datetime.utcnow(),
            "updated_at": datetime.datetime.utcnow(),
            "details": None,
            "record_count": None,
            "error_count": None
        }

        db_session = None
        try:
            db_session = get_db_session_sync(business_id=business_id)
            new_session_orm_instance = UploadSessionOrm(**session_data_to_save)
            db_session.add(new_session_orm_instance)
            db_session.commit()
            db_session.refresh(new_session_orm_instance)

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

        # Upload to Wasabi
        try:
            upload_to_wasabi(bucket=WASABI_BUCKET_NAME, path=wasabi_path, file_obj=file)
            logger.info(f"File successfully uploaded to Wasabi: {wasabi_path} for session_id: {session_id}")
        except Exception as e_wasabi:
            logger.error(f"Wasabi Error: Failed to upload file for session_id {session_id}: {e_wasabi}", exc_info=True)
            raise strawberry.GraphQLError(f"Failed to upload file to Wasabi: {str(e_wasabi)}")

        # Dispatch Celery Task
        celery_task_fn = CELERY_TASK_MAP.get(input.load_type)
        try:
            task_instance = celery_task_fn.delay(
                business_id=business_id,
                session_id=session_id,
                wasabi_file_path=wasabi_path,
                original_filename=original_filename
            )
        except Exception as e:
            logger.error(f"Error dispatching Celery task: {e}", exc_info=True)
            raise strawberry.GraphQLError(f"Failed to dispatch Celery task: {str(e)}")

        return UploadSessionType(**created_session_dict)
