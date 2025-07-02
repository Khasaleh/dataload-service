import strawberry
import datetime
import uuid  # For session_id generation
from strawberry.file_uploads import Upload
from app.graphql_types import UploadSessionType
from app.core.config import settings  # Import centralized settings
   import get_session
from app.db.models import UploadSessionOrm
from app.services.storage import upload_to_wasabi  # Assuming you have a service for uploading files to Wasabi
from app.models import UploadJobStatus
import logging
from typing import Optional

# Logger setup
logger = logging.getLogger(__name__)

# Constant for the CELERY_TASK_MAP
CELERY_TASK_MAP = {
    "products": process_products_file,
    "product_items": process_product_items_file,
    "categories": process_categories_file,
    "brands": process_brands_file,
    "return_policies": process_return_policies_file,
    "product_prices": process_product_prices_file,
    "attributes": process_attributes_file,
    "meta_tags": process_meta_tags_file,
}

# GraphQL Input Type for file upload
@strawberry.input
class UploadFileInput:
    """Input type for the file upload mutation."""
    load_type: str

# GraphQL Mutation Type
@strawberry.type
class Mutation:
    """
    Root GraphQL Mutation type.
    Defines all available write operations in the GraphQL API.
    """

    @strawberry.mutation
    async def upload_file(self, info: strawberry.types.Info, file: Upload, input: UploadFileInput) -> UploadSessionType:
        """
        Handles a file upload, creates an upload session record,
        uploads the file to Wasabi, and dispatches a Celery task for processing.
        Requires authentication.
        """
        # Authentication check
        current_user_data = info.context.get("current_user")
        if not current_user_data or not current_user_data.get("business_id"):
            raise strawberry.GraphQLError("Authentication required: User or business ID not found in context.")

        business_id = current_user_data["business_id"]

        # Validate load_type and file type
        if input.load_type not in CELERY_TASK_MAP:
            raise strawberry.GraphQLError(f"Invalid load type: {input.load_type}. Supported types are: {list(CELERY_TASK_MAP.keys())}")

        if not file.filename or not file.filename.lower().endswith('.csv'):
            raise strawberry.GraphQLError("Invalid file type. Only CSV files are allowed.")

        contents = await file.read()
        if not contents:
            raise strawberry.GraphQLError("Empty CSV file submitted.")

        session_id = str(uuid.uuid4())
        original_filename = file.filename
        wasabi_path = f"uploads/{business_id}/{session_id}/{input.load_type}/{original_filename}"

        # --- Create UploadSession record in DB ---
        session_data_to_save = {
            "session_id": session_id,
            "business_id": business_id,
            "load_type": input.load_type,
            "original_filename": original_filename,
            "wasabi_path": wasabi_path,
            "status": "pending",  # Initial status
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

        # --- Upload to Wasabi ---
        try:
            upload_to_wasabi(bucket=settings.WASABI_BUCKET_NAME, path=wasabi_path, file_obj=file)
            logger.info(f"File successfully uploaded to Wasabi: {wasabi_path} for session_id: {session_id}")
        except Exception as e_wasabi:
            logger.error(f"Wasabi Error: Failed to upload file for session_id {session_id}: {e_wasabi}", exc_info=True)
            raise strawberry.GraphQLError(f"Failed to upload file to Wasabi: {str(e_wasabi)}")

        # --- Dispatch Celery Task ---
        celery_task_fn = CELERY_TASK_MAP.get(input.load_type)
        try:
            task_instance = celery_task_fn.delay(
                business_id=business_id,
                session_id=session_id,
                wasabi_file_path=wasabi_path,
                original_filename=original_filename
            )
        except Exception as e:
            raise strawberry.GraphQLError(f"Failed to dispatch Celery task: {str(e)}")

        return UploadSessionType(**created_session_dict)
