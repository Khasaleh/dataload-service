import strawberry
from typing import Optional, List, Any, Dict # TYPE_CHECKING can be used for forward refs if models are complex
import uuid # For strawberry.ID if needed for type hints, and for mock DB
import datetime # For mock DB

from app.graphql_types import UploadSessionType, UserType
# from app.dependencies.auth import get_current_user # Not directly called in resolvers in this pattern
                                                    # but its output format is what we expect in info.context
import logging # For logging DB errors

# --- Database/Service Layer Imports ---
# (Assuming get_session can provide a synchronous session for resolver logic)
from app.db.connection import get_session as get_db_session_sync
from app.db.models import UploadSessionOrm
# --- End Database/Service Layer Imports ---

logger = logging.getLogger(__name__)

@strawberry.type
class Query:
    """
    Root GraphQL Query type.
    Defines all available read operations in the GraphQL API.
    """

    @strawberry.field
    def me(self, info: strawberry.types.Info) -> Optional[UserType]:
        """
        Retrieves the currently authenticated user's information.

        The user data is expected to be populated in `info.context['current_user']`
        by an authentication dependency/middleware.
        """
        current_user_data = info.context.get("current_user")
        if not current_user_data:
            # In a real application, you might raise a GraphQL error if user context is expected but missing.
            # e.g., raise strawberry.GraphQLError("User not authenticated.")
            # For now, returning None if no user data is found in context.
            return None
        # Assuming current_user_data is a dict with 'business_id' and 'role'
        return UserType(
            user_id=strawberry.ID(current_user_data.get("user_id", "")), # Add user_id
            username=current_user_data.get("username", ""),             # Add username
            business_id=current_user_data.get("business_id", ""),
            roles=current_user_data.get("roles", []) # Corrected role to roles and ensure it's a list
        )

    @strawberry.field
    def uploadSession(self, info: strawberry.types.Info, session_id: strawberry.ID) -> Optional[UploadSessionType]:
        """
        Retrieves a specific upload session by its session_id.

        The user must be authenticated, and the session must belong to the user's business.
        `business_id` for authorization is implicitly taken from the authenticated user's context.
        """
        current_user_data = info.context.get("current_user")
        if not current_user_data or not current_user_data.get("business_id"):
            # raise strawberry.GraphQLError("Authentication required.") # Or handle as per auth flow
            return None

        user_business_id = current_user_data["business_id"]

        db = None
        try:
            db = get_db_session_sync(business_id=user_business_id) # Get session scoped to user's business
            session_orm = db.query(UploadSessionOrm).filter(
                UploadSessionOrm.session_id == str(session_id), # Convert strawberry.ID to string for query
                UploadSessionOrm.business_id == user_business_id # Authorize: session must belong to user's business
            ).first()

            if session_orm:
                # Map ORM object to Strawberry type.
                # Strawberry can often do this automatically if types are compatible or with Pydantic integration.
                # For direct mapping:
                return UploadSessionType(
                    session_id=strawberry.ID(session_orm.session_id),
                    business_id=session_orm.business_id,
                    load_type=session_orm.load_type,
                    original_filename=session_orm.original_filename,
                    wasabi_path=session_orm.wasabi_path,
                    status=session_orm.status,
                    details=session_orm.details,
                    record_count=session_orm.record_count,
                    error_count=session_orm.error_count,
                    created_at=session_orm.created_at,
                    updated_at=session_orm.updated_at
                )
            return None # Session not found or not authorized
        except Exception as e_db:
            logger.error(f"DB Error in uploadSession resolver for session_id {session_id}: {e_db}", exc_info=True)
            # Depending on policy, you might want to return None or raise a more generic GraphQL error.
            # Raising specific errors can expose too much, so often a generic error or None is preferred.
            raise strawberry.GraphQLError(f"An error occurred while fetching upload session data.")
        finally:
            if db:
                db.close()

    @strawberry.field
    def uploadSessionsByBusiness(
        self,
        info: strawberry.types.Info,
        # business_id is implicitly taken from the authenticated user context
        skip: Optional[int] = 0,
        limit: Optional[int] = 100
    ) -> List[UploadSessionType]:
        """
        Retrieves a list of upload sessions for the authenticated user's business,
        with optional pagination (skip, limit).
        """
        current_user_data = info.context.get("current_user")
        if not current_user_data or not current_user_data.get("business_id"):
            # raise strawberry.GraphQLError("Authentication required.")
            return []

        user_business_id = current_user_data["business_id"]

        db = None
        try:
            db = get_db_session_sync(business_id=user_business_id)

            query = db.query(UploadSessionOrm).filter(UploadSessionOrm.business_id == user_business_id)

            # Add ordering for consistent pagination results
            query = query.order_by(UploadSessionOrm.created_at.desc()) # Example: newest first

            # Apply pagination
            actual_skip = skip if skip is not None else 0
            actual_limit = limit if limit is not None else 100 # Default limit

            sessions_orm_list = query.offset(actual_skip).limit(actual_limit).all()

            results: List[UploadSessionType] = []
            for session_orm in sessions_orm_list:
                results.append(UploadSessionType(
                    session_id=strawberry.ID(session_orm.session_id),
                    business_id=session_orm.business_id,
                    load_type=session_orm.load_type,
                    original_filename=session_orm.original_filename,
                    wasabi_path=session_orm.wasabi_path,
                    status=session_orm.status,
                    details=session_orm.details,
                    record_count=session_orm.record_count,
                    error_count=session_orm.error_count,
                    created_at=session_orm.created_at,
                    updated_at=session_orm.updated_at
                ))
            return results
        except Exception as e_db:
            logger.error(f"DB Error in uploadSessionsByBusiness resolver for business_id {user_business_id}: {e_db}", exc_info=True)
            raise strawberry.GraphQLError(f"An error occurred while fetching upload sessions.")
        finally:
            if db:
                db.close()
