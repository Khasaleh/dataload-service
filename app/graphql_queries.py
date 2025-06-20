import strawberry
from typing import Optional, List, Any, Dict # TYPE_CHECKING can be used for forward refs if models are complex
import uuid # For strawberry.ID if needed for type hints, and for mock DB
import datetime # For mock DB

from app.graphql_types import UploadSessionType, UserType
# from app.dependencies.auth import get_current_user # Not directly called in resolvers in this pattern
                                                    # but its output format is what we expect in info.context

# --- Mock Database and Service Functions ---
# This section simulates fetching data from a database or service layer.
# In a real application, these functions would interact with your ORM, database connections, etc.

_MOCK_DB_SESSIONS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "biz_1": {
        # Using actual UUIDs for keys in the mock DB to simulate real scenarios
        str(uuid.uuid4()): {"session_id": "sess_gql_1", "business_id": "biz_1", "load_type": "products", "original_filename": "products_gql.csv", "wasabi_path": "uploads/biz_1/sess_gql_1/products_gql.csv", "status": "completed", "details": "Processed 100 records.", "record_count": 100, "error_count": 0, "created_at": datetime.datetime.utcnow() - datetime.timedelta(days=1), "updated_at": datetime.datetime.utcnow() - datetime.timedelta(days=1, hours=-1)},
        str(uuid.uuid4()): {"session_id": "sess_gql_2", "business_id": "biz_1", "load_type": "brands", "original_filename": "brands_gql.csv", "wasabi_path": "uploads/biz_1/sess_gql_2/brands_gql.csv", "status": "processing", "details": None, "record_count": 0, "error_count": 0, "created_at": datetime.datetime.utcnow(), "updated_at": datetime.datetime.utcnow()},
    },
    "biz_2": {
         str(uuid.uuid4()): {"session_id": "sess_gql_3", "business_id": "biz_2", "load_type": "products", "original_filename": "products_biz2_gql.csv", "wasabi_path": "uploads/biz_2/sess_gql_3/products_biz2_gql.csv", "status": "failed", "details": "Critical error during validation: SKU format incorrect.", "record_count": 0, "error_count": 50, "created_at": datetime.datetime.utcnow() - datetime.timedelta(hours=2), "updated_at": datetime.datetime.utcnow() - datetime.timedelta(hours=1)},
    }
}

def get_upload_session_from_db(session_id: str, business_id_context: str) -> Optional[Dict[str, Any]]:
    """
    Mock function to retrieve a single upload session.
    In a real app, this would query the database.
    It also checks if the session_id belongs to the given business_id_context for authorization.
    """
    # Iterate through all sessions to find the one matching session_id
    for _, sessions_in_biz in _MOCK_DB_SESSIONS.items():
        for _, session_data in sessions_in_biz.items():
            if session_data["session_id"] == session_id:
                # Found the session, now check if it belongs to the requesting business
                if session_data["business_id"] == business_id_context:
                    return session_data
                else:
                    # Session found, but does not belong to the authorized business
                    return None # Or raise an explicit authorization error
    return None

def get_upload_sessions_for_business_from_db(business_id: str, skip: int, limit: int) -> List[Dict[str, Any]]:
    """
    Mock function to retrieve upload sessions for a specific business with pagination.
    In a real app, this would query the database.
    """
    sessions_for_biz = _MOCK_DB_SESSIONS.get(business_id, {})
    all_sessions_list = list(sessions_for_biz.values()) # Convert dict_values to a list
    # Sort by created_at descending for consistent pagination (optional, but good practice)
    all_sessions_list.sort(key=lambda s: s['created_at'], reverse=True)
    return all_sessions_list[skip : skip + limit]
# --- End Mock Database and Service Functions ---


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
            business_id=current_user_data.get("business_id", ""),
            role=current_user_data.get("role", "")
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

        # Fetch from DB (mocked) using user's business_id for authorization context
        session_data = get_upload_session_from_db(session_id=str(session_id), business_id_context=user_business_id)

        if session_data:
            # Ensure all fields expected by UploadSessionType are present, providing defaults if necessary
            # This is important when mapping from a dict (like our mock DB) to a Strawberry type.
            return UploadSessionType(
                session_id=strawberry.ID(session_data["session_id"]),
                business_id=session_data["business_id"],
                load_type=session_data["load_type"],
                original_filename=session_data["original_filename"],
                wasabi_path=session_data["wasabi_path"],
                status=session_data["status"],
                details=session_data.get("details"),
                record_count=session_data.get("record_count"),
                error_count=session_data.get("error_count"),
                created_at=session_data["created_at"],
                updated_at=session_data["updated_at"]
            )
        return None

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

        # Fetch from DB (mocked) with pagination
        sessions_data = get_upload_sessions_for_business_from_db(
            business_id=user_business_id,
            skip=skip if skip is not None else 0,  # Ensure skip has a default if None
            limit=limit if limit is not None else 100 # Ensure limit has a default
        )

        return [
            UploadSessionType(
                session_id=strawberry.ID(session["session_id"]),
                business_id=session["business_id"],
                load_type=session["load_type"],
                original_filename=session["original_filename"],
                wasabi_path=session["wasabi_path"],
                status=session["status"],
                details=session.get("details"),
                record_count=session.get("record_count"),
                error_count=session.get("error_count"),
                created_at=session["created_at"],
                updated_at=session["updated_at"]
            ) for session in sessions_data
        ]
```
