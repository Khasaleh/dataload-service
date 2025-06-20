from fastapi import FastAPI, Request, Depends # Added Request
from dotenv import load_dotenv
import os
import logging
import strawberry # Added Strawberry
from strawberry.fastapi import GraphQLRouter # Added GraphQLRouter

# Load environment variables from .env file for local development.
# This should be called as early as possible.
load_dotenv()

# --- GraphQL Setup ---
# Import your Root Query and Mutation types
from app.graphql_queries import Query
from app.graphql_mutations import Mutation
# Import dependency for context (e.g., current user)
from app.dependencies.auth import get_current_user

# Define a context getter function for Strawberry
# This function will be called for every GraphQL request.
# It provides context (like the request object, current user, DB sessions) to resolvers.
async def get_context(
    request: Request,
    # Example: Injecting the current user into the context.
    # get_current_user is a FastAPI dependency that will be resolved.
    # If authentication is optional for some GraphQL fields, get_current_user should
    # be updated to not raise an exception for missing/invalid tokens but return None instead.
    # For now, if get_current_user raises HTTPException, FastAPI handles it before this point.
    current_user: Optional[dict] = Depends(get_current_user) # Optional if some queries don't need auth
                                                            # Or make non-optional if all queries need auth
):
    """
    Asynchronously gets the context for a GraphQL request.
    This can include the request itself, the current authenticated user,
    database sessions, etc.
    """
    # In a real app, you might also add a database session here:
    # from app.db.connection import get_db_session_dependency # Conceptual
    # db_session = Depends(get_db_session_dependency)
    return {
        "request": request,
        "current_user": current_user,
        # "db_session": db_session, # Example for DB session
    }

# Create the Strawberry GraphQL Schema
schema = strawberry.Schema(query=Query, mutation=Mutation)

# Create the GraphQL Router
# graphiql=True enables the GraphiQL web interface at the /graphql endpoint for testing.
# Set to False in production if desired.
graphql_app_router = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphiql=True
)
# --- End GraphQL Setup ---


# Initialize FastAPI app
# Updated title and description for GraphQL focus.
app = FastAPI(
    title="Catalog Data Load Service - GraphQL API",
    description="Provides GraphQL interface for catalog data uploads, status tracking, and user authentication.",
    version="2.0.0", # Assuming version bump for API change
    contact={"name": "Fazeal Dev Team", "email": "support@fazeal.com"},
    license_info={"name": "MIT"}
    # openapi_tags are not typically used for GraphQL as it's one endpoint.
    # swagger_ui_init_oauth might also be less relevant unless you have specific OAuth for GraphiQL.
)

# Configure logging (basic example)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)
logger.info("FastAPI application startup...")


# Include the GraphQL router
# All GraphQL operations will be available under the /graphql prefix.
app.include_router(graphql_app_router, prefix="/graphql", tags=["GraphQL"])


# --- Old REST Router inclusions are now removed/commented out ---
# # from app.routes import upload, token, status_api # Old imports
# # app.include_router(upload.router, tags=["Upload"])
# # app.include_router(token.router, prefix="/api")
# # app.include_router(status_api.router)


# Optional: A root endpoint for basic health check or API discovery
@app.get("/", tags=["Root"])
async def read_root():
    logger.info("Root path '/' accessed.")
    return {"message": "Welcome to the Catalog Data Load Service. Visit /graphql for the GraphQL API."}

# Example of other app setup (e.g., CORS middleware if needed)
# from fastapi.middleware.cors import CORSMiddleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"], # Adjust for production
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

logger.info("Application setup complete. GraphQL endpoint at /graphql")