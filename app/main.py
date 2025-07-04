
# Remove Optional, Request, Depends if not used by other parts of main after GQL removal
# Keep FastAPI and logging
from fastapi import FastAPI
import logging
from app.services.storage import upload_file as upload_to_wasabi

from app.core.config import settings
# Removed GraphQL specific imports: strawberry, GraphQLRouter, Query, get_current_user (if only for GQL context)
# from app.dependencies.auth import get_current_user # This is used by REST routes via Depends, so it's still needed at a higher level but not directly in main.py for GQL

# --- Logging Configuration ---
logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)
logger.info(f"Logging configured with level: {settings.LOG_LEVEL.upper()}")

# --- GraphQL Setup Removed ---
# No more GraphQL schema, router, or context getter here.

# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Provides REST API for catalog data uploads, status tracking, and user information.", # Updated description
    version="2.0.0",
    contact={"name": "Fazeal Dev Team", "email": "support@fazeal.com"},
    license_info={"name": "MIT"},
)

logger.info(f"FastAPI application startup... Environment: {settings.ENVIRONMENT}")

# --- Include REST API routers ---
from app.routes.upload import router as upload_api_router
from app.routes.users_api import router as users_api_router
from app.routes.sessions_api import router as sessions_api_router
# Assuming status_api and token_api are still relevant or managed elsewhere.
# If they were only for GQL or are also being removed, adjust accordingly.
from app.routes.status_api import router as status_api_router
from app.routes.token import router as token_api_router

# All API endpoints will be prefixed with /api/v1 as per router definitions or here.
# The individual routers (upload, users, sessions) already have prefixes like /upload, /users, /sessions.
# So if main app prefix is /api/v1, paths become /api/v1/upload, /api/v1/users, /api/v1/sessions.
# The current setup in individual routers is:
# upload_api_router: no prefix, path defined as /api/v1/business/{business_id}/upload/{load_type}
# users_api_router: prefix="/users"
# sessions_api_router: prefix="/sessions"
# This means if we want all under /api/v1, we should include them with that prefix here.

# Let's adjust the individual router prefixes to be relative and apply /api/v1 here for consistency.
# This requires changing the prefix in app/routes/users_api.py and app/routes/sessions_api.py
# For now, I'll assume the prefixes in the routers are as they are and mount them accordingly.
# The upload router already has /api/v1 in its path.

# The requirement was:
# POST /api/v1/business/{business_id}/upload/{load_type} (from upload.py, this is fine)
# GET /api/v1/sessions/{session_id}
# GET /api/v1/sessions
# GET /api/v1/users/me

# Mounting routers:
app.include_router(upload_api_router, tags=["Uploads"]) # upload_api_router paths start with /api/v1/...
app.include_router(users_api_router, prefix="/api/v1", tags=["Users"]) # users_api_router has prefix /users -> /api/v1/users
app.include_router(sessions_api_router, prefix="/api/v1", tags=["Sessions"]) # sessions_api_router has prefix /sessions -> /api/v1/sessions

# Example for status and token if they are REST and follow /api/v1 pattern
app.include_router(status_api_router, prefix="/api/v1/status", tags=["Status"])
app.include_router(token_api_router, prefix="/api/auth", tags=["Authentication"]) # This one has /api/auth, might be intentional


@app.get("/", tags=["Root"])
async def read_root():
    logger.info("Root path '/' accessed.")
    return {"message": "Welcome to the Catalog Data Load Service REST API."} # Updated message

logger.info("Application setup complete. REST API is active.") # Updated message
