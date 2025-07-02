
from typing import Optional
from fastapi import FastAPI, Request, Depends
import logging
import strawberry
from strawberry.fastapi import GraphQLRouter

from app.core.config import settings

# --- Logging Configuration ---
logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)
logger.info(f"Logging configured with level: {settings.LOG_LEVEL.upper()}")

# --- GraphQL Setup ---
from app.graphql_queries import Query
# from app.graphql_mutations import Mutation # Mutation class is no longer used
from app.dependencies.auth import get_current_user

async def get_context(
    request: Request,
    current_user: Optional[dict] = Depends(get_current_user)
):
    return {
        "request": request,
        "current_user": current_user,
    }

# Schema is now query-only
schema = strawberry.Schema(query=Query)

graphql_app_router = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphiql=True
)

# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Provides GraphQL interface for catalog data uploads, status tracking, and user authentication. Also provides REST API for file uploads.",
    version="2.0.0",
    contact={"name": "Fazeal Dev Team", "email": "support@fazeal.com"},
    license_info={"name": "MIT"},
)

logger.info(f"FastAPI application startup... Environment: {settings.ENVIRONMENT}")

# Include GraphQL router
app.include_router(graphql_app_router, prefix=settings.API_PREFIX, tags=["GraphQL"])

# Include REST API routers
from app.routes.upload import router as upload_api_router
from app.routes.status_api import router as status_api_router # Assuming this might exist or be added
from app.routes.token import router as token_api_router # Assuming this exists for token operations via REST

app.include_router(upload_api_router, prefix="/api/v1", tags=["Uploads"])
app.include_router(status_api_router, prefix="/api/v1/status", tags=["Status"]) # Example, adjust if needed
app.include_router(token_api_router, prefix="/api/auth", tags=["Authentication"]) # Example, adjust if needed


@app.get("/", tags=["Root"])
async def read_root():
    logger.info("Root path '/' accessed.")
    return {"message": "Welcome to the Catalog Data Load Service. Visit /graphql for the GraphQL API."}

logger.info("Application setup complete. GraphQL endpoint at /graphql")
