from typing import Optional
from fastapi import FastAPI, Request, Depends
import logging
import json

import strawberry
from strawberry.fastapi import GraphQLRouter

from app.core.config import settings
from app.graphql_queries import Query
from app.graphql_mutations import Mutation
from app.dependencies.auth import get_current_user


# --- Logging Configuration ---
logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)
logger.info(f"Logging configured with level: {settings.LOG_LEVEL.upper()}")


# --- Request Logging Middleware ---
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"===== Incoming Request =====")
    logger.info(f"Method: {request.method}")
    logger.info(f"Path: {request.url.path}")

    # Log headers
    headers = dict(request.headers)
    for k, v in headers.items():
        logger.info(f"Header: {k} = {v}")

    # Log body (safe)
    body_bytes = await request.body()
    body_preview = body_bytes[:1000]

    try:
        body_json = json.loads(body_bytes.decode())
        logger.info(f"Body JSON: {json.dumps(body_json, indent=2)}")
    except Exception:
        logger.info(f"Body (raw bytes preview): {body_preview}")

    response = await call_next(request)

    logger.info(f"===== Response Status: {response.status_code} =====")
    return response


# --- GraphQL Setup ---
async def get_context(
    request: Request,
    current_user: Optional[dict] = Depends(get_current_user)
):
    return {
        "request": request,
        "current_user": current_user,
    }

schema = strawberry.Schema(query=Query, mutation=Mutation)

graphql_app_router = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphiql=True
)


# --- FastAPI App ---
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Provides GraphQL interface for catalog data uploads, status tracking, and user authentication.",
    version="2.0.0",
    contact={"name": "Fazeal Dev Team", "email": "support@fazeal.com"},
    license_info={"name": "MIT"},
)

logger.info(f"FastAPI application startup... Environment: {settings.ENVIRONMENT}")

app.include_router(graphql_app_router, prefix=settings.API_PREFIX, tags=["GraphQL"])


@app.get("/", tags=["Root"])
async def read_root():
    logger.info("Root path '/' accessed.")
    return {"message": "Welcome to the Catalog Data Load Service. Visit /graphql for the GraphQL API."}


logger.info("Application setup complete. GraphQL endpoint at /graphql")
