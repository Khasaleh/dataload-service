from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer
from dotenv import load_dotenv
import os # Often used with dotenv, good to have if other early init needs it.
import logging

# Load environment variables from .env file for local development.
# This should be called as early as possible, before other modules might try to access os.getenv.
# In production, environment variables are typically set directly in the environment.
load_dotenv()

from app.routes import upload, token, status_api # Import routers after load_dotenv

auth_tags = [
    {"name": "Upload", "description": "Upload CSV files"},
    {"name": "Validation", "description": "Validation results"},
    {"name": "Status", "description": "Track upload status"}
]

app = FastAPI(
    title="Catalog Data Load API",
    description="Upload CSVs to load products, items, prices, and more for each business",
    version="1.0.0",
    contact={"name": "Fazeal Dev Team", "email": "support@fazeal.com"},
    license_info={"name": "MIT"},
    swagger_ui_init_oauth={"usePkceWithAuthorizationCodeGrant": True, "clientId": "swagger-ui"},
    openapi_tags=auth_tags
)

app.include_router(upload.router, tags=["Upload"])
app.include_router(token.router, prefix="/api") # Keep existing prefix for token router
app.include_router(status_api.router) # Add the new status_api router, prefix is defined in the router itself