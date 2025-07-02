from fastapi import APIRouter
from jose import jwt
from datetime import datetime, timedelta
import os

# Load JWT settings from environment variables, ensure consistency with auth.py
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-default-secret-key-if-not-set") # Default should match auth.py's
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

# It's good practice to centralize these settings if used in multiple places,
# e.g., in a config.py, and import from there. For now, direct os.getenv is fine.

if SECRET_KEY == "your-default-secret-key-if-not-set":
    print("WARNING (token.py): Using default JWT_SECRET_KEY. This should be set via an environment variable for production.")

router = APIRouter()

@router.post("/api/token")
def generate_token():
    to_encode = {
        "business_id": "demo123", # This can remain hardcoded for this test token endpoint
        "role": "admin",          # Or also be made configurable if needed
        "exp": datetime.utcnow() + timedelta(hours=2)
    }
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return {"access_token": token, "token_type": "bearer"}
