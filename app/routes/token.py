from fastapi import APIRouter
from jose import jwt
from datetime import datetime, timedelta
import os

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-default-secret-key-if-not-set")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

if SECRET_KEY == "your-default-secret-key-if-not-set":
    print("WARNING (token.py): Using default JWT_SECRET_KEY. This should be set via an environment variable for production.")

router = APIRouter()

@router.post("/api/token")
def generate_token():
    to_encode = {
        "business_id": "demo123",
        "role": "admin",
        "exp": datetime.utcnow() + timedelta(hours=2)
    }
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return {"access_token": token, "token_type": "bearer"}
