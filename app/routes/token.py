from fastapi import APIRouter
from jose import jwt
from datetime import datetime, timedelta

SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"

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
