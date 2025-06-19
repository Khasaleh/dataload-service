from fastapi import Header, HTTPException
from jose import jwt, JWTError
import os

def extract_business_id(authorization: str = Header(...)) -> str:
    try:
        token = authorization.split(" ")[1]
        payload = jwt.decode(token, os.getenv("JWT_SECRET"), algorithms=["HS256"])
        return payload.get("business_id")
    except (JWTError, IndexError, AttributeError):
        raise HTTPException(status_code=403, detail="Invalid token or business ID missing.")
