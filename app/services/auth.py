# TODO: REVIEW AND POTENTIALLY REMOVE THIS FILE.
# This file (app/services/auth.py) appears to be outdated or duplicative of
# app/dependencies/auth.py. More importantly, it hardcodes the SECRET_KEY,
# which is a significant security risk. The JWT configuration and get_current_user
# dependency should be centralized, ideally in app/dependencies/auth.py,
# and should load secrets from environment variables.
# Avoid using this file for new development and plan for its deprecation/removal.

from jose import JWTError, jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token")

SECRET_KEY = "your-secret-key" # !!! HARDCODED SECRET - SECURITY RISK !!!
ALGORITHM = "HS256"

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        business_id = payload.get("business_id")
        role = payload.get("role")
        if business_id is None:
            raise HTTPException(status_code=403, detail="Missing business_id")
        return {"business_id": business_id, "role": role}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")