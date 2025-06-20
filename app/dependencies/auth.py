from jose import JWTError, jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
import os

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token")

# Standardized JWT Secret environment variable
SECRET_KEY = os.getenv("JWT_SECRET", "your-default-secret-key-if-not-set")
# JWT Algorithm to be used for encoding/decoding tokens. HS256 is a common choice.
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

if SECRET_KEY == "your-default-secret-key-if-not-set":
    # It's generally better to use logging instead of print for warnings in applications.
    # Consider replacing with: logger.warning("Using default JWT_SECRET...")
    print("WARNING (auth.py): Using default JWT_SECRET. This should be set via an environment variable for production.")

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