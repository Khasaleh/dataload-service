from jose import JWTError, jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
import os

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token")

# Load JWT settings from environment variables with defaults (though defaults for secrets are not ideal for prod)
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-default-secret-key-if-not-set") # Keep a default for local dev if needed, but ensure it's strong or overridden
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

if SECRET_KEY == "your-default-secret-key-if-not-set":
    print("WARNING: Using default JWT_SECRET_KEY. This should be set via an environment variable for production.")

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
