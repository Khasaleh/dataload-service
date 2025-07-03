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
def decode_unverified_payload(token: str) -> dict:
    try:
        parts = token.split('.')
        if len(parts) < 2:
            raise ValueError("Invalid JWT format.")
        payload_b64 = parts[1]
        # Fix padding
        payload_b64 += '=' * (-len(payload_b64) % 4)
        decoded_bytes = base64.urlsafe_b64decode(payload_b64)
        return json.loads(decoded_bytes.decode('utf-8'))
    except Exception as e:
        logger.error(f"Error decoding unverified JWT payload: {e}")
        raise HTTPException(status_code=401, detail="Invalid JWT format.")
def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = decode_unverified_payload(token)
        business_id = payload.get("business_id")
        role = payload.get("role")
        if business_id is None:
            raise HTTPException(status_code=403, detail="Missing business_id")
        return {"business_id": business_id, "role": role}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
